/**
 * st-catiecli-tasks — SillyTavern 服务端插件
 *
 * 把 catiecli 后台的「任务 API」（POST /v1/openai/tasks 提交 → GET 轮询分片）
 * 封装成一个标准 OpenAI 兼容端点，SillyTavern 用 Custom (OpenAI 兼容) 源
 * 接入即可，使用体验与直连 /v1/chat/completions 完全一致。
 *
 * 工作方式（长连接不稳定的根治）：
 *   ST ──POST /v1/chat/completions──▶ 本插件（短连接，几百 ms 就返回响应头）
 *     插件 ──POST /v1/openai/tasks──▶ catiecli：提交任务拿 task_id（秒回）
 *     插件 ──GET .../tasks/{id}?after=N──▶ catiecli：轮询增量分片
 *     插件把分片原样写入 SSE 响应流 ──▶ ST 标准 SSE 解析器照常工作
 *   ST 侧断连/重试时：插件取消任务；插件与 catiecli 之间断连时：任务在
 *   catiecli 侧继续执行，插件重试轮询即可续上（分片不丢）。
 *
 * 路由挂载在 /api/plugins/st-catiecli-tasks/ 下（受 ST 登录鉴权保护）。
 * SillyTavern 里这样配置：
 *   1. Chat Completion 源选「Custom (OpenAI-compatible)」
 *   2. Custom Endpoint 填: http://127.0.0.1:8000/api/plugins/st-catiecli-tasks/v1
 *   3. API Key 填 catiecli 的 cat- 密钥（CUSTOM_OPENAI secret）
 *   4. 连接 → 选模型 → 生成。流式开关随意，两种都支持。
 *
 * catiecli 后端地址：默认内置线上部署（开箱即用，填 key 即可）。
 * 需要指向本地/其他实例时，在 ST「自定义请求头」输入框里填（YAML 格式）：
 *
 *   X-Catiecli-Url: http://127.0.0.1:5002
 *
 * 该头随每个请求到达插件，插件按请求读取（无需重启 ST，保存即生效）。
 */

import express from 'express';

/** 默认上游：内置线上部署 */
const DEFAULT_CATIECLI_URL = 'https://catiecli-cdn.sukaka.top';

/** 从请求头解析上游地址（X-Catiecli-Url），没有时用默认线上地址 */
function upstreamFromRequest(req) {
    const raw = req.headers['x-catiecli-url'];
    const url = (Array.isArray(raw) ? raw[0] : raw)?.trim();
    return (url || DEFAULT_CATIECLI_URL).replace(/\/+$/, '');
}

/**
 * undici（Node 全局 fetch）默认 headersTimeout=300s：一次轮询若恰好赶上
 * 网络卡顿（超过 5 分钟拿不到响应头）会抛 HeadersTimeoutError。这里放宽到
 * 与任务最长轮询时间一致，避免长任务在"网络差但没断"的场景下被误杀。
 */
const FETCH_TIMEOUT_MS = (Number(process.env.CATIECLI_FETCH_TIMEOUT_MS) || 35) * 60 * 1000;
const FETCH_OPTIONS = { headersTimeout: FETCH_TIMEOUT_MS, bodyTimeout: FETCH_TIMEOUT_MS };

/** 轮询间隔与上限 */
const POLL_INTERVAL_MS = 800;   // 常规轮询节奏
const POLL_FIRST_DELAY_MS = 300;
const POLL_BUSY_DELAY_MS = 60;  // 已有新分片时快速跟进（准流式体验）
const POLL_MAX_MINUTES = 30;    // 单任务最长轮询时间

/** 网络类错误重试次数（3 次都连不上才判定真的连不上） */
const NET_RETRY_MAX = 3;
/** 网络重试退避基数（1s / 2s / 4s，上限 8s） */
const NET_RETRY_BASE_MS = 1000;

/** 判断是否网络类错误（fetch 抛 TypeError = 连接失败/DNS/断连）。
 * HTTP 状态码错误不算：服务器明确响应了，重试无意义，直接透传。 */
function isNetworkError(err) {
    return err instanceof TypeError || err?.cause?.code === 'UND_ERR_CONNECT_TIMEOUT' || err?.cause?.code === 'ECONNREFUSED'
        || err?.cause?.code === 'ECONNRESET' || err?.cause?.code === 'ETIMEDOUT' || err?.cause?.code === 'ENOTFOUND';
}

/**
 * 网络类错误自动重试包装：最多 NET_RETRY_MAX 次，指数退避。
 * 非 4xx/5xx 的失败原因（连不上）重试；HTTP 错误状态（带 statusCode）不重试直接抛。
 * 全部失败抛最后一个错误，附明确说明"连续 N 次无法连接"。
 */
async function withNetRetry(label, fn, signal) {
    let lastErr = null;
    for (let attempt = 1; attempt <= NET_RETRY_MAX; attempt++) {
        if (signal?.aborted) throw new Error('aborted');
        try {
            return await fn();
        } catch (err) {
            if (signal?.aborted) throw new Error('aborted');
            // HTTP 状态码错误：服务器可达，响应明确 —— 不重试
            if (err instanceof Error && err.statusCode !== undefined) throw err;
            if (!isNetworkError(err)) throw err;
            lastErr = err;
            if (attempt < NET_RETRY_MAX) {
                const delay = Math.min(NET_RETRY_BASE_MS * (2 ** (attempt - 1)), 8000);
                console.warn(`[st-catiecli-tasks] ${label} 网络错误（第 ${attempt}/${NET_RETRY_MAX} 次），${delay / 1000}s 后重试: ${err.message}`);
                await new Promise(r => setTimeout(r, delay));
            }
        }
    }
    throw Object.assign(
        new Error(`${label}: 连续 ${NET_RETRY_MAX} 次无法连接上游（${lastErr?.message ?? '网络错误'}），请检查网络或上游地址`),
        { statusCode: 502 },
    );
}

export const info = {
    id: 'st-catiecli-tasks',
    name: 'Catiecli Tasks Provider',
    description: 'OpenAI-compatible provider backed by the Catiecli task API (submit + poll), immune to unstable long-lived connections',
};

// ===== 错误体解析（让 ST 的 toast / 弹窗能显示完整上游报错） =====

/** 从上游错误体挖最可读的消息(标准 OpenAI / FastAPI detail 形态) */
function extractUpstreamMessage(result) {
    if (result == null) return '';
    if (typeof result === 'string') return result.trim();
    const err = result.error ?? result.detail;
    if (typeof err === 'string') return err;
    if (typeof err?.message === 'string') return err.message;
    if (typeof result?.message === 'string') return result.message;
    if (typeof err?.status === 'string') return err.status;
    return '';
}

/** 从 SSE 分片里提取 data: {...} 错误帧的消息与原文 */
function extractStreamError(chunks) {
    const text = (Array.isArray(chunks) ? chunks : []).join('');
    for (const line of text.split('\n')) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        const payload = trimmed.slice(5).trim();
        if (!payload || payload === '[DONE]') continue;
        try {
            const data = JSON.parse(payload);
            const message = extractUpstreamMessage(data);
            if (message) return { message, raw: JSON.stringify(data, null, 2) };
        } catch { /* 非 JSON 行,跳过 */ }
    }
    return null;
}

/** 任务终态 → OpenAI 错误体:优先透传上游原文,消息里带状态码与上游文案 */
function buildTaskErrorBody(env) {
    const status = env.status === 'cancelled' ? 499 : (env.status_code ?? 502);
    const parsed = extractStreamError(env.chunks);
    const upstreamMessage = extractUpstreamMessage(env.result) || parsed?.message || env.error || '';
    const message = env.status === 'cancelled'
        ? 'Task was cancelled'
        : `上游返回错误: HTTP ${status}${upstreamMessage ? ` ${upstreamMessage}` : ''}`;
    // detail:非流式用 result 原文;流式用 SSE 错误帧原文;都没有用 env.error
    let detail = null;
    if (typeof env.result === 'string') detail = env.result.slice(0, 4000);
    else if (env.result != null) detail = JSON.stringify(env.result).slice(0, 4000);
    else if (parsed?.raw) detail = parsed.raw;
    else if (env.error) detail = env.error;
    return { message, status, detail };
}

/** OpenAI 标准错误体:detail 有值时嵌进 error.metadata,供需要完整信息的前端展示 */
function openAiErrorBody(message, code, detail) {
    const error = { message, type: 'upstream_error', code };
    if (detail) error.metadata = { upstream_response: String(detail).slice(0, 4000) };
    return { error };
}

/**
 * 带取消信号与退避的 GET 轮询。返回 envelope 或 null（超时）。
 * @param {string} taskId
 * @param {number} after
 * @param {string} apiKey
 * @param {AbortSignal} signal
 * @param {string} upstream catiecli 后端地址
 * @returns {Promise<any|null>}
 */
async function pollTask(taskId, after, apiKey, signal, upstream) {
    const deadline = Date.now() + POLL_MAX_MINUTES * 60 * 1000;
    let cursor = after;
    let nextDelay = POLL_FIRST_DELAY_MS;

    while (Date.now() < deadline) {
        if (signal?.aborted) return null;
        await new Promise(r => setTimeout(r, nextDelay));
        if (signal?.aborted) return null;

        const url = new URL(`${upstream}/v1/openai/tasks/${encodeURIComponent(taskId)}`);
        url.searchParams.set('after', String(cursor));

        const data = await withNetRetry('任务轮询', async () => {
            const resp = await fetch(url, {
                headers: { 'Authorization': `Bearer ${apiKey}` },
                signal,
                ...FETCH_OPTIONS,
            });
            if (!resp.ok) {
                const text = await resp.text().catch(() => '');
                let upstreamMessage = text.slice(0, 500);
                try {
                    upstreamMessage = extractUpstreamMessage(JSON.parse(text)) || upstreamMessage;
                } catch { /* 非 JSON 原样 */ }
                throw Object.assign(new Error(`任务查询失败: HTTP ${resp.status} ${upstreamMessage}`), { statusCode: resp.status });
            }
            return resp.json();
        }, signal);

        // 运行中且有新分片 → 立即交给调用方，继续快速轮询（准流式）
        if (!data.done && Array.isArray(data.chunks) && data.chunks.length > 0) {
            cursor = data.next_after ?? cursor + data.chunks.length;
            return { data, cursor, busy: true };
        }
        if (data.done) {
            return { data, cursor: data.next_after ?? cursor, busy: false };
        }
        nextDelay = POLL_INTERVAL_MS;
    }
    return null;
}

/**
 * 提交任务。请求体原样转发（就是 chat/completions 请求体）。
 * 网络类错误（连不上）自动重试 3 次；HTTP 错误直接透传。
 */
async function submitTask(body, apiKey, signal, upstream) {
    return withNetRetry('任务提交', () => fetch(`${upstream}/v1/openai/tasks`, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        signal,
        ...FETCH_OPTIONS,
    }).then(async resp => {
        const text = await resp.text();
        if (!resp.ok) {
            let upstreamMessage = text.slice(0, 1000);
            try {
                upstreamMessage = extractUpstreamMessage(JSON.parse(text)) || upstreamMessage;
            } catch { /* 非 JSON 原样 */ }
            throw Object.assign(new Error(`任务提交失败: HTTP ${resp.status} ${upstreamMessage}`), { statusCode: resp.status });
        }
        const data = JSON.parse(text);
        if (!data?.id) throw new Error('Task submit returned no task id');
        return data.id;
    }), signal);
}

/** 取消任务（尽力而为，失败仅记日志；网络错误重试 3 次）。 */
async function cancelTask(taskId, apiKey, upstream) {
    try {
        await withNetRetry('任务取消', () => fetch(`${upstream}/v1/openai/tasks/${encodeURIComponent(taskId)}/cancel`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${apiKey}` },
            ...FETCH_OPTIONS,
        }));
    } catch (err) {
        console.warn(`[st-catiecli-tasks] cancel task ${taskId} failed:`, err.message);
    }
}

/**
 * 流式：轮询分片 → 原样写 SSE。失败时按 OpenAI 惯例写一个错误 data 帧再收尾
 * （ST 的 tryParseStreamingError 能识别 {error:{message}} 帧）。
 */
async function handleStream(req, res, body, apiKey) {
    const upstream = upstreamFromRequest(req);
    res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    let taskId = null;
    let cursor = 0;
    const abort = new AbortController();
    // 客户端断开检测：响应未结束就关闭 = ST 侧断连/点了停止。
    // 不能用 req 的 'close'：请求体读完后就会触发（Node 语义），会在
    // 请求一开始就误判断连。
    res.on('close', () => {
        if (!res.writableEnded) abort.abort();
    });

    try {
        taskId = await submitTask(body, apiKey, abort.signal, upstream);

        while (true) {
            if (abort.signal.aborted) return;
            const result = await pollTask(taskId, cursor, apiKey, abort.signal, upstream);
            if (!result) {
                res.write(`data: ${JSON.stringify({ error: { message: `任务超过 ${POLL_MAX_MINUTES} 分钟未完成,已自动取消 (task ${taskId})`, code: 504 } })}\n\n`);
                res.write('data: [DONE]\n\n');
                return;
            }
            const { data, cursor: nextCursor, busy } = result;
            cursor = nextCursor;

            for (const chunk of (data.chunks || [])) {
                res.write(chunk);
            }

            if (data.done) {
                if (!data.stream) {
                    // 理论上不会发生（本分支只在 stream=true 进入），防御性处理
                    res.write('data: [DONE]\n\n');
                } else if (data.status === 'completed') {
                    if (!(data.chunks || []).join('').includes('[DONE]')) {
                        res.write('data: [DONE]\n\n');
                    }
                } else if (data.status === 'cancelled' || data.status === 'failed') {
                    // 失败/取消:补一个 ST 可解析的 data: {...error...} 帧,
                    // 消息里带状态码 + 上游文案(从错误 SSE 帧/result 里提取)
                    const { message, status, detail } = buildTaskErrorBody(data);
                    const joined = (data.chunks || []).join('');
                    if (!extractStreamError(data.chunks)) {
                        res.write(`data: ${JSON.stringify(openAiErrorBody(message, status, detail))}\n\n`);
                    }
                    if (!joined.includes('[DONE]')) {
                        res.write('data: [DONE]\n\n');
                    }
                    console.error(`[st-catiecli-tasks] task ${taskId} ${data.status}: HTTP ${status} ${message}${detail ? `\n  upstream: ${String(detail).slice(0, 500)}` : ''}`);
                }
                return;
            }
            // busy → 继续循环（pollTask 内部已控制节奏）
        }
    } catch (err) {
        if (abort.signal.aborted) return;
        console.error('[st-catiecli-tasks] stream failed:', err.message);
        if (!res.headersSent) {
            res.status(err.statusCode ?? 502).json({ error: { message: err.message, code: err.statusCode ?? 502 } });
        } else {
            res.write(`data: ${JSON.stringify({ error: { message: err.message, code: err.statusCode ?? 502 } })}\n\n`);
            res.write('data: [DONE]\n\n');
        }
    } finally {
        if (taskId && abort.signal.aborted) {
            // ST 客户端断开 → 取消任务，不浪费上游额度
            cancelTask(taskId, apiKey, upstream);
        }
        res.end();
    }
}

/**
 * 非流式：提交 → 轮询到终态 → 原样返回 result（错误体也原样透传，
 * 状态码用任务记录的上游 status_code）。
 */
async function handleNonStream(req, res, body, apiKey) {
    const upstream = upstreamFromRequest(req);
    const abort = new AbortController();
    // 客户端断开检测：响应未结束就关闭 = ST 侧断连
    res.on('close', () => {
        if (!res.writableEnded) abort.abort();
    });

    let taskId = null;
    try {
        taskId = await submitTask(body, apiKey, abort.signal, upstream);
        let cursor = 0;
        let data = null;

        while (true) {
            const result = await pollTask(taskId, cursor, apiKey, abort.signal, upstream);
            if (!result) {
                return res.status(504).json({ error: { message: `任务超过 ${POLL_MAX_MINUTES} 分钟未完成,已自动取消 (task ${taskId})`, code: 504 } });
            }
            data = result.data;
            cursor = result.cursor;
            if (data.done) break;
        }

        if (data.status === 'failed') {
            const { message, status, detail } = buildTaskErrorBody(data);
            // 优先原样透传上游错误体(标准 OpenAI 形态);若上游体不可解析则用组装体
            const upstreamBody = (data.result && typeof data.result === 'object' && data.result.error)
                ? data.result
                : null;
            console.error(`[st-catiecli-tasks] task ${taskId} failed: HTTP ${status} ${message}${detail ? `\n  upstream: ${String(detail).slice(0, 500)}` : ''}`);
            return res.status(status).json(upstreamBody ?? openAiErrorBody(message, status, detail));
        }
        if (data.status === 'cancelled') {
            return res.status(499).json({ error: { message: 'Task was cancelled', code: 499 } });
        }
        return res.json(data.result ?? {});
    } catch (err) {
        if (abort.signal.aborted) return;
        console.error('[st-catiecli-tasks] request failed:', err.message);
        return res.status(err.statusCode ?? 502).json({ error: { message: err.message, code: err.statusCode ?? 502 } });
    } finally {
        // ST 客户端断开 / 轮询超时 → 取消任务，不浪费上游额度
        if (taskId && abort.signal.aborted) {
            cancelTask(taskId, apiKey, upstream);
        }
    }
}

/**
 * 插件初始化：注册标准 OpenAI 兼容路由。
 * @param {import('express').Router} router
 */
export async function init(router) {
    const api = express.Router();
    router.use(api);

    // 健康检查（浏览器打开插件根路径可见状态）
    router.get('/health', (req, res) => {
        res.json({ ok: true, upstream: upstreamFromRequest(req), plugin: info.id });
    });

    // GET /v1/models — 直接透传 catiecli 的模型列表（Custom 源「连接」时拉取）
    api.get('/v1/models', async (req, res) => {
        try {
            const apiKey = req.headers['authorization']?.replace(/^Bearer\s+/i, '') ?? '';
            const resp = await withNetRetry('模型列表', () => fetch(`${upstreamFromRequest(req)}/v1/models`, {
                headers: { 'Authorization': `Bearer ${apiKey}` },
                ...FETCH_OPTIONS,
            }));
            const text = await resp.text();
            res.status(resp.status).type(resp.headers.get('content-type') || 'application/json').send(text);
        } catch (err) {
            console.error('[st-catiecli-tasks] models failed:', err.message);
            res.status(err.statusCode ?? 502).json({ error: { message: err.message } });
        }
    });

    // POST /v1/chat/completions — 标准入口；内部提交任务 + 轮询
    api.post('/v1/chat/completions', express.json({ limit: '100mb' }), async (req, res) => {
        const apiKey = req.headers['authorization']?.replace(/^Bearer\s+/i, '') ?? '';
        if (!apiKey) {
            return res.status(401).json({ error: { message: 'Missing API key. Set your cat- key as the Custom OpenAI secret.', code: 401 } });
        }
        const body = req.body;
        if (!body || typeof body !== 'object') {
            return res.status(400).json({ error: { message: 'Invalid JSON body', code: 400 } });
        }
        if (body.stream) {
            return handleStream(req, res, body, apiKey);
        }
        return handleNonStream(req, res, body, apiKey);
    });

    console.log(`[st-catiecli-tasks] Provider ready. Point SillyTavern's Custom (OpenAI-compatible) source at /api/plugins/st-catiecli-tasks/v1 (default upstream: ${DEFAULT_CATIECLI_URL}; override per-request via the X-Catiecli-Url header in ST's "Custom Include Headers" field)`);
}

export async function exit() {
    // 无持久资源需要清理
}
