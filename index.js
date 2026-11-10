/**
 * Catiecli Tasks — SillyTavern UI 扩展(纯前端,Extensions 贴 git 链接即装)
 *
 * 用 catiecli 的「任务 API」(提交 + 轮询分片)接管 SillyTavern 的 Chat
 * Completion 生成请求,根治长连接网络不稳定:断网/切后台不再报废整个回复。
 *
 * 工作方式:
 *   ST 前端 sendOpenAIRequest 会 fetch('/api/backends/chat-completions/generate')
 *   本扩展 patch window.fetch 拦截该请求,改为在浏览器内直接:
 *     1. POST {catiecli}/v1/openai/tasks      提交任务(秒回 task_id)
 *     2. GET  {catiecli}/v1/openai/tasks/{id}?after=N  轮询增量分片
 *     3. 分片原样推进 ReadableStream → 返回伪 Response 给 ST 的 SSE 解析器
 *   (catiecli 全站 CORS 放行,浏览器可直连)
 *
 * 使用(Extensions 里启用本扩展后):
 *   1. API 选 Chat Completion → 源选 Custom (OpenAI-compatible)
 *   2. Custom Endpoint 填 catiecli 地址,如 https://catiecli.sukaka.top/v1
 *   3. API 密钥填 cat- 密钥(本扩展直接复用该密钥,无需另填)
 *   4. 连接(后端直连 catiecli /v1/models,标准协议)→ 选模型 → 生成
 *
 * 断网行为:提交/轮询自动重试(3 次指数退避),3 次都连不上才报错;
 * ST 侧中断(停止按钮)→ 扩展发 cancel,不浪费上游额度。
 */
// 相对路径注意:第三方扩展经 /scripts/extensions/third-party/<name>/ 提供,
// 比内置扩展深一层,所以是 ../../../(内置扩展用 ../../)
import { extension_settings, renderExtensionTemplateAsync } from '../../../extensions.js';
import { saveSettings, saveSettingsDebounced } from '../../../../script.js';
import { chat_completion_sources } from '../../../openai.js';
import { callGenericPopup, POPUP_TYPE } from '../../../popup.js';

const MODULE_NAME = 'st-catiecli-tasks';
const GENERATE_URL = '/api/backends/chat-completions/generate';
const DEFAULT_UPSTREAM = 'https://catiecli.sukaka.top';

// ===== 轮询节奏(代码内固定,非用户设置) =====
const POLL_INTERVAL_MS = 2000;   // 常规轮询间隔(等待中,无新分片)
const BUSY_POLL_INTERVAL_MS = 500; // 流式打字机跟进间隔(有新分片时)
const NET_RETRY_MAX = 3;         // 网络错误重试次数(3 次都连不上才报错)
const NET_RETRY_BASE_MS = 1000;  // 重试退避基数(1s→2s→4s)
const POLL_MAX_MINUTES = 30;     // 单任务最长轮询时间

// ===== 设置(仅用户真正会改的东西) =====
const defaultSettings = {
    enabled: false,              // 总开关:接管 Custom 源的生成请求
    apiKey: '',                  // cat- 密钥(扩展自存,不依赖 ST secret)
    upstreamOverride: '',        // 留空用 Custom Endpoint;否则强制此地址
    interceptKeyword: 'catiecli', // 接管关键词:请求体 URL 命中才接管(正则,大小写不敏感)
};

function loadSettings() {
    extension_settings[MODULE_NAME] = extension_settings[MODULE_NAME] || {};
    for (const key of Object.keys(defaultSettings)) {
        if (extension_settings[MODULE_NAME][key] === undefined) {
            extension_settings[MODULE_NAME][key] = defaultSettings[key];
        }
    }
}

function getSettings() {
    return extension_settings[MODULE_NAME] ?? defaultSettings;
}

// ===== 上游地址与密钥 =====

/** 规范化 base:去尾斜杠,剥掉 ST 后端会拼路径的 /v1、/v1beta 段 */
function normalizeBaseUrl(raw) {
    return String(raw || '').trim().replace(/\/+$/, '').replace(/\/(v1|v1beta)(\/|$)/gi, '');
}

/** 上游 base:请求体 custom_url(剥掉尾部 /v1);否则默认线上 */
function resolveUpstream(generateData) {
    const url = String(generateData?.custom_url ?? '').trim();
    if (url) return normalizeBaseUrl(url);
    return DEFAULT_UPSTREAM;
}

/** 读取 cat- 密钥:扩展设置里自带(不依赖 ST secret 存储/权限) */
function resolveApiKey() {
    const key = getSettings().apiKey?.trim();
    if (key) return key;
    throw taskApiError(
        '检查设置',
        401,
        '未设置 API 密钥:请在 Extensions(扩展)面板 → Catiecli Tasks 里填写 cat- 密钥并点击保存图标',
        null,
    );
}

// ===== 网络重试包装(连不上才重试;HTTP 错误是明确响应,直接抛) =====

function isNetworkError(err) {
    return err instanceof TypeError || ['ECONNREFUSED', 'ETIMEDOUT', 'ECONNRESET', 'ENOTFOUND', 'UND_ERR_CONNECT_TIMEOUT']
        .includes(err?.cause?.code);
}

async function withNetRetry(label, fn) {
    let lastErr = null;
    for (let attempt = 1; attempt <= NET_RETRY_MAX; attempt++) {
        try {
            return await fn();
        } catch (err) {
            if (err.isTaskApiError) throw err; // 已构造好的结构化错误:透传
            if (err instanceof Error && err.statusCode !== undefined) throw err; // HTTP 错误:透传
            if (!isNetworkError(err)) throw err;
            lastErr = err;
            if (attempt < NET_RETRY_MAX) {
                const delay = Math.min(NET_RETRY_BASE_MS * (2 ** (attempt - 1)), 8000);
                console.warn(`[st-catiecli-tasks] ${label} 网络错误(第 ${attempt}/${NET_RETRY_MAX} 次),${delay / 1000}s 后重试: ${err.message}`);
                await new Promise(r => setTimeout(r, delay));
            }
        }
    }
    throw taskApiError(
        label,
        502,
        `${label}失败: 连续 ${NET_RETRY_MAX} 次无法连接上游(${lastErr?.message ?? '网络错误'})。请检查网络或上游地址`,
        lastErr?.stack ?? null,
    );
}

// ===== 错误展示(用户友好:完整详情弹窗,ST 原生 toast 照常保留) =====

function escapeHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
}

/** 对象 → 缩进 JSON;字符串原样(供弹窗详情展示) */
function prettyJson(v) {
    if (v == null) return '';
    if (typeof v === 'string') return v;
    if (typeof v !== 'object') return String(v);
    try { return JSON.stringify(v, null, 2); } catch { return String(v); }
}

/** 从上游错误体挖最可读的消息(标准 OpenAI / FastAPI detail / Gemini 形态) */
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

/** 从 SSE 分片里提取 ST 能解析的 data: {...} 错误帧 */
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
            if (message) return { message, raw: prettyJson(data) };
        } catch { /* 非 JSON 行,跳过 */ }
    }
    return null;
}

/** 构造任务 API 错误(带结构化信息,弹窗与错误 Response 共用) */
function taskApiError(phase, statusCode, message, detail, ctx = {}) {
    const err = new Error(message);
    err.isTaskApiError = true;
    err.phase = phase;
    err.statusCode = statusCode ?? 502;
    err.detail = detail ?? null;
    err.taskId = ctx.taskId ?? '';
    err.model = ctx.model ?? '';
    err.upstream = ctx.upstream ?? '';
    return err;
}

/** 任务 API 错误 → 伪 Response(ok=false):ST 走原生报错路径,不回退直连 */
function taskErrorResponse(err) {
    return new Response(JSON.stringify({ error: { message: err.message, code: err.statusCode } }), {
        status: err.statusCode,
        headers: { 'Content-Type': 'application/json' },
    });
}

/** 同一时间只保留一个错误弹窗;期间的新错误降级为 toast */
let taskErrorPopup = null;

/** 生成失败详情弹窗:主消息 + 状态码 + 模型/任务/上游 + 完整上游错误体,可一键复制 */
function showTaskErrorPopup(err) {
    const code = err.statusCode ?? 502;
    const mainMsg = err.message || '未知错误';
    const detailText = prettyJson(err.detail).trim();
    const detailHtml = detailText
        ? `<div class="catiecli-error-detail"><div class="catiecli-error-detail-label">上游错误详情</div><pre>${escapeHtml(detailText.slice(0, 4000))}${detailText.length > 4000 ? '\n…(超出部分已截断)' : ''}</pre></div>`
        : '';
    const meta = [
        `HTTP <b>${code}</b>`,
        err.phase ? `阶段: ${escapeHtml(err.phase)}` : '',
        err.model ? `模型: ${escapeHtml(err.model)}` : '',
        err.taskId ? `任务: ${escapeHtml(err.taskId)}` : '',
        err.upstream ? `上游: ${escapeHtml(err.upstream)}` : '',
    ].filter(Boolean).map(x => `<span>${x}</span>`).join('');
    const html = `<div class="catiecli-error-popup">
        <div class="catiecli-error-title"><i class="fa-solid fa-circle-exclamation"></i>${escapeHtml(mainMsg)}</div>
        <div class="catiecli-error-meta">${meta}</div>
        ${detailHtml}
    </div>`;
    const plain = [
        `[Catiecli Tasks] ${err.phase ? `生成失败(${err.phase})` : '生成失败'}: ${mainMsg}`,
        `HTTP ${code}`,
        err.model && `模型: ${err.model}`,
        err.taskId && `任务: ${err.taskId}`,
        err.upstream && `上游: ${err.upstream}`,
        detailText,
    ].filter(Boolean).join('\n');
    console.error(`[st-catiecli-tasks] ${plain}`);
    if (taskErrorPopup) {
        toastr.error(mainMsg, 'Catiecli Tasks 生成失败', { timeOut: 10000, extendedTimeOut: 5000, preventDuplicates: true });
        return;
    }
    taskErrorPopup = callGenericPopup(html, POPUP_TYPE.TEXT, '', {
        okButton: '关闭',
        customButtons: [{
            text: '复制详情',
            action: async () => {
                try {
                    await navigator.clipboard.writeText(plain);
                    toastr.success('错误详情已复制到剪贴板', 'Catiecli Tasks', { timeOut: 3000 });
                } catch {
                    toastr.warning('复制失败,完整详情请查看浏览器控制台(F12)', 'Catiecli Tasks');
                }
            },
        }],
    }).catch(() => {}).finally(() => { taskErrorPopup = null; });
}

// ===== 任务 API 客户端 =====

async function submitTask(upstream, apiKey, body, ctx) {
    return withNetRetry('任务提交', () => fetch(`${upstream}/v1/openai/tasks`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    }).then(async resp => {
        const text = await resp.text();
        if (!resp.ok) {
            // FastAPI 的 detail 就是报错文案(无效 key / 缺 model / 无效 JSON 等)
            const text = await resp.text();
            let message = `任务提交失败: HTTP ${resp.status}`;
            try {
                message = `任务提交失败: HTTP ${resp.status} ${extractUpstreamMessage(JSON.parse(text))}`.trimEnd();
            } catch { /* 非 JSON 原样展示 */ }
            throw taskApiError('提交任务', resp.status, message, text.slice(0, 2000), ctx);
        }
        const data = JSON.parse(text);
        if (!data?.id) throw taskApiError('提交任务', 502, '任务提交未返回 task_id', text.slice(0, 2000), ctx);
        return data.id;
    }));
}

async function pollTask(upstream, apiKey, taskId, after, ctx) {
    return withNetRetry('任务轮询', () => fetch(`${upstream}/v1/openai/tasks/${encodeURIComponent(taskId)}?after=${after}`, {
        headers: { 'Authorization': `Bearer ${apiKey}` },
    }).then(async resp => {
        if (!resp.ok) {
            const text = await resp.text().catch(() => '');
            let message = `任务查询失败: HTTP ${resp.status}`;
            try {
                message = `任务查询失败: HTTP ${resp.status} ${extractUpstreamMessage(JSON.parse(text))}`.trimEnd();
            } catch { /* 非 JSON 原样展示 */ }
            throw taskApiError('查询任务', resp.status, message, text.slice(0, 2000) || null, ctx);
        }
        return resp.json();
    }));
}

async function cancelTask(upstream, apiKey, taskId) {
    try {
        await fetch(`${upstream}/v1/openai/tasks/${encodeURIComponent(taskId)}/cancel`, {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${apiKey}` },
        });
    } catch (err) {
        console.warn(`[st-catiecli-tasks] 取消任务 ${taskId} 失败:`, err.message);
    }
}

// ===== fetch 拦截:接管生成请求 =====

const originalFetch = window.fetch;
let patched = false;

/** 从 ST 的 generate_data 提取 OpenAI 协议字段(与 ST 后端发给上游的请求体同构) */
function extractOpenAIBody(generateData) {
    const body = {
        messages: generateData.messages,
        model: generateData.model,
        temperature: generateData.temperature,
        frequency_penalty: generateData.frequency_penalty,
        presence_penalty: generateData.presence_penalty,
        top_p: generateData.top_p,
        max_tokens: generateData.max_tokens,
        max_completion_tokens: generateData.max_completion_tokens,
        stream: !!generateData.stream,
        stop: generateData.stop,
        logit_bias: generateData.logit_bias,
        seed: generateData.seed,
        n: generateData.n,
        response_format: generateData.response_format,
        tools: generateData.tools,
        tool_choice: generateData.tool_choice,
        reasoning_effort: generateData.reasoning_effort,
        verbosity: generateData.verbosity,
    };
    // 移除 undefined 字段,避免 catiecli 收到无意义 null
    for (const k of Object.keys(body)) {
        if (body[k] === undefined) delete body[k];
    }
    return body;
}

/** 接管关键词正则:无效/留空时退回默认 catiecli */
function interceptKeywordRegex() {
    const raw = String(getSettings().interceptKeyword ?? '').trim() || 'catiecli';
    try {
        return new RegExp(raw, 'i');
    } catch {
        console.warn(`[st-catiecli-tasks] 接管关键词正则无效,退回默认: ${raw}`);
        return /catiecli/i;
    }
}

/** 是否应拦截本次 generate 请求:请求体 URL 命中关键词才接管,逐请求判定 */
function shouldIntercept(generateData) {
    const s = getSettings();
    if (!s.enabled) return false;
    // 仅接管 Custom (OpenAI-compatible) 源 —— 以请求体为准:
    // 第三方插件走同一个 /generate 端点,请求体自带各自的 chat_completion_source
    if (generateData?.chat_completion_source !== chat_completion_sources.CUSTOM) return false;
    if (!generateData || typeof generateData !== 'object' || !generateData.model) return false;
    // 逐请求正则判断:请求体 custom_url 命中关键词就接管,没有就不拦截
    return interceptKeywordRegex().test(String(generateData.custom_url ?? ''));
}

/** 非流式:envelope → 伪 Response(failed 时 ok=false,ST 会弹错并透传错误体) */
function envelopeToResponse(env, ctx) {
    const status = env.status === 'failed'
        ? (env.status_code ?? 502)
        : (env.status === 'cancelled' ? 499 : 200);
    if (env.status === 'completed') {
        return new Response(JSON.stringify(env.result ?? {}), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
        });
    }
    // failed / cancelled:构造完整错误并弹详情窗,再返回错误 Response 给 ST
    const upstreamMsg = extractUpstreamMessage(env.result) || extractStreamError(env.chunks)?.message || env.error || '';
    const message = env.status === 'cancelled'
        ? '任务已取消'
        : `上游返回错误: HTTP ${status}${upstreamMsg ? ` ${upstreamMsg}` : ''}`;
    const detail = prettyJson(env.result).trim() || extractStreamError(env.chunks)?.raw || env.error || null;
    const err = taskApiError(env.status === 'cancelled' ? '任务被取消' : '上游返回错误', status, message, detail, ctx);
    showTaskErrorPopup(err);
    return taskErrorResponse(err);
}

/** 流式:伪 SSE Response,分片推进 ReadableStream(准流式) */
function streamTaskResponse(upstream, apiKey, taskId, signal, ctx) {
    const encoder = new TextEncoder();

    const stream = new ReadableStream({
        async start(controller) {
            const deadline = Date.now() + POLL_MAX_MINUTES * 60 * 1000;
            let after = 0;
            try {
                let nextDelay = POLL_INTERVAL_MS;
                while (Date.now() < deadline) {
                    if (signal?.aborted) break;
                    await new Promise(r => setTimeout(r, nextDelay));
                    if (signal?.aborted) break;

                    const env = await pollTask(upstream, apiKey, taskId, after, ctx);
                    for (const chunk of (env.chunks ?? [])) {
                        controller.enqueue(encoder.encode(chunk));
                    }
                    after = env.next_after ?? after;

                    if (env.done) {
                        const joined = (env.chunks ?? []).join('');
                        if (env.status === 'failed' || env.status === 'cancelled') {
                            const status = env.status === 'cancelled' ? 499 : (env.status_code ?? 502);
                            const parsed = extractStreamError(env.chunks);
                            const upstreamMsg = parsed?.message || env.error || '';
                            const message = env.status === 'cancelled'
                                ? '任务已取消'
                                : `上游返回错误: HTTP ${status}${upstreamMsg ? ` ${upstreamMsg}` : ''}`;
                            // 详情弹窗(完整上游错误体)
                            showTaskErrorPopup(taskApiError(
                                env.status === 'cancelled' ? '任务被取消' : '上游返回错误',
                                status,
                                message,
                                parsed?.raw || prettyJson(env.result).trim() || env.error || null,
                                ctx,
                            ));
                            // chunks 里没有 ST 可解析的 data: {...error...} 帧时补一帧,
                            // 让 ST 原生流式报错路径(toastr)也能给出明确文案
                            if (!parsed) {
                                controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message, code: status } })}\n\n`));
                            }
                        }
                        // 上游 SSE 缺 [DONE] 时补齐(ST 靠它结束流)
                        if (!joined.includes('[DONE]')) {
                            controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                        }
                        break;
                    }
                    // 有新分片 → 快速跟进(准流式);否则常规节奏
                    nextDelay = (env.chunks?.length ?? 0) > 0 ? BUSY_POLL_INTERVAL_MS : POLL_INTERVAL_MS;
                }
                if (Date.now() >= deadline && !signal?.aborted) {
                    await cancelTask(upstream, apiKey, taskId);
                    const message = `任务超过 ${POLL_MAX_MINUTES} 分钟未完成,已自动取消`;
                    showTaskErrorPopup(taskApiError('轮询超时', 504, message, null, ctx));
                    controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message, code: 504 } })}\n\n`));
                    controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                }
            } catch (err) {
                try {
                    const taskErr = err.isTaskApiError ? err : taskApiError('轮询任务', err.statusCode ?? 502, err.message ?? String(err), null, ctx);
                    showTaskErrorPopup(taskErr);
                    controller.enqueue(encoder.encode(`data: ${JSON.stringify({ error: { message: taskErr.message, code: taskErr.statusCode } })}\n\n`));
                    controller.enqueue(encoder.encode('data: [DONE]\n\n'));
                } catch {
                    // controller 已关闭
                }
            } finally {
                try { controller.close(); } catch { /* already closed */ }
            }
        },
        cancel() {
            // ST 侧取消(停止按钮/断开):取消任务,不浪费上游额度
            cancelTask(upstream, apiKey, taskId);
        },
    });

    return new Response(stream, {
        status: 200,
        headers: {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
        },
    });
}

/** 把 catiecli 任务跑成一个伪 Response(SSE 流或 JSON),接口与 ST 期望一致 */
async function runTaskAsResponse(generateData, signal) {
    const upstream = resolveUpstream(generateData);
    let apiKey;
    let ctx = { model: generateData?.model ?? '', upstream };
    try {
        apiKey = await resolveApiKey();
    } catch (err) {
        const taskErr = err.isTaskApiError ? err : taskApiError('检查设置', err.statusCode ?? 401, err.message, null, ctx);
        showTaskErrorPopup(taskErr);
        return taskErrorResponse(taskErr);
    }
    const requestBody = extractOpenAIBody(generateData);
    ctx = { model: requestBody.model ?? ctx.model, upstream };

    let taskId;
    try {
        taskId = await submitTask(upstream, apiKey, requestBody, ctx);
    } catch (err) {
        // 提交阶段失败:详情弹窗 + 错误 Response(不回退直连,避免用户误以为生成正常)
        const taskErr = err.isTaskApiError ? err : taskApiError('提交任务', err.statusCode ?? 502, err.message, null, ctx);
        showTaskErrorPopup(taskErr);
        return taskErrorResponse(taskErr);
    }
    console.info(`[st-catiecli-tasks] 任务已提交: ${taskId}(stream=${requestBody.stream})`);

    if (!requestBody.stream) {
        // 非流式:轮询到终态,返回伪 JSON Response
        const deadline = Date.now() + POLL_MAX_MINUTES * 60 * 1000;
        let after = 0;
        let env = null;
        ctx.taskId = taskId;
        try {
            while (Date.now() < deadline) {
                if (signal?.aborted) throw new DOMException('aborted', 'AbortError');
                await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
                env = await pollTask(upstream, apiKey, taskId, after, ctx);
                after = env.next_after ?? after;
                if (env.done) break;
                env = null;
            }
        } catch (err) {
            // 轮询阶段失败:详情弹窗 + 错误 Response(ST 原生 toast 路径可解析)
            const taskErr = err.isTaskApiError ? err : taskApiError('轮询任务', err.statusCode ?? 502, err.message ?? String(err), null, ctx);
            showTaskErrorPopup(taskErr);
            return taskErrorResponse(taskErr);
        }
        if (!env) {
            await cancelTask(upstream, apiKey, taskId);
            const err = taskApiError('轮询超时', 504, `任务超过 ${POLL_MAX_MINUTES} 分钟未完成,已自动取消`, null, ctx);
            showTaskErrorPopup(err);
            return taskErrorResponse(err);
        }
        return envelopeToResponse(env, ctx);
    }

    ctx.taskId = taskId;
    return streamTaskResponse(upstream, apiKey, taskId, signal, ctx);
}

function installFetchPatch() {
    if (patched) return;
    patched = true;
    window.fetch = async function (input, init) {
        const url = typeof input === 'string' ? input : input?.url ?? '';
        if (url.includes(GENERATE_URL) && init?.method?.toUpperCase() === 'POST' && init?.body) {
            let generateData = null;
            try {
                generateData = JSON.parse(init.body);
            } catch { /* 非 JSON 请求体,不接管 */ }
            if (generateData && shouldIntercept(generateData)) {
                console.info('[st-catiecli-tasks] 接管生成请求 →', generateData.model);
                return await runTaskAsResponse(generateData, init.signal ?? null);
            }
        }
        return originalFetch.call(this, input, init);
    };
    console.info('[st-catiecli-tasks] fetch 拦截已安装(仅接管 Custom 源生成请求)');
}

function uninstallFetchPatch() {
    if (!patched) return;
    window.fetch = originalFetch;
    patched = false;
}

// ===== 扩展 UI(设置面板) =====

async function addSettingsPanel() {
    // 第三方扩展模板路径需带 third-party/ 前缀(静态挂载在
    // /scripts/extensions/third-party/* 下)
    const html = await renderExtensionTemplateAsync('third-party/st-catiecli-tasks', 'settings', {});
    $('#extensions_settings2').append(html);

    const s = getSettings();

    // 一键切换接管(标题栏内的开关)
    $('#catiecli_tasks_enabled').prop('checked', s.enabled).on('change', function () {
        s.enabled = $(this).prop('checked');
        if (s.enabled) {
            installFetchPatch();
        } else {
            uninstallFetchPatch();
        }
        saveSettingsDebounced();
        toastr.info(s.enabled ? '已开启任务 API 接管' : '已关闭接管,恢复 ST 原生直连', 'Catiecli Tasks');
    });

    // 密钥:显示/隐藏;有值时输入框转绿、无值时呼吸提醒
    const keyInput = $('#catiecli_tasks_key');
    keyInput.val(s.apiKey).on('input', function () {
        s.apiKey = String($(this).val());
        $(this).toggleClass('has-key', Boolean(s.apiKey?.trim()));
    });
    keyInput.toggleClass('has-key', Boolean(s.apiKey?.trim()));
    $('#catiecli_tasks_key_toggle').on('click', function () {
        const input = $('#catiecli_tasks_key');
        const isPassword = input.attr('type') === 'password';
        input.attr('type', isPassword ? 'text' : 'password');
        $(this).toggleClass('fa-eye fa-eye-slash');
    });

    // 保存按钮(密钥/上游共用行为:立即落盘 + 打勾反馈 + toast;onSave 抛错则不落盘)
    function bindSaveIcon(selector, onSave) {
        $(selector).on('click', async function () {
            try {
                await onSave();
            } catch (err) {
                console.warn('[st-catiecli-tasks] 保存失败:', err.message);
                return;
            }
            await saveSettings();
            const icon = $(this);
            icon.addClass('fa-check').removeClass('fa-save');
            setTimeout(() => icon.addClass('fa-save').removeClass('fa-check'), 1500);
        });
    }
    bindSaveIcon('#catiecli_tasks_key_save', async () => {
        s.apiKey = String($('#catiecli_tasks_key').val());
        $('#catiecli_tasks_key').toggleClass('has-key', Boolean(s.apiKey?.trim()));
        toastr.success('API 密钥已保存', 'Catiecli Tasks');
    });
    bindSaveIcon('#catiecli_tasks_save', async () => {
        s.upstreamOverride = String($('#catiecli_tasks_upstream').val());
        toastr.success('上游地址已保存', 'Catiecli Tasks');
    });
    $('#catiecli_tasks_keyword').val(s.interceptKeyword ?? 'catiecli');
    bindSaveIcon('#catiecli_tasks_keyword_save', async () => {
        const raw = String($('#catiecli_tasks_keyword').val()).trim();
        try {
            new RegExp(raw, 'i'); // 提前校验,非法正则不落盘
        } catch {
            toastr.error('接管关键词不是合法的正则表达式,未保存', 'Catiecli Tasks');
            throw new Error('invalid regex');
        }
        s.interceptKeyword = raw;
        toastr.success(raw ? `接管关键词已保存:${raw}` : '已清空,恢复默认 catiecli', 'Catiecli Tasks');
    });
}

// ===== 入口 =====

export async function init() {
    loadSettings();
    await addSettingsPanel();
    if (getSettings().enabled) {
        installFetchPatch();
    }
    console.info('[st-catiecli-tasks] Catiecli Tasks 扩展已加载(任务 API 接管模式)');
}
