/**
 * Kiro 运行时通道（`runtime.<region>.kiro.dev/v1/responses`）的离线验证。
 *
 * 这条通道是可选的（`chatTransport: 'kiro-runtime'`），它说的是 OpenAI
 * Responses API：请求体是 `{ model, input, profileArn, origin, stream }`，
 * 回的是 SSE。
 *
 * 请求契约是对活服务实测的，钉在这里的是三件事：
 * 1. 必填的头（CLI 身份、`x-amzn-kiro-origin`、`x-amzn-kiro-profile`、
 *    `TokenType: SSO_OIDC`）与必填的字段；
 * 2. 服务端拒绝的字段：`instructions` 与 `tool_choice` 一个都不能发；
 * 3. 回包里的 `usage` 是 credits 而不是 token 数，所以 token 数不许编。
 *
 * 事件名仍是按 OpenAI Responses 的公开形状实现的（插件自己那一侧）。
 *
 * Run: node tests/runtime-test.mjs
 */

import { KiroClient } from '../lib/kiro-client.js'
import { decodeSse, kiroRuntimeEndpoint, normalizeRuntimeEvent, runtimeOutcome, toRuntimeRequest } from '../lib/kiro-runtime.js'

let pass = 0
let fail = 0
const check = (name, condition, detail) => {
  if (condition) {
    pass += 1
    console.log(`PASS  ${name}`)
    return
  }
  fail += 1
  console.log(`FAIL  ${name}${detail === undefined ? '' : ` -> ${detail}`}`)
}

const ARN = 'arn:aws:codewhisperer:us-east-1:123456789012:profile/TESTPROFILE'

/** 把若干 SSE 块拼成一个字节流。 */
function sseStream(blocks) {
  const text = blocks.map(block => `${block}\n\n`).join('')
  const bytes = Buffer.from(text, 'utf8')
  return {
    async *[Symbol.asyncIterator]() {
      // 故意切成两半：SSE 的块边界与读边界无关。
      yield bytes.subarray(0, Math.floor(bytes.length / 2))
      yield bytes.subarray(Math.floor(bytes.length / 2))
    },
  }
}

// ---------------------------------------------------------------------------
// 1. 请求体：Kiro 的 conversationState -> Responses API 的 input
// ---------------------------------------------------------------------------
{
  const body = {
    profileArn: ARN,
    conversationState: {
      chatTriggerType: 'MANUAL',
      conversationId: 'c1',
      history: [
        { userInputMessage: { content: '第一问', modelId: 'm1', origin: 'KIRO_CLI' } },
        { assistantResponseMessage: { content: '第一答', toolUses: [{ toolUseId: 't1', name: 'fs_read', input: { path: 'a' } }] } },
      ],
      currentMessage: {
        userInputMessage: {
          content: '工具结果',
          modelId: 'm1',
          origin: 'KIRO_CLI',
          userInputMessageContext: {
            tools: [{ toolSpecification: { name: 'fs_read', description: '读文件', inputSchema: { json: { type: 'object', properties: {} } } } }],
            toolResults: [{ toolUseId: 't1', content: [{ text: '文件内容' }], status: 'success' }],
          },
          images: [{ format: 'png', source: { bytes: 'AAAA' } }],
        },
      },
    },
  }
  const request = toRuntimeRequest(body, { profileArn: ARN, origin: 'KIRO_CLI' })
  check('模型 id 来自 currentMessage', request.model === 'm1', request.model)
  check('必须是非流式被拒，所以 stream 恒为 true', request.stream === true, request.stream)
  check('origin 是 KIRO_CLI', request.origin === 'KIRO_CLI', request.origin)
  check('profileArn 带上', request.profileArn === ARN, request.profileArn)
  check('input 有五条（user/assistant/调用/当前轮/工具结果）', request.input.length === 5,
    JSON.stringify(request.input.length))
  check('user 轮是 input_text',
    request.input[0].role === 'user' && request.input[0].content[0].type === 'input_text'
    && request.input[0].content[0].text === '第一问', JSON.stringify(request.input[0]))
  check('assistant 轮是 output_text',
    request.input[1].role === 'assistant' && request.input[1].content[0].type === 'output_text'
    && request.input[1].content[0].text === '第一答', JSON.stringify(request.input[1]))
  check('工具调用是 function_call 条目',
    request.input[2].type === 'function_call' && request.input[2].call_id === 't1'
    && request.input[2].name === 'fs_read', JSON.stringify(request.input[2]))
  check('当前轮带图与文本',
    request.input[3].role === 'user' && request.input[3].content[0].text === '工具结果',
    JSON.stringify(request.input[3]))
  check('工具结果是 function_call_output 条目',
    request.input[4].type === 'function_call_output' && request.input[4].call_id === 't1'
    && request.input[4].output === '文件内容', JSON.stringify(request.input[4]))
  check('图片变成 input_image',
    request.input[3].content?.[1]?.type === 'input_image'
    && request.input[3].content[1].image_url === 'data:image/png;base64,AAAA',
    JSON.stringify(request.input[3].content))
  check('工具声明变成 Responses 的 tools',
    Array.isArray(request.tools) && request.tools[0]?.type === 'function'
    && request.tools[0]?.name === 'fs_read', JSON.stringify(request.tools))
  check('端点按区域拼', kiroRuntimeEndpoint('us-east-1') === 'https://runtime.us-east-1.kiro.dev/v1/responses',
    kiroRuntimeEndpoint('us-east-1'))
  // 运行时明确拒绝这两个字段：instructions -> "systemPrompt field is not enabled
  // for this user."，tool_choice -> "This request contains parameters or input
  // that are not supported."。系统提示词因此走第一条 user 轮，不进 instructions。
  check('请求体不带 instructions（运行时拒绝）', request.instructions === undefined,
    JSON.stringify(request.instructions))
  check('请求体不带 tool_choice（运行时拒绝）', request.tool_choice === undefined,
    JSON.stringify(request.tool_choice))
  check('元数据带 agent 模式', request.metadata?.['kiro:agent_mode'] === 'vibe',
    JSON.stringify(request.metadata))
  check('元数据带会话 id 与 root 会话 id',
    request.metadata?.['kiro:conversation_id'] === 'c1'
    && request.metadata?.['kiro:root_conversation_id'] === 'c1', JSON.stringify(request.metadata))
}

// ---------------------------------------------------------------------------
// 2. SSE 解码与事件归一化
// ---------------------------------------------------------------------------
{
  const blocks = [
    'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"你"}',
    'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"好"}',
    'event: response.reasoning_text.delta\ndata: {"type":"response.reasoning_text.delta","delta":"想一下"}',
    'event: response.output_item.added\ndata: {"type":"response.output_item.added","item":{"type":"function_call","call_id":"t1","name":"fs_read"}}',
    'event: response.function_call_arguments.delta\ndata: {"type":"response.function_call_arguments.delta","call_id":"t1","delta":"{\\"path\\":"}',
    'event: response.output_item.done\ndata: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"t1","name":"fs_read"}}',
  ]
  const frames = []
  for await (const frame of decodeSse(sseStream(blocks))) frames.push(frame)
  check('SSE 块数正确', frames.length === 6, String(frames.length))
  check('事件名读到了', frames[0].event === 'response.output_text.delta', frames[0].event)

  // CRLF 的流也要能切开：分隔符是 \r\n\r\n，不是 \n\n。
  const crlf = []
  for await (const frame of decodeSse(sseStream(blocks.map(block => block.replace(/\n/gu, '\r\n'))))) crlf.push(frame)
  check('CRLF 的 SSE 也能切开', crlf.length === 6, String(crlf.length))
  check('CRLF 的 SSE 事件名读到了', crlf[0].event === 'response.output_text.delta', crlf[0].event)
  const events = frames.map(frame => normalizeRuntimeEvent(frame)).filter(event => event !== undefined)
  check('正文增量归一化成 text',
    events[0].kind === 'text' && events[0].text === '你' && events[1].text === '好',
    JSON.stringify(events.slice(0, 2)))
  check('思考增量归一化成 reasoning',
    events[2].kind === 'reasoning' && events[2].text === '想一下', JSON.stringify(events[2]))
  check('工具调用开始', events[3].kind === 'tool-start' && events[3].id === 't1' && events[3].name === 'fs_read',
    JSON.stringify(events[3]))
  check('工具参数增量', events[4].kind === 'tool-delta' && events[4].id === 't1'
    && events[4].argumentsDelta === '{"path":', JSON.stringify(events[4]))
  check('工具调用结束', events[5].kind === 'tool-stop' && events[5].id === 't1', JSON.stringify(events[5]))

  const done = runtimeOutcome({
    event: 'response.completed',
    data: {
      type: 'response.completed',
      response: {
        status: 'completed',
        usage: { input_tokens: 12, output_tokens: 7, input_tokens_details: { cached_tokens: 3 } },
      },
    },
  })
  check('完成帧给出 usage', done.usage?.inputTokens === 12 && done.usage?.outputTokens === 7, JSON.stringify(done.usage))
  check('完成帧给出缓存 token', done.usage?.cacheReadTokens === 3, JSON.stringify(done.usage))
  check('完成帧给出 stopReason', done.stopReason === 'END_TURN', done.stopReason)

  const failed = runtimeOutcome({
    event: 'response.failed',
    data: { type: 'response.failed', error: { message: 'boom', code: 'server_error' } },
  })
  check('失败帧给出错误', failed.error?.message === 'boom' && failed.error?.code === 'server_error',
    JSON.stringify(failed.error))
}

// ---------------------------------------------------------------------------
// 3. 客户端：选了 kiro-runtime 就打运行时端点
// ---------------------------------------------------------------------------
{
  const calls = []
  const client = new KiroClient({
    options: () => ({ chatTransport: 'kiro-runtime' }),
    store: {
      directory: 'unused',
      active: async () => ({
        credential: {
          accessToken: 'aoa_runtime',
          method: 'idc',
          region: 'us-east-1',
          expiresAt: Date.now() + 3_600_000,
          profileArn: ARN,
        },
        id: 'a',
        label: 'A',
      }),
      read: async () => ({ accounts: [], activeId: null }),
      describe: async () => ({ activeId: null, accounts: [] }),
      clear: async () => {},
    },
    post: async request => {
      calls.push(request)
      return {
        status: 200,
        headers: { 'content-type': 'text/event-stream' },
        body: sseStream([
          'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"你好"}',
          'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":5,"output_tokens":2}}}',
        ]),
      }
    },
    sleep: async () => {},
  })
  const events = []
  for await (const event of client.sendWire({
    conversationState: {
      currentMessage: { userInputMessage: { content: 'hi', modelId: 'm1', origin: 'KIRO_CLI' } },
    },
  }, new AbortController().signal, { requestedModelId: 'claude-opus-5.5' })) events.push(event)

  check('打的是运行时端点',
    calls[0]?.url === 'https://runtime.us-east-1.kiro.dev/v1/responses', calls[0]?.url)
  const sent = JSON.parse(calls[0]?.body ?? '{}')
  check('请求体是 Responses API 的形状',
    sent.model === 'm1' && sent.stream === true && sent.origin === 'KIRO_CLI' && sent.profileArn === ARN,
    JSON.stringify(sent))
  check('请求体带 input', Array.isArray(sent.input) && sent.input.length === 1, JSON.stringify(sent.input))
  check('发出去的报文不带 instructions', sent.instructions === undefined, JSON.stringify(sent.instructions))
  check('发出去的报文不带 tool_choice', sent.tool_choice === undefined, JSON.stringify(sent.tool_choice))
  check('运行时请求用 CLI 的 user-agent',
    String(calls[0]?.headers?.['user-agent']).startsWith('KiroCLI/'), calls[0]?.headers?.['user-agent'])
  check('运行时请求带 profile ARN 头',
    calls[0]?.headers?.['x-amzn-codewhisperer-profile-arn'] === ARN, calls[0]?.headers?.['x-amzn-codewhisperer-profile-arn'])
  check('运行时请求带 kiro origin 头',
    calls[0]?.headers?.['x-amzn-kiro-origin'] === 'KIRO_CLI', calls[0]?.headers?.['x-amzn-kiro-origin'])
  check('运行时请求带 kiro profile 头',
    calls[0]?.headers?.['x-amzn-kiro-profile'] === ARN, calls[0]?.headers?.['x-amzn-kiro-profile'])
  check('运行时请求带 optout 头',
    calls[0]?.headers?.['x-amzn-codewhisperer-optout'] === 'true',
    calls[0]?.headers?.['x-amzn-codewhisperer-optout'])
  check('IdC 账号的 TokenType 是 SSO_OIDC',
    calls[0]?.headers?.TokenType === 'SSO_OIDC', calls[0]?.headers?.TokenType)
  check('SSE 正文被翻译成 text 事件',
    events.some(event => event.kind === 'text' && event.text === '你好'), JSON.stringify(events))
  check('SSE 收尾给出 stop', events.at(-1)?.kind === 'stop', JSON.stringify(events.at(-1)))
  check('usage 事件带上 token 数',
    events.some(event => event.kind === 'usage' && event.inputTokens === 5 && event.outputTokens === 2),
    JSON.stringify(events))
}

// ---------------------------------------------------------------------------
// 4. 运行时这条通道始终是 CLI 的身份（不管账号是哪一种）
// ---------------------------------------------------------------------------
{
  const calls = []
  const client = new KiroClient({
    options: () => ({ chatTransport: 'kiro-runtime' }),
    store: {
      directory: 'unused',
      active: async () => ({
        credential: {
          accessToken: 'aoa_runtime_social',
          method: 'social',
          region: 'us-east-1',
          expiresAt: Date.now() + 3_600_000,
        },
        id: 'a',
        label: 'A',
      }),
      read: async () => ({ accounts: [], activeId: null }),
      describe: async () => ({ activeId: null, accounts: [] }),
      clear: async () => {},
    },
    post: async request => {
      calls.push(request)
      return {
        status: 200,
        headers: {},
        body: sseStream([
          'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"好"}',
          'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}',
        ]),
      }
    },
    sleep: async () => {},
  })
  for await (const event of client.sendWire({
    conversationState: {
      currentMessage: { userInputMessage: { content: 'hi', modelId: 'm1', origin: 'AI_EDITOR' } },
    },
  }, new AbortController().signal)) { void event }
  check('个人账号在运行时上也报 CLI 的 user-agent',
    String(calls[0]?.headers?.['user-agent']).startsWith('KiroCLI/'), calls[0]?.headers?.['user-agent'])
  check('个人账号在运行时上不带 TokenType',
    calls[0]?.headers?.TokenType === undefined, calls[0]?.headers?.TokenType)
  check('运行时上 origin 头恒为 KIRO_CLI',
    calls[0]?.headers?.['x-amzn-kiro-origin'] === 'KIRO_CLI', calls[0]?.headers?.['x-amzn-kiro-origin'])
}

// ---------------------------------------------------------------------------
// 5. 运行时拒绝模型 id 时也按候选顺序试
// ---------------------------------------------------------------------------
{
  const sentModels = []
  const client = new KiroClient({
    options: () => ({ chatTransport: 'kiro-runtime', modelIdProbe: ['CAND_A', 'CAND_B'] }),
    store: {
      directory: 'unused',
      active: async () => ({
        credential: {
          accessToken: 'aoa_runtime_probe',
          method: 'idc',
          region: 'us-east-1',
          expiresAt: Date.now() + 3_600_000,
          profileArn: ARN,
        },
        id: 'a',
        label: 'A',
      }),
      read: async () => ({ accounts: [], activeId: null }),
      describe: async () => ({ activeId: null, accounts: [] }),
      clear: async () => {},
    },
    post: async request => {
      const body = JSON.parse(request.body)
      sentModels.push(body.model)
      if (body.model === 'CAND_B') {
        return {
          status: 200,
          headers: {},
          body: sseStream([
            'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"好"}',
            'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}',
          ]),
        }
      }
      return {
        status: 400,
        headers: {},
        body: sseStream([Buffer.from(JSON.stringify({
          message: `Invalid model ID: ${body.model}`,
          code: 'model_not_found',
        }), 'utf8')]),
      }
    },
    sleep: async () => {},
  })
  const events = []
  for await (const event of client.sendWire({
    conversationState: {
      currentMessage: { userInputMessage: { content: 'hi', modelId: 'm1', origin: 'KIRO_CLI' } },
    },
  }, new AbortController().signal, { requestedModelId: 'claude-opus-5.5' })) events.push(event)
  check('运行时按候选顺序试模型 id',
    sentModels.join(',') === 'm1,CAND_A,CAND_B', sentModels.join(','))
  check('运行时探测成功后拿到正文',
    events.some(event => event.kind === 'text' && event.text === '好'), JSON.stringify(events))
}

// ---------------------------------------------------------------------------
// 6. 运行时的 usage 是 credits，不是 token 数
// ---------------------------------------------------------------------------
{
  const credits = runtimeOutcome({
    event: 'response.completed',
    data: {
      type: 'response.completed',
      response: {
        status: 'completed',
        usage: {
          'kiro:credits': { value: 0.0115, unit: 'credit', unit_plural: 'credits' },
          'kiro:context_usage_percentage': 0.119,
        },
      },
    },
  })
  check('完成帧给出 credits', credits.usage?.credits === 0.0115, JSON.stringify(credits.usage))
  check('完成帧给出 credits 单位', credits.usage?.creditsUnit === 'credit', JSON.stringify(credits.usage))
  check('没有 token 字段时不声称测到了 token',
    credits.usage?.tokensReported === false, JSON.stringify(credits.usage))
  check('上下文百分比照常读出', credits.usage?.contextPercentage === 0.119,
    JSON.stringify(credits.usage))

  const calls = []
  const client = new KiroClient({
    options: () => ({ chatTransport: 'kiro-runtime' }),
    store: {
      directory: 'unused',
      active: async () => ({
        credential: {
          accessToken: 'aoa_runtime_credits',
          method: 'idc',
          region: 'us-east-1',
          expiresAt: Date.now() + 3_600_000,
          profileArn: ARN,
        },
        id: 'a',
        label: 'A',
      }),
      read: async () => ({ accounts: [], activeId: null }),
      describe: async () => ({ activeId: null, accounts: [] }),
      clear: async () => {},
    },
    post: async request => {
      calls.push(request)
      return {
        status: 200,
        headers: {},
        body: sseStream([
          'event: response.output_text.delta\ndata: {"type":"response.output_text.delta","delta":"你好"}',
          'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"kiro:credits":{"value":0.0115,"unit":"credit"},"kiro:context_usage_percentage":0.119}}}',
        ]),
      }
    },
    sleep: async () => {},
  })
  const events = []
  for await (const event of client.sendWire({
    conversationState: {
      currentMessage: { userInputMessage: { content: 'hi', modelId: 'm1', origin: 'KIRO_CLI' } },
    },
  }, new AbortController().signal, { requestedModelId: 'm1' })) events.push(event)
  const usage = events.find(event => event.kind === 'usage')
  check('usage 事件带上 credits', usage?.credits === 0.0115, JSON.stringify(usage))
  check('usage 事件标明 token 不是测出来的', usage?.estimated === true, JSON.stringify(usage))
  check('usage 事件不编造 token 数',
    usage?.inputTokens === 0 && usage?.outputTokens === 0, JSON.stringify(usage))
  check('usage 事件带上上下文百分比', usage?.contextPercentage === 0.119, JSON.stringify(usage))
}

// ---------------------------------------------------------------------------
// 7. 默认仍然是 eventstream 那条路
// ---------------------------------------------------------------------------
{
  const calls = []
  const client = new KiroClient({
    options: () => ({}),
    store: {
      directory: 'unused',
      active: async () => ({
        credential: {
          accessToken: 'aoa_default',
          method: 'social',
          region: 'us-east-1',
          expiresAt: Date.now() + 3_600_000,
        },
        id: 'a',
        label: 'A',
      }),
      read: async () => ({ accounts: [], activeId: null }),
      describe: async () => ({ activeId: null, accounts: [] }),
      clear: async () => {},
    },
    post: async request => {
      calls.push(request)
      return { status: 400, headers: {}, body: sseStream([]) }
    },
    sleep: async () => {},
  })
  try {
    for await (const event of client.sendWire({
      conversationState: { currentMessage: { userInputMessage: { content: 'hi', modelId: 'm1', origin: 'AI_EDITOR' } } },
    }, new AbortController().signal)) { void event }
  } catch { /* 400 是预期的 */ }
  check('默认走 CodeWhisperer 的 eventstream 端点',
    calls[0]?.url.endsWith('/generateAssistantResponse'), calls[0]?.url)
}

console.log(`\n${pass} passed, ${fail} failed`)
process.exit(fail === 0 ? 0 : 1)
