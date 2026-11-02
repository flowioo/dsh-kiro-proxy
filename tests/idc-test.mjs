/**
 * 企业账号（IAM Identity Center）的离线验证。
 *
 * 四件事在这里被钉死：
 * 1. `ListAvailableProfiles` 的请求体是 `{}` —— 服务端只认空对象，
 *    `{"maxResults":50}` 会回 400 REQUEST_BODY_INVALID，于是 profile ARN 永远
 *    解析不出来，企业请求就带着「没有 ARN」出门。
 * 2. 企业账号必须报 CLI 的身份（user-agent + `KIRO_CLI` origin），
 *    个人账号仍然报 IDE 的身份（`KiroIDE` + `AI_EDITOR`）。
 * 3. `modelIds` 把友好模型名换成上游真正认的 id。
 * 4. 上游拒绝模型 id 时，`modelIdProbe` 的候选按顺序试，第一个能用的被记住。
 *
 * Run: node tests/idc-test.mjs
 */

import { KiroClient, cliUserAgent, kiroOrigin } from '../lib/kiro-client.js'

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

/** 构造一帧 eventstream 记录。 */
function frame(headers, payload) {
  const parts = Object.entries(headers).map(([name, value]) => {
    const n = Buffer.from(name, 'utf8')
    const v = Buffer.from(value, 'utf8')
    return Buffer.concat([Buffer.from([n.length]), n, Buffer.from([7]), Buffer.from([(v.length >> 8) & 0xff, v.length & 0xff]), v])
  })
  const headerBuf = Buffer.concat(parts)
  const payloadBuf = Buffer.from(JSON.stringify(payload), 'utf8')
  const total = 12 + headerBuf.length + payloadBuf.length + 4
  const prelude = Buffer.alloc(12)
  prelude.writeUInt32BE(total, 0)
  prelude.writeUInt32BE(headerBuf.length, 4)
  return Buffer.concat([prelude, headerBuf, payloadBuf, Buffer.alloc(4)])
}

function streamOf(frames) {
  return {
    async *[Symbol.asyncIterator]() {
      for (const chunk of frames) yield chunk
    },
  }
}

const contentFrames = [
  frame({ ':event-type': 'assistantResponseEvent' }, { content: '你好' }),
  frame({ ':event-type': 'contextUsageEvent' }, { contextUsagePercentage: 3.2 }),
]

const okResponse = () => ({ status: 200, headers: {}, body: streamOf(contentFrames) })

/** 上游拒绝模型 id 的 400。 */
const invalidModel = (id) => ({
  status: 400,
  headers: {},
  body: streamOf([Buffer.from(JSON.stringify({
    message: 'Invalid model. Please select a different model to continue.',
    reason: 'INVALID_MODEL_ID',
    id,
  }), 'utf8')]),
})

const ARN = 'arn:aws:codewhisperer:us-east-1:123456789012:profile/TESTPROFILE'

/**
 * 造一个 client：post 按脚本回，postJson 记录请求体。
 * @param {object} spec - `{ credential, options, script, profiles }`.
 */
function buildClient(spec) {
  const calls = { post: [], postJson: [] }
  const client = new KiroClient({
    options: () => spec.options ?? {},
    store: {
      directory: 'unused',
      active: async () => ({ credential: spec.credential, id: 'a', label: 'A' }),
      read: async () => ({ accounts: [], activeId: null }),
      describe: async () => ({ activeId: null, accounts: [] }),
      clear: async () => {},
    },
    post: async request => {
      calls.post.push(request)
      const next = (spec.script ?? []).shift()
      if (next === undefined) throw new Error('script exhausted')
      if (typeof next === 'function') return next(request)
      return next
    },
    postJson: async (url, body, headers) => {
      calls.postJson.push({ url, body, headers })
      if (spec.profiles === undefined) return { status: 403, body: { message: 'not here' } }
      return { status: 200, body: { profiles: spec.profiles } }
    },
    sleep: async () => {},
  })
  return { client, calls }
}

/**
 * 企业凭据：带 ARN，所以不需要走 profile 发现。
 *
 * 每个用例的 accessToken 必须不同：Kiro 的 token 缓存按
 * (accessToken, refreshToken, region) 索引，同一个 token 会串到别的用例去。
 */
const idcCredential = (token = 'aoa_idc') => ({
  accessToken: token,
  method: 'idc',
  region: 'us-east-1',
  expiresAt: Date.now() + 3_600_000,
  profileArn: ARN,
})

/** 个人（Builder ID / social）凭据。 */
const socialCredential = (token = 'aoa_social') => ({
  accessToken: token,
  method: 'social',
  region: 'us-east-1',
  expiresAt: Date.now() + 3_600_000,
})

async function drain(client, model = 'claude-opus-5.5') {
  const events = []
  for await (const event of client.sendWire({
    conversationState: {
      chatTriggerType: 'MANUAL',
      conversationId: 'c1',
      currentMessage: {
        userInputMessage: { content: 'hi', modelId: model, origin: 'AI_EDITOR' },
      },
    },
  }, new AbortController().signal, { requestedModelId: model })) {
    events.push(event)
  }
  return events
}

// ---------------------------------------------------------------------------
// 1. ListAvailableProfiles 的请求体必须是 {}
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({
    credential: { ...idcCredential('aoa_discover'), profileArn: undefined },
    profiles: [{ arn: ARN, profileName: 'KiroProfile-us-east-1' }],
    script: [okResponse()],
  })
  await drain(client)
  check('profile 发现被触发', calls.postJson.length > 0, String(calls.postJson.length))
  const lookup = calls.postJson[0]
  check('profile 查询打的是 ListAvailableProfiles',
    lookup?.url.endsWith('/ListAvailableProfiles'), lookup?.url)
  check('profile 查询体是空对象',
    JSON.stringify(lookup?.body) === '{}', JSON.stringify(lookup?.body))
  check('profile 查询不带 maxResults',
    lookup?.body?.maxResults === undefined, JSON.stringify(lookup?.body))
  check('ARN 被解析出来并写进聊天请求',
    calls.post[0]?.headers['x-amzn-codewhisperer-profile-arn'] === ARN,
    calls.post[0]?.headers['x-amzn-codewhisperer-profile-arn'])
}

// ---------------------------------------------------------------------------
// 2. 企业账号报 CLI 的身份：user-agent + KIRO_CLI origin
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({ credential: idcCredential('aoa_ua'), script: [okResponse()] })
  await drain(client)
  const headers = calls.post[0]?.headers ?? {}
  check('企业账号用 CLI 的 user-agent',
    headers['user-agent'] === cliUserAgent(), headers['user-agent'])
  check('CLI user-agent 带 AmazonQ-For-CLI',
    String(headers['user-agent']).includes('app/AmazonQ-For-CLI'), headers['user-agent'])
  check('CLI user-agent 带 acp-client 标记',
    String(headers['user-agent']).includes('acp-client/kiro-cli'), headers['user-agent'])
  check('企业账号不再报 KiroIDE',
    !String(headers['user-agent']).includes('KiroIDE'), headers['user-agent'])
  check('x-amz-user-agent 同样是 CLI 身份',
    headers['x-amz-user-agent'] === headers['user-agent'], headers['x-amz-user-agent'])
  const sent = JSON.parse(calls.post[0]?.body ?? '{}')
  check('企业账号的 origin 是 KIRO_CLI',
    sent?.conversationState?.currentMessage?.userInputMessage?.origin === 'KIRO_CLI',
    sent?.conversationState?.currentMessage?.userInputMessage?.origin)
  check('企业账号带上 profile ARN 头',
    headers['x-amzn-codewhisperer-profile-arn'] === ARN, headers['x-amzn-codewhisperer-profile-arn'])
  check('企业账号的请求体带上 profile ARN',
    sent?.profileArn === ARN, sent?.profileArn)
  check('企业账号带 x-amz-target',
    headers['x-amz-target'] === 'AmazonCodeWhispererStreamingService.GenerateAssistantResponse',
    headers['x-amz-target'])
}

// ---------------------------------------------------------------------------
// 3. 个人账号不变：KiroIDE + AI_EDITOR
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({ credential: socialCredential('aoa_social'), script: [okResponse()] })
  await drain(client)
  const headers = calls.post[0]?.headers ?? {}
  check('个人账号仍用 IDE 的 user-agent',
    headers['user-agent'] === 'aws-sdk-js/3.738.0 KiroIDE', headers['user-agent'])
  const sent = JSON.parse(calls.post[0]?.body ?? '{}')
  check('个人账号的 origin 仍是 AI_EDITOR',
    sent?.conversationState?.currentMessage?.userInputMessage?.origin === 'AI_EDITOR',
    sent?.conversationState?.currentMessage?.userInputMessage?.origin)
  check('个人账号不带 x-amz-target', headers['x-amz-target'] === undefined, headers['x-amz-target'])
  check('origin 按账号分派',
    kiroOrigin('idc') === 'KIRO_CLI' && kiroOrigin('external_idp') === 'KIRO_CLI'
    && kiroOrigin('social') === 'AI_EDITOR' && kiroOrigin('builder-id') === 'AI_EDITOR',
    `${kiroOrigin('idc')} ${kiroOrigin('social')}`)
}

// ---------------------------------------------------------------------------
// 4. modelIds：友好名 -> 上游真正认的 id
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({
    credential: idcCredential('aoa_map'),
    options: { modelIds: { 'claude-opus-5.5': 'CLAUDE_OPUS_5_5_V1_0' } },
    script: [okResponse()],
  })
  const context = await client.wireContext('claude-opus-5.5')
  check('wireContext 给出映射后的 id', context.modelId === 'CLAUDE_OPUS_5_5_V1_0', context.modelId)
  check('wireContext 保留友好名', context.requestedModelId === 'claude-opus-5.5', context.requestedModelId)
  const unknown = await client.wireContext('glm-5')
  check('未映射的 id 原样通过', unknown.modelId === 'glm-5', unknown.modelId)

  // 真正发出去的那份报文也必须用映射后的 id：目录里的友好名只是展示用的。
  const { client: streaming, calls: streamCalls } = buildClient({
    credential: idcCredential('aoa_map_stream'),
    options: { modelIds: { 'claude-opus-5.5': 'CLAUDE_OPUS_5_5_V1_0' } },
    script: [okResponse()],
  })
  streaming.catalogCache.set(`us-east-1\u0000${ARN}\u0000`, {
    expiresAt: Date.now() + 60_000,
    models: [{ id: 'claude-opus-5.5', name: 'Opus 5.5', supportsEffort: false }],
  })
  const events = []
  for await (const event of streaming.stream({
    model: 'claude-opus-5.5',
    messages: [{ role: 'user', content: [{ type: 'text', text: 'hi' }] }],
  })) events.push(event)
  const sentBody = JSON.parse(streamCalls.post[0]?.body ?? '{}')
  check('发出去的报文用映射后的 id',
    sentBody?.conversationState?.currentMessage?.userInputMessage?.modelId === 'CLAUDE_OPUS_5_5_V1_0',
    sentBody?.conversationState?.currentMessage?.userInputMessage?.modelId)
  check('映射后的 id 也能拿到正文', events.some(event => event.kind === 'text'), JSON.stringify(events))
}

// ---------------------------------------------------------------------------
// 5. 探测：上游拒绝模型 id 时按候选顺序试，并记住第一个能用的
// ---------------------------------------------------------------------------
{
  const sentIds = []
  const { client, calls } = buildClient({
    credential: idcCredential('aoa_probe'),
    options: { modelIdProbe: ['CAND_A', 'CAND_B'] },
    script: [
      request => { sentIds.push(JSON.parse(request.body).conversationState.currentMessage.userInputMessage.modelId); return invalidModel('claude-opus-5.5') },
      request => { sentIds.push(JSON.parse(request.body).conversationState.currentMessage.userInputMessage.modelId); return invalidModel('CAND_A') },
      request => { sentIds.push(JSON.parse(request.body).conversationState.currentMessage.userInputMessage.modelId); return okResponse() },
      request => { sentIds.push(JSON.parse(request.body).conversationState.currentMessage.userInputMessage.modelId); return okResponse() },
    ],
  })
  const events = await drain(client)
  check('探测按候选顺序试', sentIds.join(',') === 'claude-opus-5.5,CAND_A,CAND_B', sentIds.join(','))
  check('探测成功后拿到正文', events.some(event => event.kind === 'text'), JSON.stringify(events))
  check('探测成功的 id 被记住', client.probedModelIds.get('claude-opus-5.5') === 'CAND_B',
    JSON.stringify([...client.probedModelIds]))

  // 第二次请求直接用记住的 id，不再逐个试。
  sentIds.length = 0
  const before = calls.post.length
  await drain(client)
  check('后续请求直接用记住的 id',
    sentIds.join(',') === 'CAND_B' && calls.post.length === before + 1, sentIds.join(','))
}

// ---------------------------------------------------------------------------
// 6. 探测全部失败时，报的是上游原本的错误
// ---------------------------------------------------------------------------
{
  const { client } = buildClient({
    credential: idcCredential('aoa_probe_fail'),
    options: { modelIdProbe: ['CAND_A'] },
    script: [invalidModel('claude-opus-5.5'), invalidModel('CAND_A')],
  })
  let error
  try {
    await drain(client)
  } catch (cause) {
    error = cause
  }
  check('探测失败时报错', error !== undefined, error?.message)
  check('报的是上游原文', String(error?.message).includes('Invalid model'), error?.message)
  check('错误码是 INVALID_REQUEST', error?.code === 'INVALID_REQUEST', error?.code)
  check('没有记住失败的候选', client.probedModelIds.size === 0, JSON.stringify([...client.probedModelIds]))
}

// ---------------------------------------------------------------------------
// 7. 没有配置候选时，模型 id 被拒就照旧报错（不乱试）
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({
    credential: idcCredential('aoa_no_probe'),
    script: [invalidModel('claude-opus-5.5')],
  })
  let error
  try {
    await drain(client)
  } catch (cause) {
    error = cause
  }
  check('无候选时只发一次', calls.post.length === 1, calls.post.length)
  check('无候选时报原始错误', String(error?.message).includes('Invalid model'), error?.message)
}

console.log(`\n${pass} passed, ${fail} failed`)
process.exit(fail === 0 ? 0 : 1)
