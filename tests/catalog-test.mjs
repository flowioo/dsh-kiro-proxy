/**
 * 账号目录（模型发现）的离线验证。
 *
 * 这里钉死的是"这个账号能用哪些模型"这件事的两个来源：
 *
 * 1. **账号自己的目录优先。** IdC / Enterprise 账号可用哪些模型由 Kiro 管理面
 *    （`management.<region>.kiro.dev/ListAvailableModels`）说了算；CodeWhisperer
 *    那份目录是 Kiro 的**全量**目录（Claude 全家都在里面），而这个账号根本没被
 *    授予那些模型，选上去就是一句 `INVALID_MODEL_ID`。管理面目录里的 `modelId`
 *    同时就是 wire 上那个 id，不需要再做映射。
 * 2. **退回不是失败。** 没有 profile ARN 的账号（个人 Builder 登录）没有账号
 *    目录；管理面拒绝或出错的账号也拿不到账号目录。这两种情况都必须退回
 *    CodeWhisperer 那份目录，而不是让模型列表整个挂掉。
 *
 * 另外把 `TokenType` 的映射钉死：`idc` 是 AWS SSO 的 token，头是 `SSO_OIDC`。
 *
 * Run: node tests/catalog-test.mjs
 */

import { KiroClient, cliUserAgent, kiroHeaders, kiroManagementEndpoint } from '../lib/kiro-client.js'

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

/** 管理面目录：账号真正被授予的模型（实测这份目录只有 8 个）。 */
const ACCOUNT_PAGE = {
  status: 200,
  body: {
    defaultModel: 'gpt-5.6-sol',
    models: [
      {
        modelId: 'gpt-5.6-sol',
        modelName: 'GPT 5.6 Sol',
        description: 'account default',
        tokenLimits: { maxInputTokens: 200_000, maxOutputTokens: 32_000 },
        supportedInputTypes: ['text'],
      },
      {
        modelId: 'glm-5',
        modelName: 'GLM 5',
        tokenLimits: { maxInputTokens: 200_000, maxOutputTokens: 32_000 },
        supportedInputTypes: ['text', 'image'],
      },
    ],
  },
}

/** CodeWhisperer 全量目录：Kiro 卖的所有模型，账号只被授予了其中一部分。 */
const GLOBAL_PAGE = {
  status: 200,
  body: {
    models: [
      { modelId: 'claude-opus-5.5', modelName: 'Claude Opus 5.5', tokenLimits: { maxInputTokens: 1_000_000 } },
      { modelId: 'gpt-5.6-sol', modelName: 'GPT 5.6 Sol', tokenLimits: { maxInputTokens: 1_000_000 } },
      { modelId: 'glm-5', modelName: 'GLM 5', tokenLimits: { maxInputTokens: 1_000_000 } },
    ],
  },
}

/**
 * 造一个 client：getJson 按 URL 分流到管理面 / CodeWhisperer 两份目录。
 *
 * postJson 也必须钉住：没有 ARN 的账号要走 profile 发现，而发现走的是
 * postJson（ListAvailableProfiles）。不钉它，这个用例会真的发一次网络请求
 * —— 断言"个人账号不打扰管理面"就只因为外网不通才成立，那是环境的性质，
 * 不是代码的性质。
 * @param {object} spec - `{ credential, options, management, global, profiles }`。
 */
function buildClient(spec) {
  const calls = { getJson: [], postJson: [] }
  const client = new KiroClient({
    options: () => spec.options ?? {},
    store: {
      directory: 'unused',
      active: async () => ({ credential: spec.credential, id: 'a', label: 'A' }),
      read: async () => ({ accounts: [], activeId: null }),
      describe: async () => ({ activeId: null, accounts: [] }),
      clear: async () => {},
    },
    getJson: async (url, headers, proxyUrl) => {
      calls.getJson.push({ url, headers, proxyUrl })
      const management = url.includes('management.') || url.includes('/kiro.dev/')
      const page = management ? spec.management : spec.global
      if (page === undefined) return { status: 404, body: { message: 'not scripted' } }
      if (typeof page === 'function') return page(url, headers)
      return page
    },
    postJson: async (url, body, headers) => {
      calls.postJson.push({ url, body, headers })
      if (spec.profiles === undefined) return { status: 403, body: { message: 'not scripted' } }
      return { status: 200, body: { profiles: spec.profiles } }
    },
    sleep: async () => {},
  })
  return { client, calls }
}

/**
 * 企业凭据：带 ARN，所以不需要走 profile 发现。
 * @param {string} token - accessToken；每个用例必须不同（token 缓存按 token 索引）。
 */
const idcCredential = (token = 'aoa_catalog') => ({
  accessToken: token,
  method: 'idc',
  region: 'eu-central-1',
  expiresAt: Date.now() + 3_600_000,
  profileArn: ARN,
})

/** 个人（Builder ID / social）凭据：没有 ARN。 */
const socialCredential = (token = 'aoa_catalog_social') => ({
  accessToken: token,
  method: 'social',
  region: 'us-east-1',
  expiresAt: Date.now() + 3_600_000,
})

/**
 * 个人凭据，但 ARN 已经发现了。
 *
 * 插件对任何非 api_key 凭据都会去发现 ARN，个人账号也拿得到一个。所以
 * "有没有 ARN" 分不出企业账号和个人账号，真正的分界是认证方式。
 * @param {string} token - accessToken；每个用例必须不同。
 */
const socialCredentialWithArn = (token = 'aoa_catalog_social_arn') => ({
  accessToken: token,
  method: 'social',
  region: 'us-east-1',
  expiresAt: Date.now() + 3_600_000,
  profileArn: ARN,
})

// ---------------------------------------------------------------------------
// 1. 管理面目录优先：账号只看到自己被授予的模型
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({
    credential: idcCredential('aoa_mgmt'),
    management: ACCOUNT_PAGE,
    global: GLOBAL_PAGE,
  })
  const models = await client.models()
  const ids = models.map(model => model.id)
  check('目录来自管理面而不是全量目录',
    ids.join(',') === 'gpt-5.6-sol,glm-5', ids.join(','))
  check('全量目录里的 Claude 没有混进来',
    !ids.includes('claude-opus-5.5'), ids.join(','))
  check('只打了一次目录请求', calls.getJson.length === 1, String(calls.getJson.length))
  check('打的是管理面端点',
    calls.getJson[0]?.url.startsWith('https://management.us-east-1.kiro.dev/ListAvailableModels'),
    calls.getJson[0]?.url)
  check('管理面目录里的 modelId 就是目录 id（wire id，无需映射）',
    models[0]?.id === 'gpt-5.6-sol', models[0]?.id)
  check('展示名取自目录', models[0]?.name === 'GPT 5.6 Sol', models[0]?.name)
  check('描述与上下文窗口照常解析',
    models[0]?.description === 'account default' && models[0]?.contextWindow === 200_000,
    JSON.stringify(models[0]))
  check('输入模态照常解析', models[1]?.inputModalities?.join(',') === 'text,image',
    JSON.stringify(models[1]?.inputModalities))
}

// ---------------------------------------------------------------------------
// 2. 管理面请求的形状：query、头、区域
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({
    credential: idcCredential('aoa_shape'),
    management: ACCOUNT_PAGE,
    global: GLOBAL_PAGE,
  })
  await client.models()
  const call = calls.getJson[0]
  const url = new URL(call.url)
  check('origin 是必填项', url.searchParams.get('origin') === 'AI_EDITOR', url.search)
  check('profileArn 是必填项', url.searchParams.get('profileArn') === ARN, url.search)
  check('区域取自 profile ARN（不是 token 的区域）',
    url.host === 'management.us-east-1.kiro.dev', url.host)
  check('管理面端点按区域拼',
    kiroManagementEndpoint('us-east-1') === 'https://management.us-east-1.kiro.dev/ListAvailableModels',
    kiroManagementEndpoint('us-east-1'))
  const headers = call.headers ?? {}
  check('带 bearer token', headers.authorization === 'Bearer aoa_shape', headers.authorization)
  check('带 profile ARN 头', headers['x-amzn-codewhisperer-profile-arn'] === ARN,
    headers['x-amzn-codewhisperer-profile-arn'])
  check('企业账号用 CLI 的 user-agent', headers['user-agent'] === cliUserAgent(), headers['user-agent'])
  check('带 optout 头', headers['x-amzn-codewhisperer-optout'] === 'true',
    headers['x-amzn-codewhisperer-optout'])
}

// ---------------------------------------------------------------------------
// 2b. 管理面目录分页：授予的模型超过一页时不能被静默截断
// ---------------------------------------------------------------------------
{
  const pages = [
    {
      status: 200,
      body: {
        models: [{ modelId: 'gpt-5.6-sol', modelName: 'GPT 5.6 Sol' }],
        nextToken: 'MQ==',
      },
    },
    {
      status: 200,
      body: {
        models: [{ modelId: 'glm-5', modelName: 'GLM 5' }],
      },
    },
  ]
  const { client, calls } = buildClient({
    credential: idcCredential('aoa_paged'),
    management: () => pages.shift(),
    global: GLOBAL_PAGE,
  })
  const models = await client.models()
  check('管理面目录翻页后拿到全部模型',
    models.map(model => model.id).join(',') === 'gpt-5.6-sol,glm-5',
    models.map(model => model.id).join(','))
  check('管理面目录翻页打了两次请求', calls.getJson.length === 2, String(calls.getJson.length))
  check('第二页带上了 nextToken',
    new URL(calls.getJson[1]?.url ?? 'https://x/').searchParams.get('nextToken') === 'MQ==',
    calls.getJson[1]?.url)
  check('管理面目录请求带 maxResults',
    new URL(calls.getJson[0]?.url ?? 'https://x/').searchParams.get('maxResults') === '50',
    calls.getJson[0]?.url)
}

// ---------------------------------------------------------------------------
// 3. 没有 profile ARN 的账号没有账号目录：用全量目录
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({
    credential: socialCredential('aoa_no_arn'),
    management: ACCOUNT_PAGE,
    global: GLOBAL_PAGE,
  })
  const models = await client.models()
  check('个人账号拿到的是 CodeWhisperer 目录',
    models.map(model => model.id).join(',') === 'claude-opus-5.5,gpt-5.6-sol,glm-5',
    models.map(model => model.id).join(','))
  check('个人账号不打扰管理面', calls.getJson.length === 1, String(calls.getJson.length))
  check('个人账号打的是 CodeWhisperer 目录端点',
    calls.getJson[0]?.url.startsWith('https://q.us-east-1.amazonaws.com/ListAvailableModels'),
    calls.getJson[0]?.url)
}

// ---------------------------------------------------------------------------
// 3b. 个人账号即使带着 ARN 也不许被管理面目录接管
//
// 插件对任何非 api_key 凭据都会去发现 ARN，个人账号照样拿得到一个。所以
// "有没有 ARN" 分不出企业账号和个人账号——按 ARN 判断会让个人账号的模型
// 列表被管理面那份悄悄换掉。分界必须是认证方式。
// ---------------------------------------------------------------------------
{
  const { client, calls } = buildClient({
    credential: socialCredentialWithArn('aoa_social_with_arn'),
    management: ACCOUNT_PAGE,
    global: GLOBAL_PAGE,
  })
  const models = await client.models()
  check('带 ARN 的个人账号仍拿到 CodeWhisperer 目录',
    models.map(model => model.id).join(',') === 'claude-opus-5.5,gpt-5.6-sol,glm-5',
    models.map(model => model.id).join(','))
  check('带 ARN 的个人账号不被打扰管理面', calls.getJson.length === 1, String(calls.getJson.length))
  check('带 ARN 的个人账号打的是 CodeWhisperer 目录端点',
    calls.getJson[0]?.url.startsWith('https://q.us-east-1.amazonaws.com/ListAvailableModels'),
    calls.getJson[0]?.url)
  check('带 ARN 的个人账号一次管理面请求都没有',
    calls.getJson.every(call => !call.url.includes('management.')),
    calls.getJson.map(call => call.url).join(' | '))
}

// ---------------------------------------------------------------------------
// 4. 管理面失败不拖垮模型列表：退回全量目录
// ---------------------------------------------------------------------------
{
  const failures = [
    ['非 200', { status: 400, body: { message: 'Invalid profileArn.' } }],
    ['抛错', () => { throw new Error('management plane down') }],
    ['响应没有 models 数组', { status: 200, body: { nope: true } }],
    ['空目录', { status: 200, body: { models: [] } }],
  ]
  for (const [label, management] of failures) {
    const { client, calls } = buildClient({
      credential: idcCredential(`aoa_fail_${label}`),
      management,
      global: GLOBAL_PAGE,
    })
    let models
    let error
    try {
      models = await client.models()
    } catch (cause) {
      error = cause
    }
    check(`管理面${label}时模型列表不报错`, error === undefined, error?.message)
    check(`管理面${label}时退回全量目录`,
      models?.map(model => model.id).join(',') === 'claude-opus-5.5,gpt-5.6-sol,glm-5',
      models?.map(model => model.id).join(','))
    check(`管理面${label}时两个目录都问过`, calls.getJson.length === 2, String(calls.getJson.length))
  }
}

// ---------------------------------------------------------------------------
// 5. 全量目录也失败时，模型列表照旧报错（不是静默的空列表）
// ---------------------------------------------------------------------------
{
  const { client } = buildClient({
    credential: idcCredential('aoa_both_fail'),
    management: { status: 500, body: { message: 'boom' } },
    global: { status: 500, body: { message: 'boom' } },
  })
  let error
  try {
    await client.models()
  } catch (cause) {
    error = cause
  }
  check('两个目录都失败时报错', error !== undefined, error?.message)
  check('报的是 HTTP 状态', error?.failure?.status === 500 && error?.code === 'SERVER',
    `${error?.failure?.status} ${error?.code}`)
}

// ---------------------------------------------------------------------------
// 6. TokenType 映射
// ---------------------------------------------------------------------------
{
  const tokenFor = method => ({ accessToken: 'aoa', method })
  check('idc -> SSO_OIDC', kiroHeaders(tokenFor('idc')).TokenType === 'SSO_OIDC',
    kiroHeaders(tokenFor('idc')).TokenType)
  check('external_idp -> EXTERNAL_IDP', kiroHeaders(tokenFor('external_idp')).TokenType === 'EXTERNAL_IDP',
    kiroHeaders(tokenFor('external_idp')).TokenType)
  check('api_key -> API_KEY', kiroHeaders(tokenFor('api_key')).TokenType === 'API_KEY',
    kiroHeaders(tokenFor('api_key')).TokenType)
  check('个人账号不带 TokenType', kiroHeaders(tokenFor('social')).TokenType === undefined,
    kiroHeaders(tokenFor('social')).TokenType)
}

console.log(`\n${pass} passed, ${fail} failed`)
process.exit(fail === 0 ? 0 : 1)
