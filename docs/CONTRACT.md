# dsh-kiro-proxy 内部契约（Lead 制定，所有模块共用）

本文件是各模块之间的接口契约。修改契约前必须先改本文件并通知 Lead。

## 0. 插件定位

`dsh-kiro-proxy` 是一个 DeepSeek Harness bundle，做两件事：

1. **Host 侧 provider 路由**：注册 `llm` adapter，route 名 `kiro-proxy`，把 Harness 的
   `GenerateOptions` 直接打到 Kiro 的 `generateAssistantResponse`（AWS EventStream）。
2. **反向代理**：在 Host 的 `webServer` 上挂 `/v1/*`（OpenAI 兼容）与 `/kiro-proxy/v1/*`
   （Anthropic 兼容），把外部 CLI（Claude Code / Cline / Codex 等）的请求
   翻译成 Kiro 请求并流式回吐。

凭据、模型发现、用量都读本机 Kiro CLI/IDE 已登录的 `~/.aws/sso/cache`，也支持插件自己
`$DSH_HOME/storages/kiro-proxy/accounts.json` 托管的多账号。

## 1. Kiro 线协议（wire）

请求（POST，JSON）：

```
{
  profileArn?: string,
  additionalModelRequestFields?: object,
  conversationState: {
    chatTriggerType: 'MANUAL',
    conversationId: string,              // uuid v4，可跨轮复用
    currentMessage: { userInputMessage: UserInputMessage },
    history?: HistoryEntry[]             // 严格 user/assistant 交替
  }
}
```

`UserInputMessage = { content, modelId, origin, userInputMessageContext?: { tools?, toolResults? }, images? }`
`AssistantResponseMessage = { content, toolUses?: { toolUseId, name, input }[] }`

`origin` 跟着账号走：个人账号（builder-id / google / github / social / imported / api_key）
是 `AI_EDITOR`，企业账号（`idc` / `external_idp`）是 `KIRO_CLI`——企业账号走的是 Kiro CLI
那一侧的数据面，带 IDE 的归因会被拒。

`modelId` 一般就是目录里的那个 id：账号目录（第 1a 节）给的 `modelId` 已经是数据面
认的那个 id。只有退回全局目录的账号才可能对不上，那时由 `modelIds` 设置提供
友好名到 wire id 的映射（见第 5 节）。

响应：`content-type: application/vnd.amazon.eventstream`，逐帧：

```
[4B totalLen][4B headerLen][4B preludeCRC][headers][payload JSON][4B messageCRC]
header = [1B nameLen][name][1B type=7][2B valueLen][value]
```

payload 事件（`headers[':event-type']` 或 `:exception-type`）：
`assistantResponseEvent{content}`、`reasoningContentEvent{text?,signature?,redactedContent?}`、
`toolUseEvent{toolUseId,name?,input?,stop?}`、`contextUsageEvent{contextUsagePercentage}`、
`meteringEvent{unit,unitPlural,usage}`、`metadataEvent{tokenUsage?,stopReason?,stopDetails?}`。

端点（按 authMethod 选）：

| authMethod | chat 端点 |
| --- | --- |
| `idc` / `external_idp` | `https://codewhisperer.<region>.amazonaws.com/generateAssistantResponse`（带 `x-amz-target: AmazonCodeWhispererStreamingService.GenerateAssistantResponse`） |
| 其他（builder-id / google / github / social / imported / api_key） | `https://q.<region>.amazonaws.com/generateAssistantResponse` |

公共头：`authorization: Bearer <accessToken>`、`content-type: application/json`、
`accept: application/vnd.amazon.eventstream`、`x-amzn-kiro-agent-mode: vibe`、
`x-amzn-codewhisperer-optout: true`、`Amz-Sdk-Request: attempt=1; max=3`、
`Amz-Sdk-Invocation-Id: <uuid>`、`x-amzn-codewhisperer-machine-id: kiro-desktop`；
api_key 追加 `TokenType: API_KEY`，external_idp 追加 `TokenType: EXTERNAL_IDP`，
idc 追加 `TokenType: SSO_OIDC`（Kiro CLI 自己的映射：`IdC -> SSO_OIDC`、
`external_idp -> EXTERNAL_IDP`、`api_key -> API_KEY`、`machine_token -> KIRO_MACHINE_TOKEN`）；
有 profileArn 时追加 `x-amzn-codewhisperer-profile-arn`。

`user-agent` 与 `x-amz-user-agent` 按账号分派（Kiro 按客户端身份授权，不是按归因）：

| authMethod | user-agent |
| --- | --- |
| `idc` / `external_idp` | `KiroCLI/2.34.2 KAS/0.66.62 os/<platform> md/appVersion-2.34.2 app/AmazonQ-For-CLI acp-client/kiro-cli` |
| 其他 | `aws-sdk-js/3.738.0 KiroIDE` |

实测：带 IDE 的 UA 时，企业账号的聊天请求回 `INVALID_MODEL_ID`；带 CLI 的 UA 时，
同一个请求回 `profileArn is required for this request.`——说明企业账号只有走 CLI
那条路才会被当成企业流量。

`ListAvailableProfiles`（POST）的请求体必须是 `{}`：带 `{"maxResults":50}` 会回
`400 REQUEST_BODY_INVALID`，那样 profile ARN 永远解析不出来。

## 1a. 账号目录（模型发现）

模型列表有两个来源，**账号自己的目录优先**：

| 来源 | 端点 | 含义 |
| --- | --- | --- |
| 账号目录（管理面） | `GET https://management.<region>.kiro.dev/ListAvailableModels?origin=<origin>&profileArn=<arn>` | 这个账号**被授予**的模型。`modelId` 就是 wire 上那个 id |
| 全局目录（CodeWhisperer） | `GET https://codewhisperer.<region>.amazonaws.com/ListAvailableModels?origin=AI_EDITOR&maxResults=50[&profileArn=<arn>]` | Kiro 卖的所有模型（Claude 全家都在里面） |

全局目录是"所有模型的并集"，IdC / Enterprise 账号只被授予其中一部分：选到没被授予的
id，数据面回 `400 {"message":"Invalid model. Please select a different model to
continue.","reason":"INVALID_MODEL_ID"}`。所以模型选择器里给出的每个 id 都必须是
账号目录里的那些。

账号目录的契约（实测）：

- `origin` 必填，缺了回 `400 … Value null at 'origin' failed to satisfy constraint`；
  它只影响**展示名**：`KIRO_CLI` 的 `modelName` 是 null（名字退回 id），
  `AI_EDITOR` 给的是 `"GPT 5.6 Sol"` 这样的友好名；两者的模型集合完全一致。
- `profileArn` 对 IdC 账号必填，缺了回 `400 {"message":"Invalid profileArn."}`；
  没有 ARN 的账号（个人 Builder 登录）没有账号目录，用全局目录——对那个账号来说
  全局目录就是对的。
- 区域取自 profile ARN（`arn:aws:codewhisperer:us-east-1:…` -> `us-east-1`），
  不是 token 自己带的区域。
- 头就是常规 Kiro 头：`Authorization: Bearer <token>`、企业账号的 CLI user-agent、
  `x-amzn-codewhisperer-profile-arn`。`TokenType` 收但不要求。
- 响应体与 CodeWhisperer 那份同形（`models[].modelId / modelName / description /
  tokenLimits / supportedInputTypes / additionalModelRequestFieldsSchema / …`，
  另有 `defaultModel`），所以 `parseModelPage()` 原样解析。

**退回规则**：账号目录拿不到时（非 200、抛错、响应没有 `models` 数组、空目录、
没有 profileArn）一律退回全局目录，模型列表不因此失败；两个目录都失败才报错。
目录太宽只是让用户选到用不了的模型，没有目录则是插件直接不可用。

## 1b. Kiro 运行时通道（可选）

`chatTransport: 'kiro-runtime'` 时聊天改走 `https://runtime.<region>.kiro.dev/v1/responses`
（POST，OpenAI Responses API）。实测的请求契约：

```
{
  model, input, profileArn, origin: 'KIRO_CLI', stream: true,
  metadata: { 'kiro:agent_mode': 'vibe', 'kiro:conversation_id': <id>, 'kiro:root_conversation_id': <id> }
}
```

- 缺 `profileArn` -> `profileArn is required for this request.`
- 缺 `stream` -> `Non-streaming responses are not supported for this request. Please set stream=true.`
- 缺 `origin` -> `origin is required unless the request carries the Kiro Reasoning service principal`
- 模型 id 不被接受 -> `Invalid model ID: <id>`，`code: model_not_found`
- 发 `instructions` -> `systemPrompt field is not enabled for this user.`（param `instructions`）
  ——**永远不发**，系统提示词走第一条 user 轮
- 发 `tool_choice` -> `This request contains parameters or input that are not supported.`
  （param `tool_choice`）——**永远不发**
- `reasoning: { effort }` 收，`reasoning: { type: 'adaptive' }` 拒（param `reasoning.type`）；
  插件两边都不发

必填的头（CLI 那一侧的服务按这些头认流量）：

```
authorization: Bearer <accessToken>
user-agent: <CLI user-agent>          # 运行时是 CLI 自己的服务，身份一律是 CLI 的
x-amzn-codewhisperer-optout: true
x-amzn-kiro-origin: KIRO_CLI
x-amzn-kiro-profile: <profileArn>
TokenType: SSO_OIDC                    # IdC；external_idp -> EXTERNAL_IDP，api_key -> API_KEY
```

响应是 `text/event-stream`，OpenAI Responses 事件帧
（`response.output_text.delta`、`response.reasoning_text.delta`、
`response.output_item.added`、`response.function_call_arguments.delta`、
`response.output_item.done`、`response.completed` / `response.failed`），
由 `lib/kiro-runtime.js` 翻译成第 4 节的内部事件词汇。

**运行时这条通道的 `usage` 不是 token 数**：

```
{"kiro:credits":{"value":0.0115,"unit":"credit","unit_plural":"credits"},
 "kiro:context_usage_percentage":0.119}
```

所以这条通道上 token 用量是 0，插件不编数字：usage 事件标成 `estimated: true`
（"没测到"而不是"测到 0"），并把真正测到的 credits 带出去。

**状态：请求契约与 usage 形状是实测的；响应事件名是按 OpenAI Responses 的公开形状
实现的，没有对活服务验证过。** 默认关闭（`chatTransport` 默认 `aws`）。

## 2. 模块边界

```
index.js                Host 插件入口：Config、注册 adapter、挂 web 路由
lib/kiro-error.js       kiroError()/errorCode()/errorText()          [Lead]
lib/kiro-transport.js   post/get + CONNECT 代理 + JSON 帮助函数        [Lead]
lib/kiro-auth.js        凭据读取/刷新/多账号                           [Lead]
lib/kiro-client.js      KiroClient：models()/usage()/stream()/profile() [Lead]
lib/kiro-eventstream.js decodeFrames(stream) -> AsyncIterable<Frame>  [wire]
lib/kiro-runtime.js     Kiro 运行时：请求拼装 + SSE 解码 + 事件归一化    [wire]
lib/kiro-serialize.js   Harness GenerateOptions -> WireRequest        [wire]
lib/kiro-openai.js      OpenAI chat.completions <-> Kiro              [translate]
lib/kiro-anthropic.js   Anthropic messages <-> Kiro                   [translate]
lib/adapter.js          LlmAdapter 子类                               [Lead]
lib/web.js              /v1/* 与 /kiro-proxy/v1/* 路由 + /kiro-proxy/api 状态 API [Lead]
client.js               Web UI：settings.section + conversation.input.right [ui]
```

## 3. wire 模块契约

`lib/kiro-eventstream.js`

```js
/** @typedef {{ headers: Record<string,string>, payload: Uint8Array }} WireFrame */
export async function* decodeFrames(stream /* AsyncIterable<Uint8Array> */, onActivity?: () => void): AsyncGenerator<WireFrame>
```
- 12 字节 prelude，CRC 不校验；`totalLength` 上限 16 MiB，否则 `MALFORMED_RESPONSE`。
- 流在帧中间结束 -> `STREAM_CLOSED`。
- 只用 `kiroError` 抛错。

`lib/kiro-serialize.js`

```js
export function serializeRequest(options, ctx)
// options: GenerateOptions（见下）, ctx: {
//   conversationId: string, modelId: string, profileArn?: string,
//   effort?: { schemaPath: 'output_config'|'reasoning', level: string },
//   maxTokensBounds?: {minimum:number, maximum:number},
//   toolNameMap?: (name:string)=>string,   // 可选，默认恒等
// }
// -> { body: WireRequest, toolNameMap: Map<string,string> }  // wire 名 -> harness 名
export function wireToolUseId(id: string): string
```
规则：
- 没有 system 槽：`options.system` 前置到最早一条 user 内容的开头。
- 最后一条 user 是 `currentMessage`，其余进 `history`；history 必须严格交替，
  缺失一侧用 `'Continue'`（user）/`'understood'`（assistant）补齐；合并连续同角色。
- 工具结果（`role: 'tool'`）挂到承载它的 user 轮 `userInputMessageContext.toolResults`；
  找不到对应 `toolUseId` 的结果降级成文本追加进 content。
- `content` 为字符串：text 块拼接、reasoning 块丢弃、image 块进 `images[]`
  （`{format:'png'|'jpeg'|'gif'|'webp', source:{bytes:<base64>}}`，由 ctx.images 提供
  base64；ctx 里没有就抛 `UNSUPPORTED_CONTENT`）。assistant 轮有 image 也抛。
- 工具名必须匹配 `/^[A-Za-z][A-Za-z0-9_]{0,63}$/`，否则用
  `tool_${sha1(name).slice(0,12)}` 改名并记录到返回的 map 里；tool-call 的 id 走 `wireToolUseId`。
- history 条目数上限 198，超出把最旧的折叠成一条文本 user 轮（前缀 `[earlier conversation]\n`）。
- effort：`ctx.effort` 存在时写 `additionalModelRequestFields[ctx.effort.schemaPath] = { effort: level }`；
  再有 `options.maxTokens` 且 `ctx.maxTokensBounds` 存在时写 `max_tokens`（夹到 min/max 之间）。
- 不使用 `inferenceConfig`、`temperature`、`topP`、`stop`。

`GenerateOptions`（Host 侧）：
`{ provider, model, reasoningEffort?, messages: RequestMessage[], system?, tools?: ToolSchema[], temperature?, maxTokens?, stop?, signal? }`
`messages` 里元素 `{ role: 'user'|'assistant'|'tool'|'system'|'developer', content: ContentBlock[], id?, source?, toolCallId? }`，
`ContentBlock` 是 `{type:'text',text}` / `{type:'reasoning',text}` / `{type:'image',attachment}` /
`{type:'tool-call',id,name,arguments}` / `{type:'file',attachment}`。
`ToolSchema = { name, description, parameters }`。

## 4. translate 模块契约

两边都只需要 **纯函数**，不做 I/O：

`lib/kiro-openai.js`

```js
export function openAiToWire(body, ctx)
// body: OpenAI /v1/chat/completions 请求体
// ctx: { modelId, conversationId, profileArn?, systemPrefix?, effort?: {schemaPath,level},
//        maxTokensBounds?, maxTokensField? }
// -> { body: WireRequest, stream: boolean, model: string, toolNameMap: Map<string,string> }
export function newOpenAiState(init)          // { id, model, created }
export function wireToOpenAiChunk(evt, state)
// -> string | undefined    // 要写进 SSE 的 `data: {...}` 负载（不含 `data: ` 前缀与空行），或 undefined 表示不吐
export function wireToOpenAiResponse(state)
// -> 完整非流式响应对象（须在 wireFinish 之后调用）
export function wireFinish(state, stopReason)  // -> 收尾 chunk 文本
```

`lib/kiro-anthropic.js`

```js
export function anthropicToWire(body, ctx)
export function newAnthropicState(init)           // { id, model }
export function anthropicStart(state)             // -> message_start 负载
export function wireToAnthropicEvent(evt, state)  // -> { event, data } | undefined
export function anthropicStop(state, stopReason)  // -> [{ event, data }]（message_delta + message_stop）
export function anthropicResponse(state)          // -> 非流式 /v1/messages 响应对象
```

实现补充（Lead 已确认）：

- `ctx` 额外接受 `effort: { schemaPath, level }`、`maxTokensBounds`、`maxTokensField`
  —— effort 的字段名与 max_tokens 的字段名都由模型发现决定，写死会撞
  `additionalProperties: false`。
- `evt.kind === 'error'` 不抛错，转成各协议自己的 error 负载/帧：流已经开始，
  抛出去只能掐连接，客户端看到的是没有 finish_reason 的断流。
- `MODEL_CONTEXT_WINDOW_EXCEEDED` 除了映射成 `length` / `max_tokens`，还带一个
  `kiro: { code, message }` 附加字段说明真实原因。
- 已经流出工具块却收到 `END_TURN` 时强制报 `tool_calls` / `tool_use`：报 stop
  客户端不会执行工具，工具循环会静默死掉。
- `temperature` / `topP` / `stop` / `tool_choice` 一律不发（Kiro 无对应字段）。

事件归一化（两个 translate 模块收到的 `evt` 都由 `lib/kiro-client.js` 归一化后给出）：

```js
{ kind: 'text', text }
{ kind: 'reasoning', text }
{ kind: 'tool-start', id, name }
{ kind: 'tool-delta', id, argumentsDelta }
{ kind: 'tool-stop', id }
{ kind: 'usage', inputTokens, outputTokens, cacheReadTokens?, cacheWriteTokens?, contextPercentage?, estimated?, credits?, creditsUnit? }
{ kind: 'stop', stopReason }   // END_TURN | TOOL_USE | MAX_TOKENS | MODEL_CONTEXT_WINDOW_EXCEEDED | CONTENT_FILTERED | PAUSE_TURN
{ kind: 'error', message, code }
```

`usage` 事件里 `estimated: true` 的意思是"这两个 token 数不是服务端报出来的"，
不是"这两个数是估的"——运行时那条通道只报 credits，token 数根本没测到，
这时 `credits` / `creditsUnit` 才是这一轮真正测到的数。

## 5. 状态 API（UI 依赖）

Host 挂在 `webServer`：

- `GET  /kiro-proxy/api/status` -> `{ ok:true, value: Status }`
- `POST /kiro-proxy/api/refresh` -> 强制刷新模型与用量
- `POST /kiro-proxy/api/accounts/use` `{ id }` -> 切换活动账号
- `POST /kiro-proxy/api/accounts/import` `{ json | refreshToken | apiKey, label? }`
- `POST /kiro-proxy/api/accounts/remove` `{ id }`
- `POST /kiro-proxy/api/settings` `{ proxyUrl?, region?, profileArn?, maxContextWindow?, enabledModels?, modelIds?, modelIdProbe?, chatTransport? }`
  - `modelIds`: `{ "<友好模型名>": "<上游真正认的 id>" }`。账号目录（第 1a 节）给的就是
    wire id，这份映射是给退回全局目录的账号准备的覆盖项。
  - `modelIdProbe`: `string[]`。上游回 `INVALID_MODEL_ID` / `model_not_found` 时按顺序
    试这些候选 id，第一个能用的在内存里记住（同一个友好名之后直接用它）。
    全部失败时报的是上游原本的错误，不换错误码、不换措辞。
  - `chatTransport`: `'aws'`（默认，CodeWhisperer eventstream）或 `'kiro-runtime'`（第 1b 节）。
- `POST /kiro-proxy/api/login/start` `{ method, region?, startUrl?, label?, secret?, json? }` -> `{ ok, value: LoginView, status?, accountId? }`
  - `method`: `builder-id` | `idc` | `google` | `github` | `refresh-token` | `api-key` | `external-idp`
  - `builder-id` / `idc` 走 AWS OIDC 设备码（`client/register` → `device_authorization` → 轮询 `token`），
    注册固定带 `issuerUrl = https://identitycenter.amazonaws.com/ssoins-722374e8c3c8e6c6`，
    缺了它授权页会回「无法验证您的登录凭证」。
  - `idc` 必须同时给 `startUrl`（`https://<org>.awsapps.com/start`）与 `region`。
  - `google` / `github` 走 Kiro 自己的设备码（`app.kiro.dev/account/device`）。
  - 三种导入方式不发网络请求，`state` 直接是 `success`，凭据在同一个响应里落库。
  - `LoginView = { id, method, region?, state, userCode?, verificationUri?, verificationUriComplete?, interval?, expiresAt?, error? }`，
    `state` ∈ `pending` | `success` | `error` | `expired` | `denied` | `cancelled`。
- `POST /kiro-proxy/api/login/poll` `{ id }` -> `{ ok, value: LoginView, status? }`
- `POST /kiro-proxy/api/login/cancel` `{ id }` -> `{ ok, value: { id, state } }`
- `POST /kiro-proxy/api/proxy-key` -> 生成/轮换本机反代 API key

`Status = {
  ok: true,
  accounts: [{ id, label, method, region, state: 'ready'|'refresh'|'signin', active }],
  activeAccountId: string | null,
  credentialSource: 'managed'|'kiro-cache'|'none',
  models: [{ id, name, contextWindow?, maxTokens?, inputModalities?, reasoningEfforts?, defaultEffort? }],
  enabledModels: string[],
  usage: { plan, rows: [{ label, used, limit, resetAt? }], fetchedAt } | null,
  proxy: { baseUrl, key: string, openaiBaseUrl, anthropicBaseUrl, enabled },
  config: { proxyUrl, region, profileArn, maxContextWindow, modelIds, modelIdProbe, chatTransport },
  errors: string[],
}`

UI 只依赖 `status`，不直接碰 Host Service。

## 6. 命名与风格

- ESM，纯 JS（无构建步骤），`node --check` 必须通过。
- 中文注释，英文标识符。
- 不新增 npm 依赖；只用 `node:` 内置模块。
- 所有 Host 侧资源注册必须用 `ctx.effect` / `ctx.on` 并返回 disposer。
