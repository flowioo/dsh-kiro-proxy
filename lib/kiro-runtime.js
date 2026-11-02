/**
 * The Kiro runtime transport: `https://runtime.<region>.kiro.dev/v1/responses`.
 *
 * This is the newer Kiro runtime, which speaks the OpenAI Responses API instead
 * of CodeWhisperer's eventstream. It is opt-in (`chatTransport: 'kiro-runtime'`)
 * because the legacy CodeWhisperer path is what the plugin has been measured
 * against; the runtime is here for accounts the legacy path refuses.
 *
 * The request contract was measured against the live service:
 *
 * - `profileArn` is required — a request without one is answered
 *   `profileArn is required for this request.`
 * - `stream: true` is required — `Non-streaming responses are not supported
 *   for this request. Please set stream=true.`
 * - `origin` is required — `origin is required unless the request carries the
 *   Kiro Reasoning service principal`
 * - the model id is validated the same way the legacy path validates it: an id
 *   the account does not know is answered `Invalid model ID: <id>` with
 *   `code: model_not_found`.
 * - `instructions` is refused — `systemPrompt field is not enabled for this
 *   user.` (param `instructions`)
 * - `tool_choice` is refused — `This request contains parameters or input that
 *   are not supported.` (param `tool_choice`)
 *
 * The reply's `usage` is not a token count either: it carries credits
 * (`kiro:credits`) and a context percentage. Token usage on this path is
 * therefore unknown, and the plugin says so instead of reporting zeros as if
 * they had been measured.
 *
 * The reply is `text/event-stream` in the OpenAI Responses event framing
 * (`response.output_text.delta`, `response.function_call_arguments.delta`,
 * `response.completed`, …). This module translates those frames into the same
 * normalized event vocabulary the eventstream path produces, so everything above
 * it — the Harness adapter, both proxy surfaces — is transport-agnostic.
 *
 * @module dsh-kiro-proxy/kiro-runtime
 */

/** The runtime endpoint for one region. */
export function kiroRuntimeEndpoint(region) {
  return `https://runtime.${region}.kiro.dev/v1/responses`
}

/** Responses API items the runtime understands. */
const INPUT_TEXT = 'input_text'
const OUTPUT_TEXT = 'output_text'
const INPUT_IMAGE = 'input_image'

function record(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value) ? value : undefined
}

function text(value) {
  if (typeof value !== 'string') return undefined
  const trimmed = value.trim()
  return trimmed.length > 0 ? trimmed : undefined
}

/**
 * One Kiro image block as a Responses API image part.
 * @param {object} image - `{ format, source: { bytes } }`.
 * @returns {object} the `input_image` part.
 */
function imagePart(image) {
  const format = text(image?.format) ?? 'png'
  const bytes = typeof image?.source?.bytes === 'string' ? image.source.bytes : ''
  return { type: INPUT_IMAGE, image_url: `data:image/${format};base64,${bytes}` }
}

/**
 * One Kiro user turn as a Responses API item.
 * @param {object} message - the `userInputMessage`.
 * @returns {object[]} the items for this turn.
 */
function userItems(message) {
  const content = [{ type: INPUT_TEXT, text: typeof message?.content === 'string' ? message.content : '' }]
  for (const image of message?.images ?? []) content.push(imagePart(image))
  const items = [{ role: 'user', content }]
  // 工具结果是独立条目：Responses API 用 call_id 把结果对回那次调用。
  for (const result of message?.userInputMessageContext?.toolResults ?? []) {
    const output = Array.isArray(result?.content)
      ? result.content.map(block => (typeof block?.text === 'string' ? block.text : '')).join('\n')
      : ''
    items.push({ type: 'function_call_output', call_id: result?.toolUseId, output })
  }
  return items
}

/**
 * One Kiro assistant turn as Responses API items.
 * @param {object} message - the `assistantResponseMessage`.
 * @returns {object[]} the items for this turn.
 */
function assistantItems(message) {
  const items = [{
    role: 'assistant',
    content: [{ type: OUTPUT_TEXT, text: typeof message?.content === 'string' ? message.content : '' }],
  }]
  for (const use of message?.toolUses ?? []) {
    items.push({
      type: 'function_call',
      call_id: use?.toolUseId,
      name: use?.name,
      arguments: JSON.stringify(use?.input ?? {}),
    })
  }
  return items
}

/**
 * Translate a Kiro wire body into a Responses API request.
 *
 * The Kiro body is the plugin's own wire shape (`conversationState`), so the
 * translation happens here instead of in three places: the Harness adapter and
 * both proxy surfaces all speak Kiro's vocabulary first.
 *
 * Two Responses API fields are deliberately absent, because the runtime
 * refuses them outright:
 *
 * - `instructions` -> `400 systemPrompt field is not enabled for this user.`
 *   (param `instructions`). The system prompt therefore travels inside the
 *   first user turn, exactly as it does on the eventstream path.
 * - `tool_choice` -> `400 This request contains parameters or input that are
 *   not supported.` (param `tool_choice`). The tool choice is dropped; Kiro
 *   has no equivalent on either transport.
 *
 * `reasoning` is not sent either: `reasoning: { effort }` is accepted but
 * `reasoning: { type: 'adaptive' }` is refused, and the effort vocabulary the
 * catalog advertises is not the one the runtime uses, so guessing would turn a
 * working request into a 400.
 * @param {object} body - the `generateAssistantResponse` body.
 * @param {{ profileArn?: string, origin?: string }} options - the account facts.
 * @returns {{ model: string, input: object[], origin: string, stream: true,
 *   metadata: object, profileArn?: string, tools?: object[] }} the runtime
 *   request body.
 */
export function toRuntimeRequest(body, options = {}) {
  const state = record(body?.conversationState)
  const current = record(state?.currentMessage?.userInputMessage)
  const model = text(current?.modelId) ?? text(body?.model) ?? 'auto'
  const input = []
  for (const entry of state?.history ?? []) {
    if (entry?.userInputMessage !== undefined) input.push(...userItems(entry.userInputMessage))
    else if (entry?.assistantResponseMessage !== undefined) input.push(...assistantItems(entry.assistantResponseMessage))
  }
  if (current !== undefined) input.push(...userItems(current))
  const conversationId = text(state?.conversationId)
  const request = {
    model,
    input,
    origin: options.origin ?? 'KIRO_CLI',
    // 非流式被服务端明确拒绝，所以这个字段不是可选项。
    stream: true,
    // 元数据是运行时自己的约定：agent 模式 + 会话 id。
    // 插件没有子会话（没有 root/child 之分），所以 root 就是这一轮自己。
    metadata: {
      'kiro:agent_mode': 'vibe',
      ...conversationId === undefined
        ? {}
        : { 'kiro:conversation_id': conversationId, 'kiro:root_conversation_id': conversationId },
    },
  }
  const profileArn = text(options.profileArn) ?? text(body?.profileArn)
  if (profileArn !== undefined) request.profileArn = profileArn
  const specs = current?.userInputMessageContext?.tools ?? []
  if (specs.length > 0) {
    request.tools = specs.map(spec => ({
      type: 'function',
      name: spec?.toolSpecification?.name,
      description: spec?.toolSpecification?.description ?? '',
      parameters: spec?.toolSpecification?.inputSchema?.json ?? { type: 'object', properties: {} },
    }))
  }
  return request
}

/**
 * Parse one SSE block into its event name and payload.
 * @param {string} raw - the block, without the trailing blank line.
 * @returns {{ event?: string, data: unknown, raw: string } | undefined} the frame.
 */
function parseSseBlock(raw) {
  let event
  const dataLines = []
  for (const line of raw.split('\n')) {
    if (line.length === 0 || line.startsWith(':')) continue
    const colon = line.indexOf(':')
    const field = colon === -1 ? line : line.slice(0, colon)
    let value = colon === -1 ? '' : line.slice(colon + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    if (field === 'event') event = value
    else if (field === 'data') dataLines.push(value)
  }
  if (dataLines.length === 0) return undefined
  const body = dataLines.join('\n')
  if (body === '[DONE]') return { event, data: undefined, raw: body }
  let data
  try {
    data = JSON.parse(body)
  } catch {
    data = undefined
  }
  return { event, data, raw: body }
}

/**
 * Decode an SSE byte stream into frames.
 *
 * The framing is the standard one: `event:` / `data:` lines, blocks separated
 * by a blank line. Line endings may be CRLF, and a block may be split across
 * reads, so the buffer is only cut on a real separator — and the separator
 * itself has to accept both `\n\n` and `\r\n\r\n`.
 * @param {AsyncIterable<Uint8Array>} stream - the response body.
 * @returns {AsyncGenerator<{ event?: string, data: unknown, raw: string }>} the frames.
 */
export async function* decodeSse(stream) {
  const decoder = new TextDecoder()
  const separator = /\r?\n\r?\n/u
  let buffer = ''
  for await (const chunk of stream) {
    buffer += decoder.decode(chunk, { stream: true })
    for (;;) {
      const match = separator.exec(buffer)
      if (match === null) break
      const raw = buffer.slice(0, match.index)
      buffer = buffer.slice(match.index + match[0].length)
      const frame = parseSseBlock(raw.replace(/\r\n/gu, '\n'))
      if (frame !== undefined) yield frame
    }
  }
  buffer += decoder.decode()
  const tail = parseSseBlock(buffer.replace(/\r\n/gu, '\n'))
  if (tail !== undefined) yield tail
}

/**
 * Translate one Responses API frame into the plugin's normalized vocabulary.
 *
 * The event name is the authority; the payload's own `type` field is the
 * fallback, because some servers omit the `event:` line and rely on it.
 * @param {{ event?: string, data: unknown }} frame - one SSE frame.
 * @returns {object | undefined} the normalized event.
 */
export function normalizeRuntimeEvent(frame) {
  const data = record(frame?.data)
  const type = text(frame?.event) ?? text(data?.type)
  if (type === undefined) return undefined
  if (type === 'response.output_text.delta') {
    const chunk = text(data?.delta)
    return chunk === undefined ? undefined : { kind: 'text', text: chunk }
  }
  if (type === 'response.reasoning_summary_text.delta' || type === 'response.reasoning_text.delta') {
    const chunk = text(data?.delta)
    return chunk === undefined ? undefined : { kind: 'reasoning', text: chunk }
  }
  if (type === 'response.output_item.added' || type === 'response.output_item.done') {
    const item = record(data?.item)
    if (item?.type !== 'function_call') return undefined
    const id = text(item?.call_id) ?? text(item?.id)
    if (id === undefined) return undefined
    if (type === 'response.output_item.added') {
      return { kind: 'tool-start', id, name: text(item?.name) ?? '' }
    }
    return { kind: 'tool-stop', id }
  }
  if (type === 'response.function_call_arguments.delta') {
    const id = text(data?.call_id) ?? text(data?.item_id)
    if (id === undefined) return undefined
    const chunk = typeof data?.delta === 'string' ? data.delta : ''
    return { kind: 'tool-delta', id, argumentsDelta: chunk }
  }
  return undefined
}

/**
 * Read the usage and stop reason out of a terminal Responses API frame.
 *
 * The runtime's `usage` is not a token count. Measured shape:
 * `{"kiro:credits":{"value":0.0115,"unit":"credit","unit_plural":"credits"},
 * "kiro:context_usage_percentage":0.119}` — it prices the turn in credits and
 * says nothing about tokens. So a frame without token fields has *not*
 * measured zero tokens, it has not measured tokens at all: `tokensReported`
 * says which of the two happened, and the credits are carried through because
 * they are the one number this surface really measured.
 * @param {{ event?: string, data: unknown }} frame - the terminal frame.
 * @returns {{ usage?: object, stopReason?: string, error?: { message: string, code: string } }}
 *   what the frame says about the finished turn.
 */
export function runtimeOutcome(frame) {
  const data = record(frame?.data)
  const type = text(frame?.event) ?? text(data?.type)
  if (type === 'error' || type === 'response.failed' || type === 'response.incomplete') {
    const response = record(data?.response)
    const error = record(data?.error) ?? record(response?.error)
    const message = text(error?.message) ?? text(response?.status_details?.error?.message)
    return {
      error: {
        message: message ?? `Kiro runtime failed (${type})`,
        code: text(error?.code) ?? 'SERVER',
      },
    }
  }
  if (type !== 'response.completed') return {}
  const response = record(data?.response)
  const usage = record(response?.usage)
  const credits = record(usage?.['kiro:credits'])
  const creditValue = Number(credits?.value)
  const contextPercentage = Number(usage?.['kiro:context_usage_percentage'])
  const tokens = {
    inputTokens: Number(usage?.input_tokens ?? 0),
    outputTokens: Number(usage?.output_tokens ?? 0),
    cacheReadTokens: Number(usage?.input_tokens_details?.cached_tokens ?? 0),
    // 只有服务端真的给了 token 字段，才算"测到了 token 数"。
    tokensReported: usage?.input_tokens !== undefined || usage?.output_tokens !== undefined,
    ...Number.isFinite(creditValue)
      ? { credits: creditValue, creditsUnit: text(credits?.unit) ?? 'credit' }
      : {},
    ...Number.isFinite(contextPercentage) ? { contextPercentage } : {},
  }
  const hasToolCall = Array.isArray(response?.output)
    && response.output.some(item => record(item)?.type === 'function_call')
  const status = text(response?.status)
  const stopReason = hasToolCall ? 'TOOL_USE'
    : status === 'incomplete' ? 'MAX_TOKENS'
      : 'END_TURN'
  return { usage: tokens, stopReason }
}
