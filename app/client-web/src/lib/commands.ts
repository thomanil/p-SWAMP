// The one place the client sends anything upstream.
//
// Every command is a POST — one url per operation, under the app's `/api/<app>`
// prefix. What that buys, and why it is worth a round trip per click: the
// operation is a row in the browser's Network tab and a line in the server's
// access log, it carries a status code that can say *why* it failed, and the
// whole upstream surface can be described rather than only documented.
//
// State does NOT come back through here. It arrives on the socket, on the
// server's own schedule, so there is exactly one path for it and nothing for a
// page to reconcile. A command's reply is a small acknowledgement this module
// deliberately discards.

import type { ApiPaths } from '@/api/wire'
import { CLIENT_ID } from '@/lib/clientId'
import { resolveApiUrl } from '@/lib/servers'

/** A command the server refused. Carries FastAPI's `detail` where there is one.
 *
 *  Fields are assigned in the body rather than declared as constructor
 *  parameters: `erasableSyntaxOnly` is on in this project, and parameter
 *  properties are the one TS feature that emits runtime code. */
export class CommandError extends Error {
  status: number
  detail: string

  constructor(status: number, detail: string) {
    super(`command failed (${status}): ${detail}`)
    this.name = 'CommandError'
    this.status = status
    this.detail = detail
  }
}

/** Every path in the contract that accepts a POST — i.e. every command there is.
 *
 *  Derived from the generated `paths`, so it needs no maintenance: a new command
 *  on the server is in this union as soon as the contract is regenerated, and a
 *  removed one stops type-checking at its call sites. */
export type CommandPath = {
  [P in keyof ApiPaths]: ApiPaths[P] extends { post: unknown } ? P : never
}[keyof ApiPaths]

/** The `{placeholder}` segments a command's url has, or `never` where it has
 *  none. The generator writes `path?: never` for an operation without any, which
 *  infers as `undefined` — hence the second test. */
type PathParams<P extends CommandPath> = ApiPaths[P] extends {
  post: { parameters: { path: infer Q } }
}
  ? [Q] extends [undefined]
    ? never
    : Q
  : never

/** The request body a command expects, or `never` where it takes none. A command
 *  without one has no `requestBody` key at all, hence the conditional. */
type CommandBody<P extends CommandPath> = ApiPaths[P] extends {
  post: { requestBody: { content: { 'application/json': infer B } } }
}
  ? B
  : never

/** What a given command needs supplied: its path parameters, its body, or both.
 *  Each half disappears from the type when the contract says it does not apply,
 *  so passing a body to a command that takes none is a compile error. */
type CommandOptions<P extends CommandPath> = ([PathParams<P>] extends [never]
  ? object
  : { path: PathParams<P> }) &
  ([CommandBody<P>] extends [never] ? object : { body: CommandBody<P> })

/** `[]` for a command that needs nothing, `[options]` for one that does. */
type CommandArgs<P extends CommandPath> = [PathParams<P>] extends [never]
  ? [CommandBody<P>] extends [never]
    ? []
    : [options: CommandOptions<P>]
  : [options: CommandOptions<P>]

/** Substitute `{name}` segments, encoding each value. Encoding matters: these are
 *  server-supplied identifiers, and a raw `/` or `?` in one would silently change
 *  which endpoint the request reached. */
function fillPath(template: string, params: Record<string, unknown>): string {
  return template.replace(/\{([^}]+)\}/g, (_match, name: string) => {
    const value = params[name]
    if (value === undefined) {
      throw new Error(`missing path parameter ${name} for ${template}`)
    }
    return encodeURIComponent(String(value))
  })
}

/**
 * POST one command and resolve when the server has applied it.
 *
 * `path` is the command's path **as the contract spells it** — placeholders and
 * all, e.g. `` `${ISLANDING_API_PATH}/alarms/{alarm_uuid}/acknowledge` ``, with
 * the real value passed as `options.path`. Those `*_API_PATH` consts are string
 * literals, so the template literal resolves to a literal type and is checked
 * against the generated contract: a typo, a stale path, a missing parameter or a
 * wrong body shape is a `tsc` failure here rather than a 404 or a 422 in
 * someone's browser.
 *
 * The client id is appended here rather than by each caller: it is the same
 * browser-wide value the sockets send, which is what makes a command apply to
 * the pipeline the page is watching.
 *
 * Rejects with CommandError on any non-2xx. Callers in this app log and carry on
 * (see the page hooks) — a command is fire-and-forget from the UI's point of
 * view, and the resulting state arrives on the socket or not at all.
 */
export async function postCommand<P extends CommandPath>(
  path: P,
  ...args: CommandArgs<P>
): Promise<void> {
  const options = (args[0] ?? {}) as { path?: Record<string, unknown>; body?: unknown }
  const body = options.body
  const filled = options.path ? fillPath(path, options.path) : path
  const url = `${resolveApiUrl(filled)}?client_id=${CLIENT_ID}`
  const response = await fetch(url, {
    method: 'POST',
    ...(body === undefined
      ? {}
      : { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }),
  })

  if (!response.ok) {
    throw new CommandError(response.status, await readDetail(response))
  }
}

/**
 * Send a command and carry on: the caller does not wait, and a failure is logged.
 *
 * This is what a UI handler wants nearly always, and it is deliberate rather than
 * lazy. A command that did not land produces no state change — which is what the
 * user is already looking at — and the state that *does* result arrives on the
 * socket rather than from this call, so there is nothing here to render either
 * way. `label` names the app in the log line.
 *
 * Await `postCommand` directly instead when the outcome must be handled: a form
 * that should show a validation error, say, where CommandError.detail carries
 * what the server said.
 */
export function fireCommand(label: string, promise: Promise<void>): void {
  promise.catch((error) => console.error(`${label} command failed`, error))
}

/** FastAPI's error body is `{detail: …}`, where detail is a string for our own
 *  HTTPExceptions and an array of field errors for a validation failure. Neither
 *  is guaranteed — a proxy may answer with HTML — so this never throws. */
async function readDetail(response: Response): Promise<string> {
  try {
    const body = await response.json()
    const { detail } = body as { detail?: unknown }
    if (typeof detail === 'string') return detail
    if (detail !== undefined) return JSON.stringify(detail)
    return response.statusText
  } catch {
    return response.statusText
  }
}
