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

/**
 * POST one command and resolve when the server has applied it.
 *
 * `path` is the full endpoint path, built from the app's `*_API_PATH` const —
 * e.g. `` `${TIMELINE_API_PATH}/playback/play` ``. The client id is appended
 * here rather than by each caller: it is the same browser-wide value the sockets
 * send, which is what makes a command apply to the pipeline the page is watching.
 *
 * Rejects with CommandError on any non-2xx. Callers in this app log and carry on
 * (see the page hooks) — a command is fire-and-forget from the UI's point of
 * view, and the resulting state arrives on the socket or not at all.
 */
export async function postCommand(path: string, body?: unknown): Promise<void> {
  const url = `${resolveApiUrl(path)}?client_id=${CLIENT_ID}`
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
