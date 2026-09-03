import { CLIENT_ID } from '@/lib/clientId'
import { resolveServerUrl } from '@/lib/servers'

/** Connection status, mirroring the Qt client's banner states. */
export type ConnStatus =
  | { kind: 'connecting'; label: string }
  | { kind: 'online' }
  | { kind: 'offline'; label: string; isError: boolean }

const RECONNECT_MS = 2000

/**
 * Close codes we must not retry after.
 *
 * Reconnecting is the right default — a dropped socket usually means the network
 * blinked or the server restarted. During a brief disconnect, the server keeps
 * this client's state alive for its idle grace period. These two are the
 * exceptions, and retrying them is actively harmful:
 *
 *   1008 policy violation — our client id was rejected. It will be rejected
 *        again in two seconds, and again forever.
 *   1013 try again later  — the server is at its pipeline cap. This is the one
 *        that matters: every refused client that kept retrying would add load to
 *        a server already turning people away, and none of them would ever get
 *        in any faster.
 *
 * The user's move in both cases is to reload, which is why the banner says so.
 */
const TERMINAL_CLOSE_CODES = new Set([1008, 1013])

function terminalLabel(code: number): string {
  return code === 1013
    ? 'The server is at capacity — too many streams are running. Reload to try again.'
    : 'The server rejected this client. Reload to try again.'
}

/** Options for callers whose data path shouldn't go through reactive state. */
export type ServerSocketOptions<M> = {
  /**
   * Called for each message *instead of* storing it in reactive state.
   *
   * Only worth reaching for on a high-rate stream whose payload is drawn
   * imperatively — a canvas chart, say — where a render pass per message buys
   * nothing. Setting this leaves `message` permanently null, so the caller owns
   * the data entirely; `status` still updates normally, since connection changes
   * are rare and genuinely do need a render.
   */
  onMessage?: (message: M) => void
}

/** What `useServerSocket` hands back: reactive `message`/`status` behind getters
 *  so a `.svelte` template re-renders when the connection ticks, plus the derived
 *  `connected` flag. Deliberately no way to write to the socket — see below. */
export type ServerSocket<M> = {
  readonly message: M | null
  readonly status: ConnStatus
  readonly connected: boolean
}

/**
 * The connection half of every app's socket: one WebSocket to `wsPath` on the
 * serving origin (see resolveServerUrl), auto-reconnecting every 2s while down —
 * except after a refusal the server meant, see TERMINAL_CLOSE_CODES.
 *
 * **Downstream only.** Nothing is sent up this socket; commands are POSTs, made
 * through lib/commands.ts. So this returns what the server pushed and a
 * connection status, and deliberately offers no way to write to the socket.
 *
 * Every socket identifies itself with the same browser-wide CLIENT_ID, which is
 * what makes the grid monitor's five panels views of *one* server-side pipeline
 * rather than five. See lib/clientId.ts for why that id lives there.
 *
 * Deliberately knows nothing about message *shape*. It hands back the raw `state`
 * payload the server pushed and each app's own socket module maps it to a typed
 * object — which also keeps the mapping out of the reactive graph, where it does
 * not belong.
 *
 * Call it from a component's `<script>` (directly, or via one of the page socket
 * modules): it opens the socket in a `$effect`, so the connection is torn down
 * automatically when the component is destroyed, exactly as the React hook's
 * cleanup did on unmount.
 */
export function useServerSocket<M = unknown>(
  wsPath: string,
  options: ServerSocketOptions<M> = {},
): ServerSocket<M> {
  let message = $state<M | null>(null)
  let status = $state<ConnStatus>({
    kind: 'connecting',
    label: 'Connecting to server…',
  })
  // Captured once. In every call site `onMessage` is a stable function created
  // alongside this socket, so there is nothing to keep in a ref the way React had
  // to — and the effect below reads only the plain `wsPath`, so it never reopens
  // the socket on its own.
  const onMessage = options.onMessage

  $effect(() => {
    const url = resolveServerUrl(wsPath)
    let disposed = false
    let ws: WebSocket | null = null
    let reconnect: ReturnType<typeof setTimeout> | null = null

    const connect = () => {
      if (disposed) return
      status = { kind: 'connecting', label: 'Connecting to server…' }
      // `wasOpen` lets us tell "never reached the server" (error banner) from
      // "an established connection dropped" (lost-connection banner), the way
      // the Qt client splits on_error vs on_disconnected.
      let wasOpen = false
      ws = new WebSocket(`${url}?client_id=${CLIENT_ID}`)

      ws.onopen = () => {
        wasOpen = true
        status = { kind: 'online' }
      }
      ws.onmessage = (event) => {
        let msg: unknown
        try {
          msg = JSON.parse(event.data)
        } catch (error) {
          console.error('ignored malformed WebSocket message', error)
          return
        }
        if (typeof msg !== 'object' || msg === null || !('type' in msg)) return
        if (msg.type !== 'state') return
        if (onMessage) {
          onMessage(msg as M)
          return
        }
        message = msg as M
      }
      ws.onclose = (event) => {
        if (disposed) return

        // A refusal the server meant: stay down and say why, rather than asking
        // again every 2s for as long as the tab is open.
        if (TERMINAL_CLOSE_CODES.has(event.code)) {
          status = {
            kind: 'offline',
            isError: true,
            label: terminalLabel(event.code),
          }
          return
        }

        status = {
          kind: 'offline',
          isError: true,
          label: wasOpen
            ? 'Lost connection to server. Reconnecting…'
            : 'Cannot reach the server. Is it running? Retrying…',
        }
        reconnect = setTimeout(connect, RECONNECT_MS)
      }
      // onerror always precedes onclose in browsers; let onclose own the banner.
    }

    // Defer the first connect to a microtask so the initial status change runs in
    // a callback rather than synchronously while the effect first runs. Imperceptible delay.
    queueMicrotask(connect)

    return () => {
      disposed = true
      if (reconnect) clearTimeout(reconnect)
      ws?.close()
      ws = null
    }
  })

  return {
    get message() {
      return message
    },
    get status() {
      return status
    },
    get connected() {
      return status.kind === 'online'
    },
  }
}
