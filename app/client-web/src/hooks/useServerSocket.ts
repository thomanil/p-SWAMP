import { useCallback, useEffect, useRef, useState } from 'react'

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
 * Reconnecting is the right default — a dropped socket usually means the server
 * restarted or the network blinked, and the server's state survives. These two
 * are the exceptions, and retrying them is actively harmful:
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

/** Options for callers whose data path shouldn't go through React state. */
export type ServerSocketOptions<M> = {
  /**
   * Called for each message *instead of* storing it in state.
   *
   * Only worth reaching for on a high-rate stream whose payload is drawn
   * imperatively — a canvas chart, say — where a render pass per message buys
   * nothing. Setting this leaves `message` permanently null, so the caller owns
   * the data entirely; `status` still updates normally, since connection changes
   * are rare and genuinely do need a render.
   */
  onMessage?: (message: M) => void
}

/**
 * The connection half of every app's socket: one WebSocket to `wsPath` on the
 * serving origin (see resolveServerUrl), auto-reconnecting every 2s while down —
 * except after a refusal the server meant, see TERMINAL_CLOSE_CODES.
 *
 * Every socket identifies itself with the same browser-wide CLIENT_ID, which is
 * what makes the grid monitor's five panels views of *one* server-side pipeline
 * rather than five. That id used to be rolled here, per hook; see lib/clientId.ts
 * for why it moved.
 *
 * Deliberately knows nothing about message *shape*. It hands back the raw `state`
 * payload the server pushed and each app's own hook maps it to a typed object —
 * which also keeps a mapping callback out of the effect's dependencies, where an
 * unstable one would reconnect the socket on every render.
 */
export function useServerSocket<M = unknown>(
  wsPath: string,
  options: ServerSocketOptions<M> = {},
) {
  const wsRef = useRef<WebSocket | null>(null)
  const [message, setMessage] = useState<M | null>(null)
  // Held in a ref so a caller can pass an inline function without the identity
  // of that function tearing down and reopening the socket on every render.
  // Updated after commit rather than during render: the socket only ever calls
  // it asynchronously, so it is always the committed version that runs.
  const onMessage = useRef(options.onMessage)
  useEffect(() => {
    onMessage.current = options.onMessage
  })
  const [status, setStatus] = useState<ConnStatus>({
    kind: 'connecting',
    label: 'Connecting to server…',
  })

  useEffect(() => {
    const url = resolveServerUrl(wsPath)
    let disposed = false
    let reconnect: ReturnType<typeof setTimeout> | null = null

    const connect = () => {
      if (disposed) return
      setStatus({ kind: 'connecting', label: 'Connecting to server…' })
      // `wasOpen` lets us tell "never reached the server" (error banner) from
      // "an established connection dropped" (lost-connection banner), the way
      // the Qt client splits on_error vs on_disconnected.
      let wasOpen = false
      const ws = new WebSocket(`${url}?client_id=${CLIENT_ID}`)
      wsRef.current = ws

      ws.onopen = () => {
        wasOpen = true
        setStatus({ kind: 'online' })
      }
      ws.onmessage = (event) => {
        const msg = JSON.parse(event.data)
        if (msg.type !== 'state') return
        if (onMessage.current) {
          onMessage.current(msg as M)
          return
        }
        setMessage(msg as M)
      }
      ws.onclose = (event) => {
        // Only retract the shared ref if it still points at THIS socket. A closing
        // socket's onclose can fire *after* its replacement is already installed
        // (a re-run of this effect, e.g. StrictMode's double-mount); nulling
        // unconditionally would wipe the new socket's ref, leaving send() with
        // nothing to write to — live connection, dead controls, no error banner.
        if (wsRef.current === ws) wsRef.current = null
        if (disposed) return

        // A refusal the server meant: stay down and say why, rather than asking
        // again every 2s for as long as the tab is open.
        if (TERMINAL_CLOSE_CODES.has(event.code)) {
          setStatus({
            kind: 'offline',
            isError: true,
            label: terminalLabel(event.code),
          })
          return
        }

        setStatus({
          kind: 'offline',
          isError: true,
          label: wasOpen
            ? 'Lost connection to server. Reconnecting…'
            : 'Cannot reach the server. Is it running? Retrying…',
        })
        reconnect = setTimeout(connect, RECONNECT_MS)
      }
      // onerror always precedes onclose in browsers; let onclose own the banner.
    }

    // Defer the first connect to a microtask so the initial setStatus runs in a
    // callback rather than synchronously in the effect body (cleaner re-renders;
    // satisfies react-hooks/set-state-in-effect). Imperceptible delay.
    queueMicrotask(connect)

    return () => {
      disposed = true
      if (reconnect) clearTimeout(reconnect)
      wsRef.current?.close()
      wsRef.current = null
    }
  }, [wsPath])

  const send = useCallback((action: string, extra?: Record<string, unknown>) => {
    const ws = wsRef.current
    if (ws?.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ type: 'command', action, ...extra }))
    }
  }, [])

  return { message, status, connected: status.kind === 'online', send }
}
