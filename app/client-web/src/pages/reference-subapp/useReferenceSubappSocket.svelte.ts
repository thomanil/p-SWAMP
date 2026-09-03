import type { Wire } from '@/api/wire'
import { REFERENCE_SUBAPP_API_PATH, REFERENCE_SUBAPP_WS_PATH } from '@/lib/servers'
import { fireCommand, postCommand } from '@/lib/commands'
import { useServerSocket } from '@/hooks/useServerSocket.svelte'

/** What the server pushes, generated from the api contract — the model declared
 *  in app/server-python/src/reference_subapp/api.py, not a hand-copy of it.
 *
 *  Note the page reads these fields as the server names them, snake_case and
 *  all. A socket module is welcome to *derive* something (see the grid monitor's
 *  line-outage module, which parses branch names out of a channel label), but it
 *  does not rename: a mapping layer has to be extended by hand for every new
 *  field, and a `$derived` that forgets one is perfectly valid TypeScript — so
 *  the field silently never reaches the screen, which is the very failure the
 *  generated contract exists to prevent. */
export type ReferenceSubappState = Wire['ReferenceSubappState']

/**
 * The Reference example app: state arrives on the socket, commands go up as POSTs to
 * /api/reference-subapp.
 *
 * Call it from a component's `<script>`; the returned getters are reactive, so a
 * template reading `state`/`status`/`connected` updates as the socket ticks.
 */
export function useReferenceSubappSocket() {
  const sock = useServerSocket<ReferenceSubappState>(REFERENCE_SUBAPP_WS_PATH)

  const bump = () =>
    fireCommand('reference-subapp', postCommand(`${REFERENCE_SUBAPP_API_PATH}/count/bump`))
  const reset = () =>
    fireCommand('reference-subapp', postCommand(`${REFERENCE_SUBAPP_API_PATH}/count/reset`))

  return {
    get state() {
      return sock.message
    },
    get status() {
      return sock.status
    },
    get connected() {
      return sock.connected
    },
    bump,
    reset,
  }
}
