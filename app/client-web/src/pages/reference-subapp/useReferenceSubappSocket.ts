import { useCallback, useMemo } from 'react'

import type { Wire } from '@/api/wire'
import { REFERENCE_SUBAPP_API_PATH, REFERENCE_SUBAPP_WS_PATH } from '@/lib/servers'
import { postCommand } from '@/lib/commands'
import { useServerSocket } from '@/hooks/useServerSocket'

/** What the server pushes, generated from the api contract — the model declared
 *  in app/server-python/src/reference_subapp/api.py, not a hand-copy of it. */
type ReferenceSubappMessage = Wire['ReferenceSubappState']

/** What this page works in: the client's own vocabulary, camelCase, mapped from
 *  the wire shape above. */
export type ReferenceSubappState = { count: number }

/** Fire a command and carry on; a failure is logged and nothing else happens.
 *  A command that did not land produces no state change, which is what the user
 *  already sees. */
function fire(promise: Promise<void>): void {
  promise.catch((error) => console.error('reference-subapp command failed', error))
}

/**
 * The Reference example app: state arrives on the socket, commands go up as POSTs to
 * /api/reference-subapp.
 */
export function useReferenceSubappSocket() {
  const { message, status, connected } =
    useServerSocket<ReferenceSubappMessage>(REFERENCE_SUBAPP_WS_PATH)

  const state = useMemo<ReferenceSubappState | null>(
    () => (message === null ? null : { count: message.count }),
    [message],
  )

  const bump = useCallback(
    () => fire(postCommand(`${REFERENCE_SUBAPP_API_PATH}/count/bump`)),
    [],
  )
  const reset = useCallback(
    () => fire(postCommand(`${REFERENCE_SUBAPP_API_PATH}/count/reset`)),
    [],
  )

  return { state, status, connected, bump, reset }
}
