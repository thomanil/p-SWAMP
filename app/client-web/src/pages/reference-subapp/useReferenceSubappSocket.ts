import { useCallback } from 'react'

import type { Wire } from '@/api/wire'
import { REFERENCE_SUBAPP_API_PATH, REFERENCE_SUBAPP_WS_PATH } from '@/lib/servers'
import { fireCommand, postCommand } from '@/lib/commands'
import { useServerSocket } from '@/hooks/useServerSocket'

/** What the server pushes, generated from the api contract — the model declared
 *  in app/server-python/src/reference_subapp/api.py, not a hand-copy of it.
 *
 *  Note the page reads these fields as the server names them, snake_case and
 *  all. A hook is welcome to *derive* something (see the grid monitor's
 *  line-outage hook, which parses branch names out of a channel label), but it
 *  does not rename: a mapping layer has to be extended by hand for every new
 *  field, and a `useMemo` that forgets one is perfectly valid TypeScript — so
 *  the field silently never reaches the screen, which is the very failure the
 *  generated contract exists to prevent. */
export type ReferenceSubappState = Wire['ReferenceSubappState']

/**
 * The Reference example app: state arrives on the socket, commands go up as POSTs to
 * /api/reference-subapp.
 */
export function useReferenceSubappSocket() {
  const { message, status, connected } =
    useServerSocket<ReferenceSubappState>(REFERENCE_SUBAPP_WS_PATH)

  const bump = useCallback(
    () =>
      fireCommand(
        'reference-subapp',
        postCommand(`${REFERENCE_SUBAPP_API_PATH}/count/bump`),
      ),
    [],
  )
  const reset = useCallback(
    () =>
      fireCommand(
        'reference-subapp',
        postCommand(`${REFERENCE_SUBAPP_API_PATH}/count/reset`),
      ),
    [],
  )

  return { state: message, status, connected, bump, reset }
}
