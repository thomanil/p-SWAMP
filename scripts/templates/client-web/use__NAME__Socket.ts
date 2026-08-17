import { useMemo } from 'react'

import { __WS_PATH_CONST__ } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'

/** What the server pushes (see `state_message` in
 *  app/server-python/src/__PKG__/api.py), before and after camelCasing. */
type __NAME__Message = { count: number; client_id: number }
export type __NAME__State = { count: number; clientId: number }

/** Connection handling — the client_id seed, reconnects, status — comes from
 *  useServerSocket; this hook adds only the mapping to __NAME__State. */
export function use__NAME__Socket() {
  const { message, status, connected, send } =
    useServerSocket<__NAME__Message>(__WS_PATH_CONST__)

  const state = useMemo<__NAME__State | null>(
    () => (message === null ? null : { count: message.count, clientId: message.client_id }),
    [message],
  )

  return { state, status, connected, send }
}
