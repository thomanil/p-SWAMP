import { useServerSocket } from '@/hooks/useServerSocket.svelte'
import { APP_STATUS_WS_PATH } from '@/lib/servers'
import type { Wire } from '@/api/wire'

/** One application's row, and the statuses it can report. Straight from the
 *  contract — the pydantic models in app/server-python/src/pswamp_web/wire.py,
 *  not a hand-copy of them, so a field added there is visible here at once. */
export type AppStatusRow = Wire['AppStatus']
export type AppStatusValue = AppStatusRow['status']

/** State of the PMU replay feeding this client's applications. */
export type ReplayState = Wire['ReplayStatus']

export function useAppStatusSocket() {
  const sock = useServerSocket<Wire['AppStatusTable']>(APP_STATUS_WS_PATH)

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
  }
}
