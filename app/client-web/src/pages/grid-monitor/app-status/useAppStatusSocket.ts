import { useMemo } from 'react'

import { useServerSocket } from '@/hooks/useServerSocket'
import { APP_STATUS_WS_PATH } from '@/lib/servers'
import type { Wire } from '@/api/wire'

/** The statuses a p-SWAMP application can report on itself. */
export type AppStatusValue =
  | 'OK'
  | 'Alert'
  | 'Emergency'
  | 'Initializing...'
  | 'Undefined'

export type AppStatusRow = {
  uuid: string
  appName: string
  status: AppStatusValue
  /** Data time stamp the application reported, in epoch seconds. */
  t: number
  /** Server wall clock when the report arrived. */
  receivedAt: number
  /** Nothing heard recently — the row is shown greyed out. */
  stale: boolean
}

export type ReplayState = {
  source: string
  playing: boolean
  dataRate: number
  nSamples: number
  nChannels: number
  cursor: number
  position: number
  duration: number
}

export type AppStatusState = {
  apps: AppStatusRow[]
  serverTime: number
  replay: ReplayState
}

// The wire shape, snake_case as the server sends it. Kept private: the mapping
// below is the boundary, and nothing outside this file should see these names.
type AppStatusMessage = Wire['AppStatusTable']

export function useAppStatusSocket() {
  const { message, status, connected } =
    useServerSocket<AppStatusMessage>(APP_STATUS_WS_PATH)

  const state = useMemo<AppStatusState | null>(
    () =>
      message === null
        ? null
        : {
            apps: message.apps.map((app) => ({
              uuid: app.uuid,
              appName: app.app_name,
              status: app.status,
              t: app.t,
              receivedAt: app.received_at,
              stale: app.stale,
            })),
            serverTime: message.server_time,
            replay: {
              source: message.replay.source,
              playing: message.replay.playing,
              dataRate: message.replay.data_rate,
              nSamples: message.replay.n_samples,
              nChannels: message.replay.n_channels,
              cursor: message.replay.cursor,
              position: message.replay.position,
              duration: message.replay.duration,
            },
          },
    [message],
  )

  return { state, status, connected }
}
