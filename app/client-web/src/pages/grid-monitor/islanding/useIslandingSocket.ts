import { useCallback, useMemo } from 'react'

import { useServerSocket } from '@/hooks/useServerSocket'
import { postCommand } from '@/lib/commands'
import { ISLANDING_API_PATH, ISLANDING_WS_PATH } from '@/lib/servers'

export type Island = {
  /** 0 is the main system; anything higher has separated from it. */
  index: number
  stations: string[]
  meanFreq: number | null
}

export type AlarmStatus = 'unseen' | 'acknowledged' | 'silenced' | 'not_critical'

export type Alarm = {
  uuid: string
  appName: string
  tStart: number
  tEnd: number | null
  status: AlarmStatus
  events: { t: number; type: string; message: string }[]
}

export type IslandingState = {
  t: number
  appName: string
  status: string
  islands: Island[]
  parameters: {
    windowLength: number
    meanThreshold: number
    evalFreq: number
  }
} | null

export type IslandingPageState = {
  islanding: IslandingState
  alarms: Alarm[]
}

type IslandingMessage = {
  islanding: {
    t: number
    app_uuid: string
    app_name: string
    status: string
    islands: { index: number; stations: string[]; mean_freq: number | null }[]
    parameters: {
      window_length: number
      mean_threshold: number
      eval_freq: number
    }
  } | null
  alarms: {
    alarms: {
      uuid: string
      app_uuid: string
      app_name: string
      t_start: number
      t_end: number | null
      status: AlarmStatus
      events: { t: number; type: string; message: string }[]
    }[]
  }
}

/** Fire an operator action and carry on; the updated alarm list arrives on the
 *  socket, which the server pushes as soon as it has applied the change. */
function fire(promise: Promise<void>): void {
  promise.catch((error) => console.error('islanding command failed', error))
}

export function useIslandingSocket() {
  const { message, status, connected } =
    useServerSocket<IslandingMessage>(ISLANDING_WS_PATH)

  const state = useMemo<IslandingPageState | null>(
    () =>
      message === null
        ? null
        : {
            islanding: message.islanding
              ? {
                  t: message.islanding.t,
                  appName: message.islanding.app_name,
                  status: message.islanding.status,
                  islands: message.islanding.islands.map((i) => ({
                    index: i.index,
                    stations: i.stations,
                    meanFreq: i.mean_freq,
                  })),
                  parameters: {
                    windowLength: message.islanding.parameters.window_length,
                    meanThreshold: message.islanding.parameters.mean_threshold,
                    evalFreq: message.islanding.parameters.eval_freq,
                  },
                }
              : null,
            alarms: message.alarms.alarms.map((a) => ({
              uuid: a.uuid,
              appName: a.app_name,
              tStart: a.t_start,
              tEnd: a.t_end,
              status: a.status,
              events: a.events,
            })),
          },
    [message],
  )

  // One url per action on the alarm they apply to:
  //   POST /api/islanding/alarms/<uuid>/acknowledge
  const acknowledge = useCallback(
    (uuid: string) => fire(postCommand(`${ISLANDING_API_PATH}/alarms/${uuid}/acknowledge`)),
    [],
  )
  const silence = useCallback(
    (uuid: string) => fire(postCommand(`${ISLANDING_API_PATH}/alarms/${uuid}/silence`)),
    [],
  )
  const annotate = useCallback(
    (uuid: string, message: string) =>
      fire(postCommand(`${ISLANDING_API_PATH}/alarms/${uuid}/annotate`, { message })),
    [],
  )

  return { state, status, connected, acknowledge, silence, annotate }
}
