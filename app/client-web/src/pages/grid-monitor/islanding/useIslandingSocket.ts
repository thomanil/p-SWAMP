import { useMemo } from 'react'

import { useServerSocket } from '@/hooks/useServerSocket'
import { ISLANDING_WS_PATH } from '@/lib/servers'

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

export function useIslandingSocket() {
  const { message, status, connected, send } =
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

  return { state, status, connected, send }
}
