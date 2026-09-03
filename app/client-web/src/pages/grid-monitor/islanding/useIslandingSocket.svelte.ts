import { useServerSocket } from '@/hooks/useServerSocket.svelte'
import { fireCommand, postCommand } from '@/lib/commands'
import { ISLANDING_API_PATH, ISLANDING_WS_PATH } from '@/lib/servers'
import type { Wire } from '@/api/wire'

/** One detected island; index 0 is the main system. Straight from the contract. */
export type Island = Wire['Island']

export type Alarm = Wire['Alarm']
export type AlarmStatus = Alarm['status']

/** Both halves of the page in one message: the detection result and the alarm
 *  list. They change together and are read together, so the server sends them
 *  together and a client cannot render them inconsistently. */
export type IslandingPageState = Wire['IslandingState']

export function useIslandingSocket() {
  const sock = useServerSocket<IslandingPageState>(ISLANDING_WS_PATH)

  // One url per action on the alarm they apply to:
  //   POST /api/islanding/alarms/<uuid>/acknowledge
  //
  // The path is written the way the contract spells it -- placeholder and all --
  // and the uuid goes in as a parameter, so postCommand checks it against the
  // generated operation and url-encodes it on the way out.
  //
  // Nothing here waits for the reply: the updated alarm list arrives on the
  // socket, which the server pushes as soon as it has applied the change.
  const acknowledge = (uuid: string) =>
    fireCommand(
      'islanding',
      postCommand(`${ISLANDING_API_PATH}/alarms/{alarm_uuid}/acknowledge`, {
        path: { alarm_uuid: uuid },
      }),
    )
  const silence = (uuid: string) =>
    fireCommand(
      'islanding',
      postCommand(`${ISLANDING_API_PATH}/alarms/{alarm_uuid}/silence`, {
        path: { alarm_uuid: uuid },
      }),
    )
  const annotate = (uuid: string, message: string) =>
    fireCommand(
      'islanding',
      postCommand(`${ISLANDING_API_PATH}/alarms/{alarm_uuid}/annotate`, {
        path: { alarm_uuid: uuid },
        body: { message },
      }),
    )

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
    acknowledge,
    silence,
    annotate,
  }
}
