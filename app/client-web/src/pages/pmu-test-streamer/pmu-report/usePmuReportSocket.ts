import { useCallback } from 'react'

import { fireCommand, postCommand } from '@/lib/commands'
import { PMU_REPORT_API_PATH, PMU_REPORT_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'
import type { Wire } from '@/api/wire'

/** Everything the report card shows (see `state_message` in
 *  app/server-python/src/pmu_report/api.py). */
export type PmuReportState = Wire['PmuReportState']
/** One finished report, as the module produced it onto the bus. */
export type VoltageReport = Wire['VoltageReport']

/** Which part of the source to report over; both unset means all of it. */
export type ReportRange = { start_s?: number; end_s?: number }

/**
 * The batch query/response path: `run` POSTs a job and gets back only an
 * acknowledgement; the report itself arrives on the socket some time later,
 * wearing the `request_id` sent here — which is how a page could match an
 * answer to a click if it needed to. Until then the id sits in `pending`.
 */
export function usePmuReportSocket() {
  const { message, status, connected } = useServerSocket<PmuReportState>(PMU_REPORT_WS_PATH)

  const run = useCallback(
    (range: ReportRange = {}) =>
      fireCommand(
        'pmu-report',
        postCommand(`${PMU_REPORT_API_PATH}/mean-voltage/run`, {
          body: { ...range, request_id: `ui-${Date.now().toString(36)}` },
        }),
      ),
    [],
  )

  return { state: message, status, connected, run }
}
