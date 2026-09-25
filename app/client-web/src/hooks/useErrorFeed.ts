import { useCallback, useState } from 'react'

import type { Wire } from '@/api/wire'
import { ERRORS_WS_PATH } from '@/lib/servers'
import { useServerSocket } from '@/hooks/useServerSocket'

/** One notice, as the server pushes it (see app/server-python/src/errors/). */
export type ErrorNotice = Wire['ErrorNotice']

/** How many notices the tray keeps; the newest replace the oldest. */
export const VISIBLE_NOTICES = 5

/**
 * The error feed: every operational failure in any of this browser's pipelines,
 * on whatever page it is looking at. Lives in `src/hooks/` from day one because
 * its user is the layout, not a page — it must outlive navigation, which a
 * page-owned socket cannot.
 *
 * The hook *accumulates* (a socket message is one notice, the tray shows the
 * last few) and lets the person dismiss one; it renames nothing. A notice the
 * server replays after a reconnect is recognised by its id and not shown twice.
 */
export function useErrorFeed() {
  const [notices, setNotices] = useState<ErrorNotice[]>([])
  const [seen] = useState(() => new Set<string>())

  const { connected } = useServerSocket<ErrorNotice>(ERRORS_WS_PATH, {
    onMessage: (notice) => {
      if (seen.has(notice.id)) return
      seen.add(notice.id)
      setNotices((prev) => [notice, ...prev].slice(0, VISIBLE_NOTICES))
    },
  })

  const dismiss = useCallback((id: string) => {
    setNotices((prev) => prev.filter((notice) => notice.id !== id))
  }, [])

  return { notices, dismiss, connected }
}
