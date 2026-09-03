import { cn } from '@/lib/utils'

import type { StreamRecord } from './usePmuStreamSocket'

/**
 * Renders the server-provided window of the most recent stream records, oldest
 * first. The newest record sits at the bottom, bold and accented, with a
 * sequence-number gutter; older records fade with distance (nearer = more
 * opaque), which stays correct in dark mode where a literal grey ramp would
 * brighten instead of fade.
 *
 * Rows are a fixed height and text is truncated rather than wrapped, so a long
 * record does not change the block's size as the live tail scrolls. The block is
 * padded to WINDOW_SIZE rows so it does not grow while the window fills on first
 * connect.
 */
const WINDOW_SIZE = 9

export function StreamWindow({ window }: { window: StreamRecord[] }) {
  // Pad from the top with blanks so the block is a stable height while the window
  // fills, and the newest record always sits on the bottom row.
  const padding = Math.max(0, WINDOW_SIZE - window.length)
  const rows: (StreamRecord | null)[] = [
    ...Array<null>(padding).fill(null),
    ...window,
  ]
  const lastIndex = rows.length - 1

  return (
    <div className="py-4 font-mono text-xs select-none">
      {rows.map((record, i) => {
        const isCurrent = i === lastIndex && record !== null
        // dist: 0 at the newest record, →1 at the top of the window.
        const dist = (lastIndex - i) / lastIndex
        return (
          <div
            key={i}
            className={cn(
              'flex h-6 items-center gap-3',
              isCurrent
                ? 'font-bold text-blue-600 dark:text-blue-400'
                : 'text-muted-foreground',
            )}
            style={isCurrent ? undefined : { opacity: 1 - 0.75 * dist }}
          >
            {/* Gutter: caret marks the newest record; the sequence number is
                right-aligned with tabular figures so the column can't jitter. */}
            <span className="w-3 shrink-0">{isCurrent ? '▸' : ''}</span>
            <span className="w-16 shrink-0 text-right tabular-nums">
              {record === null ? '' : record.seq}
            </span>
            <span className="truncate">{record === null ? '' : record.text}</span>
          </div>
        )
      })}
    </div>
  )
}
