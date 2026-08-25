import { cn } from '@/lib/utils'

import type { StreamRecord } from './usePmuStreamSocket'

/**
 * Renders the server-provided window of stream records, vertically and as text.
 * The current record sits in the middle, bold and
 * accented, with a line-number gutter; neighbours fade with distance (nearer =
 * more opaque), which stays correct in dark mode where a literal grey ramp would
 * brighten instead of fade.
 *
 * Rows are a fixed height and text is truncated rather than wrapped, so neither a
 * long record nor a null entry (the window running off either end of the file)
 * changes the block's size as the stream scrolls.
 */
export function StreamWindow({ window }: { window: (StreamRecord | null)[] }) {
  const radius = (window.length - 1) / 2
  return (
    <div className="py-4 font-mono text-xs select-none">
      {window.map((record, i) => {
        const offset = i - radius
        const isCurrent = offset === 0
        // dist: 0 at the current record, →1 at the window edge.
        const dist = Math.abs(offset) / radius
        return (
          <div
            key={i}
            className={cn(
              'flex h-6 items-center gap-3',
              isCurrent ? 'font-bold text-blue-600 dark:text-blue-400' : 'text-muted-foreground',
            )}
            style={isCurrent ? undefined : { opacity: 1 - 0.75 * dist }}
          >
            {/* Gutter: the caret marks the current record, and the line number is
                right-aligned with tabular figures so the column can't jitter as
                the digit count changes. */}
            <span className="w-3 shrink-0">{isCurrent ? '▸' : ''}</span>
            <span className="w-10 shrink-0 text-right tabular-nums">
              {record === null ? '' : record.lineNumber}
            </span>
            <span className="truncate">{record === null ? '' : record.text}</span>
          </div>
        )
      })}
    </div>
  )
}
