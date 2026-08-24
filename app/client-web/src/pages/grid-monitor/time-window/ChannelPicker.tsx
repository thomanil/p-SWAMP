import { useEffect, useMemo, useState } from 'react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { resolveApiUrl, TIME_WINDOW_API_PATH } from '@/lib/servers'
import { cn } from '@/lib/utils'

import type { ChannelInfo } from './useTimeWindowSocket'

/** Cap on how many traces can be shown at once — past this they stop being
 *  readable, and the Qt plot draws the same line at 50. */
const MAX_SELECTED = 12

/** The catalogue endpoint is HTTP, on the same origin everything else resolves to. */
function channelsUrl(): string {
  return resolveApiUrl(`${TIME_WINDOW_API_PATH}/channels`)
}

export function ChannelPicker({
  selected,
  onChange,
}: {
  selected: ChannelInfo[]
  onChange: (indices: number[]) => void
}) {
  const [all, setAll] = useState<ChannelInfo[]>([])
  const [measurement, setMeasurement] = useState('f')
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    // State is only touched from the fetch callbacks, never synchronously in the
    // effect body — the same reason useServerSocket defers its first setState.
    fetch(channelsUrl())
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(String(r.status)))))
      .then((body) => {
        if (cancelled) return
        setAll(body.channels as ChannelInfo[])
        setError(null)
      })
      .catch(() => {
        if (!cancelled) setError('Could not load the channel list.')
      })
    return () => {
      cancelled = true
    }
  }, [])

  const measurements = useMemo(
    () => [...new Set(all.map((c) => c.measurement))],
    [all],
  )
  const visible = useMemo(
    () => all.filter((c) => c.measurement === measurement),
    [all, measurement],
  )
  const selectedIdx = useMemo(
    () => new Set(selected.map((c) => c.idx)),
    [selected],
  )

  const toggle = (channel: ChannelInfo) => {
    const next = new Set(selectedIdx)
    if (next.has(channel.idx)) {
      // Never leave the chart with nothing to draw.
      if (next.size === 1) return
      next.delete(channel.idx)
    } else {
      if (next.size >= MAX_SELECTED) return
      next.add(channel.idx)
    }
    onChange([...next])
  }

  if (error) return <p className="text-sm text-muted-foreground">{error}</p>
  if (all.length === 0)
    return <p className="text-sm text-muted-foreground">Loading channels…</p>

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-sm text-muted-foreground">Measurement</span>
        {measurements.map((m) => (
          <Button
            key={m}
            size="sm"
            variant={m === measurement ? 'default' : 'outline'}
            onClick={() => setMeasurement(m)}
          >
            {m}
          </Button>
        ))}
        <span className="ml-auto text-xs text-muted-foreground tabular-nums">
          {selectedIdx.size}/{MAX_SELECTED} shown
        </span>
      </div>

      <div className="flex max-h-32 flex-wrap gap-1.5 overflow-y-auto">
        {visible.map((channel) => {
          const on = selectedIdx.has(channel.idx)
          return (
            <Badge
              key={channel.idx}
              variant={on ? 'default' : 'outline'}
              className={cn(
                'cursor-pointer select-none font-mono text-xs',
                !on && 'text-muted-foreground',
              )}
              onClick={() => toggle(channel)}
            >
              {channel.station}
            </Badge>
          )
        })}
      </div>
    </div>
  )
}
