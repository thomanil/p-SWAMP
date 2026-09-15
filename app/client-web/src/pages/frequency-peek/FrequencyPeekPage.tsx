import { WifiOffIcon } from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { HISTORY_LENGTH, useFrequencyPeekSocket } from './useFrequencyPeekSocket'
import type { FrequencySample } from './useFrequencyPeekSocket'
import { Alert, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import {
  Card,
  CardAction,
  CardContent,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'

const WIDTH = 520
const HEIGHT = 56

/** One station's last few seconds of frequency as a line, autoscaled to its
 *  own range so a few mHz of movement is visible. */
function Sparkline({ values }: { values: (number | null)[] }) {
  const present = values.filter((v): v is number => v !== null)
  if (present.length < 2) return <svg width={WIDTH} height={HEIGHT} />
  const min = Math.min(...present)
  const max = Math.max(...present)
  const span = Math.max(max - min, 0.01)
  const points = values
    .map((v, i) => {
      if (v === null) return null
      const x = (i / (HISTORY_LENGTH - 1)) * WIDTH
      const y = HEIGHT - 4 - ((v - min) / span) * (HEIGHT - 8)
      return `${x.toFixed(1)},${y.toFixed(1)}`
    })
    .filter((p): p is string => p !== null)
    .join(' ')
  return (
    <svg width={WIDTH} height={HEIGHT} className="max-w-full">
      <polyline points={points} fill="none" stroke="currentColor" strokeWidth={1.5} />
    </svg>
  )
}

/** The samples, right-aligned into a fixed-length window so the line scrolls
 *  in from the right rather than stretching as the buffer fills. */
function series(history: FrequencySample[], station: string): (number | null)[] {
  const values = history.map((s) => s.frequency_hz[station] ?? null)
  return [...Array<null>(Math.max(HISTORY_LENGTH - values.length, 0)).fill(null), ...values]
}

/**
 * The Frequency peek page (route `/frequency-peek`): a thin renderer over
 * useFrequencyPeekSocket. The server runs the frequency module on this
 * client's live pipeline and pushes each result; this component draws the last
 * ten seconds of it, one line per station, and the value at the cursor.
 */
export function FrequencyPeekPage() {
  const { state, history, status, connected } = useFrequencyPeekSocket()
  const latest = history[history.length - 1]
  const stations = latest ? Object.keys(latest.frequency_hz) : []

  return (
    <Card className="w-full max-w-2xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">Frequency peek</CardTitle>
        <span className="text-gray-500">
          Only the frequency, per station, straight off the live PMU stream.
        </span>
        <CardAction className="flex items-center gap-2 self-center">
          {state?.player.mode === 'live' && connected && (
            <Badge className="bg-red-600 text-white">LIVE</Badge>
          )}
          {connected ? (
            <Badge variant="default">Online</Badge>
          ) : (
            <Badge variant="outline" className="text-muted-foreground">
              <WifiOffIcon className="size-3" />
              Offline
            </Badge>
          )}
        </CardAction>
      </CardHeader>

      <CardContent className="px-6 py-4">
        {connected && latest ? (
          <div className="flex flex-col gap-3">
            {stations.map((station) => (
              <div key={station} className="flex items-center gap-4">
                <div className="w-28 shrink-0">
                  <div className="text-sm text-gray-500">{station}</div>
                  <div className="text-xl font-semibold tabular-nums">
                    {latest.frequency_hz[station]?.toFixed(3) ?? '—'}
                    <span className="ml-1 text-sm font-normal text-gray-500">Hz</span>
                  </div>
                </div>
                <div className="min-w-0 flex-1 text-sky-600">
                  <Sparkline values={series(history, station)} />
                </div>
              </div>
            ))}
            <div className="text-xs text-gray-500 tabular-nums">
              {latest.timestamp} · {history.length} of {HISTORY_LENGTH} samples
            </div>
          </div>
        ) : (
          <div className="flex min-h-[152px] items-center justify-center">
            <Alert
              variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}
              className="w-auto"
            >
              <AlertTitle>
                {status.kind === 'online' ? 'Waiting for the first frame…' : status.label}
              </AlertTitle>
            </Alert>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
