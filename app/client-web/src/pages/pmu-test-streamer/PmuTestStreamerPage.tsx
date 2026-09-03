import { PlayIcon, SquareIcon, WifiOffIcon, TriangleAlertIcon } from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { usePmuStreamSocket, type Broker } from './usePmuStreamSocket'
import { StreamWindow } from './StreamWindow'
import { Alert, AlertTitle } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Card,
  CardAction,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import { cn } from '@/lib/utils'
import type { Wire } from '@/api/wire'

const BROKERS: { id: Broker; label: string }[] = [
  { id: 'kafka', label: 'Kafka' },
  { id: 'nats', label: 'NATS' },
]

/**
 * The PMU streamer page (route `/pmu-test-streamer`) — the Kafka-vs-NATS
 * experiment. The same PMU sample is looped into BOTH a Kafka topic and a NATS
 * subject by a separate producer service; this page picks which of the two live
 * pipes the server retransmits from, and shows end-to-end latency + throughput so
 * the two are comparable at a glance.
 *
 * A thin renderer over usePmuStreamSocket: state arrives on the socket, the
 * broker toggle and play/stop go up as POSTs.
 */
export function PmuTestStreamerPage() {
  const { state, status, connected, selectBroker, play, stop } = usePmuStreamSocket()

  return (
    <Card className="w-full max-w-xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">PMU Streamer — Kafka vs NATS</CardTitle>
        <span className="text-gray-500">
          The same PMU sample is looped into a Kafka topic and a NATS subject by a
          separate producer. Pick which live pipe to retransmit and compare
          latency &amp; throughput.
        </span>
        <CardAction className="self-center">
          {connected ? (
            <Badge variant={state?.playing ? 'default' : 'secondary'}>
              {state?.playing ? 'Playing' : 'Paused'}
            </Badge>
          ) : (
            <Badge variant="outline" className="text-muted-foreground">
              <WifiOffIcon className="size-3" />
              Offline
            </Badge>
          )}
        </CardAction>
      </CardHeader>

      <CardContent className="flex flex-col gap-4 px-6 py-4">
        {/* Broker toggle: which of the two live pipes to retransmit from. */}
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Pipe</span>
          <div className="flex gap-1">
            {BROKERS.map(({ id, label }) => {
              const active = state?.broker === id
              return (
                <Button
                  key={id}
                  size="sm"
                  variant={active ? 'default' : 'outline'}
                  disabled={!connected}
                  aria-pressed={active}
                  onClick={() => selectBroker(id)}
                >
                  {label}
                </Button>
              )
            })}
          </div>
        </div>

        {/* The record window, or the status banner in its place when not
            connected; a pipe-down warning sits above it when present. */}
        {connected && state ? (
          <div>
            {state.error && (
              <Alert variant="destructive" className="mb-2">
                <TriangleAlertIcon className="size-4" />
                <AlertTitle>{state.error}</AlertTitle>
              </Alert>
            )}
            <StreamWindow window={state.window} />
          </div>
        ) : (
          <div className="flex min-h-[152px] items-center justify-center">
            <Alert
              variant={
                status.kind === 'offline' && status.isError ? 'destructive' : 'default'
              }
              className="w-auto"
            >
              <AlertTitle>
                {status.kind === 'online' ? 'Waiting for state…' : status.label}
              </AlertTitle>
            </Alert>
          </div>
        )}

        {/* Live comparison numbers for the active pipe. Latency and throughput
            each show the current reading plus min/avg/max since the last switch,
            so jitter and drift are visible rather than just the instantaneous
            value. */}
        <div className="grid grid-cols-3 gap-2">
          <StatTile
            label="Latency"
            stats={state?.metrics.latency_ms}
            unit="ms"
            digits={1}
          />
          <StatTile
            label="Throughput"
            stats={state?.metrics.throughput_hz}
            unit="rec/s"
            digits={0}
          />
          <Metric
            label="Received"
            value={state ? String(state.metrics.received) : '—'}
            unit=""
          />
        </div>
      </CardContent>

      <CardFooter className="flex-col gap-4 border-t pt-6">
        {/* Transport controls — pause/resume forwarding, disabled until connected. */}
        <div className="flex items-center justify-center gap-2">
          <Button
            variant={state?.playing ? 'outline' : 'default'}
            size="icon"
            aria-label="Play"
            disabled={!connected}
            onClick={play}
          >
            <PlayIcon />
          </Button>
          <Button
            variant={state?.playing ? 'default' : 'outline'}
            size="icon"
            aria-label="Stop"
            disabled={!connected}
            onClick={stop}
          >
            <SquareIcon />
          </Button>
        </div>
      </CardFooter>
    </Card>
  )
}

/** One labelled stat tile in the metrics row. */
function Metric({ label, value, unit }: { label: string; value: string; unit: string }) {
  return (
    <div className={cn('rounded-md border px-3 py-2 text-center')}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg tabular-nums">
        {value}
        {unit && <span className="ml-1 text-xs text-muted-foreground">{unit}</span>}
      </div>
    </div>
  )
}

/** A stat tile that shows the current reading big, plus a min/avg/max sub-line —
 *  for latency and throughput, where the spread over time is the interesting part
 *  of the Kafka-vs-NATS comparison, not just the instant value. */
function StatTile({
  label,
  stats,
  unit,
  digits,
}: {
  label: string
  stats: Wire['MetricStats'] | undefined
  unit: string
  digits: number
}) {
  const fmt = (n: number) => n.toFixed(digits)
  return (
    <div className={cn('rounded-md border px-3 py-2 text-center')}>
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="text-lg tabular-nums">
        {stats ? fmt(stats.current) : '—'}
        {unit && <span className="ml-1 text-xs text-muted-foreground">{unit}</span>}
      </div>
      <div className="mt-1 flex justify-center gap-2 text-[10px] tabular-nums text-muted-foreground">
        {stats ? (
          <>
            <span>min {fmt(stats.min)}</span>
            <span>avg {fmt(stats.mean)}</span>
            <span>max {fmt(stats.max)}</span>
          </>
        ) : (
          <span>&nbsp;</span>
        )}
      </div>
    </div>
  )
}
