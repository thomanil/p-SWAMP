import { PlayIcon, SquareIcon, WifiOffIcon } from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { useModeEstimationSocket } from './useModeEstimationSocket'
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

/** The load knob: each step is one more identification per wall-clock second. */
const SPEEDS = ['1', '2', '3', '5', '10', '20', '50']

const STATUS_CLASS: Record<string, string> = {
  OK: 'bg-green-600 text-white',
  Alert: 'bg-amber-500 text-white',
  Emergency: 'bg-red-600 text-white',
  Undetermined: 'bg-gray-500 text-white',
}

function num(value: number | null | undefined, digits = 0, unit = ''): string {
  return value === null || value === undefined ? '—' : `${value.toFixed(digits)}${unit}`
}

/**
 * The Mode estimation page (route `/mode-estimation`) — p-SWAMP's N4SID mode
 * estimation as a core module over the N44 line-trip recording. It shows the
 * electromechanical modes the latest identification found and how the
 * pipeline is keeping up with the analysis: how long one identification takes
 * (wall and CPU), how long it waited for a pool slot, how late its result was,
 * and how many fell due while the previous one was still running. Falling
 * behind is reported on the layout's error tray.
 */
export function ModeEstimationPage() {
  const { state, status, connected, play, stop, setSpeed } = useModeEstimationSocket()

  const player = state?.player
  const playing = player !== undefined && !player.paused
  const result = state?.result?.result
  const flow = state?.throughput
  const worker = flow?.module_runs === 'worker'
  const behindSpeed =
    playing && flow?.effective_speed != null && player && flow.effective_speed < player.speed * 0.9

  return (
    <Card className="w-full max-w-2xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">Mode estimation</CardTitle>
        <span className="text-gray-500">
          N4SID over every station&apos;s frequency in a {num(result?.window_s ?? 45)} s window of the
          N44 line-trip recording, once per second of data —{' '}
          {worker ? 'running as its own service, over Kafka' : 'running in the server process'}
          {result ? `, ${result.execution === 'inline' ? 'on the event loop' : `in a ${result.execution} pool`}` : ''}.
          Raise the speed to load the analysis; falling behind is reported on the error tray.
        </span>
        <CardAction className="self-center">
          {!connected ? (
            <Badge variant="outline" className="text-muted-foreground">
              <WifiOffIcon className="size-3" />
              Offline
            </Badge>
          ) : result ? (
            <Badge className={STATUS_CLASS[result.status]}>{result.status}</Badge>
          ) : (
            <Badge variant="secondary">Filling window…</Badge>
          )}
        </CardAction>
      </CardHeader>

      <CardContent className="flex flex-col gap-4 px-6 py-4">
        {!connected || !state ? (
          <div className="flex min-h-[152px] items-center justify-center">
            <Alert
              variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}
              className="w-auto"
            >
              <AlertTitle>{status.kind === 'online' ? 'Waiting for state…' : status.label}</AlertTitle>
            </Alert>
          </div>
        ) : (
          <>
            <section aria-label="Modes">
              {result ? (
                result.modes.length ? (
                  <table className="w-full text-sm">
                    <thead className="text-left text-muted-foreground">
                      <tr>
                        <th className="font-normal">Frequency</th>
                        <th className="font-normal">Damping</th>
                        <th className="font-normal">Stations taking most part</th>
                      </tr>
                    </thead>
                    <tbody className="tabular-nums">
                      {result.modes.map((mode, i) => (
                        <tr key={i}>
                          <td>{mode.freq_hz.toFixed(3)} Hz</td>
                          <td
                            className={
                              mode.damping < 0.03 ? 'text-red-600' : mode.damping < 0.07 ? 'text-amber-600' : ''
                            }
                          >
                            {(mode.damping * 100).toFixed(1)} %
                          </td>
                          <td className="text-xs">{mode.stations.join(' · ')}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <p className="text-sm">No electromechanical mode identified.</p>
                )
              ) : (
                <p className="text-sm">
                  No result yet — the first identification needs a full {num(state.result?.result.window_s ?? 45)} s
                  window of data.
                </p>
              )}
            </section>

            <section
              aria-label="Keep-up"
              className="grid grid-cols-[auto_1fr_auto_1fr] items-center gap-x-3 gap-y-1 text-sm"
            >
              <span className="text-right text-muted-foreground">Frames/s</span>
              <span className="tabular-nums">{num(flow?.frames_per_s)}</span>
              <span className="text-right text-muted-foreground">Speed</span>
              <span className={`tabular-nums ${behindSpeed ? 'text-red-600' : ''}`}>
                {num(flow?.effective_speed, 1, '×')} of {num(player?.speed, 0, '×')}
              </span>

              <span className="text-right text-muted-foreground">Identify</span>
              <span className="tabular-nums">
                {num(result?.compute_ms, 0, ' ms')} ({num(result?.compute_cpu_ms, 0, ' ms')} CPU)
              </span>
              <span className="text-right text-muted-foreground">Queued</span>
              <span className="tabular-nums">{num(result?.queue_ms, 0, ' ms')}</span>

              <span className="text-right text-muted-foreground">Latency</span>
              <span className="tabular-nums">{num(result?.latency_ms, 0, ' ms')}</span>
              <span className="text-right text-muted-foreground">Skipped</span>
              <span className={`tabular-nums ${result?.evaluations_skipped ? 'text-red-600' : ''}`}>
                {result ? `${result.evaluations_skipped} of ${result.evaluations}` : '—'}
              </span>

              <span className="text-right text-muted-foreground">Input age</span>
              <span className="tabular-nums">{worker ? num(result?.input_age_s, 3, ' s') : 'in-process'}</span>
              <span className="text-right text-muted-foreground">Dropped</span>
              <span className="tabular-nums">
                {worker
                  ? `server ${flow?.queue_dropped ?? 0} · worker ${result?.input_dropped ?? 0}`
                  : `${flow?.queue_dropped ?? 0}`}
              </span>
            </section>
          </>
        )}
      </CardContent>

      <CardFooter className="flex items-center justify-center gap-3 border-t pt-6">
        <span className="text-sm tabular-nums text-muted-foreground">
          t = {num(state?.offset_s, 1)} / {num(state?.duration_s, 0)} s
        </span>
        <Button
          variant={connected && !playing ? 'default' : 'outline'}
          size="icon"
          aria-label="Play"
          disabled={!connected}
          onClick={play}
        >
          <PlayIcon />
        </Button>
        <Button
          variant={playing ? 'default' : 'outline'}
          size="icon"
          aria-label="Stop"
          disabled={!connected}
          onClick={stop}
        >
          <SquareIcon />
        </Button>
        <Select
          value={player ? String(player.speed) : '1'}
          onValueChange={(value) => setSpeed(Number(value))}
          disabled={!connected}
        >
          <SelectTrigger className="w-24" aria-label="Speed">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {SPEEDS.map((speed) => (
              <SelectItem key={speed} value={speed}>
                {speed}×
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </CardFooter>
    </Card>
  )
}
