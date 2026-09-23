import { PlayIcon, SquareIcon, WifiOffIcon } from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { useIslandingStreamSocket } from './useIslandingStreamSocket'
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

/** The load knob: 1× is the recording's own 50 frames/s, 50× is 2500. */
const SPEEDS = ['1', '2', '5', '10', '20', '50']

function num(value: number | null | undefined, digits = 0, unit = ''): string {
  return value === null || value === undefined ? '—' : `${value.toFixed(digits)}${unit}`
}

/**
 * The Islanding stream page (route `/islanding-stream`) — p-SWAMP's islanding
 * detector as a core module over the N44 line-trip recording, driven hard. It
 * shows what the module found (the separated station groups) and how the
 * pipeline is keeping up with it: what the player emits, how far the replay
 * really advances, what crossed the topic and what was dropped on each side.
 * When either side falls behind, the core reports it on the layout's error
 * tray; this page only shows the readings.
 */
export function IslandingStreamPage() {
  const { state, status, connected, play, stop, setSpeed } = useIslandingStreamSocket()

  const player = state?.player
  const playing = player !== undefined && !player.paused
  const result = state?.result?.result
  const flow = state?.throughput
  const worker = flow?.module_runs === 'worker'
  // The speed asked for, against the speed achieved: the gap is the first sign
  // of the server itself not keeping up (the player drops time rather than
  // bursting when it is late).
  const behindSpeed =
    playing && flow?.effective_speed != null && player && flow.effective_speed < player.speed * 0.9

  return (
    <Card className="w-full max-w-2xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">Islanding stream</CardTitle>
        <span className="text-gray-500">
          The N44 line-trip recording (44 stations, 700 channels, 50 Hz) replayed through the
          core into p-SWAMP&apos;s islanding detector — {worker ? 'running as its own service, over Kafka' : 'running in the server process'}.
          Raise the speed to load it; falling behind is reported on the error tray.
        </span>
        <CardAction className="self-center">
          {!connected ? (
            <Badge variant="outline" className="text-muted-foreground">
              <WifiOffIcon className="size-3" />
              Offline
            </Badge>
          ) : result?.status === 'Emergency' ? (
            <Badge className="bg-red-600 text-white">Islanding</Badge>
          ) : result ? (
            <Badge className="bg-green-600 text-white">OK</Badge>
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
            <section aria-label="Islands" className="flex flex-col gap-2">
              <h3 className="text-sm font-medium">
                {result
                  ? result.islands.length
                    ? `${result.islands.length} separated group${result.islands.length > 1 ? 's' : ''} · ${result.main_system} of ${result.stations} stations in the main system`
                    : `No islands · all ${result.stations} stations move together`
                  : 'No result yet — the detector needs a full 10 s window'}
              </h3>
              {result?.islands.map((stations, i) => (
                <div key={i} className="flex flex-wrap items-center gap-1 text-xs">
                  <span className="mr-1 text-muted-foreground">Island {i + 1}</span>
                  {stations.map((station) => (
                    <Badge key={station} variant="outline" className="tabular-nums">
                      {station}
                    </Badge>
                  ))}
                </div>
              ))}
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

              <span className="text-right text-muted-foreground">Detect</span>
              <span className="tabular-nums">{num(result?.detect_ms, 2, ' ms')}</span>
              <span className="text-right text-muted-foreground">Frames/result</span>
              <span className="tabular-nums">{num(result?.frames_in)}</span>

              <span className="text-right text-muted-foreground">Input age</span>
              <span className="tabular-nums">
                {worker ? num(result?.input_age_s, 3, ' s') : 'in-process'}
              </span>
              <span className="text-right text-muted-foreground">Dropped</span>
              <span className="tabular-nums">
                {worker
                  ? `server ${flow?.queue_dropped ?? 0} · worker ${result?.input_dropped ?? 0}`
                  : `${flow?.queue_dropped ?? 0}`}
              </span>

              {worker && (
                <>
                  <span className="text-right text-muted-foreground">Published</span>
                  <span className="tabular-nums">{flow?.published ?? 0}</span>
                  <span className="text-right text-muted-foreground">Failed</span>
                  <span className="tabular-nums">{flow?.publish_failed ?? 0}</span>
                </>
              )}
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
