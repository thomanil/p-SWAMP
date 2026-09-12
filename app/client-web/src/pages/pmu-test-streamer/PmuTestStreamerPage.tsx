import {
  PlayIcon,
  SquareIcon,
  SkipBackIcon,
  SkipForwardIcon,
  WifiOffIcon,
} from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { usePmuStreamSocket } from './usePmuStreamSocket'
import { StreamWindow } from './StreamWindow'
import { ReportCard } from './pmu-report/ReportCard'
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

const SPEEDS = [0.25, 0.5, 1, 2, 5, 10]
const WINDOW_ROWS = 8

/**
 * The PMU test streamer page (route `/pmu-test-streamer`) — the thin slice of
 * the data-integration architecture, as a screen: one PMU stream replayed per
 * browser through the server's data gateway (this card), and a batch report
 * over the same stream, asked for by POST and answered on a socket (the card
 * below).
 *
 * A thin renderer over usePmuStreamSocket: it draws the played samples, offers
 * the transport controls, and shows a status banner (disabling controls)
 * whenever it isn't connected. Transport controls appear only when the source
 * says it is a replay — a live feed has nothing to pause.
 */
export function PmuTestStreamerPage() {
  const { header, recent, state, status, connected, play, stop, forward, back, setSpeed, seek } =
    usePmuStreamSocket()

  const replay = state?.mode === 'replay'
  const ready = connected && header !== null && state !== null

  return (
    <div className="flex w-full max-w-2xl flex-col gap-6">
      <Card className="w-full gap-0">
        <CardHeader className="border-b">
          <CardTitle className="text-lg">PMU Test Streamer</CardTitle>
          <span className="text-gray-500">
            One PMU stream per browser, replayed through the server&apos;s data gateway.
            {header ? ` Source: ${header.source || header.stream_id}, ${header.channels.length} channels at ${header.data_rate} Hz.` : ''}
          </span>
          <CardAction className="self-center">
            {connected ? (
              <Badge variant={state?.playing ? 'default' : 'secondary'}>
                {state?.mode === 'live' ? 'Live' : state?.playing ? 'Playing' : 'Paused'}
              </Badge>
            ) : (
              <Badge variant="outline" className="text-muted-foreground">
                <WifiOffIcon className="size-3" />
                Offline
              </Badge>
            )}
          </CardAction>
        </CardHeader>

        <CardContent className="px-6 py-0">
          {ready ? (
            <StreamWindow header={header} recent={recent} rows={WINDOW_ROWS} />
          ) : (
            <div className="flex min-h-[152px] items-center justify-center">
              <Alert
                variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}
                className="w-auto"
              >
                <AlertTitle>
                  {status.kind === 'online' ? 'Waiting for state…' : status.label}
                </AlertTitle>
              </Alert>
            </div>
          )}
        </CardContent>

        <CardFooter className="flex-col gap-4 border-t pt-6">
          {/* Position readout — the pass counter is how the loop at the end of
              the source becomes visible. */}
          <div className="grid w-full max-w-md grid-cols-[auto_1fr] items-center gap-x-3 gap-y-2">
            <label className="text-right text-sm text-muted-foreground">Sample</label>
            <span className="text-sm tabular-nums">
              {state && state.index >= 0
                ? `${state.index + 1} of ${state.total} · t = ${state.position_s?.toFixed(2)} s · pass ${state.passes + 1}`
                : '—'}
            </span>
            {replay && state && (
              <>
                <label className="text-right text-sm text-muted-foreground">Seek</label>
                <input
                  type="range"
                  min={0}
                  max={state.duration_s}
                  step={0.05}
                  value={state.position_s ?? 0}
                  disabled={!ready}
                  onChange={(event) => seek(Number(event.target.value))}
                  aria-label="Seek"
                />
                <label className="text-right text-sm text-muted-foreground">Speed</label>
                <select
                  className="w-24 rounded-md border bg-background px-2 py-1 text-sm"
                  value={state.speed}
                  disabled={!ready}
                  onChange={(event) => setSpeed(Number(event.target.value))}
                  aria-label="Speed"
                >
                  {[...new Set([...SPEEDS, state.speed])]
                    .sort((a, b) => a - b)
                    .map((speed) => (
                      <option key={speed} value={speed}>
                        ×{speed}
                      </option>
                    ))}
                </select>
              </>
            )}
          </div>

          {/* Transport controls — disabled until connected, absent for a live source. */}
          {replay && (
            <div className="flex items-center justify-center gap-2">
              <Button variant="outline" size="icon" aria-label="Step back" disabled={!ready} onClick={back}>
                <SkipBackIcon />
              </Button>
              <Button
                variant={state?.playing ? 'outline' : 'default'}
                size="icon"
                aria-label="Play"
                disabled={!ready}
                onClick={play}
              >
                <PlayIcon />
              </Button>
              <Button
                variant={state?.playing ? 'default' : 'outline'}
                size="icon"
                aria-label="Stop"
                disabled={!ready}
                onClick={stop}
              >
                <SquareIcon />
              </Button>
              <Button variant="outline" size="icon" aria-label="Step forward" disabled={!ready} onClick={forward}>
                <SkipForwardIcon />
              </Button>
            </div>
          )}
        </CardFooter>
      </Card>

      <ReportCard />
    </div>
  )
}
