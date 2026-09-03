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

/**
 * The PMU test streamer page (route `/pmu-test-streamer`) — a scaffold demo
 * streaming sample grid records line by line. A thin renderer over
 * usePmuStreamSocket: it draws the server-pushed
 * window of records, offers the transport controls, and shows a status banner
 * (disabling controls) whenever it isn't connected.
 *
 * There is no sequence picker: one data file, one stream. The records are a one-off
 * PMU sample committed for testing — see
 * app/server-python/src/pmu_test_streamer/sample_data.txt.
 */
export function PmuTestStreamerPage() {
  const { state, status, connected, play, stop, forward, back } = usePmuStreamSocket()

  return (
    <Card className="w-full max-w-xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">PMU Test Streamer</CardTitle>
        <span className="text-gray-500">
          Example subapp. Raw dump of PMU data streamed from server (just a static textfile of sample data for now)
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

      <CardContent className="px-6 py-0">
        {/* The record window, or the status banner in its place when not
            connected. */}
        {connected && state ? (
          <StreamWindow window={state.window} />
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
        {/* Position readout — also how the wrap-around at the end of the file
            becomes visible: the count returns to 1 rather than stalling. */}
        <div className="grid w-full max-w-xs grid-cols-[auto_1fr] items-center gap-x-3 gap-y-2">
          <label className="text-right text-sm text-muted-foreground">Record</label>
          <span className="text-sm tabular-nums">
            {state ? `${state.index + 1} of ${state.total_lines}` : '—'}
          </span>
        </div>

        {/* Transport controls — disabled until connected. */}
        <div className="flex items-center justify-center gap-2">
          <Button
            variant="outline"
            size="icon"
            aria-label="Step back"
            disabled={!connected}
            onClick={back}
          >
            <SkipBackIcon />
          </Button>
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
          <Button
            variant="outline"
            size="icon"
            aria-label="Step forward"
            disabled={!connected}
            onClick={forward}
          >
            <SkipForwardIcon />
          </Button>
        </div>
      </CardFooter>
    </Card>
  )
}
