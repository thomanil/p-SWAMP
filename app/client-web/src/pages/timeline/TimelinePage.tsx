import {
  PlayIcon,
  SquareIcon,
  SkipBackIcon,
  SkipForwardIcon,
  WifiOffIcon,
} from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { useTimelineSocket } from './useTimelineSocket'
import { TimelineWindow } from './TimelineWindow'
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

/**
 * The timeline page (route `/timeline`, and the index route) — a port of the Qt
 * client (timeline_client.py) onto shadcn/ui. A thin renderer over
 * useTimelineSocket: it draws the server-pushed window, exposes the sequence
 * picker and the transport controls, and shows a status banner (disabling
 * controls) whenever it isn't connected.
 */
export function TimelinePage() {
  const { state, status, connected, play, stop, forward, back, setSequence } =
    useTimelineSocket()

  return (
    <Card className="w-full max-w-xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">Timeline</CardTitle>
        <span className="text-gray-500">Example subapp. Streaming number sequence state, command and state streamed from the server</span>
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
        {/* Timeline, or the status banner in its place when not connected */}
        {connected && state ? (
          <TimelineWindow window={state.window} />
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
        {/* Sequence picker — the options come from the server. */}
        <div className="grid w-full max-w-xs grid-cols-[auto_1fr] items-center gap-x-3 gap-y-2">
          <label className="text-right text-sm text-muted-foreground">Sequence</label>
          <Select
            value={state?.sequenceName ?? ''}
            onValueChange={setSequence}
            disabled={!connected || !state}
          >
            <SelectTrigger className="w-full">
              <SelectValue placeholder="—" />
            </SelectTrigger>
            <SelectContent>
              {state?.sequences.map((name) => (
                <SelectItem key={name} value={name}>
                  {name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Transport controls — disabled until connected, like the Qt client. */}
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
