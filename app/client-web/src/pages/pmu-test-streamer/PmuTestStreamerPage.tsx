import { useEffect, useState } from 'react'
import {
  PlayIcon,
  RotateCcwIcon,
  SquareIcon,
  SkipBackIcon,
  SkipForwardIcon,
  WifiOffIcon,
} from 'lucide-react'

// This page's own pieces, imported relatively so the folder stays self-contained.
import { usePmuStreamSocket, type Source } from './usePmuStreamSocket'
import { FrameTable } from './FrameTable'
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

const SPEEDS = ['0.5', '1', '2', '5']

/** The sources, in the order the switch shows them. Adding a source to the
 *  server's `Source` type makes this a type error until it has a label. */
const SOURCE_LABELS: Record<Source, string> = {
  local: 'Local recording',
  remote: 'Remote recording',
  live: 'Live',
}

/** Seconds between two instants the server sent as ISO strings. */
function secondsBetween(from: string | null | undefined, to: string | null | undefined): number | null {
  if (!from || !to) return null
  return (new Date(to).getTime() - new Date(from).getTime()) / 1000
}

/** A wall-clock instant as `HH:MM:SS.mmm UTC`, for the live readout. */
function clockTime(iso: string): string {
  return `${new Date(iso).toISOString().slice(11, 23)} UTC`
}

/**
 * The PMU test streamer page (route `/pmu-test-streamer`) — the thin slice of
 * the data architecture, seen from the browser. A thin renderer over
 * usePmuStreamSocket: it draws the frame at the cursor as a station table, the
 * stats module's result for it, a source switch, and the replay
 * controls — all rendered from the server's state. On the shared live stream the
 * transport controls are disabled and a red LIVE badge says why; a control the
 * source cannot honour is disabled rather than dead.
 */
export function PmuTestStreamerPage() {
  const {
    state,
    header,
    status,
    connected,
    play,
    stop,
    forward,
    back,
    seek,
    setSpeed,
    chooseSource,
    resetStats,
  } = usePmuStreamSocket()

  // The slider's position while it is being dragged; null when it follows the
  // cursor. A drag is committed (one seek) on release — and the release is
  // listened for on the window, not only on the slider, because a pointer let
  // go outside it, a cancelled pointer or a touch would otherwise leave this
  // non-null and the thumb frozen where it was dropped. A one-second timer is
  // the last resort for any release that no event reports.
  const [scrub, setScrub] = useState<number | null>(null)
  useEffect(() => {
    if (scrub === null) return
    const commit = () => {
      seek(scrub)
      setScrub(null)
    }
    const timer = setTimeout(commit, 1000)
    window.addEventListener('pointerup', commit)
    window.addEventListener('pointercancel', commit)
    return () => {
      clearTimeout(timer)
      window.removeEventListener('pointerup', commit)
      window.removeEventListener('pointercancel', commit)
    }
  }, [scrub, seek])

  const player = state?.player
  const source = state?.source
  const live = source === 'live'
  // "Playing" is a replay notion: live is never paused, but its transport row
  // is disabled, and a filled Stop button there would read as an active one.
  const playing = player !== undefined && !player.paused && !live
  const offset = secondsBetween(player?.coverage_start, player?.cursor) ?? 0
  const duration = secondsBetween(player?.coverage_start, player?.coverage_end) ?? 0
  const interval = header ? 1 / header.data_rate : (player?.frame_interval_s ?? 0.05)
  // What the player says applies: seek/step over a source with history, the
  // switch to live over a source that can tail, and the transport row only
  // while the recording is what is open.
  const canSeek = connected && !live && player?.can_seek === true
  const transport = connected && !live
  const stats = state?.stats?.result
  const error = state?.error

  return (
    <Card className="w-full max-w-xl gap-0">
      <CardHeader className="border-b">
        <CardTitle className="text-lg">PMU Test Streamer</CardTitle>
        <span className="text-gray-500">
          A recorded or live PMU feed through the core: provider → gateway → player → bus →
          this page, with a stats module listening on the same bus.
        </span>
        <CardAction className="self-center">
          {connected && live ? (
            <Badge className="bg-red-600 text-white" aria-label="Live">
              <span className="size-2 rounded-full bg-white animate-pulse" aria-hidden />
              LIVE · {state?.live_viewers ?? 0} watching
            </Badge>
          ) : connected ? (
            <Badge variant={playing ? 'default' : 'secondary'}>
              {source ? SOURCE_LABELS[source] : 'Recorded'} ·{' '}
              {player?.ended ? 'Ended' : playing ? 'Playing' : 'Paused'}
            </Badge>
          ) : (
            <Badge variant="outline" className="text-muted-foreground">
              <WifiOffIcon className="size-3" />
              Offline
            </Badge>
          )}
        </CardAction>
      </CardHeader>

      <CardContent className="px-6 py-4">
        {connected && state && header ? (
          <FrameTable header={header} frame={state.frame} />
        ) : (
          <div className="flex min-h-[152px] items-center justify-center">
            <Alert
              variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}
              className="w-auto"
            >
              <AlertTitle>
                {status.kind === 'online'
                  ? state
                    ? 'No frame yet — press Play to see the stream'
                    : 'Waiting for state…'
                  : status.label}
              </AlertTitle>
            </Alert>
          </div>
        )}
      </CardContent>

      <CardFooter className="flex-col gap-4 border-t pt-6">
        {/* The source: this browser's own recording, or the live stream every
            viewer shares. A button fires only when it would change
            something; one the server has no source for is disabled. There
            is no toggle-group in ui/; buttons in a bordered group suffice. */}
        <div
          role="group"
          aria-label="Source"
          className="inline-flex items-center gap-1 rounded-lg border p-1"
        >
          {(Object.keys(SOURCE_LABELS) as Source[]).map((choice) => (
            <Button
              key={choice}
              size="sm"
              variant={source === choice ? 'default' : 'ghost'}
              className={
                source === choice && choice === 'live'
                  ? 'bg-red-600 text-white hover:bg-red-600/90'
                  : undefined
              }
              aria-pressed={source === choice}
              disabled={!connected || !state?.sources_available.includes(choice)}
              onClick={() => {
                if (source !== choice) chooseSource(choice)
              }}
            >
              {SOURCE_LABELS[choice]}
            </Button>
          ))}
        </div>

        {/* Position and the module's result for the frame at the cursor. */}
        <div className="grid w-full grid-cols-[auto_1fr] items-center gap-x-3 gap-y-1 text-sm">
          <span className="text-right text-muted-foreground">Frame</span>
          <span className="tabular-nums">
            {live && player?.cursor
              ? `Live · ${clockTime(player.cursor)}`
              : state?.frame_index !== null && state?.frame_index !== undefined
                ? `${state.frame_index + 1} of ${state.frame_count ?? '?'} · t = ${offset.toFixed(2)} s`
                : '—'}
          </span>
          <span className="text-right text-muted-foreground">Stats</span>
          <span className="tabular-nums">
            {stats && stats.mean_frequency_hz !== null
              ? `mean f ${stats.mean_frequency_hz.toFixed(4)} Hz` +
                ` · spread ${stats.angle_spread_deg?.toFixed(2) ?? '—'}°` +
                ` · ${stats.n_stations} stations`
              : '—'}
          </span>
          {/* The module's running figures since its last reset, and the reset
              itself: a command to the module, not to the player. */}
          <span className="text-right text-muted-foreground">Since reset</span>
          <span className="flex items-center gap-2 tabular-nums">
            {stats
              ? `${stats.frames_since_reset} frames · peak spread ${stats.peak_angle_spread_deg?.toFixed(2) ?? '—'}°`
              : '—'}
            <Button
              variant="ghost"
              size="icon"
              className="size-6"
              aria-label="Reset stats"
              disabled={!transport || !stats}
              onClick={resetStats}
            >
              <RotateCcwIcon className="size-3" />
            </Button>
          </span>
          {/* Stamped on every frame by the gateway's enricher, handed on by the module. */}
          <span className="text-right text-muted-foreground">CIM ref</span>
          <span className="font-mono text-xs">{stats?.cim_reference_id ?? '—'}</span>
        </div>

        {error && (
          <p className="w-full text-xs text-destructive" role="status">
            {error.source}: {error.message}
            {error.detail ? ` — ${error.detail}` : ''}
          </p>
        )}

        {/* Seek: a scrub bar over the recording, enabled only when the player
            says the source can be repositioned. Seeks on release (see the effect
            above), not on every pixel of a drag; a keyboard step commits on key
            release. */}
        <div className="flex w-full items-center gap-3 text-xs text-muted-foreground">
          <span className="whitespace-nowrap tabular-nums">0 s</span>
          <input
            type="range"
            aria-label="Seek"
            className="w-full"
            min={0}
            max={Math.max(duration - interval, 0)}
            step={interval}
            value={scrub ?? (live ? 0 : offset)}
            disabled={!canSeek}
            onChange={(event) => setScrub(Number(event.target.value))}
            onKeyUp={() => {
              if (scrub !== null) {
                seek(scrub)
                setScrub(null)
              }
            }}
            onBlur={() => {
              if (scrub !== null) {
                seek(scrub)
                setScrub(null)
              }
            }}
          />
          <span className="whitespace-nowrap tabular-nums">{duration.toFixed(1)} s</span>
        </div>

        {/* Transport controls — disabled until connected, and while live; seek
            and step-back additionally need a source with history. */}
        <div className="flex items-center justify-center gap-2">
          <Button
            variant="outline"
            size="icon"
            aria-label="Step back"
            disabled={!canSeek}
            onClick={back}
          >
            <SkipBackIcon />
          </Button>
          <Button
            variant={transport && !playing ? 'default' : 'outline'}
            size="icon"
            aria-label="Play"
            disabled={!transport}
            onClick={play}
          >
            <PlayIcon />
          </Button>
          <Button
            variant={playing ? 'default' : 'outline'}
            size="icon"
            aria-label="Stop"
            disabled={!transport}
            onClick={stop}
          >
            <SquareIcon />
          </Button>
          <Button
            variant="outline"
            size="icon"
            aria-label="Step forward"
            disabled={!transport}
            onClick={forward}
          >
            <SkipForwardIcon />
          </Button>
          <Select
            value={player ? String(player.speed) : '1'}
            onValueChange={(value) => setSpeed(Number(value))}
            disabled={!transport}
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
        </div>
      </CardFooter>
    </Card>
  )
}
