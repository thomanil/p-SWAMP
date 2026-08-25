import { RadioIcon, WifiOffIcon } from 'lucide-react'

import { Badge } from '@/components/ui/badge'

import { Panel } from '../Panel'
import type { PanelVariant } from '../variant'
import { AppStatusTable } from './AppStatusTable'
import { useAppStatusSocket } from './useAppStatusSocket'

/**
 * Which monitoring applications are running and what they report — the web
 * counterpart of p-SWAMP's Qt status dock.
 *
 * The applications listed here are process-wide, not per client, so every
 * browser sees the same rows. The Qt version's Stop and Open-console buttons are
 * deliberately absent: both publish to a command topic that only exists with a
 * broker behind it.
 */
export function AppStatusPanel({
  variant = 'dashboard',
}: {
  variant?: PanelVariant
}) {
  const { state, status, connected } = useAppStatusSocket()
  const replay = state?.replay
  const ready = connected && state !== null

  return (
    <Panel
      title="Application Status"
      subtitle={
        variant === 'focused'
          ? 'Monitoring applications running against the replayed PMU stream'
          : undefined
      }
      status={status}
      ready={ready}
      className={variant === 'focused' ? 'w-full max-w-3xl' : undefined}
      focusHref={variant === 'dashboard' ? '/app-status' : undefined}
      minBodyClass="min-h-[152px]"
      contentClassName="px-0 pt-0"
      badge={
        connected ? (
          <Badge variant={replay?.playing ? 'default' : 'secondary'}>
            <RadioIcon className="size-3" />
            {replay?.playing ? 'Streaming' : 'Stopped'}
          </Badge>
        ) : (
          <Badge variant="outline" className="text-muted-foreground">
            <WifiOffIcon className="size-3" />
            Offline
          </Badge>
        )
      }
      // The only view of the replay position anywhere in the client — keep it.
      footer={
        replay
          ? `PMU replay · ${replay.position.toFixed(1)}s / ${replay.duration.toFixed(0)}s · ` +
            `${replay.n_channels} channels @ ${replay.data_rate} Hz`
          : undefined
      }
    >
      <AppStatusTable apps={state?.apps ?? []} serverTime={state?.server_time ?? 0} />
    </Panel>
  )
}
