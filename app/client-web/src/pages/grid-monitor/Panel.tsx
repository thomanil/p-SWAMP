import type { ReactNode } from 'react'
import { Link } from 'react-router'
import { Maximize2Icon } from 'lucide-react'

import { Alert, AlertTitle } from '@/components/ui/alert'
import {
  Card,
  CardAction,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from '@/components/ui/card'
import type { ConnStatus } from '@/hooks/useServerSocket'
import { cn } from '@/lib/utils'

/**
 * The card every monitor panel renders inside.
 *
 * Each panel has its own WebSocket, so each reports its own connection state —
 * one card can be dark while its neighbours are live, and that should be
 * visible rather than smoothed over. This shell owns that: the waiting/offline
 * banner used to be copy-pasted, identically, in all four pages.
 *
 * `minBodyClass` reserves the body's height so the grid does not jump when the
 * first message lands, or when a table gains rows.
 */
export function Panel({
  title,
  subtitle,
  badge,
  status,
  ready,
  drawsWithoutData = false,
  focusHref,
  minBodyClass = 'min-h-[240px]',
  footer,
  className,
  contentClassName,
  children,
}: {
  title: string
  subtitle?: string
  /** Live indicator shown at the top right, beside the expand link. */
  badge?: ReactNode
  /** This panel's own socket state. */
  status: ConnStatus
  /** Connected *and* the first message has arrived. */
  ready: boolean
  /** Set when the body is worth showing before any message arrives — the grid
   *  map, whose topology is static and fetched separately from the socket that
   *  colours it. Such a panel renders its body immediately, with the waiting
   *  notice above it rather than in place of it. Panels whose body is nothing
   *  but live data (a dial with no phasors, a table with no rows) leave this
   *  off, so they show the notice alone rather than an empty frame. */
  drawsWithoutData?: boolean
  /** Link to this panel's full-size route. Omitted on that route itself, so a
   *  panel never offers to open the page you are already on. */
  focusHref?: string
  minBodyClass?: string
  footer?: ReactNode
  className?: string
  contentClassName?: string
  children: ReactNode
}) {
  const notice = (
    <Alert
      variant={
        status.kind === 'offline' && status.isError ? 'destructive' : 'default'
      }
    >
      <AlertTitle>
        {status.kind === 'online' ? 'Waiting for state…' : status.label}
      </AlertTitle>
    </Alert>
  )

  return (
    // min-w-0: a grid item defaults to min-width:auto, so a wide canvas or table
    // inside would otherwise force its whole column open.
    <Card className={cn('min-w-0 gap-0', className)}>
      <CardHeader className="border-b">
        <CardTitle className="text-base">{title}</CardTitle>
        {subtitle && <span className="text-sm text-gray-500">{subtitle}</span>}
        <CardAction className="flex items-center gap-2 self-center">
          {badge}
          {focusHref && (
            <Link
              to={focusHref}
              aria-label={`Open ${title} full size`}
              className="text-muted-foreground transition-colors hover:text-foreground"
            >
              <Maximize2Icon className="size-4" />
            </Link>
          )}
        </CardAction>
      </CardHeader>

      <CardContent className={cn('pt-6', contentClassName)}>
        {ready ? (
          children
        ) : drawsWithoutData ? (
          <div className="space-y-3">
            {notice}
            {children}
          </div>
        ) : (
          <div className={cn('flex items-center justify-center', minBodyClass)}>
            {notice}
          </div>
        )}
      </CardContent>

      {footer && (
        <CardFooter className="border-t pt-4 text-sm text-muted-foreground tabular-nums">
          {footer}
        </CardFooter>
      )}
    </Card>
  )
}
