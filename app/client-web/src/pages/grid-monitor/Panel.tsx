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

import type { PanelVariant } from './variant'

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
 *
 * **It also owns the dashboard/focused convention**, which is why it takes
 * `variant`. Every panel is rendered twice — compact in the grid on `/`, and
 * full-size on its own route — and the difference is the same three decisions
 * every time: the subtitle is worth its space only when focused, the expand link
 * only makes sense on the dashboard (a focused panel must not offer to open the
 * page you are already on), and the width applies only when the panel owns the
 * page. Each panel used to spell those out as three `variant === …` ternaries of
 * its own — six copies of one convention, and six chances to get it subtly
 * different. A panel now states facts (here is my subtitle, my route, my width)
 * and this decides when they apply.
 */
export function Panel({
  title,
  subtitle,
  badge,
  status,
  ready,
  variant = 'dashboard',
  drawsWithoutData = false,
  focusHref,
  focusedClassName,
  minBodyClass = 'min-h-[240px]',
  footer,
  className,
  contentClassName,
  children,
}: {
  title: string
  /** One line under the title, shown in the focused variant only: on the
   *  dashboard the panels around it are the context, and the space is not there
   *  to spare. */
  subtitle?: string
  /** Live indicator shown at the top right, beside the expand link. */
  badge?: ReactNode
  /** This panel's own socket state. */
  status: ConnStatus
  /** Connected *and* the first message has arrived. */
  ready: boolean
  /** How this panel is being rendered: in the dashboard grid, or as its own
   *  page. Defaults to the grid. */
  variant?: PanelVariant
  /** Set when the body is worth showing before any message arrives — the grid
   *  map, whose topology is static and fetched separately from the socket that
   *  colours it. Such a panel renders its body immediately, with the waiting
   *  notice above it rather than in place of it. Panels whose body is nothing
   *  but live data (a dial with no phasors, a table with no rows) leave this
   *  off, so they show the notice alone rather than an empty frame. */
  drawsWithoutData?: boolean
  /** This panel's own full-size route. The expand link is rendered in the
   *  dashboard variant only. */
  focusHref?: string
  /** Width (or any class) that applies only when this panel owns the page.
   *  Ignored in the grid, where the column decides. */
  focusedClassName?: string
  minBodyClass?: string
  footer?: ReactNode
  className?: string
  contentClassName?: string
  children: ReactNode
}) {
  const focused = variant === 'focused'
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
    <Card
      className={cn('min-w-0 gap-0', focused && focusedClassName, className)}
    >
      <CardHeader className="border-b">
        <CardTitle className="text-base">{title}</CardTitle>
        {focused && subtitle && (
          <span className="text-sm text-gray-500">{subtitle}</span>
        )}
        <CardAction className="flex items-center gap-2 self-center">
          {badge}
          {!focused && focusHref && (
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
