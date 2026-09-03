<script lang="ts">
  import type { Snippet } from 'svelte'
  import { Link } from 'svelte-routing'
  import { Maximize2Icon } from '@lucide/svelte'

  import { Alert, AlertTitle } from '@/components/ui/alert'
  import {
    Card,
    CardAction,
    CardContent,
    CardFooter,
    CardHeader,
    CardTitle,
  } from '@/components/ui/card'
  import type { ConnStatus } from '@/hooks/useServerSocket.svelte'
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
  type Props = {
    title: string
    /** One line under the title, shown in the focused variant only: on the
     *  dashboard the panels around it are the context, and the space is not there
     *  to spare. */
    subtitle?: string
    /** Live indicator shown at the top right, beside the expand link. */
    badge?: Snippet
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
    footer?: Snippet
    class?: string
    contentClassName?: string
    children: Snippet
  }

  let {
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
    class: className,
    contentClassName,
    children,
  }: Props = $props()

  const focused = $derived(variant === 'focused')
</script>

{#snippet notice()}
  <Alert variant={status.kind === 'offline' && status.isError ? 'destructive' : 'default'}>
    <AlertTitle>
      {status.kind === 'online' ? 'Waiting for state…' : status.label}
    </AlertTitle>
  </Alert>
{/snippet}

<!-- min-w-0: a grid item defaults to min-width:auto, so a wide canvas or table
     inside would otherwise force its whole column open. -->
<Card class={cn('min-w-0 gap-0', focused && focusedClassName, className)}>
  <CardHeader class="border-b">
    <CardTitle class="text-base">{title}</CardTitle>
    {#if focused && subtitle}
      <span class="text-sm text-gray-500">{subtitle}</span>
    {/if}
    <CardAction class="flex items-center gap-2 self-center">
      {@render badge?.()}
      {#if !focused && focusHref}
        <Link
          to={focusHref}
          aria-label={`Open ${title} full size`}
          class="text-muted-foreground transition-colors hover:text-foreground"
        >
          <Maximize2Icon class="size-4" />
        </Link>
      {/if}
    </CardAction>
  </CardHeader>

  <CardContent class={cn('pt-6', contentClassName)}>
    {#if ready}
      {@render children()}
    {:else if drawsWithoutData}
      <div class="space-y-3">
        {@render notice()}
        {@render children()}
      </div>
    {:else}
      <div class={cn('flex items-center justify-center', minBodyClass)}>
        {@render notice()}
      </div>
    {/if}
  </CardContent>

  {#if footer}
    <CardFooter class="border-t pt-4 text-sm text-muted-foreground tabular-nums">
      {@render footer()}
    </CardFooter>
  {/if}
</Card>
