<script lang="ts">
  import type { Snippet } from 'svelte'
  import { Link } from 'svelte-routing'

  import { cn } from '@/lib/utils'

  // An active-aware nav link over svelte-routing's <Link>. `end` mirrors
  // react-router's NavLink `end`: match this exact path only, rather than any
  // path it is a prefix of — the index link `/` needs it, since `/` is a prefix
  // of every route. svelte-routing hands the active state to `getProps`; we turn
  // it into the same class the React NavLink produced.
  let {
    to,
    end = false,
    children,
  }: { to: string; end?: boolean; children: Snippet } = $props()

  const getProps = ({
    isCurrent,
    isPartiallyCurrent,
  }: {
    isCurrent: boolean
    isPartiallyCurrent: boolean
  }) => {
    const active = end ? isCurrent : isPartiallyCurrent || isCurrent
    return {
      class: cn(
        'transition-colors hover:text-foreground',
        active ? 'font-medium text-foreground' : 'text-muted-foreground',
      ),
    }
  }
</script>

<Link {to} {getProps}>
  {@render children()}
</Link>
