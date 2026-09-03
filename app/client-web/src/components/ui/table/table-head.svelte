<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { HTMLThAttributes } from 'svelte/elements'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    children,
    ...rest
  }: HTMLThAttributes & { class?: string; children?: Snippet } = $props()
</script>

<th
  data-slot="table-head"
  class={cn(
    'h-10 px-2 text-left align-middle font-medium whitespace-nowrap text-foreground [&:has([role=checkbox])]:pr-0',
    className,
  )}
  {...rest}
>
  {@render children?.()}
</th>
