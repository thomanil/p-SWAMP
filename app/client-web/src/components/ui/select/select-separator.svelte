<script lang="ts">
  import type { HTMLAttributes } from 'svelte/elements'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    ...restProps
  }: HTMLAttributes<HTMLDivElement> = $props()
</script>

<div
  data-slot="select-separator"
  role="separator"
  class={cn('pointer-events-none -mx-1 my-1 h-px bg-border', className)}
  {...restProps}
></div>
