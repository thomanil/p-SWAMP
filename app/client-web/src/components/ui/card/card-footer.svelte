<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { HTMLAttributes } from 'svelte/elements'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    children,
    ...rest
  }: HTMLAttributes<HTMLDivElement> & { class?: string; children?: Snippet } = $props()
</script>

<div
  data-slot="card-footer"
  class={cn(
    'flex items-center rounded-b-xl border-t bg-muted/50 p-(--card-spacing)',
    className,
  )}
  {...rest}
>
  {@render children?.()}
</div>
