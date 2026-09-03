<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { HTMLAttributes } from 'svelte/elements'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    children,
    ...rest
  }: HTMLAttributes<HTMLElement> & { class?: string; children?: Snippet } = $props()
</script>

<caption
  data-slot="table-caption"
  class={cn('mt-4 text-sm text-muted-foreground', className)}
  {...rest}
>
  {@render children?.()}
</caption>
