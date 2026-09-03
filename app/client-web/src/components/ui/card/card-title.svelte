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
  data-slot="card-title"
  class={cn(
    'font-heading text-base leading-snug font-medium group-data-[size=sm]/card:text-sm',
    className,
  )}
  {...rest}
>
  {@render children?.()}
</div>
