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

<div data-slot="card-content" class={cn('px-(--card-spacing)', className)} {...rest}>
  {@render children?.()}
</div>
