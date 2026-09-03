<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { HTMLAttributes } from 'svelte/elements'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    children,
    ...rest
  }: HTMLAttributes<HTMLTableSectionElement> & { class?: string; children?: Snippet } = $props()
</script>

<thead data-slot="table-header" class={cn('[&_tr]:border-b', className)} {...rest}>
  {@render children?.()}
</thead>
