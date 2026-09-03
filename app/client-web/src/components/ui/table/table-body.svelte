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

<tbody data-slot="table-body" class={cn('[&_tr:last-child]:border-0', className)} {...rest}>
  {@render children?.()}
</tbody>
