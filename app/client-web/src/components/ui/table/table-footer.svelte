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

<tfoot
  data-slot="table-footer"
  class={cn('border-t bg-muted/50 font-medium [&>tr]:last:border-b-0', className)}
  {...rest}
>
  {@render children?.()}
</tfoot>
