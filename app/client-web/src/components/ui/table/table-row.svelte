<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { HTMLAttributes } from 'svelte/elements'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    children,
    ...rest
  }: HTMLAttributes<HTMLTableRowElement> & { class?: string; children?: Snippet } = $props()
</script>

<tr
  data-slot="table-row"
  class={cn(
    'border-b transition-colors hover:bg-muted/50 has-aria-expanded:bg-muted/50 data-[state=selected]:bg-muted',
    className,
  )}
  {...rest}
>
  {@render children?.()}
</tr>
