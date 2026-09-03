<script lang="ts">
  import type { Snippet } from 'svelte'
  import type { HTMLTableAttributes } from 'svelte/elements'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    children,
    ...rest
  }: HTMLTableAttributes & { class?: string; children?: Snippet } = $props()
</script>

<div data-slot="table-container" class="relative w-full overflow-x-auto">
  <table data-slot="table" class={cn('w-full caption-bottom text-sm', className)} {...rest}>
    {@render children?.()}
  </table>
</div>
