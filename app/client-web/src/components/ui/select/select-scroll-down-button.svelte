<script lang="ts">
  import { Select as SelectPrimitive } from 'bits-ui'
  import { ChevronDownIcon } from '@lucide/svelte'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    ...restProps
  }: SelectPrimitive.ScrollDownButtonProps = $props()
</script>

<SelectPrimitive.ScrollDownButton
  data-slot="select-scroll-down-button"
  class={cn(
    "z-10 flex cursor-default items-center justify-center bg-popover py-1 [&_svg:not([class*=size-])]:size-4",
    className,
  )}
  {...restProps}
>
  <ChevronDownIcon />
</SelectPrimitive.ScrollDownButton>
