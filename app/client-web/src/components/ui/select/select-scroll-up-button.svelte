<script lang="ts">
  import { Select as SelectPrimitive } from 'bits-ui'
  import { ChevronUpIcon } from '@lucide/svelte'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    ...restProps
  }: SelectPrimitive.ScrollUpButtonProps = $props()
</script>

<SelectPrimitive.ScrollUpButton
  data-slot="select-scroll-up-button"
  class={cn(
    "z-10 flex cursor-default items-center justify-center bg-popover py-1 [&_svg:not([class*=size-])]:size-4",
    className,
  )}
  {...restProps}
>
  <ChevronUpIcon />
</SelectPrimitive.ScrollUpButton>
