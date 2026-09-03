<script lang="ts">
  import { Select as SelectPrimitive } from 'bits-ui'
  import { cn } from '@/lib/utils'
  import SelectScrollUpButton from './select-scroll-up-button.svelte'
  import SelectScrollDownButton from './select-scroll-down-button.svelte'

  let {
    class: className,
    sideOffset = 4,
    children,
    ...restProps
  }: SelectPrimitive.ContentProps = $props()
</script>

<SelectPrimitive.Portal>
  <SelectPrimitive.Content
    {sideOffset}
    data-slot="select-content"
    class={cn(
      'relative z-50 max-h-96 min-w-36 overflow-x-hidden overflow-y-auto rounded-lg bg-popover text-popover-foreground shadow-md ring-1 ring-foreground/10 data-[side=bottom]:translate-y-1 data-[side=left]:-translate-x-1 data-[side=right]:translate-x-1 data-[side=top]:-translate-y-1',
      className,
    )}
    {...restProps}
  >
    <SelectScrollUpButton />
    <SelectPrimitive.Viewport
      class="h-(--bits-select-anchor-height) w-full min-w-(--bits-select-anchor-width) scroll-my-1 p-1"
    >
      {@render children?.()}
    </SelectPrimitive.Viewport>
    <SelectScrollDownButton />
  </SelectPrimitive.Content>
</SelectPrimitive.Portal>
