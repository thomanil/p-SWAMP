<script lang="ts">
  import { Select as SelectPrimitive } from 'bits-ui'
  import { CheckIcon } from '@lucide/svelte'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    value,
    label,
    children: childrenProp,
    ...restProps
  }: SelectPrimitive.ItemProps = $props()
</script>

<SelectPrimitive.Item
  {value}
  data-slot="select-item"
  class={cn(
    'relative flex w-full cursor-default items-center gap-1.5 rounded-md py-1 pr-8 pl-1.5 text-sm outline-hidden select-none data-highlighted:bg-accent data-highlighted:text-accent-foreground data-[disabled]:pointer-events-none data-[disabled]:opacity-50 [&_svg]:pointer-events-none [&_svg]:shrink-0 [&_svg:not([class*=size-])]:size-4',
    className,
  )}
  {...restProps}
>
  {#snippet children({ selected, highlighted })}
    <span class="pointer-events-none absolute right-2 flex size-4 items-center justify-center">
      {#if selected}
        <CheckIcon class="pointer-events-none" />
      {/if}
    </span>
    {#if childrenProp}
      {@render childrenProp({ selected, highlighted })}
    {:else}
      {label || value}
    {/if}
  {/snippet}
</SelectPrimitive.Item>
