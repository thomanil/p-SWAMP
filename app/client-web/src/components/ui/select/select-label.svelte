<script lang="ts">
  import { Select as SelectPrimitive } from 'bits-ui'
  import { cn } from '@/lib/utils'

  let {
    class: className,
    children,
    ...restProps
  }: SelectPrimitive.GroupHeadingProps = $props()
</script>

<SelectPrimitive.GroupHeading
  data-slot="select-label"
  class={cn('px-1.5 py-1 text-xs text-muted-foreground', className)}
  {...restProps}
>
  {@render children?.()}
</SelectPrimitive.GroupHeading>
