<script lang="ts">
  import { Select as SelectPrimitive } from 'bits-ui'

  // Root. bits-ui owns the open/value state; the wrappers below only style the
  // parts. Kept as a thin pass-through so a consumer binds `value`/`open` here.
  let {
    open = $bindable(false),
    value = $bindable(),
    ...restProps
  }: SelectPrimitive.RootProps = $props()
</script>

<SelectPrimitive.Root bind:open bind:value={value as never} {...restProps} />
