<script lang="ts">
  import { onMount } from 'svelte'
  import { navigate } from 'svelte-routing'

  import { BASE_PATH } from '@/lib/basePath'

  // The catch-all's target: redirect an unknown path to the index, the way the
  // React app's `<Navigate to="/" replace />` did. `navigate` is global (it does
  // not know the Router's basepath), so prepend BASE_PATH by hand — empty locally,
  // `/p-swamp` behind the reverse proxy.
  onMount(() => {
    navigate(`${BASE_PATH}/`, { replace: true })
  })
</script>
