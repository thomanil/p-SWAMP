<script lang="ts">
  import AlarmsPanel from './AlarmsPanel.svelte'
  import IslandMapPanel from './IslandMapPanel.svelte'
  import IslandingData from './IslandingData.svelte'

  /**
   * The full-size islanding view (route `/islanding`).
   *
   * Both panels, because both come off the one islanding socket — showing the
   * detection without the alarms it raised would be half the story. Renders the
   * same components the dashboard does, only larger.
   */
</script>

<div class="w-full max-w-5xl space-y-4">
  <IslandingData>
    <IslandMapPanel variant="focused" />
    <AlarmsPanel variant="focused" />
  </IslandingData>
</div>
