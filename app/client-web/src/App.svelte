<script lang="ts">
  import { Router, Route } from 'svelte-routing'

  import AppLayout from '@/components/AppLayout.svelte'
  import RedirectToHome from '@/components/RedirectToHome.svelte'
  import { BASE_PATH } from '@/lib/basePath'
  import GridMonitorPage from '@/pages/grid-monitor/GridMonitorPage.svelte'
  import AppStatusPanel from '@/pages/grid-monitor/app-status/AppStatusPanel.svelte'
  import IslandingFocused from '@/pages/grid-monitor/islanding/IslandingFocused.svelte'
  import LineOutagePanel from '@/pages/grid-monitor/line-outage/LineOutagePanel.svelte'
  import PhasorsPanel from '@/pages/grid-monitor/phasors/PhasorsPanel.svelte'
  import MeasurementsPanel from '@/pages/grid-monitor/time-window/MeasurementsPanel.svelte'
  import PmuTestStreamerPage from '@/pages/pmu-test-streamer/PmuTestStreamerPage.svelte'
  import ReferenceSubappPage from '@/pages/reference-subapp/ReferenceSubappPage.svelte'

  /**
   * The route table — the one place that knows which apps this client hosts.
   *
   * An app is a folder under `src/pages/<app>/`. Most own a single route; the grid
   * monitor owns several, because its panels are views of one server-side timeline
   * that are useful both together (the dashboard at `/`) and one at a time (the
   * focused routes below, which render the *same* panel components with
   * variant="focused" rather than copies of them).
   *
   * Deep links and hard refreshes work in both modes: Vite's dev server and the
   * Python server's SPAStaticFiles both fall back to index.html on an unknown
   * path. Because that fallback catches *any* such path, the `*` route below is
   * what keeps a typo'd URL from rendering the nav over an empty outlet.
   *
   * `basepath` is what makes every route below relative to wherever the app is
   * mounted — `/` locally, `/p-swamp` behind the remote reverse proxy — so the
   * paths here (and every `<Link to>` / RedirectToHome) stay written as if the app
   * owned the origin root. BASE_PATH is discovered at runtime, not configured; see
   * src/lib/basePath.ts.
   *
   * The routes are children of <AppLayout>, which renders them in its <main>
   * outlet — the Svelte equivalent of react-router's layout route + <Outlet />.
   */
</script>

<Router basepath={BASE_PATH || '/'}>
  <AppLayout>
    <Route path="/"><GridMonitorPage /></Route>

    <!-- Full-size views of individual monitor panels. -->
    <Route path="time-window"><MeasurementsPanel variant="focused" /></Route>
    <Route path="phasors"><PhasorsPanel variant="focused" /></Route>
    <Route path="islanding"><IslandingFocused /></Route>
    <Route path="line-outage"><LineOutagePanel variant="focused" /></Route>
    <Route path="app-status"><AppStatusPanel variant="focused" /></Route>

    <!-- Scaffold demo, unrelated to the p-SWAMP pipeline. -->
    <Route path="pmu-test-streamer"><PmuTestStreamerPage /></Route>

    <Route path="reference-subapp"><ReferenceSubappPage /></Route>
    <Route path="*"><RedirectToHome /></Route>
  </AppLayout>
</Router>
