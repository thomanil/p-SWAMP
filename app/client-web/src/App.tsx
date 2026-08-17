import { BrowserRouter, Navigate, Route, Routes } from 'react-router'

import { AppLayout } from '@/components/AppLayout'
import { BASE_PATH } from '@/lib/basePath'
import { PmuTestStreamerPage } from '@/pages/pmu-test-streamer/PmuTestStreamerPage'
import { TimelinePage } from '@/pages/timeline/TimelinePage'

/**
 * The route table — the one place that knows which apps this client hosts.
 * Every page is a folder under `src/pages/<route>/` holding its own component,
 * hook and views, rendered into AppLayout's outlet; adding one means a folder
 * there, a <Route> here, and a NAV_ITEMS entry in AppLayout. Only genuinely
 * cross-page code lives outside those folders (`src/components/`, `src/hooks/`,
 * `src/lib/`).
 *
 * Deep links and hard refreshes work in both modes: Vite's dev server and the
 * Python server's SPAStaticFiles both fall back to index.html on an unknown
 * path. Because that fallback catches *any* such path, the `*` route below is
 * what keeps a typo'd URL from rendering the nav over an empty outlet.
 *
 * `basename` is what makes every route below relative to wherever the app is
 * mounted — `/` locally, `/pswamp-client-server-poc` behind the remote reverse
 * proxy — so the paths here (and every `<NavLink to>` / `<Navigate to>`) stay
 * written as if the app owned the origin root. BASE_PATH is discovered at
 * runtime, not configured; see src/lib/basePath.ts.
 */
function App() {
  return (
    <BrowserRouter basename={BASE_PATH || '/'}>
      <Routes>
        <Route element={<AppLayout />}>
          <Route index element={<PmuTestStreamerPage />} />
          <Route path="pmu-test-streamer" element={<PmuTestStreamerPage />} />
          <Route path="timeline" element={<TimelinePage />} />
          <Route path="*" element={<Navigate to="/pmu-test-streamer" replace />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App
