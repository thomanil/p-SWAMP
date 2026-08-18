import { NavLink, Outlet } from 'react-router'

import { cn } from '@/lib/utils'

/**
 * The shell every page renders inside: a nav bar listing the apps, then the
 * outlet the page itself fills. Registered as the layout route in App.tsx, so
 * adding a page means adding a NAV_ITEMS entry plus a <Route>.
 *
 * Nothing here selects a backend: every socket resolves against the origin the
 * client was served from (see src/lib/servers.ts), so the several panels of the
 * grid monitor are necessarily views of one and the same server.
 */
const NAV_ITEMS = [
  // `end` on the index entry: NavLink matches descendant paths by default, and
  // "/" is a prefix of every route — without it the monitor would render as the
  // active link on every page.
  { to: '/', label: 'Monitor', end: true },
  { to: '/pmu-test-streamer', label: 'PMU Test Streamer', end: false },
  { to: '/timeline', label: 'Timeline', end: false },
]

export function AppLayout() {
  return (
    <div className="flex min-h-svh flex-col bg-background">
      <header className="flex items-center gap-6 border-b px-6 py-3">
        <span className="font-semibold tracking-tight">P-SWAMP</span>
        <nav className="flex items-center gap-4 text-sm">
          {NAV_ITEMS.map(({ to, label, end }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                cn(
                  'transition-colors hover:text-foreground',
                  isActive
                    ? 'font-medium text-foreground'
                    : 'text-muted-foreground',
                )
              }
            >
              {label}
            </NavLink>
          ))}
        </nav>
      </header>

      <main className="flex flex-1 items-center justify-center p-6">
        <Outlet />
      </main>
    </div>
  )
}
