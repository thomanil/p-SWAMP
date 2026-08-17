import { NavLink, Outlet, useLocation } from 'react-router'

import { cn } from '@/lib/utils'

/**
 * The shell every page renders inside: a nav bar listing the apps, then the
 * centered outlet the page itself fills. Registered as the layout route in
 * App.tsx, so adding a page means adding a NAV_ITEMS entry plus a <Route>.
 */
const NAV_ITEMS = [
  // `isIndex` marks the page that `/` also renders: NavLink only knows about
  // its own path, so without it nothing looks selected on the bare `/` landing.
  { to: '/pmu-test-streamer', label: 'PMU Test Streamer', isIndex: true },
  { to: '/timeline', label: 'Timeline', isIndex: false },
]

export function AppLayout() {
  const atIndex = useLocation().pathname === '/'

  return (
    <div className="flex min-h-svh flex-col bg-background">
      <header className="flex items-center gap-6 border-b px-6 py-3">
        <span className="font-semibold tracking-tight">P-SWAMP</span>
        <nav className="flex items-center gap-4 text-sm">
          {NAV_ITEMS.map(({ to, label, isIndex }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                cn(
                  'transition-colors hover:text-foreground',
                  isActive || (isIndex && atIndex)
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
