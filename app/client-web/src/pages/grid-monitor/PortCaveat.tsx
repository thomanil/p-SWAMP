import { AlertTriangleIcon } from 'lucide-react'

import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'

/**
 * Status banner at the top of the grid monitor.
 *
 * This page is a rough port of the Qt front end and is not yet something to
 * trust or build on, so it says so where it will actually be read — on the page
 * itself, rather than only in the repo docs. The split matters: the *analysis*
 * below is the long-standing Python in `src/pswamp/`, reached unchanged; only
 * this presentation layer is new and unreviewed.
 *
 * Remove it when the client has been reviewed and solidified — see
 * doc/WIP-context-port-from-qt-to-web-frontend.md §10.
 *
 * Deliberately not the `destructive` variant: nothing is broken, and red would
 * compete with the alarm panels further down, where red has to mean a grid
 * event.
 */
export function PortCaveat() {
  return (
    <Alert className="border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-900/60 dark:bg-amber-950/40 dark:text-amber-100">
      <AlertTriangleIcon className="size-4" />
      <AlertTitle>
        Preliminary rough port of the Qt frontend code
      </AlertTitle>
      <AlertDescription className="text-amber-900/90 dark:text-amber-100/90">
        An exploratory LLM ports some of the prexisting QT GUI to TS+React.
          Only GUI is ported, the python server process reuses existing Python algorithms from <code>src/pswamp/</code>.
          Note that this is incomplete, may very well have serious flaws, and it needs to be reviewed before it is iterated on further.
      </AlertDescription>
    </Alert>
  )
}
