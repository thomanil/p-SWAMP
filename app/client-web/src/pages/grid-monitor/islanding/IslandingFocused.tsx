import { AlarmsPanel } from './AlarmsPanel'
import { IslandMapPanel } from './IslandMapPanel'
import { IslandingData } from './IslandingData'

/**
 * The full-size islanding view (route `/islanding`).
 *
 * Both panels, because both come off the one islanding socket — showing the
 * detection without the alarms it raised would be half the story. Renders the
 * same components the dashboard does, only larger.
 */
export function IslandingFocused() {
  return (
    <div className="w-full max-w-5xl space-y-4">
      <IslandingData>
        <IslandMapPanel variant="focused" />
        <AlarmsPanel variant="focused" />
      </IslandingData>
    </div>
  )
}
