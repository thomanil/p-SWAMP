import {PortCaveat} from './PortCaveat'
import {AppStatusPanel} from './app-status/AppStatusPanel'
import {AlarmsPanel} from './islanding/AlarmsPanel'
import {IslandMapPanel} from './islanding/IslandMapPanel'
import {IslandingData} from './islanding/IslandingData'
import {LineOutagePanel} from './line-outage/LineOutagePanel'
import {PhasorsPanel} from './phasors/PhasorsPanel'
import {MeasurementsPanel} from './time-window/MeasurementsPanel'

/**
 * The grid monitor (route `/`) — every view of the PMU stream on one screen.
 *
 * These panels are not four loosely related pages: they are views of a *single*
 * server-side timeline. One replay of the recording drives one measurement
 * window and one islanding detector, so every panel here is showing the same
 * instant. Splitting them across routes made the one thing worth watching —
 * a disturbance propagating through all of them at once — impossible to see.
 *
 * It also mirrors p-SWAMP's own Qt front end, which is a single main window with
 * the grid view in the middle and docks for frequency, status and alarms around
 * it, rather than four separate screens.
 *
 * Each panel keeps its own WebSocket at its own natural rate — the chart at
 * 10 Hz, the dial at 5 Hz, islanding on events, status at 2 Hz — so a slow or
 * broken one degrades alone. The exception is the map and the alarm table, which
 * are two views of one socket and therefore share it through <IslandingData>.
 *
 * Every panel here also has a full-size route of its own, rendering this same
 * component with variant="focused"; the expand icon in each header links to it.
 */
export function GridMonitorPage() {
    return (
        // self-stretch cancels the layout's vertical centering, which is meant for a
        // single card, without touching the shell the demo pages rely on.
        <div className="w-full max-w-[1600px] self-stretch space-y-4">
            <PortCaveat/>

            <MeasurementsPanel variant="dashboard"/>

            <IslandingData>
                <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)]">
                    <AlarmsPanel variant="dashboard"/>
                    <AppStatusPanel variant="dashboard"/>
                </div>

                <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                    {/* minmax(0,...) rather than 1fr: a grid item defaults to
              min-width:auto, so the canvas, the tables and the station lists
              would otherwise force their columns wider than the viewport. */}
                    <IslandMapPanel variant="dashboard"/>
                    <PhasorsPanel variant="dashboard"/>
                </div>

                {/* Above the alarm table on purpose: an outage event is the *cause*
            an operator reads first, and the alarms below are downstream of it. */}
                <div className="mt-4">
                    <LineOutagePanel variant="dashboard"/>
                </div>


            </IslandingData>
        </div>
    )
}
