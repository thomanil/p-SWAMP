/**
 * How a panel is being rendered.
 *
 * `dashboard` is the grid on `/`, where panels are compact and each offers a
 * link to its own route. `focused` is that route: the same component, given more
 * room and its full set of controls. One component serving both is what keeps
 * the two from drifting apart.
 */
export type PanelVariant = 'dashboard' | 'focused'
