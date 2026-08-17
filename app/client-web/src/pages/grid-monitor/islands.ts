/**
 * Colours for detected islands, shared by every panel that draws them.
 *
 * Index 0 is the main system and is deliberately neutral: it is the *absence* of
 * a split, so it should recede while a genuine island stands out. The rest are
 * distinguishable at small sizes and against both page backgrounds.
 *
 * One definition, because the phasor dial and the grid map now sit side by side
 * on the monitor. They previously each had their own copy that disagreed at
 * index 0 — the main system was blue on one and grey on the other, a few inches
 * apart, with the dial's legend calling the blue one "main".
 */
export const ISLAND_COLORS = [
  '#64748b', // slate — the main system
  '#dc2626',
  '#d97706',
  '#7c3aed',
  '#0891b2',
]

/** The colour for an island index, wrapping if the detector reports more
 *  groups than there are colours. */
export function islandColor(index: number | null | undefined): string {
  return ISLAND_COLORS[(index ?? 0) % ISLAND_COLORS.length]
}
