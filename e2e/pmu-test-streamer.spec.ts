import { test, expect, type Page } from '@playwright/test'

import { choose } from './helpers'

/** The "Frame" readout: `N of M · t = …` in a recording, `Live · …` while live. */
function frameReadout(page: Page) {
  return page.getByText('Frame', { exact: true }).locator('xpath=following-sibling::span[1]')
}

async function open(page: Page) {
  await page.goto('/pmu-test-streamer')
  await expect(page.getByRole('button', { name: 'Play' })).toBeEnabled()
}

test.describe('PMU test streamer', () => {
  test('play shows frames and the stats module result, stop pauses', async ({ page }) => {
    await open(page)
    await page.getByRole('button', { name: 'Play' }).click()
    await expect(page.getByText(/^Recorded · Playing$/)).toBeVisible()
    await expect(frameReadout(page)).toHaveText(/^\d+ of 60 · t = /)
    // The stats module's result for the frame at the cursor.
    await expect(page.getByText(/^mean f \d+\.\d{4} Hz · spread .* · 5 stations$/)).toBeVisible()
    // The frame table: five stations with values.
    await expect(page.getByRole('row')).toHaveCount(6)

    await page.getByRole('button', { name: 'Stop' }).click()
    await expect(page.getByText(/^Recorded · Paused$/)).toBeVisible()
  })

  test('step forward and step back move one frame', async ({ page }) => {
    await open(page)
    const forward = page.getByRole('button', { name: 'Step forward' })
    await forward.click()
    await expect(frameReadout(page)).toHaveText(/^1 of 60 /)
    await forward.click()
    await forward.click()
    await expect(frameReadout(page)).toHaveText(/^3 of 60 /)
    await page.getByRole('button', { name: 'Step back' }).click()
    await expect(frameReadout(page)).toHaveText(/^2 of 60 /)
  })

  test('seek moves the cursor to the chosen offset', async ({ page }) => {
    await open(page)
    await page.getByRole('slider', { name: 'Seek' }).fill('1.5')
    await page.getByRole('slider', { name: 'Seek' }).blur()
    // A seek lands paused before the frame at the offset; one step shows it.
    await page.getByRole('button', { name: 'Step forward' }).click()
    await expect(frameReadout(page)).toHaveText(/^31 of 60 · t = 1\.50 s$/)
  })

  test('speed is applied by the player', async ({ page }) => {
    await open(page)
    await choose(page, 'Speed', '2×')
    await page.reload()
    await expect(page.getByRole('combobox', { name: 'Speed' })).toHaveText('2×')
  })

  test('switch to the live source and back to the recording', async ({ page }) => {
    await open(page)
    const source = page.getByRole('group', { name: 'Source' })
    // The configured sources, by name; the recording is active.
    await expect(source.getByRole('button')).toHaveText(['sample', 'live'])
    await expect(source.getByRole('button', { name: 'sample' })).toHaveAttribute('aria-pressed', 'true')

    await source.getByRole('button', { name: 'live' }).click()
    await expect(source.getByRole('button', { name: 'live' })).toHaveAttribute('aria-pressed', 'true')
    await expect(page.getByLabel('Live', { exact: true }).first()).toBeVisible()
    await expect(page.getByText('LIVE', { exact: true })).toBeVisible()
    await expect(frameReadout(page)).toHaveText(/^Live · /)
    await expect(page.getByText(/^mean f /)).toBeVisible()
    // The transport belongs to a recording: disabled while live.
    for (const name of ['Play', 'Stop', 'Step forward', 'Step back']) {
      await expect(page.getByRole('button', { name })).toBeDisabled()
    }
    await expect(page.getByRole('slider', { name: 'Seek' })).toBeDisabled()

    await source.getByRole('button', { name: 'sample' }).click()
    await expect(page.getByText(/^Recorded · Paused$/)).toBeVisible()
    await expect(page.getByRole('button', { name: 'Play' })).toBeEnabled()
    await page.getByRole('button', { name: 'Play' }).click()
    await expect(page.getByText(/^Recorded · Playing$/)).toBeVisible()
    await expect(frameReadout(page)).toHaveText(/^\d+ of 60 /)
  })
})
