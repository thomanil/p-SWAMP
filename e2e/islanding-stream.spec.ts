import { test, expect } from '@playwright/test'

import { choose } from './helpers'

test.describe('Islanding stream', () => {
  test('finds the islands of the line trip, and reports how it keeps up', async ({ page }) => {
    test.setTimeout(90_000)
    await page.goto('/islanding-stream')
    // The first state message: the pipeline exists, so commands will land.
    await expect(page.getByText(/^t = \d+\.\d \/ \d+ s$/)).toBeVisible()

    await choose(page, 'Speed', '10×')
    await page.getByRole('button', { name: 'Play' }).click()

    const islands = page.getByRole('region', { name: 'Islands' })
    await expect(islands.getByRole('heading')).toHaveText(/separated group/, { timeout: 60_000 })
    for (const station of ['6500', '6700', '6701']) {
      await expect(islands.getByText(station, { exact: true })).toBeVisible()
    }
    await expect(page.getByText('Islanding', { exact: true })).toBeVisible()

    const keepUp = page.getByRole('region', { name: 'Keep-up' })
    await expect(keepUp).toContainText(/Frames\/s\s*\d+/)
    await expect(keepUp).toContainText(/Detect\s*\d+\.\d{2} ms/)

    // Stopped: the replay clock holds still (read twice, a second apart).
    await page.getByRole('button', { name: 'Stop' }).click()
    const clock = page.getByText(/^t = \d+\.\d \/ \d+ s$/)
    await expect(async () => {
      const at = await clock.textContent()
      await page.waitForTimeout(1_000)
      expect(await clock.textContent()).toBe(at)
    }).toPass({ timeout: 10_000 })
  })
})
