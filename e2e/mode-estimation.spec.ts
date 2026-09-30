import { test, expect } from '@playwright/test'

import { choose } from './helpers'

test.describe('Mode estimation', () => {
  test('identifies modes once the window fills, and reports how it keeps up', async ({ page }) => {
    test.setTimeout(120_000)
    await page.goto('/mode-estimation')
    // The first state message: the pipeline exists, so commands will land.
    await expect(page.getByText(/^t = \d+\.\d \/ \d+ s$/)).toBeVisible()

    await choose(page, 'Speed', '10×')
    await page.getByRole('button', { name: 'Play' }).click()

    const modes = page.getByRole('region', { name: 'Modes' })
    await expect(modes).toContainText(/\d+\.\d{3} Hz|No electromechanical mode identified/, {
      timeout: 90_000,
    })

    const keepUp = page.getByRole('region', { name: 'Keep-up' })
    await expect(keepUp).toContainText(/Identify\s*\d+ ms/)
    await expect(keepUp).toContainText(/Skipped\s*\d+ of \d+/)

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
