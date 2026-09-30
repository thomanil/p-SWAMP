import { test, expect } from '@playwright/test'

test.describe('Frequency peek', () => {
  test('shows every station live, and keeps updating', async ({ page }) => {
    await page.goto('/frequency-peek')
    await expect(page.getByText('LIVE', { exact: true })).toBeVisible()
    await expect(page.getByText('Online', { exact: true })).toBeVisible()

    // One reading per station of the sample: five.
    const readings = page.getByText(/^\d{2}\.\d{3}Hz$/)
    await expect(readings).toHaveCount(5)

    const samples = page.getByText(/ · \d+ of \d+ samples$/)
    await expect(samples).toBeVisible()
    const count = async () => Number((await samples.textContent())?.match(/ · (\d+) of /)?.[1])
    const first = await count()
    await expect.poll(count).toBeGreaterThan(first)
  })
})
