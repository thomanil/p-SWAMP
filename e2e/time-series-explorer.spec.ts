import { test, expect, type Page } from '@playwright/test'

import { compose, composeServices } from './helpers'

// Serial: the last case stops the remote data stub, which the others read
// from when the stack is compose.
test.describe.configure({ mode: 'serial' })

async function open(page: Page) {
  await page.goto('/time-series-explorer')
  await expect(page.getByText(/ · \d+\.\d{2} s$/)).toBeVisible() // the coverage line
}

async function setRange(page: Page, from: number, to: number) {
  await page.getByLabel('Range start, seconds from coverage start').fill(String(from))
  await page.getByLabel('Range end, seconds from coverage start').fill(String(to))
}

test.describe('Timeseries Db Explorer', () => {
  test('play range plays and ends paused at the end of the range', async ({ page }) => {
    await open(page)
    await setRange(page, 0, 1)
    await page.getByRole('button', { name: 'Play range' }).click()
    await expect(page.getByText(/^playing ×1/)).toBeVisible()
    await expect(page.getByText(/^ended at the end of the range/)).toBeVisible({ timeout: 10_000 })
    await expect(page.getByText(/cursor t = 0\.95 s/)).toBeVisible()
  })

  test('stop pauses a range that is playing', async ({ page }) => {
    await open(page)
    await setRange(page, 0, 3)
    await page.getByRole('button', { name: 'Play range' }).click()
    await expect(page.getByText(/^playing ×1/)).toBeVisible()
    await page.getByRole('button', { name: 'Stop' }).click()
    await expect(page.getByText(/^paused/)).toBeVisible()
  })

  test('count rows answers with the rows in the range', async ({ page }) => {
    await open(page)
    await setRange(page, 0, 3)
    await page.getByRole('button', { name: 'Count rows' }).click()
    // 20 frames a second for three seconds, in the sample and the stub alike.
    await expect(page.getByText(/^60 rows in \[/)).toBeVisible({ timeout: 10_000 })
  })

  test('a failed provider reaches the error tray, and retry recovers', async ({ page }) => {
    test.skip(
      !composeServices().includes('remote-data-stub'),
      'needs the compose stack, whose explorer reads the remote data stub',
    )
    test.setTimeout(120_000)
    compose('stop remote-data-stub')
    try {
      await page.goto('/time-series-explorer')
      await expect(page.getByText('none reported by the provider')).toBeVisible({ timeout: 30_000 })
      const tray = page.getByRole('region', { name: 'Errors' })
      await expect(tray).toContainText('time-series-explorer')

      // The tray belongs to the layout: it is still there on another page.
      await page.getByRole('navigation').getByRole('link', { name: 'Reference example' }).click()
      await expect(tray).toContainText('time-series-explorer')

      compose('start remote-data-stub')
      await page.getByRole('navigation').getByRole('link', { name: 'Timeseries Db Explorer' }).click()
      await expect(async () => {
        await page.getByRole('button', { name: 'Retry provider' }).click()
        await expect(page.getByText(/ · \d+\.\d{2} s$/)).toBeVisible({ timeout: 3_000 })
      }).toPass({ timeout: 60_000 })
    } finally {
      compose('start remote-data-stub')
    }
  })
})
