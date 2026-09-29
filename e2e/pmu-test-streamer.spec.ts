import { test, expect } from '@playwright/test'

// The server data architecture, driven from the browser: a recording per
// browser (its own replay, the stats module's result, the gateway's CIM
// reference, a command to the module), the same page over a remote data
// service, and one live stream every browser shares.

test.describe('PMU test streamer', () => {
  test('plays the local recording with the module result and the CIM reference', async ({ page }) => {
    await page.goto('/pmu-test-streamer')
    await expect(page.getByText('Local recording · Paused')).toBeVisible()

    await page.getByRole('button', { name: 'Play' }).click()
    await expect(page.getByText('Local recording · Playing')).toBeVisible()
    await expect(page.getByText(/mean f \d+\.\d+ Hz/)).toBeVisible()
    await expect(page.getByText('n44-stub')).toBeVisible()

    // A command to the module, not the player: its running count restarts.
    await expect(page.getByText(/^\d{2,} frames/)).toBeVisible()
    await page.getByRole('button', { name: 'Reset stats' }).click()
    await page.getByRole('button', { name: 'Stop' }).click()
    await expect(page.getByText(/^[0-9] frames/)).toBeVisible()
  })

  test('the remote recording comes from the remote data service', async ({ page }) => {
    await page.goto('/pmu-test-streamer')
    await page.getByRole('button', { name: 'Remote recording' }).click()
    await expect(page.getByText('Remote recording · Paused')).toBeVisible()
    await page.getByRole('button', { name: 'Step forward' }).click()
    // The stub serves a minute (1200 frames); the image's recording holds 60.
    await expect(page.getByText(/^1 of 1200 ·/)).toBeVisible()
  })

  test('two browsers share one live stream', async ({ browser }) => {
    // Two contexts: two localStorage client ids, two recordings.
    const [a, b] = await Promise.all([browser.newContext(), browser.newContext()])
    const [pageA, pageB] = await Promise.all([a.newPage(), b.newPage()])
    for (const page of [pageA, pageB]) {
      await page.goto('/pmu-test-streamer')
      await expect(page.getByText('Local recording · Paused')).toBeVisible()
      await page.getByRole('button', { name: 'Live' }).click()
    }
    for (const page of [pageA, pageB]) {
      await expect(page.getByText('LIVE · 2 watching')).toBeVisible()
      await expect(page.getByText(/^Live · \d{2}:\d{2}/)).toBeVisible()
      // The shared stream takes no transport commands from a viewer.
      await expect(page.getByRole('button', { name: 'Play' })).toBeDisabled()
    }

    await pageA.getByRole('button', { name: 'Local recording' }).click()
    await expect(pageA.getByText('Local recording · Paused')).toBeVisible()
    await expect(pageB.getByText('LIVE · 1 watching')).toBeVisible()
    await Promise.all([a.close(), b.close()])
  })
})
