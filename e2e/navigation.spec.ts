import { test, expect } from '@playwright/test'

import { recordSockets } from './helpers'

/** Every nav entry, the route it opens and a heading that proves the page rendered. */
const PAGES = [
  { label: 'Monitor', path: '/', heading: 'Live Measurements' },
  { label: 'PMU Test Streamer', path: '/pmu-test-streamer', heading: 'PMU Test Streamer' },
  { label: 'Reference example', path: '/reference-subapp', heading: 'Reference example' },
  { label: 'Frequency peek', path: '/frequency-peek', heading: 'Frequency peek' },
  { label: 'Timeseries Db Explorer', path: '/time-series-explorer', heading: 'Timeseries Db Explorer' },
  { label: 'Islanding stream', path: '/islanding-stream', heading: 'Islanding stream' },
  { label: 'Mode estimation', path: '/mode-estimation', heading: 'Mode estimation' },
]

test.describe('Navigation and layout', () => {
  test('every nav link routes to its page', async ({ page }) => {
    await page.goto('/reference-subapp')
    for (const { label, path, heading } of PAGES) {
      await page.getByRole('navigation').getByRole('link', { name: label, exact: true }).click()
      await expect(page).toHaveURL(new RegExp(`${path === '/' ? '/$' : path}$`))
      await expect(page.getByText(heading, { exact: true }).first()).toBeVisible()
    }
  })

  test('a hard refresh on every deep link serves the page', async ({ page }) => {
    for (const { path, heading } of PAGES) {
      await page.goto(path)
      await page.reload()
      await expect(page.getByText(heading, { exact: true }).first()).toBeVisible()
    }
  })

  test('an unknown path redirects to the monitor', async ({ page }) => {
    await page.goto('/no-such-page')
    await expect(page).toHaveURL(/\/$/)
    await expect(page.getByText('Live Measurements', { exact: true })).toBeVisible()
  })

  test('the API doc link is offered on localhost', async ({ page }) => {
    await page.goto('/reference-subapp')
    await expect(page.getByRole('link', { name: 'API doc' })).toBeVisible()
  })

  test('the error tray socket is open on every page', async ({ page }) => {
    for (const { path } of PAGES.filter((p) => p.path !== '/')) {
      const sockets = recordSockets(page)
      await page.goto(path)
      await expect.poll(() => sockets.some((url) => url.includes('/api/errors/ws'))).toBe(true)
    }
  })
})
