import { test, expect, type Page } from '@playwright/test'

import { recordSockets } from './helpers'

/** A panel's card, found by its title. */
function panel(page: Page, title: string) {
  return page.locator('[data-slot="card"]').filter({
    has: page.getByText(title, { exact: true }),
  })
}

const PANELS = [
  'Live Measurements',
  'Alarms',
  'Application Status',
  'Islanding Detection',
  'Voltage Phasors',
  'Line Outages',
]

test.describe('Grid monitor', () => {
  test('the dashboard renders every panel over exactly six sockets', async ({ page }) => {
    const sockets = recordSockets(page)
    await page.goto('/')
    for (const title of PANELS) {
      const card = panel(page, title)
      await expect(card).toBeVisible()
      await expect(card.getByText('Waiting for state…')).toHaveCount(0, { timeout: 15_000 })
    }
    await expect(panel(page, 'Live Measurements')).toContainText(/\d+ channels · 50 Hz/)
    await expect(page.getByRole('img', { name: 'Voltage phasors by station' })).toBeVisible()
    await expect(panel(page, 'Application Status').getByRole('row')).not.toHaveCount(1)
    // Five panels' sockets and the layout's error tray; one pipeline behind them.
    await page.waitForTimeout(2_000)
    expect(new Set(sockets).size).toBe(6)
  })

  test('the line trip splits off 6500, 6700 and 6701, and raises an alarm', async ({ page }) => {
    test.setTimeout(120_000)
    await page.goto('/')
    const map = panel(page, 'Islanding Detection')
    await expect(map).toContainText(/\d islands?/, { timeout: 90_000 })
    for (const station of ['6500', '6700', '6701']) {
      await expect(map).toContainText(station)
    }
    // Every station is in exactly one group: 44 in all.
    const counts = await map.getByText(/^\d+ stations/).allTextContents()
    const total = counts.map((t) => Number(t.match(/^(\d+)/)?.[1])).reduce((a, b) => a + b, 0)
    expect(total).toBe(44)

    await expect(panel(page, 'Alarms').getByText('Unseen').first()).toBeVisible({ timeout: 30_000 })
    await expect(panel(page, 'Line Outages').getByText('Disconnected').first()).toBeVisible()
  })

  test('an alarm can be acknowledged, annotated and silenced', async ({ page }) => {
    test.setTimeout(120_000)
    await page.goto('/islanding')
    const alarms = panel(page, 'Alarms')
    const row = alarms.getByRole('row').filter({ hasText: 'Unseen' }).first()
    await expect(row).toBeVisible({ timeout: 90_000 })

    await row.getByRole('button', { name: 'Acknowledge' }).click()
    await expect(alarms.getByText('Acknowledged').first()).toBeVisible()

    await alarms.getByRole('row').filter({ hasText: 'Acknowledged' }).first().click()
    await page.getByRole('textbox', { name: 'Annotation' }).fill('e2e note')
    await page.getByRole('button', { name: 'Annotate' }).click()
    await expect(alarms.getByText('e2e note')).toBeVisible()

    await alarms.getByRole('row').filter({ hasText: 'Acknowledged' }).first()
      .getByRole('button', { name: 'Silence' }).click()
    await expect(alarms.getByText('Silenced').first()).toBeVisible()
  })

  test('the focused views render, and the channel picker changes the chart', async ({ page }) => {
    await page.goto('/time-window')
    const measurements = panel(page, 'Live Measurements')
    await expect(measurements).toContainText(/\d+ channels · 50 Hz/)
    const shown = measurements.getByText(/^\d+\/\d+ shown$/)
    const before = await shown.textContent()
    const off = measurements.locator('[data-slot="badge"]').filter({ hasText: /^\d{4}$/ })
      .and(page.locator('.text-muted-foreground')).first()
    await off.click()
    await expect(shown).not.toHaveText(before ?? '')

    await page.goto('/phasors')
    await expect(page.getByRole('img', { name: 'Voltage phasors by station' })).toBeVisible()
    await page.getByRole('button', { name: 'Equal lengths' }).click()

    await page.goto('/line-outage')
    await expect(panel(page, 'Line Outages').getByText('Waiting for state…')).toHaveCount(0, { timeout: 15_000 })

    await page.goto('/app-status')
    await expect(panel(page, 'Application Status').getByText('Waiting for state…')).toHaveCount(0, { timeout: 15_000 })
  })
})
