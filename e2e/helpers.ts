import { execSync } from 'node:child_process'
import path from 'node:path'

import { expect, type Page } from '@playwright/test'

const REPO_ROOT = path.resolve(__dirname, '..')

/**
 * The compose services running right now, or an empty list when the server
 * under test is something else (a bare `docker run`, a minikube port-forward).
 * The specs run unchanged against both; the few flows that need a compose
 * service -- the remote data stub, to stop and restart it -- ask this first.
 */
export function composeServices(): string[] {
  try {
    const out = execSync('docker compose ps --services --status running', {
      cwd: REPO_ROOT,
      stdio: ['ignore', 'pipe', 'ignore'],
    })
    return out.toString().split('\n').map((s) => s.trim()).filter(Boolean)
  } catch {
    return []
  }
}

export function compose(args: string): void {
  execSync(`docker compose ${args}`, { cwd: REPO_ROOT, stdio: 'ignore' })
}

/** Pick `option` from a shadcn/Radix select whose trigger has `label`. */
export async function choose(page: Page, label: string, option: string): Promise<void> {
  await page.getByRole('combobox', { name: label }).click()
  await page.getByRole('option', { name: option, exact: true }).click()
  await expect(page.getByRole('combobox', { name: label })).toHaveText(option)
}

/** Every WebSocket the page opens from now on, by url. */
export function recordSockets(page: Page): string[] {
  const urls: string[] = []
  page.on('websocket', (ws) => urls.push(ws.url()))
  return urls
}
