import { execSync } from 'node:child_process'
import path from 'node:path'

/**
 * Always runs once after the whole test run, regardless of pass/fail —
 * to ensure that the docker compose environment is torn down and cleaned up.
 */
export default function globalTeardown() {
  // E2E_KEEP_STACK=1 leaves a stack that was already running up, for repeated
  // runs against the same one (a dev stack, or a bare `docker run`).
  if (process.env.E2E_KEEP_STACK === '1') return
  // docker-compose.yml is at the repo root, one level up from this file —
  // must run there regardless of the process's own cwd.
  execSync('docker compose down --remove-orphans', {
    cwd: path.resolve(__dirname, '..'),
    stdio: 'inherit',
  })
}
