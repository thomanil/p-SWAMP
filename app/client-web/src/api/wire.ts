/**
 * The api contract, as the rest of the client sees it.
 *
 * `schema.ts` beside this file is GENERATED from `doc/api/openapi.json`, which is
 * itself generated from the server (see `scripts/generate-api-contract.sh`). This
 * file is the small hand-written layer over it, so no page has to know the shape
 * of a generated module.
 *
 * `Wire[...]` is every model the server publishes, including the WebSocket
 * message shapes — those reach the document through the `x-websocket-channels`
 * extension, since OpenAPI itself has no notion of a socket. So a page hook says:
 *
 *     type TimelineMessage = Wire['TimelineState']
 *
 * instead of hand-copying the Python model's fields into TypeScript and hoping
 * the two stay in step. They are snake_case, as the server sends them; each hook
 * still maps to its own camelCase domain type, which is the client's own
 * vocabulary and deliberately not the server's.
 *
 * Don't edit `schema.ts`. Change the Python model, run
 * `scripts/generate-api-contract.sh`, and commit both artifacts — until you do,
 * `scripts/error_check.sh` fails.
 */
import type { components, paths } from './schema'

/** Every model in the contract, keyed by name. */
export type Wire = components['schemas']

/** Every path in the contract, keyed by url. Used to type `postCommand`. */
export type ApiPaths = paths
