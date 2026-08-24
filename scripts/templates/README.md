# Subapp templates

What `../generate-new-subapp.sh` copies into a new subapp. Edit these to change
what every new page/api starts life as; the script itself only derives names and
patches the registries.

- `server-python/` → `app/server-python/src/<pkg>/`
- `client-web/` → `app/client-web/src/pages/<slug>/`

Every template is named `<filename>.template`, and the suffix is stripped when
it is rendered — `use__NAME__Socket.ts.template` becomes
`useGridOverviewSocket.ts`. That suffix is not decoration: it disables IDE error checking etc.

Both the file contents and the file *names* are rendered, so a template can be
called `use__NAME__Socket.ts.template`. The tokens, for the example name
`grid-overview` with nav label `Grid Overview`:

| Token | Becomes | Used for |
|---|---|---|
| `__SLUG__` | `grid-overview` | the URL, the page folder, the `/api/<app>` prefix |
| `__PKG__` | `grid_overview` | the Python package |
| `__NAME__` | `GridOverview` | the React component, the hook, the model class |
| `__WS_PATH_CONST__` | `GRID_OVERVIEW_WS_PATH` | the ws path const in `lib/servers.ts` |
| `__API_PATH_CONST__` | `GRID_OVERVIEW_API_PATH` | the REST prefix const, same file |
| `__LABEL__` | `Grid Overview` | the nav entry and page title |

Two path consts, because a subapp uses two transports: state comes **down** over
the WebSocket, commands go **up** as POSTs (see AGENTS.md). The generated counter
exercises both — `POST /api/<slug>/count/bump` changes it, the socket carries the
new value back — so a new subapp starts with the convention already wired rather
than as something to remember.

The rendered Python has to pass `scripts/error_check.sh` — ruff, at 88 columns —
so keep the text after a token on any line short. The name is capped at 32
characters, which is the margin these templates are written to.
