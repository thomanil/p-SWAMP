# Subapp templates

What `../generate-new-subapp.sh` copies into a new subapp. Edit these to change
what every new page/api starts life as; the script itself only derives names and
patches the registries.

- `server-python/` → `app/server-python/src/<pkg>/`
- `client-web/` → `app/client-web/src/pages/<slug>/`

Both the file contents and the file *names* are rendered, so a template can be
called `use__NAME__Socket.ts`. The tokens, for the example name
`grid-overview` with nav label `Grid Overview`:

| Token | Becomes | Used for |
|---|---|---|
| `__SLUG__` | `grid-overview` | the URL, the page folder, the `/api/<app>` prefix |
| `__PKG__` | `grid_overview` | the Python package |
| `__NAME__` | `GridOverview` | the React component, the hook, the model class |
| `__WS_PATH_CONST__` | `GRID_OVERVIEW_WS_PATH` | the ws path const in `lib/servers.ts` |
| `__LABEL__` | `Grid Overview` | the nav entry and page title |

The rendered Python has to pass `scripts/error_check.sh` — ruff, at 88 columns —
so keep the text after a token on any line short. The name is capped at 32
characters, which is the margin these templates are written to.
