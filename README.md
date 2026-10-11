# pi-opencode-zen

A [Pi](https://pi.dev) extension that exposes OpenCode Zen's models through
the OpenAI Responses endpoint (`https://opencode.ai/zen/v1/responses`),
including the free (`-free`) models, with working tool calling.

## What you get

- A `opencode-zen` provider in `/model` with the live Zen catalog
  (refreshed from `GET /zen/v1/models`, static fallback offline).
- Free-tier models usable from Pi, with Pi's real tools (read, bash, edit,
  write, ...) executing natively via a text-protocol adapter.

## Install

From a checkout:

```bash
pi install /path/to/pi-opencode-zen
```

From git:

```bash
pi install git:github.com/rabden/pi-opencode-zen
```

From npm (once published):

```bash
pi install npm:@rabden/pi-opencode-zen
```

Trial run without installing:

```bash
pi --extension /path/to/pi-opencode-zen
```

## Configure

```bash
export OPENCODE_API_KEY=public   # free models; or your real Zen API key
pi
# /model -> opencode-zen/<id>   (e.g. muse-spark-1.3-contributor-free)
```

`/login opencode-zen` also works to store a key in `auth.json`.

### Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `OPENCODE_API_KEY` | — | `public` for free models, or a Zen key |
| `PI_ZEN_ALL_MODELS` | off | `1` lists the full Zen catalog; by default only `-free` models are listed (paid ones 402 for anonymous keys) |
| `PI_ZEN_TEXT_TOOLS` | `1` | text-protocol tool calling; `0` = pure chat (drops the tool loadout) |
| `PI_ZEN_TOOLS` | — | `keep` disables all request shaping (raw passthrough) |
| `PI_ZEN_BASE_URL` | `https://opencode.ai/zen/v1` | force a base URL (testing) |
| `PI_ZEN_DEBUG` | off | `1` dumps wire summaries to `/tmp/pi-zen-debug.json` |

## How it works

The extension registers the provider on Pi's `openai-responses`
implementation and shapes traffic for the Zen gateway inside the Pi
process via a single `globalThis.fetch` patch scoped to `opencode.ai`
URLs:

1. Strips OpenAI-SDK `X-Stainless-*` fingerprint headers and sets the
   official CLI `User-Agent`.
2. Rews `x-opencode-session(-id)` / `x-opencode-request` as
   structurally valid OpenCode SortableIDs (stable per Pi session).
3. Injects a captured OpenCode prompt as the leading developer message
   (the gateway requires it verbatim) and defuses it with a precedence
   preamble on Pi's own system message.
4. Removes `tools` / `tool_choice` and any tool-typed history items, and
   collapses the conversation into a single user transcript message so
   every request fits the gateway's input shape budget.

Tool calling then runs as a text protocol: the system prompt teaches the
model to emit fenced

````
```tool
{"name": "read", "arguments": {"path": "/tmp/x"}}
```
````

blocks; a `message_end` handler converts them into real `toolCall`
blocks, so Pi's agent loop executes the actual tools (parallel batches
and multi-round loops included) and feeds results back through the
transcript.

## Verified behaviors

- plain chat (print mode)
- `read` / `bash` / `write` tools executing for real
- multi-round tasks (write then read back in one prompt)
- every wire round is exactly `[developer, system, user]`, no `tools` key

## Limits and caveats

- The marker prompt (`marker.ts`) and UA string must stay byte-exact;
  OpenCode rotating either breaks this until re-captured.
- The marker persona occasionally bleeds through (title-flavored
  answers); the preamble + transcript reminder suppress it — retry the
  prompt if it recurs.
- Free-tier quota is per IP + client class and managed by OpenCode
  (community reports ~200 requests/day). The endpoint also throttles
  under heavy probing; transient failures usually resolve on retry.
- Sessions mixing zen and other providers keep their own tool
  loadouts; switching restores the previous set (chat mode only).
- This is an unofficial, personal-use integration. It is not affiliated
  with or endorsed by OpenCode, and it may stop working at any time as
  the service evolves. Review your obligations under OpenCode's terms
  before use.

## License

MIT
