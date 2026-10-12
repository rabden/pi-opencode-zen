# pi-opencode-zen

**Use OpenCode Zen's free models outside OpenCode — right inside [Pi](https://pi.dev).**

OpenCode Zen gives away large daily quotas on strong models
(`muse-spark-1.3-contributor-free`, `exo-free`, `space-bunny-free`, …),
but the free tier is gated to the official OpenCode client: point any
other tool at `https://opencode.ai/zen/v1` and you get
`403 FreeTierError: "OpenCode's free tier can only be used from within
OpenCode"`.

This extension reverse-engineers that gate and satisfies it from inside
Pi — so the free models show up in `/model` like any other provider, and
Pi's **real tools work** (read, bash, edit, write, multi-round loops),
not just chat.

Full reverse-engineering write-up — every gate rule, what failed, what
works, and how to re-capture artifacts when OpenCode rotates them:
[docs/zen-gate-notes.md](docs/zen-gate-notes.md).

## Quick start

```bash
pi install git:github.com/rabden/pi-opencode-zen
# or once published: pi install npm:@rabden/pi-opencode-zen

export OPENCODE_API_KEY=public     # that's the whole auth for free models
pi
```

```
/model  →  opencode-zen/muse-spark-1.3-contributor-free
```

Try it:

```bash
OPENCODE_API_KEY=public pi --model opencode-zen/muse-spark-1.3-contributor-free \
  --print "Run 'echo hello-from-zen' and reply with its output"
```

Verified working: plain chat, `read`/`bash`/`write` executing for real,
multi-round tasks (write a file, read it back, quote it), and **image
input** (`@image.jpg` attachments on multimodal frees like muse-spark) —
all on free models, zero credentials beyond `Bearer public`.

## How it works (short version)

The gate validates the *content* of each request, not the transport:
official client `User-Agent`, no SDK fingerprint headers, structurally
valid OpenCode session ids, a verbatim OpenCode prompt as a leading
developer message, no `tools` declarations, and (with a custom system
prompt present) at most two further input items. See
[docs/zen-gate-notes.md](docs/zen-gate-notes.md) for the rule table and
the experiments behind it.

The extension meets all of that in-process via one `globalThis.fetch`
patch scoped to `opencode.ai`:

- official UA, stripped `X-Stainless-*`, freshly minted OpenCode
  SortableIDs (stable per Pi session for routing/cache affinity),
- the required marker prompt injected as `input[0]`, defused by a
  precedence preamble so Pi's instructions govern,
- every request collapsed to `[developer, system, user(transcript)]`,
- **tool calling as a text protocol**: the model emits fenced
  ` ```tool {"name","arguments"} ` blocks; a `message_end` handler turns
  them into real tool calls, so Pi executes actual tools and feeds
  results back through the transcript. Parallel blocks run in parallel.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `OPENCODE_API_KEY` | — | `public` for free models; a real Zen key also works |
| `PI_ZEN_ALL_MODELS` | off | `1` lists the full (paid) Zen catalog; default lists `-free` models only |
| `PI_ZEN_VISION` | `1` | image input (`@file` attachments); `0` declares text-only models |
| `PI_ZEN_TEXT_TOOLS` | `1` | text-protocol tool calling; `0` = chat only (drops the tool loadout) |
| `PI_ZEN_TOOLS` | — | `keep` disables all request shaping (raw passthrough) |
| `PI_ZEN_BASE_URL` | `https://opencode.ai/zen/v1` | force a base URL (testing) |
| `PI_ZEN_DEBUG` | off | `1` dumps wire summaries to `/tmp/pi-zen-debug.json` |

## Limits

- The marker prompt and UA are pinned to OpenCode CLI 1.19.0. If Zen
  rotates them, requests 403 until re-captured (procedure in the notes).
- The marker's title-generator persona occasionally bleeds through;
  the preamble suppresses it — retry the prompt if an answer looks
  like a title.
- Free-tier quota is per IP and managed by OpenCode (community reports
  ~200 requests/day); the endpoint also throttles under rapid probing.
- Images from the most recent user turn are carried on the wire; earlier
  turns' images degrade to `[image attached]` transcript markers, and
  tool-result images (e.g. `read` on a PNG) arrive as text only.
- Sessions on this provider are best kept to fresh conversations; the
  wire transcript grows every round.

## Disclaimer

Unofficial, not affiliated with OpenCode. Free-tier access outside the
official client may conflict with OpenCode's terms — review them before
use. This may break at any time as the service evolves.

## License

MIT
