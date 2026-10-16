# Architecture plan: full stream reassembly (deferred)

Status: **deferred by choice** (2026-10-15). The current forward+patch
design is kept; this note records the alternative so a future switch is
cheap. Revisit the trigger below first.

## Context

The SSE rewriter (`stream.ts`) uses **forward + patch**: upstream events
are passed through, and the rewriter only intervenes where the
text-protocol tool calls live —

- `response.output_text.delta`: fence-holdback state machine per item,
- text/done events: rewritten to that item's fence-free text,
- item close: synthesized native `function_call` events appended.

The alternative (zenshim's model) is **full reassembly**: buffer the
three things that matter (text deltas, reasoning deltas, usage), discard
the upstream shape entirely, and emit a fully synthesized Responses
stream — our own ids and indices, `[reasoning?, message(text),
function_call…]` — like zenshim's `StreamTranslator` does for its three
client dialects.

## Why we kept forward+patch

| Dimension | forward+patch | full reassembly |
|---|---|---|
| Fence/JSON parsing surface (heredocs, variants, holdback) | identical in both | identical in both |
| Upstream event-shape quirks (multi-item turns, `content_part.done`-only closes, odd `output_index`) | exposed — every quirk is a patch site | immune — only 3 event types are read |
| New/unmodeled upstream features (refusals, annotations, …) | pass through free | silently dropped |
| Thinking/text signatures | upstream's, verbatim | ours (harmless: replay flattens to a transcript) |
| Empty text blocks (defused fence item) | cosmetic wart | gone |

The fence parser — where most real-world failures lived (heredocs) — is
the same code either way; reassembly only removes the event-shape patch
layer, which produced exactly one bug so far (cross-item prose
duplication, fixed by per-item state). Forward+patch is additionally
future-proof against *content* changes; reassembly against *shape*
changes. Given the endpoint's history (chat→responses move, item
shapes, model deprecations), both threats are real.

## Hardening done instead

Item state is keyed by **item id** (not `output_index`), with an
index→id resolver for done-events that carry only an index and a
last-item fallback. Covered by
`test("id-keyed routing survives missing and renumbered output_index")`.
This closed the last known shape-fragile spot (the `lastTextIndex`
fallback dance).

## Trigger to revisit

**One more event-shape bug** (a bug whose root cause is a patch site in
`feedFrame`, not the fence parser). That is the signal the patch layer
is fighting the endpoint more than it's worth.

## Plan, if triggered

1. Keep: fence parser (`pumpText`/`scanToolBlock`/`safeTail`),
   `harvestToolBlocks`, call-id minting, the `message_end` fallback.
2. Drop: per-item state map, `flushedById`, done-event patching,
   `patchTerminalOutput`, index tracking.
3. Add: a synthesizer that, from the buffered pieces, emits the
   canonical event sequence (see zenshim `stream.rs`
   `responses_finish` for the exact recipe):
   `response.created` → `[reasoning item + summary deltas]` →
   `message item + text deltas` (cleaned) → `function_call items`
   (added / arguments delta / done / item done per call) →
   `response.completed` (status from upstream, usage forwarded,
   re-emit `response.failed`/`incomplete` faithfully).
4. Requirements to satisfy pi's processor (all already encoded in
   tests): `output_item.added` before any delta; every function_call
   gets its `output_item.done` (unfinished-call guard); text
   signature ids ≤ 64 chars; status mapping (`incomplete` +
   `max_output_tokens` → length).
5. Tests: ~80% carry over unchanged — they assert on the emitted SSE
   stream and the resulting `AssistantMessage`, not on patch internals.
   Add one regression test per upstream quirk observed in the wild
   (multi-item turn, `content_part.done`-only close, index-less deltas)
   to prove immunity.

Effort estimate: a focused afternoon.
