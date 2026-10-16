/**
 * Zen SSE stream rewrite: text-protocol tool calls -> native Responses events.
 *
 * The free-tier gate rejects any non-official `tools` declaration, so the
 * model calls tools as fenced ```tool blocks in its text output. Pi's
 * chat UI renders assistant text as it streams — converting those blocks
 * only at message_end (the previous approach) means the raw fences are
 * visible while the answer streams, and any fence the regex misses stays
 * in the final message.
 *
 * This module ports zenshim's stream translator to the Pi extension: the
 * fetch patch wraps each /responses SSE body in a TransformStream that
 *
 *   - runs a fence-holdback state machine over response.output_text.delta
 *     events, so text that may be part of a ```tool block never reaches
 *     the client as visible text (a partial fence opener at a chunk tail
 *     is held back until it is clear whether it starts a call),
 *   - re-emits unparseable blocks as plain text (the model can see its
 *     own malformed call and self-correct),
 *   - when a complete block closes, parses {"name","arguments"} and
 *   - when a message output item closes, emits native Responses
 *     function_call events (output_item.added,
 *     response.function_call_arguments.delta/.done, output_item.done) so
 *     pi-ai's stream processor builds real toolCall blocks — the same UI
 *     as models with native tool calling, live during streaming, with
 *     stopReason "toolUse".
 *
 * Text state is tracked PER OUTPUT ITEM: the live Zen upstream splits a
 * turn into several items — typically a reasoning item, a prose message
 * item, and the ```tool fence as its own message item — so each item's
 * done-event must be rewritten with that item's fence-free text only
 * (verified against the real endpoint, 2026-10-15; see
 * test/stream.test.ts "real Zen shape").
 *
 * Everything else in the upstream stream (response.created/in_progress,
 * reasoning events, usage, terminal events) is forwarded, with message
 * text fields rewritten to the fence-free per-item text.
 *
 * The rewriter is deliberately dependency-free and framework-free so it
 * can be unit-tested standalone (test/stream.test.ts) and fed into
 * pi-ai's real processResponsesStream for conformance checks.
 */

/** A tool call minted from a fenced ```tool block. */
export interface ZenToolCall {
	/** call_id (what a tool result references). */
	id: string;
	/** Responses item id; must start with "fc_". */
	itemId: string;
	name: string;
	/** Serialized JSON arguments. */
	arguments: string;
}

const FENCE_MARKER = "```tool";
/** Fence opener: "```tool", tolerating variants ("```tools", "``` tool", …). */
const FENCE_OPEN_RE = /```[ \t]*tool/;

let callCounter = 0;

/** Mint globally-unique call ids (shared with the message_end fallback). */
export function mintToolCallIds(): { id: string; itemId: string } {
	callCounter += 1;
	return { id: `call_zen_${callCounter}`, itemId: `fc_zen_${callCounter}` };
}

/**
 * Parse the payload of a fenced block (text between "```tool" and the
 * closing "```"). Returns null when it is not a well-formed tool call;
 * callers then re-emit the block as plain text for self-correction.
 */
export function parseToolBlock(payload: string): { name: string; arguments: string } | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(payload.trim());
	} catch {
		return null;
	}
	if (typeof parsed !== "object" || parsed === null) return null;
	const { name, arguments: args } = parsed as { name?: unknown; arguments?: unknown };
	if (typeof name !== "string" || name.length === 0) return null;
	if (args !== undefined && typeof args !== "object" && typeof args !== "string") return null;
	return { name, arguments: typeof args === "string" ? args : JSON.stringify(args ?? {}) };
}

/**
 * How much of the buffer is safe to flush live: everything except a
 * tail that could still grow into a fence opener (a prefix of
 * "```tool", or a bare run of backticks, possibly space-padded).
 */
function safeTail(buffer: string): number {
	const maxHold = Math.min(FENCE_MARKER.length + 2, buffer.length);
	for (let hold = maxHold; hold >= 1; hold--) {
		const tail = buffer.slice(buffer.length - hold);
		if (FENCE_MARKER.startsWith(tail)) return buffer.length - hold;
		if (/^`+[ \t]*$/.test(tail)) return buffer.length - hold;
	}
	return buffer.length;
}

/**
 * Find the end of a fenced tool block starting at `from` (just past
 * the opener marker): the FIRST balanced JSON object — scanning braces
 * with string/escape awareness — followed by the closing fence.
 *
 * This is what makes bash tool calls work: command strings routinely
 * contain triple backticks (heredocs writing markdown, echoed code
 * fences…), so "close at the first ```" truncates the JSON and the
 * call is lost. Brace scanning finds the real end of the JSON first,
 * then expects the closing fence after it.
 *
 * Returns null while the block is still incomplete (streaming) or
 * when the text is not a well-formed block (prose mentioning ```tool).
 */
function scanToolBlock(s: string, from: number): { payload: string; end: number } | null {
	let start = -1;
	for (let i = from; i < s.length; i++) {
		if (s[i] === "{") {
			start = i;
			break;
		}
	}
	if (start === -1) return null;
	let depth = 0;
	let inString = false;
	let escape = false;
	for (let j = start; j < s.length; j++) {
		const ch = s[j];
		if (inString) {
			if (escape) escape = false;
			else if (ch === "\\") escape = true;
			else if (ch === '"') inString = false;
			continue;
		}
		if (ch === '"') inString = true;
		else if (ch === "{") depth++;
		else if (ch === "}") {
			depth--;
			if (depth === 0) {
				const rest = s.slice(j + 1);
				const closing = /^\s*```/.exec(rest);
				if (closing === null) return null; // JSON complete, closing fence not arrived yet
				return { payload: s.slice(start, j + 1), end: j + 1 + closing[0].length };
			}
		}
	}
	return null; // braces still unbalanced (streaming or malformed)
}

/** Fence-holdback state for one message output item. */
interface ItemTextState {
	/** Unflushed tail: starts at an open fence or a partial fence opener. */
	pending: string;
	/** Text flushed (or re-emitted) for this item — its final text. */
	flushed: string;
	itemId: string;
	/** Output index to use on frames synthesized for this item. */
	outputIndex: number;
	finalized: boolean;
}

/**
 * Consume an item's pending buffer: flush text that cannot be part
 * of a tool block, harvest complete blocks as calls, re-emit
 * unparseable blocks as text. With final=false an open or partial
 * fence is held; with final=true the remainder is decided (unclosed
 * fences stay text so the model can see its own malformed call and
 * self-correct).
 */
function pumpText(state: ItemTextState, final: boolean, calls: ZenToolCall[]): string {
	let outText = "";
	for (;;) {
		const openMatch = FENCE_OPEN_RE.exec(state.pending);
		if (openMatch === null) {
			const safe = final ? state.pending.length : safeTail(state.pending);
			if (safe > 0) {
				outText += state.pending.slice(0, safe);
				state.pending = state.pending.slice(safe);
			}
			break;
		}
		if (openMatch.index > 0) {
			outText += state.pending.slice(0, openMatch.index);
			state.pending = state.pending.slice(openMatch.index);
		}
		const scanned = scanToolBlock(state.pending, openMatch[0].length);
		if (scanned === null) {
			if (final) {
				// Unclosed / malformed fence at stream end: keep it visible
			// as text for self-correction.
				outText += state.pending;
				state.pending = "";
			}
			break;
		}
		const call = parseToolBlock(scanned.payload);
		if (call) {
			const ids = mintToolCallIds();
			calls.push({ ...ids, ...call });
		} else {
			// Not a valid call: keep it visible as text.
			outText += state.pending.slice(0, scanned.end);
		}
		state.pending = state.pending.slice(scanned.end);
	}
	state.flushed += outText;
	return outText;
}

/**
 * Harvest fenced tool blocks from a complete string (the message_end
 * fallback path shares this parser with the stream rewriter).
 */
export function harvestToolBlocks(text: string): { cleanedText: string; calls: ZenToolCall[] } {
	const state: ItemTextState = { pending: text, flushed: "", itemId: "", finalized: false };
	const calls: ZenToolCall[] = [];
	pumpText(state, true, calls);
	return { cleanedText: state.flushed, calls };
}

export interface ZenStreamRewriter {
	/** Feed one upstream SSE frame (without its trailing blank line); returns zero or more output frames. */
	feedFrame(frame: string): string[];
	/** True once a terminal response event was forwarded. */
	isFinished(): boolean;
}

/**
 * State machine over one Zen /responses SSE stream. Feed it frames (the
 * framing-aware wrapper is {@link wrapZenSseResponse}); it returns the
 * frames to pass downstream, re-rendered as needed.
 */
export function createZenStreamRewriter(): ZenStreamRewriter {
	const calls: ZenToolCall[] = [];
	let emittedCalls = 0;
	let maxOutputIndex = -1;
	let terminalSeen = false;
	let sequence = 0;

	/**
	 * Per-message-item text state, keyed by item id (Zen splits prose and
	 * fences across items). Keying by id rather than output_index keeps
	 * routing correct when the upstream omits or renumbers indices — the
	 * one shape-fragile spot left in the forward+patch design.
	 */
	const itemStates = new Map<string, ItemTextState>();
	/** output_index -> item id, for done-events that carry only an index. */
	const idByIndex = new Map<number, string>();
	/** Item id -> final fence-free text, for terminal-response output patching. */
	const flushedById = new Map<string, string>();
	/** Fallback target for events that carry neither id nor index. */
	let lastItemId = "";

	const stateFor = (itemId: unknown, outputIndex: unknown): ItemTextState => {
		let id = typeof itemId === "string" && itemId.length > 0 ? itemId : "";
		if (id === "") {
			// Resolve via index when the id is missing.
			if (typeof outputIndex === "number" && Number.isInteger(outputIndex) && idByIndex.has(outputIndex)) {
				id = idByIndex.get(outputIndex)!;
			} else {
				id = lastItemId || "msg_zen_0";
			}
		}
		lastItemId = id;
		let state = itemStates.get(id);
		if (state === undefined) {
			const index = typeof outputIndex === "number" && Number.isInteger(outputIndex) ? outputIndex : idByIndex.size;
			state = { pending: "", flushed: "", itemId: id, outputIndex: index, finalized: false };
			itemStates.set(id, state);
			if (!idByIndex.has(index)) idByIndex.set(index, id);
		}
		return state;
	};

	const render = (type: string, data: Record<string, unknown>): string => {
		data.type = type;
		data.sequence_number = sequence++;
		return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
	};

	const deltaFrame = (state: ItemTextState, text: string): string =>
		render("response.output_text.delta", {
			item_id: state.itemId,
			output_index: state.outputIndex,
			content_index: 0,
			delta: text,
		});

	/** Pump an item's text (see module-level pumpText); closes over the shared call list. */
	const pumpState = (state: ItemTextState, final: boolean): string => pumpText(state, final, calls);

	/** Decide an item's remainder and return any residual live delta. */
	const finalizeItem = (state: ItemTextState): string => {
		if (state.finalized) return "";
		state.finalized = true;
		return pumpState(state, true);
	};

	/**
	 * Native function_call events for calls harvested but not yet
	 * emitted. Indexes continue after every output item seen so far.
	 */
	const emitCallFrames = (): string[] => {
		if (emittedCalls >= calls.length) return [];
		const out: string[] = [];
		let index = maxOutputIndex + 1;
		for (; emittedCalls < calls.length; emittedCalls++) {
			const call = calls[emittedCalls];
			out.push(
				render("response.output_item.added", {
					output_index: index,
					item: {
						id: call.itemId,
						type: "function_call",
						status: "in_progress",
						call_id: call.id,
						name: call.name,
						arguments: "",
					},
				}),
				render("response.function_call_arguments.delta", {
					item_id: call.itemId,
					output_index: index,
					delta: call.arguments,
				}),
				render("response.function_call_arguments.done", {
					item_id: call.itemId,
					output_index: index,
					arguments: call.arguments,
				}),
				render("response.output_item.done", {
					output_index: index,
					item: {
						id: call.itemId,
						type: "function_call",
						status: "completed",
						call_id: call.id,
						name: call.name,
						arguments: call.arguments,
					},
				}),
			);
			index++;
		}
		maxOutputIndex = index - 1;
		return out;
	};

	const trackOutputIndex = (value: unknown): void => {
		if (typeof value === "number" && Number.isInteger(value) && value > maxOutputIndex) {
			maxOutputIndex = value;
		}
	};

	/** Rewrite every output_text field of a terminal response.output item. */
	const patchTerminalOutput = (output: unknown[]): unknown[] => {
		return output.map((outputItem) => {
			if (
				outputItem &&
				typeof outputItem === "object" &&
				outputItem.type === "message" &&
				typeof outputItem.id === "string" &&
				Array.isArray(outputItem.content)
			) {
				const text = flushedById.get(outputItem.id);
				if (text === undefined) return outputItem;
				return {
					...outputItem,
					content: outputItem.content.map((block: any) =>
						block && typeof block === "object" && block.type === "output_text"
							? { ...block, text }
							: block,
					),
				};
			}
			return outputItem;
		});
	};

	const feedFrame = (frame: string): string[] => {
		// Parse the frame: an optional "event:" line plus "data:" lines.
		let eventName = "";
		const dataLines: string[] = [];
		for (const rawLine of frame.split(/\r?\n/)) {
			const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
			if (line.startsWith(":")) continue;
			if (line.startsWith("event:")) eventName = line.slice(6).trim();
			else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
		}
		if (dataLines.length === 0) return [`${frame}\n\n`]; // keep-alive / comment
		const dataStr = dataLines.join("\n");
		if (dataStr === "[DONE]") return [`${frame}\n\n`];
		let parsed: Record<string, any>;
		try {
			parsed = JSON.parse(dataStr) as Record<string, any>;
		} catch {
			return [`${frame}\n\n`];
		}
		const type = eventName || (typeof parsed.type === "string" ? parsed.type : "");

		switch (type) {
			case "response.output_item.added": {
				trackOutputIndex(parsed.output_index);
				if (parsed.item && typeof parsed.item === "object" && parsed.item.type === "message") {
					stateFor(parsed.item.id, parsed.output_index);
				}
				return [render(type, parsed)];
			}

			case "response.output_text.delta": {
				if (typeof parsed.delta !== "string") return [render(type, parsed)];
				trackOutputIndex(parsed.output_index);
				const state = stateFor(parsed.item_id, parsed.output_index);
				state.pending += parsed.delta;
				const text = pumpState(state, false);
				if (text.length === 0) return [];
				parsed.delta = text;
				return [render(type, parsed)];
			}

			case "response.output_text.done": {
				const state = stateFor(parsed.item_id, parsed.output_index);
				const extra = finalizeItem(state);
				const out: string[] = [];
				if (extra.length > 0) out.push(deltaFrame(state, extra));
				flushedById.set(state.itemId, state.flushed);
				if (typeof parsed.text === "string") parsed.text = state.flushed;
				out.push(render(type, parsed));
				return out;
			}

			case "response.content_part.added": {
				return [render(type, parsed)];
			}

			case "response.content_part.done": {
				// Some upstream items close with content_part.done only (no
				// output_text.done): finalize there too.
				const state = stateFor(parsed.item_id, parsed.output_index);
				const out: string[] = [];
				const extra = finalizeItem(state);
				if (extra.length > 0) out.push(deltaFrame(state, extra));
				flushedById.set(state.itemId, state.flushed);
				if (
					parsed.part &&
					typeof parsed.part === "object" &&
					typeof parsed.part.text === "string"
				) {
					parsed.part.text = state.flushed;
				}
				out.push(render(type, parsed));
				return out;
			}

			case "response.output_item.done": {
				trackOutputIndex(parsed.output_index);
				const item = parsed.item;
				if (item && typeof item === "object" && item.type === "message") {
					const state = stateFor(item.id, parsed.output_index);
					const out: string[] = [];
					const extra = finalizeItem(state);
					if (extra.length > 0) out.push(deltaFrame(state, extra));
					flushedById.set(state.itemId, state.flushed);
					if (Array.isArray(item.content)) {
						const text = state.flushed;
						item.content = item.content.map((block: any) =>
							block && typeof block === "object" && block.type === "output_text"
								? { ...block, text }
								: block,
						);
					}
					out.push(render(type, parsed));
					out.push(...emitCallFrames());
					return out;
				}
				return [render(type, parsed)];
			}

			case "response.completed":
			case "response.incomplete": {
				terminalSeen = true;
				const out: string[] = [];
				// Finalize every item (upstream closed without per-item done
				// events): residual text first, then any unemitted calls.
				for (const state of itemStates.values()) {
					const extra = finalizeItem(state);
					if (extra.length > 0) out.push(deltaFrame(state, extra));
					flushedById.set(state.itemId, state.flushed);
				}
				out.push(...emitCallFrames());
				const responseObj = parsed.response;
				if (responseObj && typeof responseObj === "object" && Array.isArray(responseObj.output)) {
					responseObj.output = patchTerminalOutput(responseObj.output);
					if (calls.length > 0 && !responseObj.output.some((outputItem: any) => outputItem?.type === "function_call")) {
						responseObj.output = [
							...responseObj.output,
							...calls.map((call) => ({
								id: call.itemId,
								type: "function_call",
								status: "completed",
								call_id: call.id,
								name: call.name,
								arguments: call.arguments,
							})),
						];
					}
				}
				out.push(render(type, parsed));
				return out;
			}

			default:
				// response.created / in_progress, reasoning events,
				// response.failed, unknown types: forward untouched.
				return [render(type, parsed)];
		}
	};

	return {
		feedFrame,
		isFinished: () => terminalSeen,
	};
}

/**
 * Wrap a Zen /responses Response whose body is an SSE stream: the
 * returned Response carries the rewritten stream. All other responses
 * should not be wrapped. `onUpstreamChunk` (debug) observes the raw
 * upstream bytes before rewriting.
 */
export function wrapZenSseResponse(response: Response, onUpstreamChunk?: (chunk: Uint8Array) => void): Response {
	if (response.body === null) return response;
	const rewriter = createZenStreamRewriter();
	const decoder = new TextDecoder();
	const encoder = new TextEncoder();
	let buffer = "";
	const transform = new TransformStream<Uint8Array, Uint8Array>({
		transform(chunk, controller) {
			try {
				onUpstreamChunk?.(chunk);
			} catch {
				// Debug tee must never break the stream.
			}
			buffer += decoder.decode(chunk, { stream: true });
			for (;;) {
				const match = /\r?\n\r?\n/.exec(buffer);
				if (match === null) break;
				const frame = buffer.slice(0, match.index);
				buffer = buffer.slice(match.index + match[0].length);
				for (const out of rewriter.feedFrame(frame)) controller.enqueue(encoder.encode(out));
			}
		},
		flush(controller) {
			buffer += decoder.decode();
			const rest = buffer.trim();
			if (rest.length === 0) return;
			// A frame-shaped tail goes through the rewriter; anything else
			// (e.g. a body that was not SSE after all) is forwarded raw.
			if (/^(?:event|data|:)\s?/m.test(rest)) {
				for (const out of rewriter.feedFrame(rest)) controller.enqueue(encoder.encode(out));
			} else {
				controller.enqueue(encoder.encode(buffer));
			}
		},
	});
	const headers = new Headers(response.headers);
	// The body length changed; these headers describe the upstream body.
	headers.delete("content-length");
	headers.delete("content-encoding");
	return new Response(response.body.pipeThrough(transform), {
		status: response.status,
		statusText: response.statusText,
		headers,
	});
}
