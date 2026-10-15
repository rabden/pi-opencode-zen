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

const FENCE_OPEN = "```tool";

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
 * How much of the buffer is safe to flush live: everything except a tail
 * that could still grow into a fence opener ("`", "``", "```", "```t", …).
 */
function safeTail(buffer: string): number {
	const maxHold = Math.min(FENCE_OPEN.length - 1, buffer.length);
	for (let hold = maxHold; hold >= 1; hold--) {
		if (buffer.endsWith(FENCE_OPEN.slice(0, hold))) return buffer.length - hold;
	}
	return buffer.length;
}

/** Fence-holdback state for one message output item. */
interface ItemTextState {
	/** Unflushed tail: starts at an open fence or a partial fence opener. */
	pending: string;
	/** Text flushed (or re-emitted) for this item — its final text. */
	flushed: string;
	itemId: string;
	finalized: boolean;
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

	/** Per-output-item text state (Zen splits prose and fences across items). */
	const itemStates = new Map<number, ItemTextState>();
	/** Item id -> final fence-free text, for terminal-response output patching. */
	const flushedById = new Map<string, string>();
	/** Fallback target for events that omit output_index. */
	let lastTextIndex = 0;

	const stateFor = (outputIndex: unknown): ItemTextState => {
		const index = typeof outputIndex === "number" && Number.isInteger(outputIndex) ? outputIndex : lastTextIndex;
		lastTextIndex = index;
		let state = itemStates.get(index);
		if (state === undefined) {
			state = { pending: "", flushed: "", itemId: `msg_zen_${index}`, finalized: false };
			itemStates.set(index, state);
		}
		return state;
	};

	const render = (type: string, data: Record<string, unknown>): string => {
		data.type = type;
		data.sequence_number = sequence++;
		return `event: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
	};

	const deltaFrame = (state: ItemTextState, index: number, text: string): string =>
		render("response.output_text.delta", {
			item_id: state.itemId,
			output_index: index,
			content_index: 0,
			delta: text,
		});

	/**
	 * Consume an item's pending buffer: flush text that cannot be part
	 * of a tool block, harvest complete blocks as calls, re-emit
	 * unparseable blocks as text. With final=false an open or partial
	 * fence is held; with final=true the remainder is decided (unclosed
	 * fences become text unless they still parse as a call).
	 */
	const pumpState = (state: ItemTextState, final: boolean): string => {
		let outText = "";
		for (;;) {
			const open = state.pending.indexOf(FENCE_OPEN);
			if (open === -1) {
				const safe = final ? state.pending.length : safeTail(state.pending);
				if (safe > 0) {
					outText += state.pending.slice(0, safe);
					state.pending = state.pending.slice(safe);
				}
				break;
			}
			if (open > 0) {
				outText += state.pending.slice(0, open);
				state.pending = state.pending.slice(open);
			}
			const close = state.pending.indexOf("```", FENCE_OPEN.length);
			if (close === -1) {
				if (final) {
					const call = parseToolBlock(state.pending.slice(FENCE_OPEN.length));
					if (call) {
						const ids = mintToolCallIds();
						calls.push({ ...ids, ...call });
					} else {
						outText += state.pending;
					}
					state.pending = "";
				}
				break;
			}
			const call = parseToolBlock(state.pending.slice(FENCE_OPEN.length, close));
			if (call) {
				const ids = mintToolCallIds();
				calls.push({ ...ids, ...call });
			} else {
				// Not a valid call: keep it visible as text.
				outText += state.pending.slice(0, close + 3);
			}
			state.pending = state.pending.slice(close + 3);
		}
		state.flushed += outText;
		return outText;
	};

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
					const state = stateFor(parsed.output_index);
					if (typeof parsed.item.id === "string") state.itemId = parsed.item.id;
				}
				return [render(type, parsed)];
			}

			case "response.output_text.delta": {
				if (typeof parsed.delta !== "string") return [render(type, parsed)];
				trackOutputIndex(parsed.output_index);
				const state = stateFor(parsed.output_index);
				if (typeof parsed.item_id === "string") state.itemId = parsed.item_id;
				state.pending += parsed.delta;
				const text = pumpState(state, false);
				if (text.length === 0) return [];
				parsed.delta = text;
				return [render(type, parsed)];
			}

			case "response.output_text.done": {
				const state = stateFor(parsed.output_index);
				const extra = finalizeItem(state);
				const out: string[] = [];
				if (extra.length > 0) {
					const index = typeof parsed.output_index === "number" ? parsed.output_index : lastTextIndex;
					out.push(deltaFrame(state, index, extra));
				}
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
				const state = stateFor(parsed.output_index);
				const out: string[] = [];
				const extra = finalizeItem(state);
				if (extra.length > 0) {
					const index = typeof parsed.output_index === "number" ? parsed.output_index : lastTextIndex;
					out.push(deltaFrame(state, index, extra));
				}
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
					const state = stateFor(parsed.output_index);
					const out: string[] = [];
					const extra = finalizeItem(state);
					if (extra.length > 0) {
						const index = typeof parsed.output_index === "number" ? parsed.output_index : lastTextIndex;
						out.push(deltaFrame(state, index, extra));
					}
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
				for (const [index, state] of itemStates) {
					const extra = finalizeItem(state);
					if (extra.length > 0) out.push(deltaFrame(state, index, extra));
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
