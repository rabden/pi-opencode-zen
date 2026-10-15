/**
 * Tests for stream.ts — the Zen SSE rewriter.
 *
 * Two layers:
 *  1. Structural: run upstream SSE text through wrapZenSseResponse and
 *     assert on the emitted frames (no fence leaks, native function_call
 *     events, terminal event intact).
 *  2. Conformance: feed the rewritten event stream into pi-ai's REAL
 *     processResponsesStream and assert on the AssistantMessage it
 *     builds — the exact code path Pi uses for native-tool providers.
 *
 * Run: node test/stream.test.ts   (or: bun test/stream.test.ts)
 * PI_AI_DIST can point at another pi-ai install; the conformance layer
 * is skipped when it cannot be imported.
 */

import assert from "node:assert/strict";
import { wrapZenSseResponse } from "../stream.ts";

const PI_AI_DIST =
	process.env.PI_AI_DIST ??
	"/home/rabden/.pi/agent/install/releases/1.2.0/node_modules/@earendil-works/pi-ai/dist/api/openai-responses-shared.js";

// ---------------------------------------------------------------------------
// Upstream stream builder: a realistic Zen /responses SSE stream.
// ---------------------------------------------------------------------------

interface UpstreamOptions {
	deltas: string[];
	reasoning?: string[];
	usage?: Record<string, unknown>;
	status?: "completed" | "incomplete";
	itemDone?: boolean;
	textDone?: boolean;
}

function frame(type: string, data: Record<string, unknown>): string {
	return `event: ${type}\ndata: ${JSON.stringify({ sequence_number: -1, ...data, type })}\n\n`;
}

function buildUpstream(options: UpstreamOptions): string {
	const fullText = options.deltas.join("");
	const out: string[] = [];
	let seq = 0;
	const n = (): number => seq++;
	out.push(
		frame("response.created", {
			sequence_number: n(),
			response: { id: "resp_zen_test", status: "in_progress", output: [] },
		}),
	);
	out.push(frame("response.in_progress", { sequence_number: n(), response: { id: "resp_zen_test", status: "in_progress" } }));
	let index = 0;
	if (options.reasoning && options.reasoning.length > 0) {
		out.push(
			frame("response.output_item.added", {
				sequence_number: n(),
				output_index: index,
				item: { id: "rs_1", type: "reasoning", status: "in_progress", summary: [] },
			}),
		);
		for (const delta of options.reasoning) {
			out.push(
				frame("response.reasoning_summary_text.delta", {
					sequence_number: n(),
					item_id: "rs_1",
					output_index: index,
					delta,
				}),
			);
		}
		out.push(
			frame("response.output_item.done", {
				sequence_number: n(),
				output_index: index,
				item: { id: "rs_1", type: "reasoning", status: "completed", summary: [{ type: "summary_text", text: options.reasoning.join("") }] },
			}),
		);
		index++;
	}
	out.push(
		frame("response.output_item.added", {
			sequence_number: n(),
			output_index: index,
			item: { id: "msg_1", type: "message", status: "in_progress", role: "assistant", content: [] },
		}),
	);
	out.push(
		frame("response.content_part.added", {
			sequence_number: n(),
			item_id: "msg_1",
			output_index: index,
			content_index: 0,
			part: { type: "output_text", text: "", annotations: [] },
		}),
	);
	for (const delta of options.deltas) {
		out.push(
			frame("response.output_text.delta", {
				sequence_number: n(),
				item_id: "msg_1",
				output_index: index,
				content_index: 0,
				delta,
			}),
		);
	}
	if (options.textDone !== false) {
		out.push(
			frame("response.output_text.done", {
				sequence_number: n(),
				item_id: "msg_1",
				output_index: index,
				content_index: 0,
				text: fullText,
			}),
		);
		out.push(
			frame("response.content_part.done", {
				sequence_number: n(),
				item_id: "msg_1",
				output_index: index,
				content_index: 0,
				part: { type: "output_text", text: fullText, annotations: [] },
			}),
		);
	}
	if (options.itemDone !== false) {
		out.push(
			frame("response.output_item.done", {
				sequence_number: n(),
				output_index: index,
				item: {
					id: "msg_1",
					type: "message",
					status: "completed",
					role: "assistant",
					content: [{ type: "output_text", text: fullText, annotations: [] }],
				},
			}),
		);
	}
	const status = options.status ?? "completed";
	out.push(
		frame(`response.${status}`, {
			sequence_number: n(),
			response: {
				id: "resp_zen_test",
				status,
				output: [
					{
						id: "msg_1",
						type: "message",
						status: "completed",
						role: "assistant",
						content: [{ type: "output_text", text: fullText, annotations: [] }],
					},
				],
				usage: options.usage ?? { input_tokens: 10, output_tokens: 7, total_tokens: 17 },
			},
		}),
	);
	return out.join("");
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async function rewrite(upstream: string, chunkSize = 13): Promise<string> {
	const encoder = new TextEncoder();
	const chunks: Uint8Array[] = [];
	for (let i = 0; i < upstream.length; i += chunkSize) {
		chunks.push(encoder.encode(upstream.slice(i, i + chunkSize)));
	}
	const body = new ReadableStream<Uint8Array>({
		start(controller) {
			for (const chunk of chunks) controller.enqueue(chunk);
			controller.close();
		},
	});
	const response = wrapZenSseResponse(
		new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } }),
	);
	return await response.text();
}

function parseEvents(sse: string): Record<string, any>[] {
	const events: Record<string, any>[] = [];
	for (const frameText of sse.split(/\r?\n\r?\n/)) {
		if (frameText.trim().length === 0) continue;
		const dataLines = frameText
			.split(/\r?\n/)
			.filter((line) => line.startsWith("data:"))
			.map((line) => line.slice(5).replace(/^ /, ""));
		if (dataLines.length === 0) continue;
		const data = dataLines.join("\n");
		if (data === "[DONE]") continue;
		try {
			events.push(JSON.parse(data) as Record<string, any>);
		} catch {
			// forwarded non-JSON frame
		}
	}
	return events;
}

interface PiRun {
	output: Record<string, any>;
	streamEvents: Record<string, any>[];
}

async function loadPiProcessor(): Promise<((...args: any[]) => Promise<void>) | undefined> {
	try {
		const mod = (await import(PI_AI_DIST)) as { processResponsesStream?: (...args: any[]) => Promise<void> };
		return mod.processResponsesStream;
	} catch {
		return undefined;
	}
}

async function runThroughPi(events: Record<string, any>[]): Promise<PiRun> {
	const processResponsesStream = await loadPiProcessor();
	assert.ok(processResponsesStream, "processResponsesStream not importable");
	const output: Record<string, any> = {
		role: "assistant",
		content: [],
		provider: "opencode-zen",
		model: "muse-spark-1.3-contributor-free",
	};
	const streamEvents: Record<string, any>[] = [];
	const model: Record<string, any> = {
		provider: "opencode-zen",
		id: "muse-spark-1.3-contributor-free",
		api: "openai-responses",
		input: ["text"],
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
	};
	await processResponsesStream(
		events as any,
		output as any,
		{ push: (event: any) => streamEvents.push(event) } as any,
		model as any,
	);
	return { output, streamEvents };
}

const allText = (events: Record<string, any>[]): string =>
	events
		.filter((e) => e.type === "response.output_text.delta" && typeof e.delta === "string")
		.map((e) => e.delta)
		.join("");

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const tests: Array<{ name: string; fn: () => Promise<void> }> = [];

const test = (name: string, fn: () => Promise<void>): void => {
	tests.push({ name, fn });
};

test("tool block split across deltas becomes a native call, not text", async () => {
	const upstream = buildUpstream({
		deltas: ["Let me check.\n``", "`tool\n{\"name\": \"calc\", \"argu", "ments\": {\"expr\": \"2+2\"}}\n```", " Done."],
	});
	const out = await rewrite(upstream);
	const events = parseEvents(out);
	assert.ok(!out.includes("```"), "no fence leaks into the rewritten stream");
	const deltas = allText(events);
	assert.equal(deltas.includes("Let me check."), true);
	assert.equal(deltas.includes(" Done."), true);
	const added = events.filter((e) => e.type === "response.output_item.added" && e.item?.type === "function_call");
	assert.equal(added.length, 1);
	assert.equal(added[0].item.name, "calc");
	assert.equal(added[0].item.call_id.startsWith("call_zen_"), true);
	assert.equal(added[0].item.id.startsWith("fc_"), true);
	const argsDone = events.find((e) => e.type === "response.function_call_arguments.done");
	assert.equal(argsDone.arguments, '{"expr":"2+2"}');
	const itemDone = events.filter((e) => e.type === "response.output_item.done" && e.item?.type === "function_call");
	assert.equal(itemDone.length, 1);
	assert.equal(itemDone[0].item.status, "completed");
	// Message item text is rewritten clean, function item comes after it.
	const doneIndexes = events
		.filter((e) => e.type === "response.output_item.done")
		.map((e) => ({ type: e.item?.type, index: e.output_index }));
	assert.deepEqual(
		doneIndexes.map((d) => d.type),
		["message", "function_call"],
	);
	assert.ok(doneIndexes[1].index > doneIndexes[0].index, "function call item after message item");
	const messageDone = events.find((e) => e.type === "response.output_item.done" && e.item?.type === "message");
	assert.equal(messageDone.item.content[0].text.includes("```"), false);
	const completed = events.find((e) => e.type === "response.completed");
	assert.ok(completed, "terminal event preserved");
	assert.equal(completed.response.usage.input_tokens, 10);
	const fnInOutput = completed.response.output.find((i: Record<string, any>) => i.type === "function_call");
	assert.equal(fnInOutput.name, "calc");
	// Conformance with pi's real processor.
	const pi = await runThroughPi(events);
	assert.equal(pi.output.stopReason, "toolUse");
	assert.equal(pi.output.content[0].type, "text");
	assert.equal(pi.output.content[0].text, "Let me check.\n Done.");
	const call = pi.output.content.find((b: Record<string, any>) => b.type === "toolCall");
	assert.equal(call.name, "calc");
	assert.deepEqual(call.arguments, { expr: "2+2" });
	assert.equal(typeof call.id, "string");
	const kinds = pi.streamEvents.map((e) => e.type);
	assert.ok(kinds.includes("toolcall_start") && kinds.includes("toolcall_end"));
	assert.ok(pi.streamEvents.every((e) => JSON.stringify(e).indexOf("```tool") === -1 || e.type === "toolcall_end" || e.type === "toolcall_start" || e.type === "toolcall_delta" || e.type === "text_delta" || e.type === "text_start" || e.type === "text_end"));
});

test("plain text passes through untouched, stopReason stop", async () => {
	const upstream = buildUpstream({ deltas: ["Hello ", "world", " café ✓ 😀"] });
	const out = await rewrite(upstream);
	const events = parseEvents(out);
	assert.equal(allText(events), "Hello world café ✓ 😀");
	assert.equal(events.some((e) => e.type === "response.output_item.added" && e.item?.type === "function_call"), false);
	const pi = await runThroughPi(events);
	assert.equal(pi.output.stopReason, "stop");
	assert.equal(pi.output.content[0].text, "Hello world café ✓ 😀");
	assert.equal(pi.output.content.length, 1);
	assert.equal(pi.output.usage.input, 10);
});

test("partial fence opener at a chunk tail is held, then flushed", async () => {
	const upstream = buildUpstream({ deltas: ["Use `x", " and `", "`code` too"] });
	const out = await rewrite(upstream);
	const events = parseEvents(out);
	assert.equal(allText(events), "Use `x and ``code` too");
	const pi = await runThroughPi(events);
	assert.equal(pi.output.content[0].text, "Use `x and ``code` too");
	assert.equal(pi.output.stopReason, "stop");
});

test("unparseable block is re-emitted as text for self-correction", async () => {
	const upstream = buildUpstream({
		deltas: ["Trying.\n```tool\nnot json at all\n```\nAfter."],
	});
	const out = await rewrite(upstream);
	const events = parseEvents(out);
	assert.equal(events.some((e) => e.type === "response.function_call_arguments.done"), false);
	const text = allText(events);
	assert.ok(text.includes("```tool\nnot json at all\n```"), "raw block stays visible");
	assert.ok(text.includes("After."));
	const pi = await runThroughPi(events);
	assert.equal(pi.output.stopReason, "stop");
	assert.equal(pi.output.content.filter((b: Record<string, any>) => b.type === "toolCall").length, 0);
});

test("unclosed fence at stream end stays text", async () => {
	const upstream = buildUpstream({ deltas: ["```tool\n{\"name\": \"x\""] });
	const out = await rewrite(upstream);
	const events = parseEvents(out);
	assert.ok(allText(events).includes("```tool"));
	const pi = await runThroughPi(events);
	assert.equal(pi.output.stopReason, "stop");
});

test("two parallel calls in one turn both translate", async () => {
	const upstream = buildUpstream({
		deltas: [
			"Reading both.\n```tool\n{\"name\": \"read\", \"arguments\": {\"path\": \"a\"}}\n```\n```tool\n{\"name\": \"bash\", \"arguments\": {\"command\": \"ls\"}}\n```",
		],
	});
	const out = await rewrite(upstream);
	const events = parseEvents(out);
	const added = events.filter((e) => e.type === "response.output_item.added" && e.item?.type === "function_call");
	assert.equal(added.length, 2);
	const pi = await runThroughPi(events);
	const calls = pi.output.content.filter((b: Record<string, any>) => b.type === "toolCall");
	assert.equal(calls.length, 2);
	assert.deepEqual(calls.map((c: Record<string, any>) => c.name).sort(), ["bash", "read"]);
	assert.deepEqual(calls[0].arguments, { path: "a" });
	assert.equal(pi.output.stopReason, "toolUse");
});

test("pure tool turn (no prose) still yields a call and an empty text block", async () => {
	const upstream = buildUpstream({ deltas: ["```tool\n{\"name\": \"now\", \"arguments\": {}}\n```"] });
	const out = await rewrite(upstream);
	const events = parseEvents(out);
	const pi = await runThroughPi(events);
	const call = pi.output.content.find((b: Record<string, any>) => b.type === "toolCall");
	assert.equal(call.name, "now");
	assert.deepEqual(call.arguments, {});
	assert.equal(pi.output.stopReason, "toolUse");
});

test("string arguments pass through as JSON", async () => {
	const upstream = buildUpstream({ deltas: ['```tool\n{"name": "t", "arguments": "{\\"k\\": 1}"}\n```'] });
	const events = parseEvents(await rewrite(upstream));
	const pi = await runThroughPi(events);
	const call = pi.output.content.find((b: Record<string, any>) => b.type === "toolCall");
	assert.deepEqual(call.arguments, { k: 1 });
});

test("reasoning events are forwarded untouched", async () => {
	const upstream = buildUpstream({
		deltas: ["Answer."],
		reasoning: ["Think", " hard"],
	});
	const events = parseEvents(await rewrite(upstream));
	const pi = await runThroughPi(events);
	const thinking = pi.output.content.find((b: Record<string, any>) => b.type === "thinking");
	assert.equal(thinking.thinking, "Think hard");
	assert.equal(pi.output.content.find((b: Record<string, any>) => b.type === "text").text, "Answer.");
});

test("missing message item.done: calls emitted before response.completed", async () => {
	const upstream = buildUpstream({ deltas: ["Go.\n```tool\n{\"name\": \"go\", \"arguments\": {}}\n```"], itemDone: false, textDone: false });
	const events = parseEvents(await rewrite(upstream));
	const completedIndex = events.findIndex((e) => e.type === "response.completed");
	const callDoneIndex = events.findIndex((e) => e.type === "response.output_item.done" && e.item?.type === "function_call");
	assert.ok(completedIndex > 0 && callDoneIndex > 0 && callDoneIndex < completedIndex, "call item done before terminal event");
	const pi = await runThroughPi(events);
	assert.equal(pi.output.stopReason, "toolUse");
	assert.equal(pi.output.content.find((b: Record<string, any>) => b.type === "toolCall").name, "go");
});

test("incomplete status with max_output_tokens maps to length stop reason", async () => {
	const base = buildUpstream({ deltas: ["Partial"], status: "incomplete" });
	// Real truncation events carry incomplete_details.reason; patch the
	// terminal event before feeding pi's processor.
	const events = parseEvents(await rewrite(base));
	const terminal = events.find((e) => e.type === "response.incomplete");
	assert.ok(terminal, "terminal event forwarded");
	terminal.response.incomplete_details = { reason: "max_output_tokens" };
	const pi = await runThroughPi(events);
	assert.equal(pi.output.stopReason, "length");
});

test("CRLF framing is handled", async () => {
	const upstream = buildUpstream({ deltas: ["Hi.\n```tool\n{\"name\": \"c\", \"arguments\": {}}\n```"] }).replaceAll("\n", "\r\n");
	const events = parseEvents(await rewrite(upstream));
	const pi = await runThroughPi(events);
	assert.equal(pi.output.content.find((b: Record<string, any>) => b.type === "toolCall").name, "c");
	// The newline before the removed fence stays (markdown-invisible).
	assert.equal(pi.output.content.find((b: Record<string, any>) => b.type === "text").text, "Hi.\n");
});

test("text before a fence streams live; text after the fence closes streams too", async () => {
	const upstream = buildUpstream({
		deltas: ["Before.\n```tool\n{\"name\": \"t\", \"arguments\": {}}\n```\nAfter."],
	});
	const events = parseEvents(await rewrite(upstream));
	const deltas = events.filter((e) => e.type === "response.output_text.delta");
	const beforeFenceArrives = deltas.findIndex((e) => e.delta.includes("Before."));
	assert.ok(beforeFenceArrives >= 0);
	// "After." must arrive as its own delta after the fence closed (live pump),
	// not only via the done-event rewrite.
	assert.ok(deltas.some((e) => e.delta.includes("After.")), "post-fence text streams live");
});

test("real Zen shape: prose item and fence item separate — no duplicated text", async () => {
	// Captured from the live endpoint 2026-10-15: a reasoning item, a prose
	// message item, and the ```tool fence as its OWN message item.
	const seq = (() => {
		let n = 0;
		return () => n++;
	})();
	const f = (type: string, data: Record<string, unknown>): string => `event: ${type}\ndata: ${JSON.stringify({ sequence_number: seq(), ...data, type })}\n\n`;
	const upstream = [
		f("response.created", { response: { id: "resp_r", status: "in_progress", output: [] } }),
		f("response.output_item.added", { output_index: 0, item: { id: "rs_a", type: "reasoning", status: "in_progress", summary: [] } }),
		f("response.reasoning_summary_text.delta", { item_id: "rs_a", output_index: 0, delta: "Thinking." }),
		f("response.output_item.done", { output_index: 0, item: { id: "rs_a", type: "reasoning", status: "completed", summary: [{ type: "summary_text", text: "Thinking." }] } }),
		f("response.output_item.added", { output_index: 1, item: { id: "rs_b", type: "message", status: "in_progress", role: "assistant", content: [] } }),
		f("response.content_part.added", { item_id: "rs_b", output_index: 1, content_index: 0, part: { type: "output_text", text: "", annotations: [] } }),
		f("response.output_text.delta", { item_id: "rs_b", output_index: 1, content_index: 0, delta: "Running probe command." }),
		f("response.output_text.done", { item_id: "rs_b", output_index: 1, content_index: 0, text: "Running probe command." }),
		f("response.content_part.done", { item_id: "rs_b", output_index: 1, content_index: 0, part: { type: "output_text", text: "Running probe command.", annotations: [] } }),
		f("response.output_item.done", { output_index: 1, item: { id: "rs_b", type: "message", status: "completed", role: "assistant", content: [{ type: "output_text", text: "Running probe command.", annotations: [] }] } }),
		f("response.output_item.added", { output_index: 2, item: { id: "msg_c", type: "message", status: "in_progress", role: "assistant", content: [] } }),
		f("response.content_part.added", { item_id: "msg_c", output_index: 2, content_index: 0, part: { type: "output_text", text: "", annotations: [] } }),
		f("response.output_text.delta", { item_id: "msg_c", output_index: 2, content_index: 0, delta: "```" }),
		f("response.output_text.delta", { item_id: "msg_c", output_index: 2, content_index: 0, delta: 'tool\n{"name": "bash",' }),
		f("response.output_text.delta", { item_id: "msg_c", output_index: 2, content_index: 0, delta: ' "arguments": {"command": "echo hi"}}\n```' }),
		f("response.content_part.done", { item_id: "msg_c", output_index: 2, content_index: 0, part: { type: "output_text", text: '```tool\n{"name": "bash", "arguments": {"command": "echo hi"}}\n```', annotations: [] } }),
		f("response.output_item.done", { output_index: 2, item: { id: "msg_c", type: "message", status: "completed", role: "assistant", content: [{ type: "output_text", text: '```tool\n{"name": "bash", "arguments": {"command": "echo hi"}}\n```', annotations: [] }] } }),
		f("response.completed", {
			response: {
				id: "resp_r",
				status: "completed",
				output: [
					{ id: "rs_a", type: "reasoning", status: "completed", summary: [] },
					{ id: "rs_b", type: "message", status: "completed", role: "assistant", content: [{ type: "output_text", text: "Running probe command.", annotations: [] }] },
					{ id: "msg_c", type: "message", status: "completed", role: "assistant", content: [{ type: "output_text", text: '```tool\n{"name": "bash", "arguments": {"command": "echo hi"}}\n```', annotations: [] }] },
			],
				usage: { input_tokens: 10, output_tokens: 5, total_tokens: 15 },
			},
		}),
	].join("");
	const out = await rewrite(upstream);
	assert.ok(!out.includes("```tool"), "no fence leaks anywhere in the rewritten stream");
	const events = parseEvents(out);
	const pi = await runThroughPi(events);
	const texts = pi.output.content.filter((b: Record<string, any>) => b.type === "text").map((b: Record<string, any>) => b.text);
	assert.equal(texts.filter((t: string) => t.includes("Running probe command.")).length, 1, "prose appears exactly once (no cross-item duplication)");
	const thinking = pi.output.content.find((b: Record<string, any>) => b.type === "thinking");
	assert.equal(thinking.thinking, "Thinking.");
	const call = pi.output.content.find((b: Record<string, any>) => b.type === "toolCall");
	assert.equal(call.name, "bash");
	assert.deepEqual(call.arguments, { command: "echo hi" });
	assert.equal(pi.output.stopReason, "toolUse");
	// Terminal response.output patched per item id.
	const completed = events.find((e) => e.type === "response.completed");
	const outputTexts = completed.response.output.filter((i: Record<string, any>) => i.type === "message").map((i: Record<string, any>) => i.content[0].text);
	assert.deepEqual(outputTexts, ["Running probe command.", ""]);
});

test("non-SSE responses are not mangled by the rewriter path (sanity)", async () => {
	// wrapZenSseResponse is only called for SSE bodies by index.ts; verify it
	// still behaves when handed a JSON body by mistake: frames without data
	// lines pass through.
	const encoder = new TextEncoder();
	const body = new ReadableStream<Uint8Array>({
		start(controller) {
			controller.enqueue(encoder.encode('{"ok": true}'));
			controller.close();
		},
	});
	const response = wrapZenSseResponse(new Response(body, { status: 200, headers: { "content-type": "application/json" } }));
	assert.equal(await response.text(), '{"ok": true}');
});

// ---------------------------------------------------------------------------

async function main(): Promise<void> {
	let failed = 0;
	for (const { name, fn } of tests) {
		try {
			await fn();
			console.log(`  ok  ${name}`);
		} catch (error) {
			failed++;
			console.error(`FAIL  ${name}`);
			console.error(error instanceof Error ? error.stack : String(error));
		}
	}
	const pi = await loadPiProcessor();
	console.log(pi ? "\n(conformance ran against real processResponsesStream)" : "\n(pi-ai not importable: structural tests only)");
	if (failed > 0) {
		process.exit(1);
	}
	console.log(`\n${tests.length} tests passed`);
}

await main();
