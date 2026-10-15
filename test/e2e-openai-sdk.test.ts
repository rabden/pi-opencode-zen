/**
 * End-to-end test: mock Zen /responses server -> patched globalThis.fetch
 * (response wrap only, as installed by index.ts) -> the REAL OpenAI SDK
 * streaming client, the same way pi-ai drives it.
 *
 * Verifies the rewritten SSE stream is accepted by the OpenAI SDK and
 * surfaces as clean text deltas + native function_call items.
 *
 * Run: node test/e2e-openai-sdk.test.ts
 */

import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { wrapZenSseResponse } from "../stream.ts";

const OPENAI_DIST =
	process.env.OPENAI_DIST ??
	"/home/rabden/.pi/agent/install/releases/1.2.0/node_modules/@earendil-works/pi-ai/node_modules/openai/index.mjs";

interface Collected {
	text: string;
	calls: Array<{ name: string; arguments: string }>;
	completed: boolean;
	usage: unknown;
}

async function loadOpenAI(): Promise<(new (opts: Record<string, unknown>) => any) | undefined> {
	try {
		const mod = (await import(OPENAI_DIST)) as { default: new (opts: Record<string, unknown>) => any };
		return mod.default;
	} catch {
		return undefined;
	}
}

async function main(): Promise<void> {
	const sse = [
		`event: response.created\ndata: ${JSON.stringify({ type: "response.created", response: { id: "resp_e2e", status: "in_progress", output: [] } })}\n\n`,
		`event: response.in_progress\ndata: ${JSON.stringify({ type: "response.in_progress", response: { id: "resp_e2e", status: "in_progress" } })}\n\n`,
		`event: response.output_item.added\ndata: ${JSON.stringify({ type: "response.output_item.added", output_index: 0, item: { id: "msg_e2e", type: "message", status: "in_progress", role: "assistant", content: [] } })}\n\n`,
		`event: response.content_part.added\ndata: ${JSON.stringify({ type: "response.content_part.added", item_id: "msg_e2e", output_index: 0, content_index: 0, part: { type: "output_text", text: "", annotations: [] } })}\n\n`,
	];
	const fullText = 'Checking.\n```tool\n{"name": "bash", "arguments": {"command": "echo hi"}}\n```\nAll set.';
	const tick = "`";
	const fence = `${tick}${tick}${tick}`;
	const pieces: string[] = [
		"Chec",
		`king.\n${tick}${tick}`,
		`${tick}tool\n{"name": "bash", "argu`,
		`ments": {"command": "echo hi"}}\n${fence}\nAll set.`,
	];
	for (const piece of pieces) {
		sse.push(`event: response.output_text.delta\ndata: ${JSON.stringify({ type: "response.output_text.delta", item_id: "msg_e2e", output_index: 0, content_index: 0, delta: piece })}\n\n`);
	}
	sse.push(
		`event: response.output_text.done\ndata: ${JSON.stringify({ type: "response.output_text.done", item_id: "msg_e2e", output_index: 0, content_index: 0, text: fullText })}\n\n`,
		`event: response.content_part.done\ndata: ${JSON.stringify({ type: "response.content_part.done", item_id: "msg_e2e", output_index: 0, content_index: 0, part: { type: "output_text", text: fullText, annotations: [] } })}\n\n`,
		`event: response.output_item.done\ndata: ${JSON.stringify({ type: "response.output_item.done", output_index: 0, item: { id: "msg_e2e", type: "message", status: "completed", role: "assistant", content: [{ type: "output_text", text: fullText, annotations: [] }] } })}\n\n`,
		`event: response.completed\ndata: ${JSON.stringify({ type: "response.completed", response: { id: "resp_e2e", status: "completed", output: [{ id: "msg_e2e", type: "message", status: "completed", role: "assistant", content: [{ type: "output_text", text: fullText, annotations: [] }] }], usage: { input_tokens: 12, output_tokens: 9, total_tokens: 21 } } })}\n\n`,
	);
	const body = sse.join("");

	const server: Server = createServer((req, res) => {
		assert.equal(req.method, "POST");
		res.writeHead(200, { "content-type": "text/event-stream" });
		// Chop the body into small TCP-sized writes.
		let i = 0;
		const timer = setInterval(() => {
			if (i >= body.length) {
				clearInterval(timer);
				res.end();
				return;
			}
			res.write(body.slice(i, i + 17));
			i += 17;
		}, 1);
	});
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const port = (server.address() as { port: number }).port;

	// The response-side half of index.ts's fetch patch (request shaping is
	// irrelevant here; the mock server accepts anything).
	const baseFetch = globalThis.fetch.bind(globalThis);
	globalThis.fetch = (async (input: unknown, init?: RequestInit): Promise<Response> => {
		const response = await baseFetch(input as RequestInfo | URL, init);
		if (
			response.ok &&
			response.body !== null &&
			(response.headers.get("content-type") ?? "").includes("text/event-stream") &&
			String(input).includes("/responses")
		) {
			return wrapZenSseResponse(response);
		}
		return response;
	}) as typeof globalThis.fetch;

	const { default: OpenAI } = (await import(OPENAI_DIST)) as { default: new (opts: Record<string, unknown>) => any };
	const client = new OpenAI({ apiKey: "public", baseURL: `http://127.0.0.1:${port}/v1`, dangerouslyAllowBrowser: true });

	const stream = await client.responses.create({ model: "muse-spark-1.3-contributor-free", input: "hi", stream: true });
	const collected: Collected = { text: "", calls: [], completed: false, usage: undefined };
	let currentCall: { name: string; arguments: string } | null = null;
	for await (const event of stream) {
		if (event.type === "response.output_text.delta") collected.text += event.delta;
		else if (event.type === "response.output_item.added" && event.item?.type === "function_call") {
			currentCall = { name: event.item.name, arguments: "" };
			collected.calls.push(currentCall);
		} else if (event.type === "response.function_call_arguments.delta") {
			currentCall!.arguments += event.delta;
		} else if (event.type === "response.completed") {
			collected.completed = true;
			collected.usage = event.response.usage;
		}
	}

	assert.equal(collected.completed, true, "stream completed");
	assert.ok(!collected.text.includes("```"), `no fence in streamed text: ${JSON.stringify(collected.text)}`);
	assert.equal(collected.text.trim(), "Checking.\n\nAll set.");
	assert.equal(collected.calls.length, 1);
	assert.equal(collected.calls[0].name, "bash");
	assert.deepEqual(JSON.parse(collected.calls[0].arguments), { command: "echo hi" });
	assert.deepEqual(collected.usage, { input_tokens: 12, output_tokens: 9, total_tokens: 21 });

	server.close();
	globalThis.fetch = baseFetch;
	console.log("ok  e2e: OpenAI SDK streams the rewritten SSE as native tool calls");
}

const openai = await loadOpenAI();
if (openai === undefined) {
	console.log("skip e2e: OpenAI SDK not importable (set OPENAI_DIST to run)");
} else {
	await main().catch((error) => {
		console.error(error instanceof Error ? error.stack : String(error));
		process.exit(1);
	});
}
