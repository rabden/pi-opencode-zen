#!/usr/bin/env node
/**
 * Regenerate models-meta.ts from pi-ai's bundled OpenCode catalog.
 * Usage: PI_AI_DATA=/path/to/pi-ai/dist/providers/data/opencode.json node scripts/generate-models-meta.mjs
 * (default path targets the active pi install layout)
 */
import { readFileSync, writeFileSync } from "node:fs";
import { globSync } from "node:fs";
const candidates = [
	process.env.PI_AI_DATA,
	...globSync(
		`${process.env.HOME}/.pi/agent/install/releases/*/node_modules/@earendil-works/pi-ai/dist/providers/data/opencode.json`,
	),
].filter(Boolean);
if (candidates.length === 0) {
	console.error("opencode.json not found; set PI_AI_DATA");
	process.exit(1);
}
const path = candidates[0];
const raw = JSON.parse(readFileSync(path, "utf8"));

/**
 * Empirical corrections over pi-ai's catalog, verified against the live
 * endpoint (2026-10-15): a rejected reasoning effort makes upstream
 * answer 400 naming the supported set — for muse-spark-1.3-contributor:
 * [minimal, low, medium, high, xhigh, max]. pi-ai's snapshot wrongly
 * hides max. Keep this table when regenerating.
 */
const OVERRIDES = {
	"muse-spark-1.2": { thinkingLevelMap: { off: null, minimal: "minimal", low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" } },
	"muse-spark-1.3": { thinkingLevelMap: { off: null, minimal: "minimal", low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" } },
	"muse-spark-1.3-contributor-free": { thinkingLevelMap: { off: null, minimal: "minimal", low: "low", medium: "medium", high: "high", xhigh: "xhigh", max: "max" } },
};
const entries = {};
for (const models of Object.values(raw)) {
	for (const [key, m] of Object.entries(models)) {
		if (!key.startsWith("chat:")) continue;
			const meta = {};
		if (m.name) meta.name = m.name;
		meta.api = m.api;
		meta.reasoning = m.reasoning ?? false;
		if (m.input) meta.input = m.input;
		if (m.inputLimits) meta.inputLimits = m.inputLimits;
		if (m.cost) meta.cost = m.cost;
		if (typeof m.contextWindow === "number") meta.contextWindow = m.contextWindow;
		if (typeof m.maxTokens === "number") meta.maxTokens = m.maxTokens;
		if (m.thinkingLevelMap) meta.thinkingLevelMap = m.thinkingLevelMap;
		const override = OVERRIDES[m.id];
		entries[m.id] = override ? { ...meta, ...override } : meta;
	}
}
const header = `/**
 * Zen model metadata (names, thinking levels, context/output limits),
 * generated from pi-ai's bundled OpenCode catalog
 * (dist/providers/data/opencode.json) — the same metadata pi's built-in
 * opencode provider uses. Regenerate with:
 *   node scripts/generate-models-meta.mjs
 * compat flags are deliberately NOT carried over: they are tuned for
 * each model's native API, while the gate serves every free model on
 * /responses and the extension must keep its verified wire shape.
 */
export interface ZenModelMeta {
\tname?: string;
\treasoning: boolean;
\tinput?: string[];
\tinputLimits?: Record<string, unknown>;
\tcost?: { input: number; output: number; cacheRead: number; cacheWrite: number };
\tcontextWindow?: number;
\tmaxTokens?: number;
\tthinkingLevelMap?: Record<string, string | null>;
}

export const ZEN_MODEL_META: Record<string, ZenModelMeta> = `;
writeFileSync(
	new URL("../models-meta.ts", import.meta.url).pathname,
	header + JSON.stringify(entries, null, "\t") + ";\n",
);
console.log(`wrote ${Object.keys(entries).length} entries from ${path}`);
