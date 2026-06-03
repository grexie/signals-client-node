import process from "node:process";
import { readFileSync } from "node:fs";
import { SignalsClient, SignalsManager } from "@grexie/signals-client";

loadDotEnv(".env");

const token = env("SIGNALS_WEBSOCKET_TOKEN", "");
const websocketUrl = env("SIGNALS_WEBSOCKET_URL", "wss://signals.grexie.com/ws");
const instruments = env("SIGNALS_INSTRUMENTS", "BTC-USDT-SWAP")
  .split(",")
  .map((value) => value.trim().toUpperCase())
  .filter(Boolean);
const equity = Number(env("SIGNALS_INITIAL_EQUITY", "1000"));

const abort = new AbortController();
process.once("SIGINT", () => abort.abort());
process.once("SIGTERM", () => abort.abort());

const client = new SignalsClient(token, { url: websocketUrl });
await client.connect();

const manager = new SignalsManager(client, {
  assets: [{ venue: "okx", currency: "USDT", cash: equity, available: equity, equity, maxUsage: 1 }]
}, {
  venue: "okx",
  instruments,
  risk: { maxMarginRatio: 1, maxConcurrentPositions: 1, minLeverage: 1, maxLeverage: 1 }
});

manager.on("message", (event) => {
  console.log(`info instrument=${event.instrument} stage=${event.stage} message="${event.message}"`);
});
manager.on("intent", (intent) => {
  console.log(`intent action=${intent.action ?? ""} reason=${intent.reason ?? ""} instrument=${intent.instrument} side=${intent.side} contracts=${intent.contractSize ?? 0} reduce_only=${!!intent.reduceOnly}`);
});
manager.on("protection", (event) => {
  console.log(`tpsl instrument=${event.instrument} side=${event.side} tp=${event.takeProfitPrice ?? 0} sl=${event.stopLossPrice ?? 0}`);
});
manager.on("withdrawal", (event) => {
  console.log(`withdraw currency=${event.currency} amount=${event.amount}`);
});

console.log(`signalsbot listening instruments=${instruments.join(",")} ws=${websocketUrl}`);
await manager.run(abort.signal);

function env(name, fallback) {
  return process.env[name] || fallback;
}

function loadDotEnv(path) {
  try {
    const text = readFileSync(path, "utf8");
    for (const line of text.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const index = trimmed.indexOf("=");
      if (index <= 0) continue;
      const key = trimmed.slice(0, index).trim();
      const value = trimmed.slice(index + 1).trim().replace(/^["']|["']$/g, "");
      process.env[key] ??= value;
    }
  } catch {
  }
}
