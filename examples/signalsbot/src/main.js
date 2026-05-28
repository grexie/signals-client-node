import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import process from "node:process";
import WebSocket from "ws";
import {
  PositionManager,
  SignalsClient,
  productionPositionManagerConfig
} from "@grexie/signals-client";

const defaultSignalsWsUrl = "wss://signals.grexie.com/ws";
const defaultOkxBaseUrl = "https://www.okx.com";
const defaultOkxWsUrl = "wss://ws.okx.com:8443";
const defaultDbPath = "./data/signalsbot.json";
const defaultEquity = 10000;

loadDotEnv(".env");

const command = process.argv[2] ?? "papertrader";
if (command === "clean") {
  cleanDb();
  process.exit(0);
}
if (command !== "papertrader") {
  console.error("usage: signalsbot [papertrader|clean]");
  process.exit(2);
}

const config = loadConfig();
const store = new FileStore(config.dbPath);
store.load();

const manager = new PositionManager(undefined, productionPositionManagerConfig({
  initialState: reviveState(store.state),
  persist: (state) => {
    store.state = serializeState(state);
    store.save();
  }
}));
const bot = new SignalsBot(manager, store, config.initialEquity);
bot.closedRealized = store.closedRealized();
bot.lastClosedCount = (store.state.closedTrades ?? []).length;
bot.syncAsset();

for (const instrument of config.instruments) {
  const metadata = await fetchOkxInstrument(config.okxBaseUrl, instrument);
  manager.instrumentManager().updateInstrument(metadata);
  const tick = await fetchLatestCandle(config.okxBaseUrl, config.candleBar, instrument);
  if (tick) {
    bot.latestPriceByKey.set(positionKey("okx", instrument), tick);
    await bot.handleOrders(manager.updatePrice("okx", instrument, tick.price, tick.timestamp));
  }
  console.log(
    `Loaded OKX instrument instrument=${metadata.instrument} settlement=${metadata.settlementCurrency} lot=${fmt(metadata.lotSize)} min=${fmt(metadata.minSize)} tick=${fmt(metadata.tickSize)} contract=${fmt(metadata.contractValue)}`
  );
}

if ((store.state.positions?.length ?? 0) > 0 || (store.state.closedTrades?.length ?? 0) > 0) {
  console.log(`Hydrated position manager state open_positions=${store.state.positions?.length ?? 0} closed_trades=${store.state.closedTrades?.length ?? 0}`);
}

const abort = new AbortController();
process.once("SIGINT", () => abort.abort());
process.once("SIGTERM", () => abort.abort());

subscribeOkxCandles(config.okxWsUrl, config.candleBar, config.instruments, abort.signal, (tick) => {
  bot.enqueue(() => {
    bot.latestPriceByKey.set(positionKey("okx", tick.instrument), tick);
    bot.handleOrders(manager.updatePrice("okx", tick.instrument, tick.price, tick.timestamp));
  });
});

const statsTimer = setInterval(() => {
  bot.enqueue(() => bot.reportStats());
}, config.statsIntervalMs);
abort.signal.addEventListener("abort", () => clearInterval(statsTimer), { once: true });

const client = new SignalsClient(config.token, { url: config.websocketUrl });
await client.connect();
for (const instrument of config.instruments) {
  client.subscribe("okx", instrument);
  console.log(`Subscribed to Grexie Signals venue=okx instrument=${instrument}`);
}
console.log(`signalsbot running instruments=${config.instruments.join(",")} db=${config.dbPath} ws=${config.websocketUrl}`);

try {
  for await (const event of client.events(abort.signal)) {
    await bot.enqueue(() => bot.handleSignalEvent(event));
  }
} finally {
  client.close();
}

class SignalsBot {
  constructor(manager, store, initialEquity) {
    this.manager = manager;
    this.store = store;
    this.initialEquity = initialEquity;
    this.closedRealized = 0;
    this.lastClosedCount = 0;
    this.latestPriceByKey = new Map();
    this.lock = Promise.resolve();
  }

  enqueue(fn) {
    const run = this.lock.then(async () => fn());
    this.lock = run.catch(() => {});
    return run.catch((error) => console.error(error));
  }

  async handleSignalEvent(event) {
    switch (event.type) {
      case "ready":
        console.log(`Signals websocket ready message="${event.message}"`);
        return;
      case "info":
        console.log(`Instrument info instrument=${event.instrument} stage=${event.stage} replay=${!!event.replay} message="${event.message}"`);
        return;
      case "error":
        console.log(`Signals websocket error code=${event.code ?? ""} message="${event.message ?? ""}"`);
        return;
      case "subscribed":
        console.log(`Subscription confirmed subscription=${event.subscriptionId} instrument=${event.instrument}`);
        return;
      case "unsubscribed":
        console.log(`Subscription removed subscription=${event.subscriptionId ?? 0} instrument=${event.instrument ?? ""} code=${event.code ?? ""} message="${event.message ?? ""}"`);
        return;
      case "signal": {
        if (!event.signal.price || event.signal.price <= 0) {
          const tick = this.latestPriceByKey.get(positionKey(event.venue || "okx", event.instrument));
          if (tick) {
            event.signal.price = tick.price;
            event.signal.timestamp ??= tick.timestamp;
          }
        }
        if (!event.signal.price || event.signal.price <= 0) {
          console.log(`Signal skipped instrument=${event.instrument} side=${event.signal.side} confidence=${fmt(event.signal.confidence)} reason=no OKX candle price yet`);
          return;
        }
        const orders = this.manager.handleEvent(event);
        console.log(`Signal received instrument=${event.signal.instrument} side=${event.signal.side} confidence=${fmt(event.signal.confidence)} price=${fmt(event.signal.price)} replay=${!!event.replay} orders=${orders.length}`);
        await this.handleOrders(orders);
        return;
      }
      default:
        return;
    }
  }

  async handleOrders(orders) {
    if (orders.length === 0) return;
    for (const order of orders) logOrder(order);
    const trades = this.manager.closedTrades();
    if (this.lastClosedCount < trades.length) {
      const newTrades = trades.slice(this.lastClosedCount);
      for (const trade of newTrades) {
        this.closedRealized += trade.realizedPnl ?? 0;
        logClosedTrade(trade, this.initialEquity);
      }
      this.lastClosedCount = trades.length;
    }
    this.syncAsset();
    this.store.orders.push(...orders.map(serializeOrder));
    this.store.snapshots.push(this.snapshot());
    this.store.save();
  }

  syncAsset() {
    const openRealized = this.manager.positions().reduce((sum, position) => sum + (position.realizedPnl ?? 0), 0);
    const equity = Math.max(this.initialEquity + this.closedRealized + openRealized, 1);
    this.manager.assetManager().updateAsset({
      currency: "USDT",
      cash: equity,
      available: equity,
      equity,
      used: 0,
      updatedAt: new Date()
    });
  }

  snapshot() {
    const stats = this.manager.stats();
    const realizedPnl = this.closedRealized + stats.realizedPnl;
    const unrealizedPnl = stats.unrealizedPnl;
    return {
      timestamp: new Date().toISOString(),
      equity: this.initialEquity + realizedPnl,
      realizedPnl,
      unrealizedPnl,
      totalPnl: realizedPnl + unrealizedPnl,
      fees: stats.fees,
      realizedPct: ratio(realizedPnl, this.initialEquity),
      unrealizedPct: ratio(unrealizedPnl, this.initialEquity),
      totalPct: ratio(realizedPnl + unrealizedPnl, this.initialEquity)
    };
  }

  async reportStats() {
    const snapshot = this.snapshot();
    const positions = this.manager.positions();
    console.log(`Position manager stats equity=${money(snapshot.equity)} realized=${money(snapshot.realizedPnl)} unrealized=${money(snapshot.unrealizedPnl)} total=${money(snapshot.totalPnl)} fees=${money(snapshot.fees)} open_positions=${positions.length}`);
    for (const position of positions) {
      const unrealized = positionUnrealized(position);
      console.log(`Open position instrument=${position.instrument} side=${positionSide(position)} size=${fmt(position.size)} entry=${fmt(position.entryPrice)} last=${fmt(position.lastPrice)} unrealized=${money(unrealized)} pnl=${percent(ratio(unrealized, snapshot.equity))} confidence=${fmt(position.confidence)} tp=${fmt(position.takeProfit)} sl=${fmt(position.stopLoss)}`);
    }
    this.store.snapshots.push(snapshot);
    this.store.save();
  }
}

class FileStore {
  constructor(path) {
    this.path = path;
    this.state = { positions: [], closedTrades: [] };
    this.orders = [];
    this.snapshots = [];
  }

  load() {
    if (!existsSync(this.path)) return;
    const data = JSON.parse(readFileSync(this.path, "utf8"));
    this.state = data.state ?? { positions: data.positions ?? [], closedTrades: data.closedTrades ?? [] };
    this.orders = data.orders ?? [];
    this.snapshots = data.snapshots ?? [];
  }

  save() {
    mkdirSync(dirname(this.path), { recursive: true });
    const tmp = `${this.path}.tmp`;
    writeFileSync(tmp, JSON.stringify({
      state: this.state,
      orders: this.orders.slice(-1000),
      snapshots: this.snapshots.slice(-2880)
    }, null, 2));
    renameSync(tmp, this.path);
  }

  closedRealized() {
    return (this.state.closedTrades ?? []).reduce((sum, trade) => sum + (Number(trade.realizedPnl) || 0), 0);
  }
}

function loadDotEnv(path) {
  if (!existsSync(path)) return;
  const text = readFileSync(path, "utf8");
  for (const rawLine of text.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index < 0) continue;
    const key = line.slice(0, index).trim();
    const value = line.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
    if (key && process.env[key] === undefined) process.env[key] = value;
  }
}

function cleanDb() {
  const path = env("SIGNALS_DB_PATH", defaultDbPath);
  rmSync(path, { force: true });
  console.log(`Cleaned signalsbot local database path=${path}`);
}

function loadConfig() {
  const token = env("SIGNALS_WEBSOCKET_TOKEN", "");
  if (!token) throw new Error("SIGNALS_WEBSOCKET_TOKEN is required");
  const instruments = splitCsv(env("SIGNALS_INSTRUMENTS", "DOGE-USDT-SWAP"));
  if (instruments.length === 0) throw new Error("SIGNALS_INSTRUMENTS must contain at least one OKX instrument");
  return {
    token,
    websocketUrl: env("SIGNALS_WEBSOCKET_URL", defaultSignalsWsUrl),
    instruments,
    dbPath: env("SIGNALS_DB_PATH", defaultDbPath),
    initialEquity: envNumber("SIGNALS_INITIAL_EQUITY", defaultEquity),
    statsIntervalMs: parseDurationMs(env("SIGNALS_STATS_INTERVAL", "5m")),
    okxBaseUrl: env("SIGNALS_OKX_BASE_URL", defaultOkxBaseUrl).replace(/\/+$/, ""),
    okxWsUrl: env("SIGNALS_OKX_WEBSOCKET_URL", defaultOkxWsUrl).replace(/\/+$/, ""),
    candleBar: env("SIGNALS_OKX_CANDLE_BAR", "1m")
  };
}

async function fetchOkxInstrument(baseUrl, instrument) {
  const url = new URL(`${baseUrl}/api/v5/public/instruments`);
  url.searchParams.set("instType", "SWAP");
  url.searchParams.set("instId", instrument);
  const body = await fetchJson(url);
  if (body.code !== "0" || !body.data?.length) throw new Error(`okx instrument ${instrument}: ${body.code} ${body.msg ?? ""}`);
  const row = body.data[0];
  return {
    venue: "okx",
    instrument: row.instId,
    settlementCurrency: row.settleCcy || "USDT",
    lotSize: Number(row.lotSz) || 0,
    minSize: Number(row.minSz) || 0,
    tickSize: Number(row.tickSz) || 0,
    contractValue: Number(row.ctVal) || 0,
    contractMultiplier: Number(row.ctMult) || 1,
    maxLeverage: 1
  };
}

async function fetchLatestCandle(baseUrl, bar, instrument) {
  const url = new URL(`${baseUrl}/api/v5/market/candles`);
  url.searchParams.set("instId", instrument);
  url.searchParams.set("bar", bar);
  url.searchParams.set("limit", "1");
  const body = await fetchJson(url);
  if (body.code !== "0" || !body.data?.length) return undefined;
  return tickFromOkxCandle(instrument, body.data[0]);
}

async function fetchJson(url) {
  const response = await fetch(url, { headers: { "user-agent": "grexie-signalsbot-node-example/0.1" } });
  if (!response.ok) throw new Error(`http ${response.status}: ${await response.text()}`);
  return response.json();
}

function subscribeOkxCandles(wsBaseUrl, bar, instruments, signal, onTick) {
  const channel = `candle${bar}`;
  let retryMs = 1000;
  const connect = () => {
    if (signal.aborted) return;
    const ws = new WebSocket(`${wsBaseUrl}/ws/v5/business`);
    ws.on("open", () => {
      retryMs = 1000;
      ws.send(JSON.stringify({
        op: "subscribe",
        args: instruments.map((instrument) => ({ channel, instId: instrument }))
      }));
      console.log(`Connected OKX candle websocket channel=${channel} instruments=${instruments.join(",")}`);
    });
    ws.on("message", (data) => {
      const text = data.toString();
      if (text === "ping") {
        ws.send("pong");
        return;
      }
      try {
        const msg = JSON.parse(text);
        if (msg.event === "error" || msg.code) {
          console.error(`okx subscription error ${msg.code}: ${msg.msg}`);
          ws.close();
          return;
        }
        for (const row of msg.data ?? []) {
          const tick = tickFromOkxCandle(msg.arg?.instId, row);
          if (tick) onTick(tick);
        }
      } catch (error) {
        console.error(`okx candle parse: ${error.message}`);
      }
    });
    ws.on("close", () => {
      if (signal.aborted) return;
      setTimeout(connect, retryMs);
      retryMs = Math.min(retryMs * 2, 60000);
    });
    ws.on("error", (error) => {
      console.error(`okx candle websocket: ${error.message}`);
      ws.close();
    });
    signal.addEventListener("abort", () => ws.close(), { once: true });
  };
  connect();
}

function tickFromOkxCandle(instrument, row) {
  if (!instrument || !Array.isArray(row) || row.length < 5) return undefined;
  const price = Number(row[4]);
  const timestamp = new Date(Number(row[0]));
  if (!Number.isFinite(price) || price <= 0 || Number.isNaN(timestamp.getTime())) return undefined;
  return { instrument, price, timestamp };
}

function logOrder(order) {
  let action = "Order";
  if (Math.abs(order.previousSize) <= 1e-9 && Math.abs(order.targetSize) > 1e-9) action = "Position Opened";
  else if (sameSign(order.previousSize, order.targetSize) && Math.abs(order.targetSize) > Math.abs(order.previousSize)) action = "Added margin to position";
  else if (sameSign(order.previousSize, order.targetSize) && Math.abs(order.targetSize) < Math.abs(order.previousSize)) action = "Removed margin from position";
  else if (Math.abs(order.targetSize) <= 1e-9 && Math.abs(order.previousSize) > 1e-9) action = "Position close order";
  else if (!sameSign(order.previousSize, order.targetSize)) action = "Position flip reduction";
  console.log(`${action} instrument=${order.instrument} side=${order.side} reason=${order.reason} delta=${fmt(order.sizeDelta)} previous=${fmt(order.previousSize)} target=${fmt(order.targetSize)} price=${fmt(order.price)} margin=${money(order.margin)} notional=${money(order.notional)} fee=${money(order.estimatedFeeValue)} leverage=${fmt(order.leverage)} confidence=${fmt(order.confidence)} expected_edge=${fmt(order.expectedEdge)} tp=${fmt(order.takeProfit)} sl=${fmt(order.stopLoss)} reduce_only=${!!order.reduceOnly}`);
}

function logClosedTrade(trade, initialEquity) {
  console.log(`Position Closed instrument=${trade.instrument} side=${trade.side} reason=${trade.exitReason ?? ""} pnl=${percent(ratio(trade.realizedPnl ?? 0, initialEquity))} realized=${money(trade.realizedPnl ?? 0)} gross=${money(trade.realizedGross ?? 0)} fees=${money(trade.fees ?? 0)} entry=${fmt(trade.entryPrice)} exit=${fmt(trade.exitPrice)} size=${fmt(trade.size)} move=${percent(trade.exitMove ?? 0)} mfe=${percent(trade.mfe ?? 0)} mae=${percent(trade.mae ?? 0)} opened_at=${dateText(trade.openedAt)} closed_at=${dateText(trade.closedAt)}`);
}

function serializeState(state) {
  return {
    positions: (state.positions ?? []).map((position) => ({
      ...position,
      openedAt: dateText(position.openedAt),
      lastSignalAt: dateText(position.lastSignalAt)
    })),
    closedTrades: (state.closedTrades ?? []).map((trade) => ({
      ...trade,
      openedAt: dateText(trade.openedAt),
      closedAt: dateText(trade.closedAt)
    }))
  };
}

function reviveState(state) {
  return {
    positions: (state.positions ?? []).map((position) => ({
      ...position,
      openedAt: position.openedAt ? new Date(position.openedAt) : undefined,
      lastSignalAt: position.lastSignalAt ? new Date(position.lastSignalAt) : undefined
    })),
    closedTrades: (state.closedTrades ?? []).map((trade) => ({
      ...trade,
      openedAt: trade.openedAt ? new Date(trade.openedAt) : undefined,
      closedAt: trade.closedAt ? new Date(trade.closedAt) : undefined
    }))
  };
}

function serializeOrder(order) {
  return {
    ...order,
    timestamp: dateText(order.timestamp)
  };
}

function positionUnrealized(position) {
  if (!position.entryPrice || !position.lastPrice || !position.size) return 0;
  const move = position.size < 0
    ? (position.entryPrice - position.lastPrice) / position.entryPrice
    : (position.lastPrice - position.entryPrice) / position.entryPrice;
  return move * Math.abs(position.size) * (position.entryPrice || 1);
}

function positionSide(position) {
  if ((position.size ?? 0) < 0) return "sell";
  if ((position.size ?? 0) > 0) return "buy";
  return "";
}

function env(key, fallback) {
  return process.env[key]?.trim() || fallback;
}

function envNumber(key, fallback) {
  const value = Number(process.env[key]);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

function splitCsv(value) {
  return value.split(",").map((item) => item.trim().toUpperCase()).filter(Boolean);
}

function parseDurationMs(value) {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)?$/.exec(value.trim());
  if (!match) return 5 * 60 * 1000;
  const amount = Number(match[1]);
  const unit = match[2] || "ms";
  return amount * ({ ms: 1, s: 1000, m: 60000, h: 3600000 }[unit] ?? 1);
}

function positionKey(venue, instrument) {
  return `${venue.trim().toLowerCase()}:${instrument.trim().toUpperCase()}`;
}

function sameSign(a, b) {
  if (Math.abs(a ?? 0) <= 1e-9 || Math.abs(b ?? 0) <= 1e-9) return true;
  return (a < 0) === (b < 0);
}

function ratio(value, basis) {
  return basis ? value / basis : 0;
}

function money(value) {
  return `${(Number(value) || 0) >= 0 ? "+" : ""}${(Number(value) || 0).toFixed(2)} USDT`;
}

function percent(value) {
  return `${(Number(value) || 0) >= 0 ? "+" : ""}${((Number(value) || 0) * 100).toFixed(2)}%`;
}

function fmt(value) {
  return (Number(value) || 0).toFixed(8);
}

function dateText(value) {
  if (!value) return "";
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}
