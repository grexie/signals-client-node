import { describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import {
  SignalsClient,
  SignalsManager,
  type SignalsEvent,
  parseEvent
} from "../src/index.js";

describe("parseEvent", () => {
  it("parses signal replay events", () => {
    const event = parseEvent(JSON.stringify({
      type: "signal",
      subscriptionId: 42,
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      timestamp: "2026-05-26T00:00:00Z",
      replay: true,
      signal: { confidence: 0.8, side: "buy", takeProfit: 0.01, stopLoss: 0.004, managePositionsOnly: true }
    }));
    expect(event.type).toBe("signal");
    if (event.type !== "signal") return;
    expect(event.signal.venue).toBe("okx");
    expect(event.signal.instrument).toBe("BTC-USDT-SWAP");
    expect(event.signal.managePositionsOnly).toBe(true);
    expect(event.replay).toBe(true);
  });

  it("parses info, backtest, error, and router events", () => {
    expect(parseEvent(JSON.stringify({
      type: "info",
      subscriptionId: 3,
      venue: "okx",
      instrument: "DOGE-USDT-SWAP",
      stage: "ready",
      message: "ready",
      replay: true,
      replayedAt: "2026-05-26T00:00:01Z"
    }))).toMatchObject({ type: "info", stage: "ready", replay: true });
    expect(parseEvent(JSON.stringify({
      type: "backtest",
      subscriptionId: 3,
      venue: "okx",
      instrument: "BASKET:1",
      timestamp: "2026-05-31T17:00:00Z",
      backtest: { accepted: true, candidate: { total: 0.12 } }
    }))).toMatchObject({ type: "backtest", subscriptionId: 3, backtest: { accepted: true } });
    expect(parseEvent(JSON.stringify({
      type: "basket_updated",
      subscriptionId: 12,
      venue: "okx",
      message: "active"
    }))).toMatchObject({ type: "basket_updated", subscriptionId: 12, venue: "okx" });
    expect(parseEvent(JSON.stringify({
      type: "order_router_forwarded",
      subscriptionId: 12
    }))).toMatchObject({ type: "order_router_forwarded", subscriptionId: 12 });
    expect(parseEvent(JSON.stringify({
      type: "create-market-order",
      subscriptionId: 12,
      intentId: "intent_1",
      reason: "preempted_by_better_route",
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      orderType: "market",
      contractSize: 3,
      margin: 125.5,
      leverage: 2,
      confidence: 0.73
    }))).toMatchObject({ type: "create-market-order", subscriptionId: 12, intentId: "intent_1", reason: "preempted_by_better_route", contractSize: 3, margin: 125.5, leverage: 2, confidence: 0.73 });
    expect(parseEvent(JSON.stringify({
      type: "update-tpsl",
      subscriptionId: 12,
      intentId: "intent_2",
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      takeProfitPrice: 72100,
      stopLossPrice: 70050,
      takeProfit: 0.03,
      stopLoss: 0.0007
    }))).toMatchObject({ type: "update-tpsl", subscriptionId: 12, takeProfitPrice: 72100, stopLossPrice: 70050 });
    expect(parseEvent(JSON.stringify({
      type: "withdraw",
      subscriptionId: 12,
      intentId: "withdraw_1",
      venue: "okx",
      currency: "USDT",
      amount: 42
    }))).toMatchObject({ type: "withdraw", subscriptionId: 12, currency: "USDT", amount: 42 });
    expect(parseEvent(JSON.stringify({
      type: "error",
      code: "forbidden",
      message: "no access"
    }))).toMatchObject({ type: "error", code: "forbidden" });
  });
});

describe("SignalsClient", () => {
  it("subscribes over an authenticated websocket", async () => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const port = (server.address() as { port: number }).port;
    const received = new Promise<Record<string, unknown>>((resolve, reject) => {
      server.on("connection", (socket, request) => {
        try {
          expect(request.headers.authorization).toBe("Bearer ws_test");
          socket.send(JSON.stringify({ type: "ready", message: "ok" }));
          socket.on("message", (data) => resolve(JSON.parse(data.toString())));
        } catch (error) {
          reject(error);
        }
      });
    });

    const client = new SignalsClient("ws_test", { url: `ws://127.0.0.1:${port}` });
    await client.connect();
    await expect(client.receive()).resolves.toMatchObject({ type: "ready" });
    client.subscribe("okx", "ETH-USDT-SWAP");
    await expect(received).resolves.toMatchObject({
      type: "subscribe",
      venue: "okx",
      instrument: "ETH-USDT-SWAP"
    });
    client.close();
    server.close();
  });

  it("drops leaked basket state frames from the websocket", async () => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const port = (server.address() as { port: number }).port;
    server.on("connection", (socket) => {
      socket.send(JSON.stringify({ type: "basket_state", subscriptionId: 9, venue: "okx", message: "active" }));
      socket.send(JSON.stringify({ type: "order_router_forwarded", subscriptionId: 9 }));
    });

    const client = new SignalsClient("ws_test", { url: `ws://127.0.0.1:${port}` });
    await client.connect();
    await expect(client.receive()).resolves.toMatchObject({ type: "order_router_forwarded", subscriptionId: 9 });
    client.close();
    server.close();
  });

  it("fans events out to independent consumers", async () => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const port = (server.address() as { port: number }).port;
    server.on("connection", (socket) => {
      socket.send(JSON.stringify({ type: "ready", message: "ok" }));
    });

    const client = new SignalsClient("ws_test", { url: `ws://127.0.0.1:${port}` });
    const first = client.events();
    const second = client.events();
    const firstEvent = first.next();
    const secondEvent = second.next();
    await client.connect();

    await expect(firstEvent).resolves.toMatchObject({ value: { type: "ready" } });
    await expect(secondEvent).resolves.toMatchObject({ value: { type: "ready" } });
    await first.return?.();
    await second.return?.();
    client.close();
    server.close();
  });
});

describe("SignalsManager", () => {
  it("subscribes with state and emits server router intents", async () => {
    const client = new FakeManagerClient([
      { type: "subscribed", subscriptionId: 9, venue: "okx", instrument: "BTC-USDT-SWAP" },
      { type: "create-market-order", subscriptionId: 9, intentId: "intent_1", venue: "okx", instrument: "BTC-USDT-SWAP", side: "buy", contractSize: 3 }
    ]);
    const manager = new SignalsManager(client, {
      assets: [{ venue: "okx", currency: "USDT", available: 100, equity: 100 }],
      positions: [{ venue: "okx", instrument: "BTC-USDT-SWAP", size: 2, entryPrice: 100, lastPrice: 101 }]
    }, {
      venue: "okx",
      instruments: ["BTC-USDT-SWAP"]
    });
    const intents: SignalsEvent[] = [];
    manager.on("intent", (intent) => intents.push(intent));

    await manager.run();

    expect(client.sent[0]).toMatchObject({
      type: "subscribe",
      venue: "okx",
      instruments: ["BTC-USDT-SWAP"],
      assets: [{ currency: "USDT" }],
      positions: [{ instrument: "BTC-USDT-SWAP" }]
    });
    expect(client.sent).toContainEqual(expect.objectContaining({ type: "update-asset", subscriptionId: 9, currency: "USDT" }));
    expect(client.sent).toContainEqual(expect.objectContaining({ type: "update-position", subscriptionId: 9, instrument: "BTC-USDT-SWAP" }));
    expect(intents).toHaveLength(1);
    expect(intents[0]).toMatchObject({ type: "create-market-order", intentId: "intent_1", contractSize: 3 });
  });

  it("updates local snapshots and forwards changes after subscription", () => {
    const client = new FakeManagerClient([]);
    const manager = new SignalsManager(client, {}, { venue: "okx", instruments: ["ETH-USDT-SWAP"] });
    manager.handleEvent({ type: "subscribed", subscriptionId: 15, venue: "okx", instrument: "ETH-USDT-SWAP" });

    manager.updateAsset({ currency: "usdt", available: 50, maxUsage: 0.5 });
    manager.updatePosition({ instrument: "ETH-USDT-SWAP", size: -4, entryPrice: 2000 });

    expect(manager.availableOrderCash("USDT")).toBe(25);
    expect(manager.state().positions?.[0]).toMatchObject({ instrument: "ETH-USDT-SWAP", status: "open", size: -4 });
    expect(client.sent).toContainEqual(expect.objectContaining({ type: "update-asset", subscriptionId: 15, currency: "USDT" }));
    expect(client.sent).toContainEqual(expect.objectContaining({ type: "update-position", subscriptionId: 15, side: "sell", size: 4 }));
  });
});

class FakeManagerClient {
  readonly sent: Record<string, unknown>[] = [];

  constructor(private readonly stream: SignalsEvent[]) {}

  async *events(): AsyncIterable<SignalsEvent> {
    yield* this.stream;
  }

  subscribeBasket(request: unknown): void {
    this.sent.push({ type: "subscribe", ...(request as Record<string, unknown>) });
  }

  unsubscribe(subscriptionId: number): void {
    this.sent.push({ type: "unsubscribe", subscriptionId });
  }

  updateAsset(subscriptionId: number, asset: unknown): void {
    this.sent.push({ type: "update-asset", subscriptionId, ...(asset as Record<string, unknown>) });
  }

  updatePosition(subscriptionId: number, position: { instrument: string; size: number }): void {
    this.sent.push({ type: "update-position", subscriptionId, instrument: position.instrument, side: position.size < 0 ? "sell" : "buy", size: Math.abs(position.size) });
  }

  addInstrument(subscriptionId: number, instrument: string): void {
    this.sent.push({ type: "add-instrument", subscriptionId, instrument });
  }

  removeInstrument(subscriptionId: number, instrument: string): void {
    this.sent.push({ type: "remove-instrument", subscriptionId, instrument });
  }

  updateConfig(subscriptionId: number, config: unknown): void {
    this.sent.push({ type: "update-config", subscriptionId, ...(config as Record<string, unknown>) });
  }

  scheduleWithdrawal(subscriptionId: number, withdrawal: unknown): void {
    this.sent.push({ type: "schedule-withdrawal", subscriptionId, ...(withdrawal as Record<string, unknown>) });
  }
}
