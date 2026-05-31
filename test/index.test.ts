import { describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import {
  PositionManager,
  SignalsClient,
  type Order,
  type SignalsEvent,
  AssetManager,
  InstrumentManager,
  parseEvent,
  productionPositionManagerConfig
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

  it("parses info and error events", () => {
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
      type: "error",
      code: "forbidden",
      message: "no access"
    }))).toMatchObject({ type: "error", code: "forbidden" });
  });

  it("parses order-router execution events", () => {
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
      leverage: 2
    }))).toMatchObject({ type: "create-market-order", subscriptionId: 12, intentId: "intent_1", reason: "preempted_by_better_route", contractSize: 3 });
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
  });
});

function orderBudgetCost(order: Order | undefined): number {
  if (!order) return 0;
  return Math.max(0, order.margin) + Math.max(0, order.estimatedFee);
}

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

  it("unsubscribes by id", async () => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
    await new Promise<void>((resolve) => server.once("listening", resolve));
    const port = (server.address() as { port: number }).port;
    const received = new Promise<Record<string, unknown>>((resolve) => {
      server.on("connection", (socket) => socket.on("message", (data) => resolve(JSON.parse(data.toString()))));
    });
    const client = new SignalsClient("ws_test", { url: `ws://127.0.0.1:${port}` });
    await client.connect();
    client.unsubscribe(77);
    await expect(received).resolves.toMatchObject({ type: "unsubscribe", subscriptionId: 77 });
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

describe("PositionManager", () => {
  it("opens and flips instead of getting stuck long", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0.2,
      rebalanceIntervalMs: 60 * 60 * 1000,
      flipFlopWindowMs: 0,
      minLeverage: 1,
      maxLeverage: 5
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    const buy = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.8,
      takeProfit: 0.02,
      stopLoss: 0.004,
      score: 0.5,
      price: 100,
      timestamp: "2026-05-26T00:00:00Z"
    });
    expect(buy).toHaveLength(1);
    expect(buy[0]?.side).toBe("buy");
    expect(buy[0]?.reason).toBe("opening");
    expect(orderBudgetCost(buy[0])).toBeCloseTo(0.1);

    const sell = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.9,
      takeProfit: 0.02,
      stopLoss: 0.004,
      score: -0.6,
      price: 99,
      timestamp: "2026-05-26T00:01:00Z"
    });
    expect(sell).toHaveLength(1);
    expect(sell[0]?.side).toBe("sell");
    expect(sell[0]?.reason).toBe("flip");
    expect(sell[0]?.targetSize).toBeCloseTo(0);
    expect(sell[0]?.sizeDelta).toBeCloseTo(-(buy[0]?.targetSize ?? 0));

    const openShort = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.9,
      takeProfit: 0.02,
      stopLoss: 0.004,
      score: -0.6,
      price: 99,
      timestamp: "2026-05-26T00:02:00Z"
    });
    expect(openShort).toHaveLength(1);
    expect(openShort[0]?.side).toBe("sell");
    expect(openShort[0]?.reason).toBe("opening");
  });

  it("suppresses flip-flops by default inside the configured window", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      rebalanceIntervalMs: 60 * 60 * 1000
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.8,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-26T00:00:00Z"
    })).toHaveLength(1);
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.99,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 99.95,
      timestamp: "2026-05-26T00:05:00Z"
    })).toHaveLength(0);
    const outsideWindow = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.99,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 99.95,
      timestamp: "2026-05-26T00:31:00Z"
    });
    expect(outsideWindow).toHaveLength(1);
    expect(outsideWindow[0]?.reason).toBe("flip");
  });

  it("ignores signals after an instrument is removed", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig());
    manager.assetManager().updateAsset({ currency: "USDT", available: 1000, equity: 1000 });
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    manager.instrumentManager().removeInstrument("okx", "BTC-USDT-SWAP");
    const orders = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.03,
      stopLoss: 0.01,
      score: 1,
      price: 100,
      timestamp: "2026-05-30T12:00:00Z"
    });
    expect(orders).toHaveLength(0);
    expect(manager.instrumentManager().instrument("okx", "BTC-USDT-SWAP")).toBeUndefined();
  });

  it("allows an explicit high-confidence flip threshold inside the configured window", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      rebalanceIntervalMs: 60 * 60 * 1000,
      flipFlopWindowMs: 30 * 60 * 1000,
      signalFlipMinConfidence: 0.72
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.8,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-26T00:00:00Z"
    })).toHaveLength(1);
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.70,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 99.95,
      timestamp: "2026-05-26T00:05:00Z"
    })).toHaveLength(0);
    const flip = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.72,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 99.95,
      timestamp: "2026-05-26T00:06:00Z"
    });
    expect(flip).toHaveLength(1);
    expect(flip[0]?.reason).toBe("flip");
  });

  it("uses confidence as allocation weight", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0.2
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "DOGE-USDT-SWAP" });
    const orders = manager.handleSignal({
      venue: "okx",
      instrument: "DOGE-USDT-SWAP",
      side: "buy",
      confidence: 0.15,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 0.2
    });
    expect(orders).toHaveLength(1);
    expect(orderBudgetCost(orders[0])).toBeCloseTo(0.1);
  });

  it("does not open or increase exposure for managePositionsOnly signals", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0.01,
      minOrderDelta: 0,
      minLeverage: 1,
      maxLeverage: 5
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    const blockedOpen = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.9,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-26T00:00:00Z",
      managePositionsOnly: true
    });
    expect(blockedOpen).toHaveLength(0);
    expect(manager.positions()).toHaveLength(0);

    const opened = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.7,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-26T00:01:00Z"
    });
    expect(opened).toHaveLength(1);
    expect(opened[0]?.reason).toBe("opening");

    const sameSide = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-26T00:02:00Z",
      managePositionsOnly: true
    });
    expect(sameSide).toHaveLength(0);

    const closed = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.51,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 99,
      timestamp: "2026-05-26T00:03:00Z",
      managePositionsOnly: true
    });
    expect(closed).toHaveLength(1);
    expect(closed[0]?.reason).toBe("closing");
    expect(closed[0]?.targetSize).toBeCloseTo(0);

    const blockedShort = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 0.51,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 99,
      timestamp: "2026-05-26T00:04:00Z",
      managePositionsOnly: true
    });
    expect(blockedShort).toHaveLength(0);
    expect(manager.positions()).toHaveLength(0);
  });

  it("closes on trailing stop after favorable giveback", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    const orders = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.8,
      takeProfit: 0.5,
      stopLoss: 0.2,
      price: 100,
      timestamp: "2026-05-26T00:00:00Z",
      trailingStopActivation: 0.02,
      trailingStopDistance: 0.01,
      trailingStopMinProfit: 0.001
    });
    expect(orders).toHaveLength(1);
    expect(orders[0]?.trailingStopActivation).toBeCloseTo(0.02);
    expect(manager.updatePrice("okx", "BTC-USDT-SWAP", 103, new Date("2026-05-26T00:01:00Z"))).toHaveLength(0);
    const closed = manager.updatePrice("okx", "BTC-USDT-SWAP", 101.8, new Date("2026-05-26T00:02:00Z"));
    expect(closed).toHaveLength(1);
    expect(closed[0]?.reason).toBe("trailing_stop");
    const trade = manager.closedTrades()[0];
    expect(trade?.exitReason).toBe("trailing_stop");
    expect(trade?.mfe ?? 0).toBeGreaterThan(0.029);
    expect(trade?.realizedPnl ?? 0).toBeGreaterThan(0);
  });

  it("persists and hydrates trailing stop state", () => {
    const snapshots: ReturnType<PositionManager["state"]>[] = [];
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      persist: (state) => snapshots.push(state)
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.8,
      takeProfit: 0.5,
      stopLoss: 0.2,
      price: 100,
      trailingStopActivation: 0.02,
      trailingStopDistance: 0.01,
      trailingStopMinProfit: 0.001
    });
    manager.updatePrice("okx", "BTC-USDT-SWAP", 104);
    const latest = snapshots.at(-1);
    expect(latest?.positions).toHaveLength(1);
    expect(latest?.positions[0]?.trailingStopActivation).toBeCloseTo(0.02);
    expect(latest?.positions[0]?.mfe ?? 0).toBeGreaterThan(0.039);

    const rehydrated = new PositionManager(undefined, productionPositionManagerConfig({
      initialState: latest
    }));
    expect(rehydrated.positions()).toHaveLength(1);
    expect(rehydrated.positions()[0]?.mfe).toBe(latest?.positions[0]?.mfe);
  });

  it("quantizes emitted target size to executable lots", () => {
    const assets = new AssetManager();
    assets.updateAsset({ currency: "USDT", equity: 1000, available: 1000 });
    const instruments = new InstrumentManager();
    instruments.updateInstrument({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      settlementCurrency: "USDT",
      lotSize: 1,
      minSize: 1,
      tickSize: 0.1
    });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.5,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      assetManager: assets,
      instrumentManager: instruments
    }));
    const orders = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.15,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 333
    });
    expect(orders).toHaveLength(1);
    expect(orders[0]?.quantity).toBe(1);
    expect(orders[0]?.sizeDelta).toBeCloseTo(1);
    expect(orders[0]?.targetSize).toBeCloseTo(1);
  });

  it("ignores signals for unconfigured instruments", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0
    }));
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "SOL-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100
    })).toHaveLength(0);
    expect(manager.positions()).toHaveLength(0);

    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "SOL-USDT-SWAP" });
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "SOL-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100
    })).toHaveLength(1);
  });

  it("ignores replay signal events", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    const event: SignalsEvent = {
      type: "signal",
      subscriptionId: 3,
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      replay: true,
      signal: {
        venue: "okx",
        instrument: "BTC-USDT-SWAP",
        side: "buy",
        confidence: 1,
        takeProfit: 0.02,
        stopLoss: 0.004,
        price: 100
      }
    };
    expect(manager.handleEvent(event)).toHaveLength(0);
    expect(manager.positions()).toHaveLength(0);
    expect(manager.handleEvent({ ...event, replay: false })).toHaveLength(1);
  });

  it("adapts leverage by confidence, edge, and score within configured caps", () => {
    const leverageFor = (instrument: string, confidence: number, takeProfit: number, score: number): number => {
      const manager = new PositionManager(undefined, productionPositionManagerConfig({
        maxMarginRatio: 1,
        minExpectedEdge: 0,
        minOrderDelta: 0,
        minLeverage: 1,
        maxLeverage: 5
      }));
      manager.instrumentManager().updateInstrument({ venue: "okx", instrument });
      const [order] = manager.handleSignal({
        venue: "okx",
        instrument,
        side: "buy",
        confidence,
        takeProfit,
        stopLoss: 0,
        score,
        price: 100
      });
      return order.leverage;
    };

    const low = leverageFor("LOW-USDT-SWAP", 0.2, 0, 0);
    const scored = leverageFor("SCORE-USDT-SWAP", 0.2, 0, 1);
    const high = leverageFor("HIGH-USDT-SWAP", 1, 0.02, 1);
    expect(low).toBeGreaterThanOrEqual(1);
    expect(high).toBeLessThanOrEqual(5);
    expect(scored).toBeGreaterThan(low);
    expect(high).toBeCloseTo(5);
  });

  it("updates config without clearing state or instrument metadata", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 1,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      rebalanceIntervalMs: 60 * 60 * 1000,
      flipFlopWindowMs: 0,
      minLeverage: 5,
      maxLeverage: 5
    }));
    manager.instrumentManager().updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP" });
    const [opening] = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      score: 1,
      price: 100,
      timestamp: new Date("2026-05-26T00:00:00Z")
    });
    expect(opening?.leverage).toBeCloseTo(5);

    manager.updateConfig(productionPositionManagerConfig({
      maxMarginRatio: 1,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      rebalanceIntervalMs: 60 * 60 * 1000,
      flipFlopWindowMs: 0,
      minLeverage: 1,
      maxLeverage: 1
    }));
    expect(manager.positions()).toHaveLength(1);
    const [closing] = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "sell",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      score: -1,
      price: 99,
      timestamp: new Date("2026-05-26T00:01:00Z")
    });
    expect(closing?.reduceOnly).toBe(true);
    expect(closing?.leverage).toBeCloseTo(1);
  });

  it("turns abstract sizing into rounded concrete orders", () => {
    const assets = new AssetManager();
    assets.updateAsset({ currency: "USDT", cash: 1000, available: 900, used: 100, equity: 1000 });
    const instruments = new InstrumentManager();
    instruments.updateInstrument({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      settlementCurrency: "USDT",
      lotSize: 0.001,
      minSize: 0.002,
      tickSize: 0.1,
      maxLeverage: 2
    });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      minLeverage: 1,
      maxLeverage: 5,
      assetManager: assets,
      instrumentManager: instruments
    }));
    const [order] = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100.07
    });
    expect(order?.price).toBeCloseTo(100.1);
    expect(order?.settlementCurrency).toBe("USDT");
    expect(order?.leverage).toBeLessThanOrEqual(2);
    expect(order?.quantity).toBeGreaterThan(0);
    expect(order?.notional).toBeGreaterThan(0);
    expect(order?.estimatedFeeValue).toBeGreaterThan(0);
  });

  it("rejects openings below instrument minimum size", () => {
    const assets = new AssetManager();
    assets.updateAsset({ currency: "USDT", equity: 10 });
    const instruments = new InstrumentManager();
    instruments.updateInstrument({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      settlementCurrency: "USDT",
      lotSize: 0.001,
      minSize: 1,
      tickSize: 0.1
    });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.01,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      assetManager: assets,
      instrumentManager: instruments
    }));
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100
    })).toHaveLength(0);
  });

  it("phases reductions before openings", () => {
    const assets = new AssetManager();
    assets.updateAsset({ currency: "USDT", cash: 1000, available: 1000, equity: 1000 });
    const instruments = new InstrumentManager();
    instruments.updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP", settlementCurrency: "USDT" });
    instruments.updateInstrument({ venue: "okx", instrument: "ETH-USDT-SWAP", settlementCurrency: "USDT" });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.2,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      assetManager: assets,
      instrumentManager: instruments
    }));
    manager.addPosition({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      size: 2,
      confidence: 1,
      entryPrice: 100,
      lastPrice: 100
    });
    const reductions = manager.handleSignal({
      venue: "okx",
      instrument: "ETH-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-26T00:00:00Z"
    });
    expect(reductions).toHaveLength(1);
    expect(reductions[0]?.instrument).toBe("BTC-USDT-SWAP");
    expect(reductions[0]?.side).toBe("sell");
    expect(reductions[0]?.targetSize).toBeCloseTo((100 / (1 + (reductions[0]?.leverage ?? 1) * (reductions[0]?.feeRate ?? 0))) / (reductions[0]?.price ?? 100));

    const openings = manager.handleSignal({
      venue: "okx",
      instrument: "ETH-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-26T00:01:00Z"
    });
    expect(openings).toHaveLength(1);
    expect(openings[0]?.instrument).toBe("ETH-USDT-SWAP");
    expect(openings[0]?.side).toBe("buy");
  });

  it("caps openings to available exposure", () => {
    const assets = new AssetManager();
    assets.updateAsset({ currency: "USDT", cash: 1000, available: 50, equity: 1000 });
    const instruments = new InstrumentManager();
    instruments.updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP", settlementCurrency: "USDT" });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 0.2,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      assetManager: assets,
      instrumentManager: instruments
    }));
    const orders = manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100
    });
    expect(orders).toHaveLength(1);
    expect(orderBudgetCost(orders[0])).toBeLessThanOrEqual(50 + 1e-9);
    expect(orders[0]?.margin).toBeLessThan(50);
  });

  it("caps openings to remaining portfolio budget without asset snapshots", () => {
    const instruments = new InstrumentManager();
    instruments.updateInstrument({ venue: "okx", instrument: "BTC-USDT-SWAP", settlementCurrency: "USDT" });
    instruments.updateInstrument({ venue: "okx", instrument: "ETH-USDT-SWAP", settlementCurrency: "USDT" });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 1,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      rebalanceIntervalMs: 6 * 60 * 60 * 1000,
      minLeverage: 1,
      maxLeverage: 1,
      instrumentManager: instruments
    }));

    expect(manager.handleSignal({
      venue: "okx",
      instrument: "BTC-USDT-SWAP",
      side: "buy",
      confidence: 0.51,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-27T00:00:00Z"
    })).toHaveLength(1);

    manager.handleSignal({
      venue: "okx",
      instrument: "ETH-USDT-SWAP",
      side: "buy",
      confidence: 0.51,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: "2026-05-27T00:01:00Z"
    });

    const total = manager.positions().reduce((sum, position) => sum + Math.abs(position.size), 0);
    expect(total).toBeLessThanOrEqual(1 + 1e-9);
  });

  it("closes positions below the minimum position size ratio", () => {
    const lastSignalAt = new Date(Date.now() - 60_000);
    const assets = new AssetManager();
    assets.updateAsset({ currency: "USDT", cash: 1000, available: 0.5, used: 999.5, equity: 1000 });
    const instruments = new InstrumentManager();
    instruments.updateInstrument({ venue: "okx", instrument: "DUST-USDT-SWAP", settlementCurrency: "USDT", lotSize: 0.1, minSize: 0.1 });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      maxMarginRatio: 1,
      minPositionSizeRatio: 0.01,
      minExpectedEdge: 0,
      minOrderDelta: 0,
      rebalanceIntervalMs: 6 * 60 * 60 * 1000,
      assetManager: assets,
      instrumentManager: instruments
    }));
    manager.addPosition({
      venue: "okx",
      instrument: "DUST-USDT-SWAP",
      size: 0.005,
      confidence: 0.5,
      entryPrice: 100,
      lastPrice: 100,
      lastSignalAt
    });

    const orders = manager.handleSignal({
      venue: "okx",
      instrument: "DUST-USDT-SWAP",
      side: "buy",
      confidence: 1,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 100,
      timestamp: new Date(lastSignalAt.getTime() + 60_000)
    });
    expect(orders).toHaveLength(1);
    expect(orders[0]?.side).toBe("sell");
    expect(orders[0]?.reason).toBe("closing");
    expect(orders[0]?.targetSize).toBeCloseTo(0);
    expect(orders[0]?.sizeDelta).toBeCloseTo(-0.005);
    expect(orders[0]?.quantity).toBeCloseTo(0.005);
  });

  it("reports stats by instrument and settlement currency", () => {
    const assets = new AssetManager();
    assets.updateAsset({ currency: "USDT", cash: 1000, available: 800, used: 200, equity: 1000 });
    const instruments = new InstrumentManager();
    instruments.updateInstrument({
      venue: "okx",
      instrument: "ETH-USDT-SWAP",
      settlementCurrency: "USDT",
      lotSize: 0.01,
      minSize: 0.01,
      tickSize: 0.01
    });
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      assetManager: assets,
      instrumentManager: instruments
    }));
    manager.addPosition({
      venue: "okx",
      instrument: "ETH-USDT-SWAP",
      size: 0.1,
      confidence: 0.8,
      entryPrice: 100,
      lastPrice: 110,
      leverage: 2,
      realizedPnl: 0.01,
      fees: 0.001
    });
    const stats = manager.stats();
    expect(stats.equity).toBe(1000);
    expect(stats.available).toBe(800);
    expect(stats.byInstrument["okx:ETH-USDT-SWAP"]?.settlementCurrency).toBe("USDT");
    expect(stats.byInstrument["okx:ETH-USDT-SWAP"]?.quantity).toBeGreaterThan(0);
    expect(stats.totalPnlPercent).toBeGreaterThan(0);
  });
});
