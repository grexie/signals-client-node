import { describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import {
  PositionManager,
  SignalsClient,
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
      signal: { confidence: 0.8, side: "buy", takeProfit: 0.01, stopLoss: 0.004 }
    }));
    expect(event.type).toBe("signal");
    if (event.type !== "signal") return;
    expect(event.signal.venue).toBe("okx");
    expect(event.signal.instrument).toBe("BTC-USDT-SWAP");
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
});

describe("PositionManager", () => {
  it("opens and flips instead of getting stuck long", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      positionSize: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0.2,
      rebalanceIntervalMs: 60 * 60 * 1000,
      minLeverage: 1,
      maxLeverage: 5
    }));
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
    expect(buy[0]?.targetSize).toBeCloseTo(0.1);

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
    expect(sell[0]?.sizeDelta).toBeLessThan(-0.19);
  });

  it("scales min order delta by configured position size", () => {
    const manager = new PositionManager(undefined, productionPositionManagerConfig({
      positionSize: 0.1,
      minExpectedEdge: 0,
      minOrderDelta: 0.2
    }));
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "DOGE-USDT-SWAP",
      side: "buy",
      confidence: 0.15,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 0.2
    })).toHaveLength(0);
    expect(manager.handleSignal({
      venue: "okx",
      instrument: "DOGE-USDT-SWAP",
      side: "buy",
      confidence: 0.25,
      takeProfit: 0.02,
      stopLoss: 0.004,
      price: 0.2
    })).toHaveLength(1);
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
      positionSize: 0.1,
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
      positionSize: 0.01,
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
