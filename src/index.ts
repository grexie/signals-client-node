import { EventEmitter } from "node:events";
import WebSocket from "ws";

export type SignalsWebSocketToken = string;
export type Side = "buy" | "sell";

export interface SignalComponent {
  timeframe: string;
  side: Side;
  confidence: number;
  weight: number;
  signedScore: number;
  takeProfit: number;
  stopLoss: number;
  probability?: number[];
}

export interface Signal {
  venue: string;
  instrument: string;
  timeframe?: string;
  confidence: number;
  side: Side;
  takeProfit: number;
  stopLoss: number;
  score?: number;
  components?: SignalComponent[];
  timestamp?: string | Date;
  price?: number;
}

export interface ReadyEvent {
  type: "ready";
  message: string;
}

export interface SubscribedEvent {
  type: "subscribed";
  subscriptionId: number;
  venue: string;
  instrument: string;
}

export interface UnsubscribedEvent {
  type: "unsubscribed";
  subscriptionId?: number;
  venue?: string;
  instrument?: string;
  code?: string;
  message?: string;
}

export interface InfoEvent {
  type: "info";
  subscriptionId: number;
  venue: string;
  instrument: string;
  stage: string;
  message: string;
  timestamp?: string;
  replay?: boolean;
  replayedAt?: string;
}

export interface SignalEvent {
  type: "signal";
  subscriptionId: number;
  venue: string;
  instrument: string;
  signal: Signal;
  timestamp?: string;
  replay?: boolean;
  replayedAt?: string;
}

export interface ErrorEvent {
  type: "error";
  code?: string;
  message?: string;
}

export type SignalsEvent =
  | ReadyEvent
  | SubscribedEvent
  | UnsubscribedEvent
  | InfoEvent
  | SignalEvent
  | ErrorEvent;

export interface SignalsClientOptions {
  url?: string;
  baseUrl?: string;
  headers?: Record<string, string>;
  WebSocketCtor?: typeof WebSocket;
}

interface Waiter {
  resolve: (event: SignalsEvent) => void;
  reject: (error: Error) => void;
  cleanup?: () => void;
}

interface RawServerEvent {
  type?: string;
  subscriptionId?: number;
  venue?: string;
  instrument?: string;
  code?: string;
  message?: string;
  stage?: string;
  timestamp?: string;
  replay?: boolean;
  replayedAt?: string;
  signal?: Partial<Signal>;
}

const defaultWebSocketUrl = "wss://signals.grexie.com/ws";

export class SignalsClient extends EventEmitter {
  private readonly token: SignalsWebSocketToken;
  private readonly options: Required<Omit<SignalsClientOptions, "baseUrl">>;
  private ws?: WebSocket;
  private readonly queue: SignalsEvent[] = [];
  private readonly waiters: Waiter[] = [];
  private terminalError?: Error;

  constructor(token: SignalsWebSocketToken, options: SignalsClientOptions = {}) {
    super();
    this.token = token;
    this.options = {
      url: options.url ?? websocketUrlFromBase(options.baseUrl) ?? defaultWebSocketUrl,
      headers: options.headers ?? {},
      WebSocketCtor: options.WebSocketCtor ?? WebSocket
    };
  }

  connect(): Promise<void> {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      return Promise.resolve();
    }
    const headers = {
      ...this.options.headers,
      ...(this.token ? { Authorization: `Bearer ${this.token}` } : {})
    };
    this.terminalError = undefined;
    this.ws = new this.options.WebSocketCtor(this.options.url, { headers });
    this.ws.on("message", (data) => this.accept(parseEvent(data.toString())));
    this.ws.on("error", (error) => this.fail(error instanceof Error ? error : new Error(String(error))));
    this.ws.on("close", () => this.closeStreams(new Error("signals-client: websocket closed")));
    return new Promise((resolve, reject) => {
      this.ws?.once("open", () => resolve());
      this.ws?.once("error", reject);
    });
  }

  close(): void {
    this.ws?.close();
    this.ws = undefined;
  }

  subscribe(venue: string, instrument: string): void {
    this.send({ type: "subscribe", venue, instrument });
  }

  unsubscribe(subscriptionId: number): void {
    this.send({ type: "unsubscribe", subscriptionId });
  }

  unsubscribeInstrument(venue: string, instrument: string): void {
    this.send({ type: "unsubscribe", venue, instrument });
  }

  receive(signal?: AbortSignal): Promise<SignalsEvent> {
    const queued = this.queue.shift();
    if (queued) {
      return Promise.resolve(queued);
    }
    if (this.terminalError) {
      return Promise.reject(this.terminalError);
    }
    if (signal?.aborted) {
      return Promise.reject(new Error("signals-client: receive aborted"));
    }
    return new Promise((resolve, reject) => {
      const waiter: Waiter = { resolve, reject };
      if (signal) {
        const onAbort = () => {
          this.removeWaiter(waiter);
          reject(new Error("signals-client: receive aborted"));
        };
        waiter.cleanup = () => signal.removeEventListener("abort", onAbort);
        signal.addEventListener("abort", onAbort, { once: true });
      }
      this.waiters.push(waiter);
    });
  }

  async *events(signal?: AbortSignal): AsyncIterableIterator<SignalsEvent> {
    const queue: SignalsEvent[] = [];
    let wake: (() => void) | undefined;
    let terminalError: Error | undefined = this.terminalError;
    let closed = !!this.terminalError;
    const notify = () => {
      wake?.();
      wake = undefined;
    };
    const onEvent = (event: SignalsEvent) => {
      queue.push(event);
      notify();
    };
    const onError = (error: Error) => {
      terminalError = error;
      notify();
    };
    const onClose = () => {
      closed = true;
      notify();
    };
    this.on("event", onEvent);
    this.on("client-error", onError);
    this.on("client-close", onClose);
    try {
      for (;;) {
        if (signal?.aborted) {
          throw new Error("signals-client: events aborted");
        }
        if (queue.length === 0 && terminalError) {
          throw terminalError;
        }
        if (queue.length === 0 && closed) {
          return;
        }
        if (queue.length === 0) {
          await new Promise<void>((resolve, reject) => {
            wake = resolve;
            if (!signal) return;
            const onAbort = () => reject(new Error("signals-client: events aborted"));
            signal.addEventListener("abort", onAbort, { once: true });
            const previousWake = wake;
            wake = () => {
              signal.removeEventListener("abort", onAbort);
              previousWake?.();
            };
          });
        }
        while (queue.length > 0) {
          yield queue.shift() as SignalsEvent;
        }
      }
    } finally {
      this.off("event", onEvent);
      this.off("client-error", onError);
      this.off("client-close", onClose);
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<SignalsEvent> {
    yield* this.events();
  }

  private send(payload: unknown): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error("signals-client: websocket is not connected");
    }
    this.ws.send(JSON.stringify(payload));
  }

  private accept(event: SignalsEvent): void {
    this.emit("event", event);
    this.emit(event.type, event);
    const waiter = this.waiters.shift();
    if (waiter) {
      waiter.cleanup?.();
      waiter.resolve(event);
      return;
    }
    this.queue.push(event);
  }

  private fail(error: Error): void {
    this.terminalError = error;
    this.emit("client-error", error);
    this.rejectWaiters(error);
  }

  private closeStreams(error: Error): void {
    this.terminalError = error;
    this.emit("client-close");
    this.rejectWaiters(error);
  }

  private rejectWaiters(error: Error): void {
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift();
      waiter?.cleanup?.();
      waiter?.reject(error);
    }
  }

  private removeWaiter(waiter: Waiter): void {
    const index = this.waiters.indexOf(waiter);
    if (index >= 0) {
      this.waiters.splice(index, 1);
    }
  }
}

export function parseEvent(raw: string): SignalsEvent {
  const msg = JSON.parse(raw) as RawServerEvent;
  switch (msg.type) {
    case "ready":
      return { type: "ready", message: msg.message ?? "" };
    case "subscribed":
      return {
        type: "subscribed",
        subscriptionId: Number(msg.subscriptionId ?? 0),
        venue: msg.venue ?? "",
        instrument: msg.instrument ?? ""
      };
    case "unsubscribed":
      return {
        type: "unsubscribed",
        subscriptionId: msg.subscriptionId,
        venue: msg.venue,
        instrument: msg.instrument,
        code: msg.code,
        message: msg.message
      };
    case "info":
      return {
        type: "info",
        subscriptionId: Number(msg.subscriptionId ?? 0),
        venue: msg.venue ?? "",
        instrument: msg.instrument ?? "",
        stage: msg.stage ?? "",
        message: msg.message ?? "",
        timestamp: msg.timestamp,
        replay: msg.replay,
        replayedAt: msg.replayedAt
      };
    case "signal": {
      const signal = { ...(msg.signal ?? {}) } as Signal;
      signal.venue ||= msg.venue ?? "";
      signal.instrument ||= msg.instrument ?? "";
      signal.timestamp ||= msg.timestamp;
      return {
        type: "signal",
        subscriptionId: Number(msg.subscriptionId ?? 0),
        venue: msg.venue ?? signal.venue,
        instrument: msg.instrument ?? signal.instrument,
        signal,
        timestamp: msg.timestamp,
        replay: msg.replay,
        replayedAt: msg.replayedAt
      };
    }
    case "error":
      return { type: "error", code: msg.code, message: msg.message };
    default:
      throw new Error(`signals-client: unsupported websocket event type ${String(msg.type)}`);
  }
}

export interface InstrumentConfig {
  makerFeeRate?: number;
  takerFeeRate?: number;
  minLeverage?: number;
  maxLeverage?: number;
}

export interface AssetSnapshot {
  currency: string;
  cash?: number;
  available?: number;
  used?: number;
  equity?: number;
  updatedAt?: Date;
}

export class AssetManager {
  private readonly assetsByCurrency = new Map<string, Required<AssetSnapshot>>();

  updateAsset(snapshot: AssetSnapshot): void {
    if (!snapshot.currency) return;
    this.assetsByCurrency.set(snapshot.currency, {
      currency: snapshot.currency,
      cash: snapshot.cash ?? 0,
      available: snapshot.available ?? 0,
      used: snapshot.used ?? 0,
      equity: snapshot.equity ?? 0,
      updatedAt: snapshot.updatedAt ?? new Date()
    });
  }

  asset(currency: string): Required<AssetSnapshot> | undefined {
    return this.assetsByCurrency.get(currency);
  }

  assets(): Required<AssetSnapshot>[] {
    return [...this.assetsByCurrency.values()].sort((a, b) => a.currency.localeCompare(b.currency));
  }
}

export interface InstrumentMetadata {
  venue: string;
  instrument: string;
  settlementCurrency?: string;
  lotSize?: number;
  minSize?: number;
  tickSize?: number;
  maxLeverage?: number;
}

export class InstrumentManager {
  private readonly instrumentsByKey = new Map<string, Required<InstrumentMetadata>>();

  updateInstrument(metadata: InstrumentMetadata): void {
    if (!metadata.venue || !metadata.instrument) return;
    this.instrumentsByKey.set(positionKey(metadata.venue, metadata.instrument), {
      venue: metadata.venue,
      instrument: metadata.instrument,
      settlementCurrency: metadata.settlementCurrency ?? "USDT",
      lotSize: metadata.lotSize ?? 0,
      minSize: metadata.minSize ?? 0,
      tickSize: metadata.tickSize ?? 0,
      maxLeverage: metadata.maxLeverage ?? 0
    });
  }

  instrument(venue: string, instrument: string): Required<InstrumentMetadata> | undefined {
    return this.instrumentsByKey.get(positionKey(venue, instrument));
  }

  instruments(): Required<InstrumentMetadata>[] {
    return [...this.instrumentsByKey.values()].sort((a, b) => (
      a.venue === b.venue ? a.instrument.localeCompare(b.instrument) : a.venue.localeCompare(b.venue)
    ));
  }
}

export interface PositionManagerConfig {
  positionSize?: number;
  minExpectedEdge?: number;
  minOrderDelta?: number;
  rebalanceIntervalMs?: number;
  makerFeeRate?: number;
  takerFeeRate?: number;
  minLeverage?: number;
  maxLeverage?: number;
  instruments?: Record<string, InstrumentConfig>;
  assetManager?: AssetManager;
  instrumentManager?: InstrumentManager;
}

export interface Position {
  venue: string;
  instrument: string;
  size: number;
  confidence: number;
  entryPrice?: number;
  lastPrice?: number;
  takeProfit?: number;
  stopLoss?: number;
  leverage?: number;
  realizedGross?: number;
  fees?: number;
  realizedPnl?: number;
  openedAt?: Date;
  lastSignalAt?: Date;
}

export interface Order {
  venue: string;
  instrument: string;
  side: Side;
  reason: string;
  sizeDelta: number;
  previousSize: number;
  targetSize: number;
  price?: number;
  confidence: number;
  score?: number;
  expectedEdge: number;
  feeRate: number;
  estimatedFee: number;
  estimatedFeeValue: number;
  quantity: number;
  notional: number;
  settlementCurrency: string;
  minSize: number;
  lotSize: number;
  tickSize: number;
  leverage: number;
  takeProfit?: number;
  stopLoss?: number;
  timestamp: Date;
  subscriptionId?: number;
  replay?: boolean;
}

export interface ClosedTrade {
  venue: string;
  instrument: string;
  side: Side;
  size: number;
  entryPrice?: number;
  exitPrice?: number;
  realizedGross: number;
  fees: number;
  realizedPnl: number;
  openedAt?: Date;
  closedAt: Date;
}

export interface InstrumentPositionStats {
  venue: string;
  instrument: string;
  settlementCurrency: string;
  side: Side | "";
  size: number;
  quantity: number;
  notional: number;
  realizedPnl: number;
  unrealizedPnl: number;
  fees: number;
  realizedPnlPercent: number;
  unrealizedPnlPercent: number;
  totalPnlPercent: number;
  leverage: number;
}

export interface CurrencyPositionStats {
  settlementCurrency: string;
  equity: number;
  available: number;
  used: number;
  realizedPnl: number;
  unrealizedPnl: number;
  fees: number;
  realizedPnlPercent: number;
  unrealizedPnlPercent: number;
  totalPnlPercent: number;
}

export interface PositionStats {
  equity: number;
  available: number;
  used: number;
  realizedPnl: number;
  unrealizedPnl: number;
  fees: number;
  realizedPnlPercent: number;
  unrealizedPnlPercent: number;
  totalPnlPercent: number;
  byInstrument: Record<string, InstrumentPositionStats>;
  byCurrency: Record<string, CurrencyPositionStats>;
}

const productionDefaults = {
  makerFeeRate: 0.0002,
  takerFeeRate: 0.0005,
  positionSize: 1,
  minExpectedEdge: 0.0045,
  minOrderDelta: 0.2,
  rebalanceIntervalMs: 6 * 60 * 60 * 1000,
  minLeverage: 1,
  maxLeverage: 1,
  assetManager: undefined,
  instrumentManager: undefined
} as const;

export function productionPositionManagerConfig(overrides: PositionManagerConfig = {}): PositionManagerConfig {
  return { ...productionDefaults, ...overrides, instruments: overrides.instruments ?? {} };
}

export class PositionManager {
  private readonly client?: SignalsClient;
  private readonly config: Required<PositionManagerConfig>;
  private readonly assets: AssetManager;
  private readonly instrumentMetadata: InstrumentManager;
  private readonly positionsByKey = new Map<string, Position>();
  private readonly closed: ClosedTrade[] = [];

  constructor(client?: SignalsClient, config: PositionManagerConfig = productionPositionManagerConfig()) {
    this.client = client;
    this.config = normalizeConfig(config);
    this.assets = config.assetManager ?? new AssetManager();
    this.instrumentMetadata = config.instrumentManager ?? new InstrumentManager();
  }

  assetManager(): AssetManager {
    return this.assets;
  }

  instrumentManager(): InstrumentManager {
    return this.instrumentMetadata;
  }

  async *run(signal?: AbortSignal): AsyncIterableIterator<Order> {
    if (!this.client) {
      throw new Error("signals-client: PositionManager has no SignalsClient");
    }
    for await (const event of this.client.events(signal)) {
      for (const order of this.handleEvent(event)) {
        yield order;
      }
    }
  }

  addPosition(position: Position): void {
    const key = positionKey(position.venue, position.instrument);
    this.positionsByKey.set(key, { ...position, leverage: position.leverage ?? this.minLeverage(key) });
  }

  updatePosition(position: Position): void {
    this.addPosition(position);
  }

  closePosition(venue: string, instrument: string): Order[] {
    const key = positionKey(venue, instrument);
    const position = this.positionsByKey.get(key);
    if (!position || Math.abs(position.size) <= 1e-9) return [];
    const now = new Date();
    const delta = -position.size;
    const order = this.orderForDelta(key, position, delta, 0, undefined, "closing", now, position.confidence);
    this.applyDelta(key, position, delta, position.lastPrice ?? position.entryPrice, this.takerFeeRate(key), now);
    return [order];
  }

  positions(): Position[] {
    return [...this.positionsByKey.values()].map((position) => ({ ...position }));
  }

  closedTrades(): ClosedTrade[] {
    return this.closed.map((trade) => ({ ...trade }));
  }

  stats(): PositionStats {
    const stats: PositionStats = {
      equity: 0,
      available: 0,
      used: 0,
      realizedPnl: 0,
      unrealizedPnl: 0,
      fees: 0,
      realizedPnlPercent: 0,
      unrealizedPnlPercent: 0,
      totalPnlPercent: 0,
      byInstrument: {},
      byCurrency: {}
    };
    for (const asset of this.assets.assets()) {
      stats.equity += asset.equity;
      stats.available += asset.available;
      stats.used += asset.used;
      stats.byCurrency[asset.currency] = {
        settlementCurrency: asset.currency,
        equity: asset.equity,
        available: asset.available,
        used: asset.used,
        realizedPnl: 0,
        unrealizedPnl: 0,
        fees: 0,
        realizedPnlPercent: 0,
        unrealizedPnlPercent: 0,
        totalPnlPercent: 0
      };
    }
    for (const [key, position] of this.positionsByKey) {
      const metadata = this.instrumentFor(position.venue, position.instrument);
      const asset = this.assets.asset(metadata.settlementCurrency);
      const equity = positiveOr(asset?.equity, (asset?.cash ?? 0) + (asset?.used ?? 0), 1);
      const price = roundToTick(position.lastPrice ?? position.entryPrice ?? 0, metadata.tickSize);
      const notionalRaw = Math.abs(position.size) * equity * positiveOr(position.leverage, this.minLeverage(key), 1);
      const quantity = price > 0 ? roundDownToStep(notionalRaw / price, metadata.lotSize) : 0;
      const notional = quantity * price;
      const realized = (position.realizedPnl ?? 0) * equity;
      const unrealized = move(position) * Math.abs(position.size) * equity;
      const fees = (position.fees ?? 0) * equity;
      stats.byInstrument[key] = {
        venue: position.venue,
        instrument: position.instrument,
        settlementCurrency: metadata.settlementCurrency,
        side: positionSide(position),
        size: position.size,
        quantity,
        notional,
        realizedPnl: realized,
        unrealizedPnl: unrealized,
        fees,
        realizedPnlPercent: position.realizedPnl ?? 0,
        unrealizedPnlPercent: move(position) * Math.abs(position.size),
        totalPnlPercent: (position.realizedPnl ?? 0) + move(position) * Math.abs(position.size),
        leverage: position.leverage ?? this.minLeverage(key)
      };
      stats.realizedPnl += realized;
      stats.unrealizedPnl += unrealized;
      stats.fees += fees;
      const currency = stats.byCurrency[metadata.settlementCurrency] ?? {
        settlementCurrency: metadata.settlementCurrency,
        equity,
        available: asset?.available ?? 0,
        used: asset?.used ?? 0,
        realizedPnl: 0,
        unrealizedPnl: 0,
        fees: 0,
        realizedPnlPercent: 0,
        unrealizedPnlPercent: 0,
        totalPnlPercent: 0
      };
      currency.realizedPnl += realized;
      currency.unrealizedPnl += unrealized;
      currency.fees += fees;
      if (currency.equity > 0) {
        currency.realizedPnlPercent = currency.realizedPnl / currency.equity;
        currency.unrealizedPnlPercent = currency.unrealizedPnl / currency.equity;
        currency.totalPnlPercent = (currency.realizedPnl + currency.unrealizedPnl) / currency.equity;
      }
      stats.byCurrency[metadata.settlementCurrency] = currency;
    }
    if (stats.equity <= 0) stats.equity = 1;
    stats.realizedPnlPercent = stats.realizedPnl / stats.equity;
    stats.unrealizedPnlPercent = stats.unrealizedPnl / stats.equity;
    stats.totalPnlPercent = (stats.realizedPnl + stats.unrealizedPnl) / stats.equity;
    return stats;
  }

  updatePrice(venue: string, instrument: string, price: number, timestamp = new Date()): Order[] {
    if (price <= 0) throw new Error("signals-client: price must be positive");
    const key = positionKey(venue, instrument);
    const position = this.positionsByKey.get(key);
    if (!position) return [];
    position.lastPrice = price;
    if (!exitTriggered(position, price)) return [];
    const reason = takeProfitTriggered(position, price) ? "take_profit" : "stop_loss";
    const feeRate = reason === "take_profit" ? this.makerFeeRate(key) : this.takerFeeRate(key);
    const delta = -position.size;
    const order = this.orderForDelta(key, position, delta, 0, undefined, reason, timestamp, position.confidence);
    order.feeRate = feeRate;
    order.estimatedFee = Math.abs(delta) * feeRate;
    order.estimatedFeeValue = order.notional * feeRate;
    if (!this.orderMeetsInstrumentMinimum(order)) return [];
    this.applyDelta(key, position, delta, price, feeRate, timestamp);
    return [order];
  }

  handleEvent(event: SignalsEvent): Order[] {
    if (event.type !== "signal") return [];
    if (event.replay) return [];
    const signal = { ...event.signal };
    signal.venue ||= event.venue;
    signal.instrument ||= event.instrument;
    signal.timestamp ||= event.timestamp;
    return this.handleSignal(signal).map((order) => ({
      ...order,
      subscriptionId: event.subscriptionId,
      replay: event.replay
    }));
  }

  handleSignal(signal: Signal): Order[] {
    if (!signal.venue || !signal.instrument) {
      throw new Error("signals-client: signal venue and instrument are required");
    }
    if (!this.instrumentMetadata.instrument(signal.venue, signal.instrument)) return [];
    const now = signalDate(signal.timestamp);
    const key = positionKey(signal.venue, signal.instrument);
    const targetSign = sideSign(signal.side);
    const targetConfidence = clamp01(signal.confidence);
    if (!targetSign || targetConfidence <= 0) return [];
    const edge = feeAdjustedExpectedEdge(signal, this.takerFeeRate(key));
    if (this.config.minExpectedEdge > 0 && edge < this.config.minExpectedEdge) return [];

    let position = this.positionsByKey.get(key);
    const targetSize = targetSign * this.config.positionSize * targetConfidence;
    const minOrderDelta = this.effectiveMinOrderDelta();
    if (!position || Math.abs(position.size) <= 1e-9) {
      if (Math.abs(targetSize) < minOrderDelta) return [];
      position = {
        venue: signal.venue,
        instrument: signal.instrument,
        size: 0,
        confidence: targetConfidence,
        entryPrice: signal.price,
        lastPrice: signal.price,
        openedAt: now,
        lastSignalAt: now
      };
      this.positionsByKey.set(key, position);
    } else {
      const isFlip = sign(position.size) !== 0 && sign(position.size) !== targetSign;
      if (!isFlip && this.config.rebalanceIntervalMs > 0 && position.lastSignalAt) {
        if (now.getTime() < position.lastSignalAt.getTime() + this.config.rebalanceIntervalMs) return [];
      }
      if (!isFlip && minOrderDelta > 0 && Math.abs(targetSize - position.size) < minOrderDelta) return [];
    }

    position.confidence = targetConfidence;
    position.lastSignalAt = now;
    if (signal.price && signal.price > 0) {
      position.lastPrice = signal.price;
      position.entryPrice ??= signal.price;
    }
    if (!position.takeProfit || !position.stopLoss || positionSide(position) !== signal.side) {
      position.takeProfit = signal.takeProfit;
      position.stopLoss = signal.stopLoss;
    } else {
      position.takeProfit = blendRisk(position.takeProfit, signal.takeProfit, 0.5);
      position.stopLoss = blendRisk(position.stopLoss, signal.stopLoss, 0.5);
    }
    position.leverage = this.selectLeverage(key, targetConfidence, edge, signal.score);

    return this.rebalance(now, { [key]: targetSign }, {
      [key]: {
        confidence: targetConfidence,
        score: signal.score,
        expectedEdge: edge,
        takeProfit: signal.takeProfit,
        stopLoss: signal.stopLoss
      }
    });
  }

  private rebalance(now: Date, sideOverrides: Record<string, number>, contexts: Record<string, SignalContext>): Order[] {
    const weights = new Map<string, number>();
    const sides = new Map<string, number>();
    let totalWeight = 0;
    for (const [key, position] of this.positionsByKey) {
      if (Math.abs(position.size) <= 1e-9 && position.confidence <= 0) continue;
      const hasOverride = Object.hasOwn(sideOverrides, key);
      let weight = clamp01(position.confidence);
      if (!hasOverride && weight <= 0) weight = confidenceFromSize(position, this.config.positionSize);
      let side = sign(position.size);
      if (hasOverride) side = sideOverrides[key] ?? side;
      weights.set(key, weight);
      sides.set(key, side);
      if (weight > 1e-9 && side !== 0) totalWeight += weight;
    }
    const usedBudget = Math.min(this.config.positionSize, totalWeight);
    const reductions: RebalanceCandidate[] = [];
    const openings: RebalanceCandidate[] = [];
    for (const key of [...this.positionsByKey.keys()].sort()) {
      const position = this.positionsByKey.get(key);
      if (!position) continue;
      const weight = weights.get(key) ?? 0;
      const side = sides.get(key) ?? 0;
      const targetSize = totalWeight > 0 ? side * usedBudget * weight / totalWeight : 0;
      let delta = targetSize - position.size;
      if (isFlipTarget(position.size, targetSize)) delta = -position.size;
      if (Math.abs(delta) <= 1e-9) {
        position.confidence = weight;
        continue;
      }
      const hasOverride = Object.hasOwn(sideOverrides, key);
      if (this.shouldSkipRebalanceDelta(position, targetSize, delta, now, hasOverride)) {
        position.confidence = weight;
        continue;
      }
      const context = contexts[key] ?? { confidence: position.confidence, expectedEdge: 0 };
      const candidate: RebalanceCandidate = {
        key,
        position,
        delta,
        weight,
        context,
        reason: orderReason(position, targetSize)
      };
      if (isExposureReduction(position.size, position.size + delta)) {
        reductions.push(candidate);
      } else {
        openings.push(candidate);
      }
    }
    if (reductions.length > 0) return this.materializeRebalanceOrders(reductions, now);
    return this.materializeRebalanceOrders(openings, now, new Map<string, number>());
  }

  private materializeRebalanceOrders(candidates: RebalanceCandidate[], now: Date, openingExposureByCurrency?: Map<string, number>): Order[] {
    const orders: Order[] = [];
    for (const candidate of candidates) {
      let delta = candidate.delta;
      if (openingExposureByCurrency && !isExposureReduction(candidate.position.size, candidate.position.size + delta)) {
        const metadata = this.instrumentFor(candidate.position.venue, candidate.position.instrument);
        const used = openingExposureByCurrency.get(metadata.settlementCurrency) ?? 0;
        const available = this.availableExposureBudget(metadata.settlementCurrency) - used;
        if (available <= 1e-9) {
          candidate.position.confidence = candidate.weight;
          continue;
        }
        if (Math.abs(delta) > available) delta = sign(delta) * available;
      }
      const order = this.orderForDelta(
        candidate.key,
        candidate.position,
        delta,
        candidate.context.expectedEdge,
        candidate.context.score,
        candidate.reason,
        now,
        candidate.context.confidence
      );
      order.takeProfit = candidate.context.takeProfit;
      order.stopLoss = candidate.context.stopLoss;
      if (!this.orderMeetsInstrumentMinimum(order)) {
        candidate.position.confidence = candidate.weight;
        continue;
      }
      orders.push(order);
      if (openingExposureByCurrency && !isExposureReduction(order.previousSize, order.targetSize)) {
        openingExposureByCurrency.set(order.settlementCurrency, (openingExposureByCurrency.get(order.settlementCurrency) ?? 0) + Math.abs(order.sizeDelta));
      }
      this.applyDelta(candidate.key, candidate.position, delta, candidate.position.lastPrice ?? candidate.position.entryPrice, this.takerFeeRate(candidate.key), now);
      const current = this.positionsByKey.get(candidate.key);
      if (current) current.confidence = candidate.weight;
    }
    return orders;
  }

  private availableExposureBudget(currency: string): number {
    const asset = this.assets.asset(currency);
    if (!asset) return Number.POSITIVE_INFINITY;
    const equity = positiveOr(asset.equity, asset.cash + asset.used, asset.cash);
    if (equity <= 0) return asset.available > 0 ? Number.POSITIVE_INFINITY : 0;
    if (asset.available <= 0) return 0;
    return Math.max(0, asset.available / equity);
  }

  private shouldSkipRebalanceDelta(position: Position, targetSize: number, delta: number, now: Date, hasOverride: boolean): boolean {
    const isClosing = Math.abs(targetSize) <= 1e-9 && Math.abs(position.size) > 1e-9;
    const isOpening = Math.abs(position.size) <= 1e-9 && Math.abs(targetSize) > 1e-9;
    const isFlip = Math.abs(position.size) > 1e-9 && Math.abs(targetSize) > 1e-9 && !sameSign(position.size, targetSize);
    if (isClosing || isOpening || isFlip) return false;
    if (this.effectiveMinOrderDelta() > 0 && Math.abs(delta) < this.effectiveMinOrderDelta()) return true;
    if (!hasOverride && this.config.rebalanceIntervalMs > 0 && position.lastSignalAt) {
      return now.getTime() < position.lastSignalAt.getTime() + this.config.rebalanceIntervalMs;
    }
    return false;
  }

  private orderForDelta(key: string, position: Position, delta: number, edge: number, score: number | undefined, reason: string, now: Date, confidence: number): Order {
    const feeRate = this.takerFeeRate(key);
    const leverage = this.selectLeverage(key, confidence, edge, score);
    position.leverage = leverage;
    const metadata = this.instrumentFor(position.venue, position.instrument);
    const asset = this.assets.asset(metadata.settlementCurrency);
    const equity = positiveOr(asset?.equity, (asset?.cash ?? 0) + (asset?.used ?? 0), 1);
    const price = roundToTick(position.lastPrice ?? position.entryPrice ?? 0, metadata.tickSize);
    let notional = Math.abs(delta) * equity * leverage;
    const quantity = price > 0 ? roundDownToStep(notional / price, metadata.lotSize) : 0;
    notional = quantity * price;
    return {
      venue: position.venue,
      instrument: position.instrument,
      side: delta < 0 ? "sell" : "buy",
      reason,
      sizeDelta: delta,
      previousSize: position.size,
      targetSize: position.size + delta,
      price,
      confidence,
      score,
      expectedEdge: edge,
      feeRate,
      estimatedFee: Math.abs(delta) * feeRate,
      estimatedFeeValue: notional * feeRate,
      quantity,
      notional,
      settlementCurrency: metadata.settlementCurrency,
      minSize: metadata.minSize,
      lotSize: metadata.lotSize,
      tickSize: metadata.tickSize,
      leverage,
      timestamp: now
    };
  }

  private applyDelta(key: string, position: Position, delta: number, price: number | undefined, feeRate: number, now: Date): void {
    if (Math.abs(delta) <= 1e-9) return;
    if (position.size === 0 || sameSign(position.size, delta)) {
      const opened = Math.abs(position.size) <= 1e-9;
      const nextAbs = Math.abs(position.size) + Math.abs(delta);
      if (price && price > 0) {
        position.entryPrice = nextAbs > 0 && Math.abs(position.size) > 1e-9 && position.entryPrice
          ? (position.entryPrice * Math.abs(position.size) + price * Math.abs(delta)) / nextAbs
          : price;
        position.lastPrice = price;
      }
      if (opened) position.openedAt = now;
      const fee = Math.abs(delta) * feeRate;
      position.fees = (position.fees ?? 0) + fee;
      position.realizedPnl = (position.realizedPnl ?? 0) - fee;
      position.size += delta;
      return;
    }
    if (price && price > 0) position.lastPrice = price;
    const closing = Math.min(Math.abs(position.size), Math.abs(delta));
    const gross = move(position) * closing;
    const fee = closing * feeRate;
    position.realizedGross = (position.realizedGross ?? 0) + gross;
    position.fees = (position.fees ?? 0) + fee;
    position.realizedPnl = (position.realizedPnl ?? 0) + gross - fee;
    const closed: ClosedTrade = {
      venue: position.venue,
      instrument: position.instrument,
      side: positionSide(position) as Side,
      size: closing,
      entryPrice: position.entryPrice,
      exitPrice: price,
      realizedGross: position.realizedGross,
      fees: position.fees,
      realizedPnl: position.realizedPnl,
      openedAt: position.openedAt,
      closedAt: now
    };
    const remaining = Math.abs(delta) - closing;
    if (remaining <= 1e-9) {
      position.size += delta;
      if (Math.abs(position.size) <= 1e-9) {
        this.positionsByKey.delete(key);
        this.closed.push(closed);
      }
      return;
    }
    this.closed.push(closed);
    position.size = sign(delta) * remaining;
    position.entryPrice = price;
    position.lastPrice = price;
    position.openedAt = now;
    position.confidence = 0;
    position.realizedGross = 0;
    position.fees = remaining * this.takerFeeRate(key);
    position.realizedPnl = -position.fees;
  }

  private effectiveMinOrderDelta(): number {
    return this.config.minOrderDelta <= 0 ? 0 : this.config.minOrderDelta * this.config.positionSize;
  }

  private selectLeverage(key: string, confidence: number, edge: number, score = 0): number {
    const minLev = this.minLeverage(key);
    const maxLev = Math.max(this.maxLeverage(key), minLev);
    if (maxLev === minLev) return minLev;
    const edgeScore = clamp01(edge / Math.max(this.config.minExpectedEdge * 3, 0.001));
    const quality = clamp01(clamp01(confidence) * 0.65 + edgeScore * 0.25 + clamp01(Math.abs(score)) * 0.1);
    return minLev + (maxLev - minLev) * quality;
  }

  private makerFeeRate(key: string): number {
    return this.config.instruments[key]?.makerFeeRate ?? this.config.makerFeeRate;
  }

  private takerFeeRate(key: string): number {
    return this.config.instruments[key]?.takerFeeRate ?? this.config.takerFeeRate;
  }

  private minLeverage(key: string): number {
    return this.config.instruments[key]?.minLeverage ?? this.config.minLeverage;
  }

  private maxLeverage(key: string): number {
    const configured = this.config.instruments[key]?.maxLeverage ?? this.config.maxLeverage;
    const [venue, instrument] = splitKey(key);
    const metadataMax = this.instrumentMetadata.instrument(venue, instrument)?.maxLeverage ?? 0;
    return metadataMax > 0 ? Math.min(configured || metadataMax, metadataMax) : configured;
  }

  private instrumentFor(venue: string, instrument: string): Required<InstrumentMetadata> {
    return this.instrumentMetadata.instrument(venue, instrument) ?? {
      venue,
      instrument,
      settlementCurrency: "USDT",
      lotSize: 0,
      minSize: 0,
      tickSize: 0,
      maxLeverage: 0
    };
  }

  private orderMeetsInstrumentMinimum(order: Order): boolean {
    if (order.reason === "closing" || order.reason === "flip") return true;
    if (order.minSize > 0 && order.quantity > 0 && order.quantity < order.minSize) return false;
    if (order.minSize > 0 && order.quantity <= 0) return false;
    return true;
  }
}

interface SignalContext {
  confidence: number;
  score?: number;
  expectedEdge: number;
  takeProfit?: number;
  stopLoss?: number;
}

interface RebalanceCandidate {
  key: string;
  position: Position;
  delta: number;
  weight: number;
  context: SignalContext;
  reason: string;
}

function websocketUrlFromBase(baseUrl?: string): string | undefined {
  if (!baseUrl) return undefined;
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "http:" ? "ws:" : "wss:";
  url.pathname = "/ws";
  url.search = "";
  return url.toString();
}

function normalizeConfig(config: PositionManagerConfig): Required<PositionManagerConfig> {
  return {
    positionSize: Math.min(Math.max(config.positionSize ?? productionDefaults.positionSize, 0), 1),
    minExpectedEdge: Math.max(config.minExpectedEdge ?? productionDefaults.minExpectedEdge, 0),
    minOrderDelta: Math.min(Math.max(config.minOrderDelta ?? productionDefaults.minOrderDelta, 0), 1),
    rebalanceIntervalMs: Math.max(config.rebalanceIntervalMs ?? productionDefaults.rebalanceIntervalMs, 0),
    makerFeeRate: Math.max(config.makerFeeRate ?? productionDefaults.makerFeeRate, 0),
    takerFeeRate: Math.max(config.takerFeeRate ?? productionDefaults.takerFeeRate, 0),
    minLeverage: Math.max(config.minLeverage ?? productionDefaults.minLeverage, 0),
    maxLeverage: Math.max(config.maxLeverage ?? productionDefaults.maxLeverage, 0),
    instruments: config.instruments ?? {},
    assetManager: config.assetManager ?? new AssetManager(),
    instrumentManager: config.instrumentManager ?? new InstrumentManager()
  };
}

function feeAdjustedExpectedEdge(signal: Signal, takerFeeRate: number): number {
  return expectedEdge(signal) - 2 * takerFeeRate;
}

function expectedEdge(signal: Signal): number {
  const confidence = clamp01(signal.confidence);
  return confidence * Math.max(signal.takeProfit, 0) - (1 - confidence) * Math.max(signal.stopLoss, 0);
}

function signalDate(timestamp: string | Date | undefined): Date {
  if (timestamp instanceof Date) return timestamp;
  if (timestamp) return new Date(timestamp);
  return new Date();
}

function positionKey(venue: string, instrument: string): string {
  return `${venue}:${instrument}`;
}

function splitKey(key: string): [string, string] {
  const separator = key.indexOf(":");
  return separator === -1 ? ["", key] : [key.slice(0, separator), key.slice(separator + 1)];
}

function sideSign(side: Side): number {
  if (side === "buy") return 1;
  if (side === "sell") return -1;
  return 0;
}

function sign(value: number): number {
  if (value < 0) return -1;
  if (value > 0) return 1;
  return 0;
}

function sameSign(a: number, b: number): boolean {
  return sign(a) === sign(b);
}

function clamp01(value: number): number {
  return Math.min(Math.max(value, 0), 1);
}

function positiveOr(...values: Array<number | undefined>): number {
  for (const value of values) {
    if (value !== undefined && value > 0) return value;
  }
  return 0;
}

function roundDownToStep(value: number, step?: number): number {
  if (value <= 0 || !step || step <= 0) return value;
  return Math.floor(value / step) * step;
}

function roundToTick(value: number, tick?: number): number {
  if (value <= 0 || !tick || tick <= 0) return value;
  return Math.round(value / tick) * tick;
}

function positionSide(position: Position): Side | "" {
  if (position.size < 0) return "sell";
  if (position.size > 0) return "buy";
  return "";
}

function confidenceFromSize(position: Position, positionSize: number): number {
  return positionSize <= 0 ? clamp01(Math.abs(position.size)) : clamp01(Math.abs(position.size) / positionSize);
}

function move(position: Position): number {
  if (!position.entryPrice || !position.lastPrice) return 0;
  if (position.size < 0) return (position.entryPrice - position.lastPrice) / position.entryPrice;
  return (position.lastPrice - position.entryPrice) / position.entryPrice;
}

function takeProfitPrice(position: Position): number | undefined {
  if (!position.entryPrice || !position.takeProfit) return undefined;
  return position.size < 0
    ? position.entryPrice * (1 - position.takeProfit)
    : position.entryPrice * (1 + position.takeProfit);
}

function stopLossPrice(position: Position): number | undefined {
  if (!position.entryPrice || !position.stopLoss) return undefined;
  return position.size < 0
    ? position.entryPrice * (1 + position.stopLoss)
    : position.entryPrice * (1 - position.stopLoss);
}

function takeProfitTriggered(position: Position, price: number): boolean {
  const target = takeProfitPrice(position);
  if (!target) return false;
  return position.size < 0 ? price <= target : price >= target;
}

function stopLossTriggered(position: Position, price: number): boolean {
  const target = stopLossPrice(position);
  if (!target) return false;
  return position.size < 0 ? price >= target : price <= target;
}

function exitTriggered(position: Position, price: number): boolean {
  return takeProfitTriggered(position, price) || stopLossTriggered(position, price);
}

function orderReason(position: Position, targetSize: number): string {
  if (Math.abs(position.size) <= 1e-9) return "opening";
  if (Math.abs(targetSize) <= 1e-9) return "closing";
  if (!sameSign(position.size, targetSize)) return "flip";
  return "rebalance";
}

function isFlipTarget(previousSize: number, targetSize: number): boolean {
  return Math.abs(previousSize) > 1e-9 && Math.abs(targetSize) > 1e-9 && !sameSign(previousSize, targetSize);
}

function isExposureReduction(previousSize: number, targetSize: number): boolean {
  if (Math.abs(previousSize) <= 1e-9) return false;
  if (Math.abs(targetSize) <= 1e-9) return true;
  if (!sameSign(previousSize, targetSize)) return true;
  return Math.abs(targetSize) < Math.abs(previousSize) - 1e-9;
}

function blendRisk(current: number, incoming: number, gate: number): number {
  if (current <= 0) return incoming;
  if (incoming <= 0) return current;
  const clamped = clamp01(gate);
  return current * (1 - clamped) + incoming * clamped;
}
