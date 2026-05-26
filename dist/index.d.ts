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
export type SignalsEvent = ReadyEvent | SubscribedEvent | UnsubscribedEvent | InfoEvent | SignalEvent | ErrorEvent;
export interface SignalsClientOptions {
    url?: string;
    baseUrl?: string;
    headers?: Record<string, string>;
    WebSocketCtor?: typeof WebSocket;
}
export declare class SignalsClient extends EventEmitter {
    private readonly token;
    private readonly options;
    private ws?;
    private readonly queue;
    private readonly waiters;
    private terminalError?;
    constructor(token: SignalsWebSocketToken, options?: SignalsClientOptions);
    connect(): Promise<void>;
    close(): void;
    subscribe(venue: string, instrument: string): void;
    unsubscribe(subscriptionId: number): void;
    unsubscribeInstrument(venue: string, instrument: string): void;
    receive(signal?: AbortSignal): Promise<SignalsEvent>;
    events(signal?: AbortSignal): AsyncIterableIterator<SignalsEvent>;
    [Symbol.asyncIterator](): AsyncIterableIterator<SignalsEvent>;
    private send;
    private accept;
    private fail;
    private closeStreams;
    private rejectWaiters;
    private removeWaiter;
}
export declare function parseEvent(raw: string): SignalsEvent;
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
export declare class AssetManager {
    private readonly assetsByCurrency;
    updateAsset(snapshot: AssetSnapshot): void;
    asset(currency: string): Required<AssetSnapshot> | undefined;
    assets(): Required<AssetSnapshot>[];
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
export declare class InstrumentManager {
    private readonly instrumentsByKey;
    updateInstrument(metadata: InstrumentMetadata): void;
    instrument(venue: string, instrument: string): Required<InstrumentMetadata> | undefined;
    instruments(): Required<InstrumentMetadata>[];
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
export declare function productionPositionManagerConfig(overrides?: PositionManagerConfig): PositionManagerConfig;
export declare class PositionManager {
    private readonly client?;
    private readonly config;
    private readonly assets;
    private readonly instrumentMetadata;
    private readonly positionsByKey;
    private readonly closed;
    constructor(client?: SignalsClient, config?: PositionManagerConfig);
    assetManager(): AssetManager;
    instrumentManager(): InstrumentManager;
    run(signal?: AbortSignal): AsyncIterableIterator<Order>;
    addPosition(position: Position): void;
    updatePosition(position: Position): void;
    closePosition(venue: string, instrument: string): Order[];
    positions(): Position[];
    closedTrades(): ClosedTrade[];
    stats(): PositionStats;
    updatePrice(venue: string, instrument: string, price: number, timestamp?: Date): Order[];
    handleEvent(event: SignalsEvent): Order[];
    handleSignal(signal: Signal): Order[];
    private rebalance;
    private materializeRebalanceOrders;
    private availableExposureBudget;
    private shouldSkipRebalanceDelta;
    private orderForDelta;
    private applyDelta;
    private effectiveMinOrderDelta;
    private selectLeverage;
    private makerFeeRate;
    private takerFeeRate;
    private minLeverage;
    private maxLeverage;
    private instrumentFor;
    private orderMeetsInstrumentMinimum;
}
//# sourceMappingURL=index.d.ts.map