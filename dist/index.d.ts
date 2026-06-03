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
    trailingStopActivation?: number;
    trailingStopDistance?: number;
    trailingStopMinProfit?: number;
    score?: number;
    components?: SignalComponent[];
    modelVariant?: string;
    modelVersion?: string;
    predictionMode?: string;
    confidenceMapping?: string;
    upProbability?: number;
    downProbability?: number;
    directionalEdge?: number;
    normalizedEdge?: number;
    expectedValue?: number;
    regime?: string;
    regimeConfidence?: number;
    volatilityState?: string;
    squeezeState?: string;
    trendState?: string;
    atrPercent?: number;
    signalTTL?: number;
    generatedAt?: string | Date;
    artifactID?: string;
    artifactVersion?: string;
    rejectedReason?: string;
    managePositionsOnly?: boolean;
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
export interface BacktestEvent {
    type: "backtest";
    subscriptionId: number;
    venue: string;
    instrument: string;
    backtest: unknown;
    timestamp?: string;
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
export interface CreateMarketOrderEvent {
    type: "create-market-order";
    subscriptionId: number;
    intentId?: string;
    action?: string;
    reason?: string;
    venue?: string;
    instrument: string;
    side: Side;
    orderType?: string;
    contractSize?: number;
    margin?: number;
    leverage?: number;
    confidence?: number;
    reduceOnly?: boolean;
    takeProfitPrice?: number;
    stopLossPrice?: number;
    takeProfit?: number;
    stopLoss?: number;
    timestamp?: string;
}
export interface UpdateTPSLEvent {
    type: "update-tpsl";
    subscriptionId: number;
    intentId?: string;
    venue?: string;
    instrument: string;
    side: Side;
    takeProfitPrice?: number;
    stopLossPrice?: number;
    takeProfit?: number;
    stopLoss?: number;
    timestamp?: string;
}
export interface WithdrawEvent {
    type: "withdraw";
    subscriptionId: number;
    intentId?: string;
    venue?: string;
    currency: string;
    amount: number;
    timestamp?: string;
}
export interface ErrorEvent {
    type: "error";
    code?: string;
    message?: string;
}
export type SignalsEvent = ReadyEvent | SubscribedEvent | UnsubscribedEvent | InfoEvent | BacktestEvent | SignalEvent | CreateMarketOrderEvent | UpdateTPSLEvent | WithdrawEvent | ErrorEvent;
export interface SignalEventSource {
    events(signal?: AbortSignal): AsyncIterable<SignalsEvent>;
}
export interface SignalsClientOptions {
    url?: string;
    baseUrl?: string;
    headers?: Record<string, string>;
    WebSocketCtor?: typeof WebSocket;
}
export interface RiskConfig {
    maxMarginRatio?: number;
    maxConcurrentPositions?: number;
    maxDrawdown?: number;
    switchBuffer?: number;
    minLeverage?: number;
    maxLeverage?: number;
    profitWithdrawRatio?: number;
}
export interface RuntimeConfig {
    profitWithdrawRatio?: number;
}
export interface AssetSnapshot {
    venue?: string;
    currency: string;
    cash?: number;
    available?: number;
    used?: number;
    equity?: number;
    maxUsage?: number;
    updatedAt?: Date | string;
}
export interface Position {
    venue?: string;
    instrument: string;
    status?: string;
    size: number;
    confidence?: number;
    entryPrice?: number;
    lastPrice?: number;
    takeProfit?: number;
    stopLoss?: number;
    takeProfitPrice?: number;
    stopLossPrice?: number;
    trailingStopActivation?: number;
    trailingStopDistance?: number;
    trailingStopMinProfit?: number;
    margin?: number;
    leverage?: number;
    mfe?: number;
    mae?: number;
    realizedGross?: number;
    fees?: number;
    realizedPnL?: number;
    openedAt?: string | Date;
    lastSignalAt?: string | Date;
}
export interface SubscribeRequest {
    venue: string;
    instruments: string[];
    mode?: string;
    risk?: RiskConfig;
    profitWithdrawRatio?: number;
    assets?: AssetSnapshot[];
    positions?: Position[];
}
export interface WithdrawalRequest {
    venue?: string;
    currency: string;
    amount: number;
    reason?: string;
}
export interface SignalsManagerConfig {
    venue?: string;
    instruments?: string[];
    mode?: string;
    risk?: RiskConfig;
    profitWithdrawRatio?: number;
}
export interface SignalsManagerState {
    assets?: AssetSnapshot[];
    positions?: Position[];
}
export type Intent = CreateMarketOrderEvent;
export interface SignalsManagerClient extends SignalEventSource {
    subscribeBasket(request: SubscribeRequest): void;
    unsubscribe(subscriptionId: number): void;
    updateAsset(subscriptionId: number, asset: AssetSnapshot): void;
    updatePosition(subscriptionId: number, position: Position): void;
    addInstrument(subscriptionId: number, instrument: string): void;
    removeInstrument(subscriptionId: number, instrument: string): void;
    updateConfig(subscriptionId: number, config: RuntimeConfig): void;
    scheduleWithdrawal(subscriptionId: number, withdrawal: WithdrawalRequest): void;
}
export declare class SignalsClient extends EventEmitter implements SignalsManagerClient {
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
    subscribeBasket(request: SubscribeRequest): void;
    updateAsset(subscriptionId: number, asset: AssetSnapshot): void;
    updatePosition(subscriptionId: number, position: Position): void;
    addInstrument(subscriptionId: number, instrument: string): void;
    removeInstrument(subscriptionId: number, instrument: string): void;
    updateConfig(subscriptionId: number, config: RuntimeConfig): void;
    scheduleWithdrawal(subscriptionId: number, withdrawal: WithdrawalRequest): void;
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
export declare class SignalsManager extends EventEmitter {
    private readonly client;
    private cfg;
    private subscriptionId;
    private readonly assetsByCurrency;
    private readonly positionsByKey;
    constructor(client: SignalsManagerClient, state?: SignalsManagerState, config?: SignalsManagerConfig);
    run(signal?: AbortSignal): Promise<void>;
    subscribe(): void;
    updateAsset(asset: AssetSnapshot): void;
    updatePosition(position: Position): void;
    addInstrument(instrument: string): void;
    removeInstrument(instrument: string): void;
    updateConfig(config: RuntimeConfig): void;
    scheduleWithdrawal(withdrawal: WithdrawalRequest): void;
    subscription(): number;
    assets(): AssetSnapshot[];
    positions(): Position[];
    availableOrderCash(currency: string): number;
    state(): SignalsManagerState;
    handleEvent(event: SignalsEvent): void;
    private acceptsEvent;
    private applyTPSLUpdate;
    private recordAsset;
    private recordPosition;
}
export declare function parseEvent(raw: string): SignalsEvent;
//# sourceMappingURL=index.d.ts.map