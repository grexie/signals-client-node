import { EventEmitter } from "node:events";
import WebSocket from "ws";
export type SignalsWebSocketToken = string;
export type Side = "buy" | "sell";
/** One timeframe contribution to an aggregate public signal. */
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
/** Public signal payload emitted by the Grexie Signals websocket. */
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
/** Router risk settings sent when subscribing to a basket. */
export interface RiskConfig {
    /** Fraction of total cash the router may reserve for active positions. Defaults to 1. */
    maxMarginRatio?: number;
    /** Extra cash buffer applied to lot margin and fees before orders are allowed. Defaults to 0. */
    minLotHaircutRatio?: number;
    /** Maximum simultaneous active positions; zero leaves it unset. */
    maxConcurrentPositions?: number;
    /** Optional drawdown guard; zero leaves it unset. */
    maxDrawdown?: number;
    /** Score buffer required before switching instruments. */
    switchBuffer?: number;
    /** Minimum leverage the router may request; zero leaves it unset. */
    minLeverage?: number;
    /** Maximum leverage the router may request; zero leaves it unset. */
    maxLeverage?: number;
    /** Fraction of profits eligible for withdrawal events. */
    profitWithdrawRatio?: number;
}
/** Runtime router risk patch sent after a basket has subscribed. */
export interface RuntimeConfig {
    maxMarginRatio?: number;
    minLotHaircutRatio?: number;
    maxConcurrentPositions?: number;
    maxDrawdown?: number;
    switchBuffer?: number;
    minLeverage?: number;
    maxLeverage?: number;
    profitWithdrawRatio?: number;
}
/** Account state for one settlement currency. */
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
/** Current venue position snapshot for one instrument. */
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
/** Basket subscription request sent to the server-managed router. */
export interface SubscribeRequest {
    venue: string;
    instruments: string[];
    mode?: string;
    risk?: RiskConfig;
    profitWithdrawRatio?: number;
    assets?: AssetSnapshot[];
    positions?: Position[];
}
/** Withdrawal request scheduled against the router basket. */
export interface WithdrawalRequest {
    venue?: string;
    currency: string;
    amount: number;
    reason?: string;
}
/** Configuration for one SignalsManager basket. */
export interface SignalsManagerConfig {
    venue?: string;
    instruments?: string[];
    mode?: string;
    risk?: RiskConfig;
    profitWithdrawRatio?: number;
}
/** Durable SignalsManager state for restart hydration. */
export interface SignalsManagerState {
    assets?: AssetSnapshot[];
    positions?: Position[];
}
export type Intent = CreateMarketOrderEvent;
/** Transport contract used by SignalsManager. */
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
    /** Create a websocket client.
     * @param token Bearer token used for websocket authentication.
     * @param options Optional websocket URL, base URL, headers, and constructor overrides.
     */
    constructor(token: SignalsWebSocketToken, options?: SignalsClientOptions);
    /** Open the websocket connection. */
    connect(): Promise<void>;
    /** Close the websocket connection. */
    close(): void;
    /** Subscribe to a legacy single-instrument stream.
     * @param venue Venue code.
     * @param instrument Instrument symbol.
     */
    subscribe(venue: string, instrument: string): void;
    /** Subscribe to a server-managed router basket.
     * @param request Basket subscription request, including instruments, risk, assets, and positions.
     */
    subscribeBasket(request: SubscribeRequest): void;
    /** Publish an account asset snapshot.
     * @param subscriptionId Server subscription id.
     * @param asset Asset snapshot to send.
     */
    updateAsset(subscriptionId: number, asset: AssetSnapshot): void;
    /** Publish a venue position snapshot.
     * @param subscriptionId Server subscription id.
     * @param position Position snapshot to send.
     */
    updatePosition(subscriptionId: number, position: Position): void;
    /** Add an instrument to an existing basket subscription. */
    addInstrument(subscriptionId: number, instrument: string): void;
    /** Remove an instrument from an existing basket subscription. */
    removeInstrument(subscriptionId: number, instrument: string): void;
    /** Send a runtime router config patch. */
    updateConfig(subscriptionId: number, config: RuntimeConfig): void;
    /** Schedule a withdrawal request for the router subscription. */
    scheduleWithdrawal(subscriptionId: number, withdrawal: WithdrawalRequest): void;
    /** Unsubscribe by server subscription id. */
    unsubscribe(subscriptionId: number): void;
    /** Unsubscribe a legacy single-instrument stream. */
    unsubscribeInstrument(venue: string, instrument: string): void;
    /** Wait for the next typed websocket event. */
    receive(signal?: AbortSignal): Promise<SignalsEvent>;
    /** Yield an independent stream of typed websocket events. */
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
    /** Create a basket manager.
     * @param client Transport used to talk to the Signals websocket.
     * @param state Optional durable state from a previous run.
     * @param config Basket subscription config.
     */
    constructor(client: SignalsManagerClient, state?: SignalsManagerState, config?: SignalsManagerConfig);
    /** Subscribe, process events until the stream ends, then unsubscribe. */
    run(signal?: AbortSignal): Promise<void>;
    /** Subscribe the configured basket and send current snapshots. */
    subscribe(): void;
    /** Record and, once subscribed, send an account asset snapshot. */
    updateAsset(asset: AssetSnapshot): void;
    /** Record and, once subscribed, send a venue position snapshot. */
    updatePosition(position: Position): void;
    /** Add an instrument locally and to the live subscription. */
    addInstrument(instrument: string): void;
    /** Remove an instrument locally and from the live subscription. */
    removeInstrument(instrument: string): void;
    /** Apply and optionally send a runtime router config patch. */
    updateConfig(config: RuntimeConfig): void;
    /** Schedule a withdrawal through the live router subscription. */
    scheduleWithdrawal(withdrawal: WithdrawalRequest): void;
    /** Return the active server subscription id, or 0 before subscribe. */
    subscription(): number;
    /** Return asset snapshots sorted by currency. */
    assets(): AssetSnapshot[];
    /** Return open position snapshots sorted by venue/instrument. */
    positions(): Position[];
    /** Return available cash after applying the asset maxUsage cap. */
    availableOrderCash(currency: string): number;
    /** Return durable state suitable for restart hydration. */
    state(): SignalsManagerState;
    /** Apply one typed websocket event to the manager. */
    handleEvent(event: SignalsEvent): void;
    private acceptsEvent;
    private applyTPSLUpdate;
    private recordAsset;
    private recordPosition;
}
/** Parse one raw websocket JSON message into a typed event. */
export declare function parseEvent(raw: string): SignalsEvent;
//# sourceMappingURL=index.d.ts.map