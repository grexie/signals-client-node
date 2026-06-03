import { EventEmitter } from "node:events";
import WebSocket from "ws";
const defaultWebSocketUrl = "wss://signals.grexie.com/ws";
export class SignalsClient extends EventEmitter {
    token;
    options;
    ws;
    queue = [];
    waiters = [];
    terminalError;
    /** Create a websocket client.
     * @param token Bearer token used for websocket authentication.
     * @param options Optional websocket URL, base URL, headers, and constructor overrides.
     */
    constructor(token, options = {}) {
        super();
        this.token = token;
        this.options = {
            url: options.url ?? websocketUrlFromBase(options.baseUrl) ?? defaultWebSocketUrl,
            headers: options.headers ?? {},
            WebSocketCtor: options.WebSocketCtor ?? WebSocket
        };
    }
    /** Open the websocket connection. */
    connect() {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            return Promise.resolve();
        }
        const headers = {
            ...this.options.headers,
            ...(this.token ? { Authorization: `Bearer ${this.token}` } : {})
        };
        this.terminalError = undefined;
        this.ws = new this.options.WebSocketCtor(this.options.url, { headers });
        this.ws.on("message", (data) => {
            const raw = data.toString();
            if (isIgnoredWebSocketMessage(raw))
                return;
            this.accept(parseEvent(raw));
        });
        this.ws.on("error", (error) => this.fail(error instanceof Error ? error : new Error(String(error))));
        this.ws.on("close", () => this.closeStreams(new Error("signals-client: websocket closed")));
        return new Promise((resolve, reject) => {
            this.ws?.once("open", () => resolve());
            this.ws?.once("error", reject);
        });
    }
    /** Close the websocket connection. */
    close() {
        this.ws?.close();
        this.ws = undefined;
    }
    /** Subscribe to a legacy single-instrument stream.
     * @param venue Venue code.
     * @param instrument Instrument symbol.
     */
    subscribe(venue, instrument) {
        this.send({ type: "subscribe", venue, instrument });
    }
    /** Subscribe to a server-managed router basket.
     * @param request Basket subscription request, including instruments, risk, assets, and positions.
     */
    subscribeBasket(request) {
        this.send({ type: "subscribe", ...request, risk: normalizeRiskConfig(request.risk ?? {}) });
    }
    /** Publish an account asset snapshot.
     * @param subscriptionId Server subscription id.
     * @param asset Asset snapshot to send.
     */
    updateAsset(subscriptionId, asset) {
        this.send({ type: "update-asset", subscriptionId, ...asset });
    }
    /** Publish a venue position snapshot.
     * @param subscriptionId Server subscription id.
     * @param position Position snapshot to send.
     */
    updatePosition(subscriptionId, position) {
        this.send({
            type: "update-position",
            subscriptionId,
            venue: position.venue,
            instrument: position.instrument,
            side: positionSide(position),
            status: position.status,
            size: Math.abs(position.size),
            entryPrice: position.entryPrice,
            markPrice: position.lastPrice,
            margin: position.margin,
            leverage: position.leverage,
            takeProfitPrice: position.takeProfitPrice,
            stopLossPrice: position.stopLossPrice
        });
    }
    /** Add an instrument to an existing basket subscription. */
    addInstrument(subscriptionId, instrument) {
        this.send({ type: "add-instrument", subscriptionId, instrument });
    }
    /** Remove an instrument from an existing basket subscription. */
    removeInstrument(subscriptionId, instrument) {
        this.send({ type: "remove-instrument", subscriptionId, instrument });
    }
    /** Send a runtime router config patch. */
    updateConfig(subscriptionId, config) {
        this.send({ type: "update-config", subscriptionId, ...config });
    }
    /** Schedule a withdrawal request for the router subscription. */
    scheduleWithdrawal(subscriptionId, withdrawal) {
        this.send({ type: "schedule-withdrawal", subscriptionId, ...withdrawal });
    }
    /** Unsubscribe by server subscription id. */
    unsubscribe(subscriptionId) {
        this.send({ type: "unsubscribe", subscriptionId });
    }
    /** Unsubscribe a legacy single-instrument stream. */
    unsubscribeInstrument(venue, instrument) {
        this.send({ type: "unsubscribe", venue, instrument });
    }
    /** Wait for the next typed websocket event. */
    receive(signal) {
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
            const waiter = { resolve, reject };
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
    /** Yield an independent stream of typed websocket events. */
    async *events(signal) {
        const queue = [];
        let wake;
        let terminalError = this.terminalError;
        let closed = !!this.terminalError;
        const notify = () => {
            wake?.();
            wake = undefined;
        };
        const onEvent = (event) => {
            queue.push(event);
            notify();
        };
        const onError = (error) => {
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
                    await new Promise((resolve, reject) => {
                        wake = resolve;
                        if (!signal)
                            return;
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
                    yield queue.shift();
                }
            }
        }
        finally {
            this.off("event", onEvent);
            this.off("client-error", onError);
            this.off("client-close", onClose);
        }
    }
    async *[Symbol.asyncIterator]() {
        yield* this.events();
    }
    send(payload) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            throw new Error("signals-client: websocket is not connected");
        }
        this.ws.send(JSON.stringify(payload));
    }
    accept(event) {
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
    fail(error) {
        this.terminalError = error;
        this.emit("client-error", error);
        this.rejectWaiters(error);
    }
    closeStreams(error) {
        this.terminalError = error;
        this.emit("client-close");
        this.rejectWaiters(error);
    }
    rejectWaiters(error) {
        while (this.waiters.length > 0) {
            const waiter = this.waiters.shift();
            waiter?.cleanup?.();
            waiter?.reject(error);
        }
    }
    removeWaiter(waiter) {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) {
            this.waiters.splice(index, 1);
        }
    }
}
export class SignalsManager extends EventEmitter {
    client;
    cfg;
    subscriptionId = 0;
    assetsByCurrency = new Map();
    positionsByKey = new Map();
    /** Create a basket manager.
     * @param client Transport used to talk to the Signals websocket.
     * @param state Optional durable state from a previous run.
     * @param config Basket subscription config.
     */
    constructor(client, state = {}, config = {}) {
        super();
        this.client = client;
        this.cfg = normalizeSignalsManagerConfig(config);
        for (const asset of state.assets ?? []) {
            this.recordAsset(asset);
        }
        for (const position of state.positions ?? []) {
            this.recordPosition(position);
        }
    }
    /** Subscribe, process events until the stream ends, then unsubscribe. */
    async run(signal) {
        let backoffMs = 1000;
        for (;;) {
            if (signal?.aborted)
                break;
            try {
                await this.connectIfSupported();
                this.subscriptionId = 0;
                this.subscribe();
                for await (const event of this.client.events(signal)) {
                    this.handleEvent(event);
                }
                if (!this.canReconnect() || signal?.aborted)
                    break;
                backoffMs = 1000;
            }
            catch (error) {
                if (signal?.aborted)
                    break;
                if (!this.canReconnect())
                    throw error;
            }
            this.subscriptionId = 0;
            await reconnectDelay(backoffMs, signal);
            backoffMs = Math.min(backoffMs * 2, 30_000);
        }
        if (this.subscriptionId > 0) {
            this.sendLive(() => this.client.unsubscribe(this.subscriptionId));
            this.subscriptionId = 0;
        }
    }
    /** Subscribe the configured basket and send current snapshots. */
    subscribe() {
        this.client.subscribeBasket({
            venue: this.cfg.venue,
            instruments: [...this.cfg.instruments],
            mode: this.cfg.mode || undefined,
            risk: this.cfg.risk,
            profitWithdrawRatio: this.cfg.profitWithdrawRatio || undefined,
            assets: this.assets(),
            positions: this.positions()
        });
    }
    /** Record and, once subscribed, send an account asset snapshot. */
    updateAsset(asset) {
        const next = this.recordAsset(asset);
        if (next && this.subscriptionId > 0) {
            this.sendLive(() => this.client.updateAsset(this.subscriptionId, next));
        }
    }
    /** Record and, once subscribed, send a venue position snapshot. */
    updatePosition(position) {
        const next = this.recordPosition(position);
        if (next && this.subscriptionId > 0) {
            this.sendLive(() => this.client.updatePosition(this.subscriptionId, next));
        }
    }
    /** Add an instrument locally and to the live subscription. */
    addInstrument(instrument) {
        const normalized = normalizeInstrument(instrument);
        if (!normalized)
            return;
        this.cfg.instruments = normalizeInstrumentList([...this.cfg.instruments, normalized]);
        if (this.subscriptionId > 0) {
            this.sendLive(() => this.client.addInstrument(this.subscriptionId, normalized));
        }
    }
    /** Remove an instrument locally and from the live subscription. */
    removeInstrument(instrument) {
        const normalized = normalizeInstrument(instrument);
        this.cfg.instruments = this.cfg.instruments.filter((current) => current !== normalized);
        if (this.subscriptionId > 0) {
            this.sendLive(() => this.client.removeInstrument(this.subscriptionId, normalized));
        }
    }
    /** Apply and optionally send a runtime router config patch. */
    updateConfig(config) {
        const runtime = normalizeRuntimeConfig(config);
        this.cfg.risk = applyRuntimeConfigToRisk(this.cfg.risk, runtime);
        this.cfg.profitWithdrawRatio = runtime.profitWithdrawRatio ?? 0;
        if (this.subscriptionId > 0) {
            this.sendLive(() => this.client.updateConfig(this.subscriptionId, runtime));
        }
    }
    /** Schedule a withdrawal through the live router subscription. */
    scheduleWithdrawal(withdrawal) {
        if (this.subscriptionId <= 0) {
            throw new Error("signals-client: basket is not subscribed");
        }
        this.client.scheduleWithdrawal(this.subscriptionId, {
            ...withdrawal,
            venue: normalizeVenue(withdrawal.venue || this.cfg.venue),
            currency: withdrawal.currency.trim().toUpperCase()
        });
    }
    /** Return the active server subscription id, or 0 before subscribe. */
    subscription() {
        return this.subscriptionId;
    }
    /** Return asset snapshots sorted by currency. */
    assets() {
        return [...this.assetsByCurrency.values()].sort((a, b) => a.currency.localeCompare(b.currency));
    }
    /** Return open position snapshots sorted by venue/instrument. */
    positions() {
        return [...this.positionsByKey.values()].sort((a, b) => positionKey(a.venue, a.instrument).localeCompare(positionKey(b.venue, b.instrument)));
    }
    /** Return available cash after applying the asset maxUsage cap. */
    availableOrderCash(currency) {
        const asset = this.assetsByCurrency.get(currency.trim().toUpperCase());
        return Math.max(0, asset?.available ?? 0) * clamp01(positiveOr(asset?.maxUsage, 1));
    }
    /** Return durable state suitable for restart hydration. */
    state() {
        return { assets: this.assets(), positions: this.positions() };
    }
    /** Apply one typed websocket event to the manager. */
    handleEvent(event) {
        if (!this.acceptsEvent(event))
            return;
        if (event.type === "subscribed" && event.subscriptionId > 0) {
            this.subscriptionId = event.subscriptionId;
            for (const asset of this.assets()) {
                this.sendLive(() => this.client.updateAsset(this.subscriptionId, asset));
            }
            for (const position of this.positions()) {
                this.sendLive(() => this.client.updatePosition(this.subscriptionId, position));
            }
        }
        else if (event.type === "unsubscribed" && event.subscriptionId === this.subscriptionId) {
            this.subscriptionId = 0;
        }
        else if (event.type === "update-tpsl") {
            this.applyTPSLUpdate(event);
            this.emit("protection", event);
        }
        else if (event.type === "create-market-order") {
            this.emit("intent", event);
        }
        else if (event.type === "withdraw") {
            this.emit("withdrawal", event);
        }
        else if (event.type === "backtest") {
            this.emit("backtest", event);
        }
        else if (event.type === "info") {
            this.emit("message", event);
        }
        else if (event.type === "error") {
            this.emit("manager-error", event);
        }
        this.emit("event", event);
    }
    acceptsEvent(event) {
        const subscriptionId = eventSubscriptionId(event);
        if (this.subscriptionId > 0 && subscriptionId > 0) {
            return subscriptionId === this.subscriptionId;
        }
        if (event.type === "subscribed") {
            return normalizeVenue(event.venue) === this.cfg.venue && (!event.instrument || this.cfg.instruments.includes(normalizeInstrument(event.instrument)));
        }
        if ("venue" in event && "instrument" in event) {
            return instrumentInConfig(this.cfg, event.venue, event.instrument);
        }
        return true;
    }
    applyTPSLUpdate(event) {
        const key = positionKey(event.venue || this.cfg.venue, event.instrument);
        const existing = this.positionsByKey.get(key);
        if (!existing)
            return;
        this.positionsByKey.set(key, {
            ...existing,
            takeProfit: event.takeProfit ?? existing.takeProfit,
            stopLoss: event.stopLoss ?? existing.stopLoss,
            takeProfitPrice: event.takeProfitPrice ?? existing.takeProfitPrice,
            stopLossPrice: event.stopLossPrice ?? existing.stopLossPrice
        });
    }
    recordAsset(asset) {
        const currency = asset.currency?.trim().toUpperCase();
        if (!currency)
            return undefined;
        const next = {
            ...asset,
            venue: normalizeVenue(asset.venue || this.cfg.venue),
            currency,
            maxUsage: clamp01(positiveOr(asset.maxUsage, 1)),
            updatedAt: asset.updatedAt ?? new Date()
        };
        this.assetsByCurrency.set(currency, next);
        return next;
    }
    recordPosition(position) {
        const instrument = normalizeInstrument(position.instrument);
        if (!instrument)
            return undefined;
        const next = {
            ...position,
            venue: normalizeVenue(position.venue || this.cfg.venue),
            instrument,
            status: (position.status || (Math.abs(position.size) > 1e-9 ? "open" : "closed")).trim().toLowerCase(),
            lastPrice: positiveOr(position.lastPrice, position.entryPrice, 0)
        };
        const key = positionKey(next.venue, next.instrument);
        if (next.status === "closed" || Math.abs(next.size) <= 1e-9) {
            this.positionsByKey.delete(key);
        }
        else {
            this.positionsByKey.set(key, next);
        }
        return next;
    }
    canReconnect() {
        return typeof this.client.connect === "function";
    }
    async connectIfSupported() {
        if (this.client.connect) {
            await this.client.connect();
        }
    }
    sendLive(send) {
        try {
            send();
        }
        catch (error) {
            if (!this.canReconnect()) {
                throw error;
            }
        }
    }
}
/** Parse one raw websocket JSON message into a typed event. */
export function parseEvent(raw) {
    const msg = JSON.parse(raw);
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
        case "basket_updated":
            return {
                type: "basket_updated",
                subscriptionId: Number(msg.subscriptionId ?? 0),
                venue: msg.venue,
                basketId: msg.basketId,
                message: msg.message
            };
        case "order_router_forwarded":
            return {
                type: "order_router_forwarded",
                subscriptionId: Number(msg.subscriptionId ?? 0),
                venue: msg.venue,
                basketId: msg.basketId,
                message: msg.message
            };
        case "info":
            return {
                type: "info",
                subscriptionId: Number(msg.subscriptionId ?? 0),
                venue: msg.venue ?? "",
                instrument: msg.instrument ?? "",
                level: normalizeInfoLevel(msg.level),
                stage: msg.stage ?? "",
                message: msg.message ?? "",
                timestamp: msg.timestamp,
                replay: msg.replay,
                replayedAt: msg.replayedAt
            };
        case "backtest":
            return {
                type: "backtest",
                subscriptionId: Number(msg.subscriptionId ?? 0),
                venue: msg.venue ?? "",
                instrument: msg.instrument ?? "",
                backtest: msg.backtest ?? {},
                timestamp: msg.timestamp
            };
        case "signal": {
            const signal = { ...(msg.signal ?? {}) };
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
        case "create-market-order":
            return {
                type: "create-market-order",
                subscriptionId: Number(msg.subscriptionId ?? 0),
                intentId: msg.intentId,
                action: msg.action,
                reason: msg.reason,
                venue: msg.venue,
                instrument: msg.instrument ?? "",
                side: msg.side ?? "buy",
                orderType: msg.orderType,
                contractSize: msg.contractSize,
                margin: msg.margin,
                leverage: msg.leverage,
                confidence: msg.confidence,
                reduceOnly: msg.reduceOnly,
                takeProfitPrice: msg.takeProfitPrice,
                stopLossPrice: msg.stopLossPrice,
                takeProfit: msg.takeProfit,
                stopLoss: msg.stopLoss,
                timestamp: msg.timestamp
            };
        case "update-tpsl":
            return {
                type: "update-tpsl",
                subscriptionId: Number(msg.subscriptionId ?? 0),
                intentId: msg.intentId,
                venue: msg.venue,
                instrument: msg.instrument ?? "",
                side: msg.side ?? "buy",
                takeProfitPrice: msg.takeProfitPrice,
                stopLossPrice: msg.stopLossPrice,
                takeProfit: msg.takeProfit,
                stopLoss: msg.stopLoss,
                timestamp: msg.timestamp
            };
        case "withdraw":
            return {
                type: "withdraw",
                subscriptionId: Number(msg.subscriptionId ?? 0),
                intentId: msg.intentId,
                venue: msg.venue,
                currency: msg.currency ?? "",
                amount: msg.amount ?? 0,
                timestamp: msg.timestamp
            };
        case "error":
            return { type: "error", code: msg.code, message: msg.message };
        default:
            throw new Error(`signals-client: unsupported websocket event type ${String(msg.type)}`);
    }
}
function normalizeInfoLevel(value) {
    const level = typeof value === "string" ? value.trim().toLowerCase() : "";
    if (level === "error" || level === "warn" || level === "debug") {
        return level;
    }
    return "info";
}
function isIgnoredWebSocketMessage(raw) {
    try {
        return JSON.parse(raw).type === "basket_state";
    }
    catch {
        return false;
    }
}
function websocketUrlFromBase(baseUrl) {
    if (!baseUrl)
        return undefined;
    const url = new URL(baseUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.pathname = "/ws";
    url.search = "";
    url.hash = "";
    return url.toString();
}
function reconnectDelay(ms, signal) {
    if (signal?.aborted) {
        return Promise.resolve();
    }
    return new Promise((resolve) => {
        const timeout = setTimeout(resolve, ms);
        if (!signal)
            return;
        signal.addEventListener("abort", () => {
            clearTimeout(timeout);
            resolve();
        }, { once: true });
    });
}
function normalizeSignalsManagerConfig(config) {
    return {
        venue: normalizeVenue(config.venue || "okx"),
        instruments: normalizeInstrumentList(config.instruments ?? []),
        mode: config.mode?.trim() ?? "",
        risk: normalizeRiskConfig(config.risk ?? {}),
        profitWithdrawRatio: clamp01(config.profitWithdrawRatio ?? 0)
    };
}
function normalizeRiskConfig(risk) {
    const maxLeverage = Math.max(0, risk.maxLeverage ?? 0);
    return {
        maxMarginRatio: clamp01(positiveOr(risk.maxMarginRatio, 1)),
        minLotHaircutRatio: Math.max(0, finiteOr(risk.minLotHaircutRatio, 0)),
        maxConcurrentPositions: Math.max(0, Math.trunc(finiteOr(risk.maxConcurrentPositions, 0))),
        maxDrawdown: Math.max(0, finiteOr(risk.maxDrawdown, 0)),
        switchBuffer: Math.max(0, finiteOr(risk.switchBuffer, 0)),
        minLeverage: maxLeverage > 0 ? Math.min(Math.max(0, finiteOr(risk.minLeverage, 0)), maxLeverage) : Math.max(0, finiteOr(risk.minLeverage, 0)),
        maxLeverage,
        profitWithdrawRatio: clamp01(risk.profitWithdrawRatio ?? 0)
    };
}
function normalizeRuntimeConfig(config) {
    const maxLeverage = Math.max(0, finiteOr(config.maxLeverage, 0));
    return {
        maxMarginRatio: clamp01(config.maxMarginRatio ?? 0),
        minLotHaircutRatio: Math.max(0, finiteOr(config.minLotHaircutRatio, 0)),
        maxConcurrentPositions: Math.max(0, Math.trunc(finiteOr(config.maxConcurrentPositions, 0))),
        maxDrawdown: Math.max(0, finiteOr(config.maxDrawdown, 0)),
        switchBuffer: Math.max(0, finiteOr(config.switchBuffer, 0)),
        minLeverage: maxLeverage > 0 ? Math.min(Math.max(0, finiteOr(config.minLeverage, 0)), maxLeverage) : Math.max(0, finiteOr(config.minLeverage, 0)),
        maxLeverage,
        profitWithdrawRatio: clamp01(config.profitWithdrawRatio ?? 0)
    };
}
function applyRuntimeConfigToRisk(risk, config) {
    return normalizeRiskConfig({
        ...risk,
        ...(config.maxMarginRatio && config.maxMarginRatio > 0 ? { maxMarginRatio: config.maxMarginRatio } : {}),
        ...(config.minLotHaircutRatio && config.minLotHaircutRatio > 0 ? { minLotHaircutRatio: config.minLotHaircutRatio } : {}),
        ...(config.maxConcurrentPositions && config.maxConcurrentPositions > 0 ? { maxConcurrentPositions: config.maxConcurrentPositions } : {}),
        ...(config.maxDrawdown && config.maxDrawdown > 0 ? { maxDrawdown: config.maxDrawdown } : {}),
        ...(config.switchBuffer && config.switchBuffer > 0 ? { switchBuffer: config.switchBuffer } : {}),
        ...(config.minLeverage && config.minLeverage > 0 ? { minLeverage: config.minLeverage } : {}),
        ...(config.maxLeverage && config.maxLeverage > 0 ? { maxLeverage: config.maxLeverage } : {}),
        profitWithdrawRatio: config.profitWithdrawRatio ?? 0
    });
}
function normalizeVenue(venue) {
    return (venue || "okx").trim().toLowerCase();
}
function normalizeInstrument(instrument) {
    return (instrument || "").trim().toUpperCase();
}
function normalizeInstrumentList(instruments) {
    return [...new Set(instruments.map(normalizeInstrument).filter(Boolean))].sort();
}
function instrumentInConfig(config, venue, instrument) {
    return normalizeVenue(venue) === config.venue && (!instrument || config.instruments.includes(normalizeInstrument(instrument)));
}
function eventSubscriptionId(event) {
    return "subscriptionId" in event ? Number(event.subscriptionId ?? 0) : 0;
}
function positionKey(venue, instrument) {
    return `${normalizeVenue(venue)}:${normalizeInstrument(instrument)}`;
}
function positionSide(position) {
    if (position.size < 0)
        return "sell";
    if (position.size > 0)
        return "buy";
    return "";
}
function clamp01(value) {
    if (!Number.isFinite(value))
        return 0;
    return Math.max(0, Math.min(1, value));
}
function positiveOr(...values) {
    for (const value of values) {
        if (value !== undefined && Number.isFinite(value) && value > 0)
            return value;
    }
    return 0;
}
function finiteOr(value, fallback) {
    return value !== undefined && Number.isFinite(value) ? value : fallback;
}
//# sourceMappingURL=index.js.map