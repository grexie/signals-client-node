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
    constructor(token, options = {}) {
        super();
        this.token = token;
        this.options = {
            url: options.url ?? websocketUrlFromBase(options.baseUrl) ?? defaultWebSocketUrl,
            headers: options.headers ?? {},
            WebSocketCtor: options.WebSocketCtor ?? WebSocket
        };
    }
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
        this.ws.on("message", (data) => this.accept(parseEvent(data.toString())));
        this.ws.on("error", (error) => this.fail(error instanceof Error ? error : new Error(String(error))));
        this.ws.on("close", () => this.closeStreams(new Error("signals-client: websocket closed")));
        return new Promise((resolve, reject) => {
            this.ws?.once("open", () => resolve());
            this.ws?.once("error", reject);
        });
    }
    close() {
        this.ws?.close();
        this.ws = undefined;
    }
    subscribe(venue, instrument) {
        this.send({ type: "subscribe", venue, instrument });
    }
    unsubscribe(subscriptionId) {
        this.send({ type: "unsubscribe", subscriptionId });
    }
    unsubscribeInstrument(venue, instrument) {
        this.send({ type: "unsubscribe", venue, instrument });
    }
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
        case "error":
            return { type: "error", code: msg.code, message: msg.message };
        default:
            throw new Error(`signals-client: unsupported websocket event type ${String(msg.type)}`);
    }
}
export class AssetManager {
    assetsByCurrency = new Map();
    updateAsset(snapshot) {
        if (!snapshot.currency)
            return;
        this.assetsByCurrency.set(snapshot.currency, {
            currency: snapshot.currency,
            cash: snapshot.cash ?? 0,
            available: snapshot.available ?? 0,
            used: snapshot.used ?? 0,
            equity: snapshot.equity ?? 0,
            updatedAt: snapshot.updatedAt ?? new Date()
        });
    }
    asset(currency) {
        return this.assetsByCurrency.get(currency);
    }
    assets() {
        return [...this.assetsByCurrency.values()].sort((a, b) => a.currency.localeCompare(b.currency));
    }
}
export class InstrumentManager {
    instrumentsByKey = new Map();
    updateInstrument(metadata) {
        if (!metadata.venue || !metadata.instrument)
            return;
        this.instrumentsByKey.set(positionKey(metadata.venue, metadata.instrument), {
            venue: metadata.venue,
            instrument: metadata.instrument,
            settlementCurrency: metadata.settlementCurrency ?? "USDT",
            lotSize: metadata.lotSize ?? 0,
            minSize: metadata.minSize ?? 0,
            tickSize: metadata.tickSize ?? 0,
            contractValue: metadata.contractValue ?? 0,
            contractMultiplier: metadata.contractMultiplier ?? 0,
            maxLeverage: metadata.maxLeverage ?? 0
        });
    }
    instrument(venue, instrument) {
        return this.instrumentsByKey.get(positionKey(venue, instrument));
    }
    instruments() {
        return [...this.instrumentsByKey.values()].sort((a, b) => (a.venue === b.venue ? a.instrument.localeCompare(b.instrument) : a.venue.localeCompare(b.venue)));
    }
}
const productionDefaults = {
    makerFeeRate: 0.0002,
    takerFeeRate: 0.0005,
    maxMarginRatio: 1,
    positionSize: 0,
    minExpectedEdge: 0.0045,
    minOrderDelta: 0.2,
    minPositionSizeRatio: 0.01,
    rebalanceIntervalMs: 6 * 60 * 60 * 1000,
    minLeverage: 1,
    maxLeverage: 1,
    availableMarginBuffer: 0.1,
    executableMarginBuffer: 0.001,
    assetManager: undefined,
    instrumentManager: undefined
};
export function productionPositionManagerConfig(overrides = {}) {
    const config = { ...productionDefaults, ...overrides, instruments: overrides.instruments ?? {} };
    if (overrides.maxMarginRatio === undefined && overrides.positionSize !== undefined && overrides.positionSize > 0 && overrides.positionSize <= 1) {
        config.maxMarginRatio = overrides.positionSize;
    }
    return config;
}
export class PositionManager {
    client;
    config;
    assets;
    instrumentMetadata;
    positionsByKey = new Map();
    closed = [];
    constructor(client, config = productionPositionManagerConfig()) {
        this.client = client;
        this.config = normalizeConfig(config);
        this.assets = this.config.assetManager;
        this.instrumentMetadata = this.config.instrumentManager;
        this.hydrateState(this.config.initialState);
    }
    assetManager() {
        return this.assets;
    }
    instrumentManager() {
        return this.instrumentMetadata;
    }
    updateConfig(config) {
        const next = normalizeConfig({
            ...config,
            instruments: config.instruments ?? { ...this.config.instruments },
            assetManager: config.assetManager ?? this.assets,
            instrumentManager: config.instrumentManager ?? this.instrumentMetadata,
            initialState: undefined,
            persist: config.persist ?? this.config.persist
        });
        this.config = next;
        this.assets = next.assetManager;
        this.instrumentMetadata = next.instrumentManager;
    }
    async *run(signal) {
        if (!this.client) {
            throw new Error("signals-client: PositionManager has no SignalsClient");
        }
        for await (const event of this.client.events(signal)) {
            for (const order of this.handleEvent(event)) {
                yield order;
            }
        }
    }
    addPosition(position) {
        const key = positionKey(position.venue, position.instrument);
        this.positionsByKey.set(key, { ...position, leverage: position.leverage ?? this.minLeverage(key) });
        this.persist();
    }
    updatePosition(position) {
        this.addPosition(position);
    }
    replacePositions(positions) {
        this.positionsByKey.clear();
        for (const position of positions) {
            if (!position.venue || !position.instrument || Math.abs(position.size) <= 1e-9)
                continue;
            const key = positionKey(position.venue, position.instrument);
            this.positionsByKey.set(key, { ...position, leverage: position.leverage ?? this.minLeverage(key) });
        }
        this.persist();
    }
    closePosition(venue, instrument) {
        const key = positionKey(venue, instrument);
        const position = this.positionsByKey.get(key);
        if (!position || Math.abs(position.size) <= 1e-9)
            return [];
        const now = new Date();
        const delta = -position.size;
        const order = this.orderForDelta(key, position, delta, 0, undefined, "closing", now, position.confidence);
        if (!this.orderMeetsInstrumentMinimum(order))
            return [];
        this.applyDelta(key, position, order.sizeDelta, position.lastPrice ?? position.entryPrice, this.takerFeeRate(key), now, "closing");
        this.persist();
        return [order];
    }
    positions() {
        return [...this.positionsByKey.values()].map((position) => ({ ...position }));
    }
    closedTrades() {
        return this.closed.map((trade) => ({ ...trade }));
    }
    state() {
        return {
            positions: this.positions(),
            closedTrades: this.closedTrades()
        };
    }
    stats() {
        const stats = {
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
            const equity = positiveOr(asset?.equity, (asset?.cash ?? 0) + (asset?.used ?? 0), asset?.cash, 1);
            const price = roundToTick(position.lastPrice ?? position.entryPrice ?? 0, metadata.tickSize);
            const contractNotional = instrumentContractNotional(price, metadata);
            const quantity = contractNotional > 0 ? roundDownToStep(Math.abs(position.size), metadata.lotSize) : Math.abs(position.size);
            const notional = quantity * contractNotional;
            const realized = position.realizedPnl ?? 0;
            const unrealized = this.positionUnrealizedPnl(key, position);
            const fees = position.fees ?? 0;
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
                realizedPnlPercent: ratioOrZero(position.realizedPnl ?? 0, equity),
                unrealizedPnlPercent: ratioOrZero(unrealized, equity),
                totalPnlPercent: ratioOrZero((position.realizedPnl ?? 0) + unrealized, equity),
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
        if (stats.equity <= 0)
            stats.equity = 1;
        stats.realizedPnlPercent = stats.realizedPnl / stats.equity;
        stats.unrealizedPnlPercent = stats.unrealizedPnl / stats.equity;
        stats.totalPnlPercent = (stats.realizedPnl + stats.unrealizedPnl) / stats.equity;
        return stats;
    }
    updatePrice(venue, instrument, price, timestamp = new Date()) {
        if (price <= 0)
            throw new Error("signals-client: price must be positive");
        const key = positionKey(venue, instrument);
        const position = this.positionsByKey.get(key);
        if (!position)
            return [];
        position.lastPrice = price;
        updateExcursion(position);
        const reason = exitReason(position, price);
        if (!reason) {
            this.persist();
            return [];
        }
        const feeRate = reason === "take_profit" ? this.makerFeeRate(key) : this.takerFeeRate(key);
        const delta = -position.size;
        const order = this.orderForDelta(key, position, delta, 0, undefined, reason, timestamp, position.confidence);
        order.feeRate = feeRate;
        order.estimatedFee = feeValueForNotional(order.notional, feeRate);
        order.estimatedFeeValue = order.notional * feeRate;
        if (!this.orderMeetsInstrumentMinimum(order)) {
            this.persist();
            return [];
        }
        this.applyDelta(key, position, order.sizeDelta, price, feeRate, timestamp, reason);
        this.persist();
        return [order];
    }
    handleEvent(event) {
        if (event.type !== "signal")
            return [];
        if (event.replay)
            return [];
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
    handleSignal(signal) {
        if (!signal.venue || !signal.instrument) {
            throw new Error("signals-client: signal venue and instrument are required");
        }
        if (!this.instrumentMetadata.instrument(signal.venue, signal.instrument))
            return [];
        const now = signalDate(signal.timestamp);
        const key = positionKey(signal.venue, signal.instrument);
        const targetSign = sideSign(signal.side);
        const targetConfidence = clamp01(signal.confidence);
        if (!targetSign || targetConfidence <= 0)
            return [];
        const edge = feeAdjustedExpectedEdge(signal, this.takerFeeRate(key));
        if (this.config.minExpectedEdge > 0 && edge < this.config.minExpectedEdge)
            return [];
        const trailing = this.trailingConfigForSignal(key, signal);
        let position = this.positionsByKey.get(key);
        const portfolioBudget = this.maxPortfolioMarginBudget();
        const minOrderDelta = this.effectiveMinOrderDelta();
        if (!position || Math.abs(position.size) <= 1e-9) {
            if (portfolioBudget < minOrderDelta || !this.meetsMinimumPositionSize(portfolioBudget))
                return [];
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
            resetExcursion(position);
            this.positionsByKey.set(key, position);
        }
        else {
            const isFlip = sign(position.size) !== 0 && sign(position.size) !== targetSign;
            const belowMinimum = !this.meetsMinimumPositionSize(this.positionMargin(key, position));
            if (!isFlip && !belowMinimum && this.config.rebalanceIntervalMs > 0 && position.lastSignalAt) {
                if (now.getTime() < position.lastSignalAt.getTime() + this.config.rebalanceIntervalMs)
                    return [];
            }
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
        }
        else {
            position.takeProfit = blendRisk(position.takeProfit, signal.takeProfit, 0.5);
            position.stopLoss = blendRisk(position.stopLoss, signal.stopLoss, 0.5);
        }
        if (trailing.activation > 0 && trailing.distance > 0) {
            position.trailingStopActivation = trailing.activation;
            position.trailingStopDistance = trailing.distance;
            position.trailingStopMinProfit = trailing.minProfit;
        }
        position.leverage = this.selectLeverage(key, targetConfidence, edge, signal.score);
        const orders = this.rebalance(now, { [key]: targetSign }, {
            [key]: {
                confidence: targetConfidence,
                score: signal.score,
                expectedEdge: edge,
                takeProfit: signal.takeProfit,
                stopLoss: signal.stopLoss,
                trailingStopActivation: trailing.activation,
                trailingStopDistance: trailing.distance,
                trailingStopMinProfit: trailing.minProfit
            }
        });
        this.persist();
        return orders;
    }
    hydrateState(state) {
        if (!state)
            return;
        this.positionsByKey.clear();
        for (const position of state.positions ?? []) {
            if (!position.venue || !position.instrument || Math.abs(position.size) <= 1e-9)
                continue;
            const key = positionKey(position.venue, position.instrument);
            this.positionsByKey.set(key, { ...position, leverage: position.leverage ?? this.minLeverage(key) });
        }
        this.closed.splice(0, this.closed.length, ...((state.closedTrades ?? []).map((trade) => ({ ...trade }))));
    }
    persist() {
        this.config.persist?.(this.state());
    }
    rebalance(now, sideOverrides, contexts) {
        const portfolioBudget = this.maxPortfolioMarginBudget();
        if (portfolioBudget <= 0 || this.positionsByKey.size === 0)
            return [];
        const weights = new Map();
        const sides = new Map();
        for (const [key, position] of this.positionsByKey) {
            if (Math.abs(position.size) <= 1e-9 && position.confidence <= 0)
                continue;
            const hasOverride = Object.hasOwn(sideOverrides, key);
            let weight = clamp01(position.confidence);
            if (!hasOverride && weight <= 0)
                weight = clamp01(this.positionMargin(key, position) / portfolioBudget);
            let side = sign(position.size);
            if (hasOverride)
                side = sideOverrides[key] ?? side;
            weights.set(key, weight);
            sides.set(key, side);
        }
        const targets = this.allocateTargetSizes([...this.positionsByKey.keys()].sort(), weights, sides, contexts);
        const reductions = [];
        const openings = [];
        for (const key of [...this.positionsByKey.keys()].sort()) {
            const position = this.positionsByKey.get(key);
            if (!position)
                continue;
            const weight = weights.get(key) ?? 0;
            let targetSize = targets.get(key) ?? 0;
            if (Math.abs(position.size) > 1e-9 && !this.meetsMinimumPositionSize(this.positionMargin(key, position))) {
                targetSize = 0;
            }
            else if (targetSize !== 0 && !this.meetsMinimumPositionSize(this.marginForQuantity(key, position, targetSize))) {
                if (Math.abs(position.size) <= 1e-9) {
                    position.confidence = weight;
                    continue;
                }
                targetSize = 0;
            }
            let delta = targetSize - position.size;
            if (isFlipTarget(position.size, targetSize))
                delta = -position.size;
            if (Math.abs(delta) <= 1e-9) {
                position.confidence = weight;
                continue;
            }
            const hasOverride = Object.hasOwn(sideOverrides, key);
            if (this.shouldSkipRebalanceDelta(key, position, targetSize, delta, now, hasOverride)) {
                position.confidence = weight;
                continue;
            }
            const context = contexts[key] ?? { confidence: position.confidence, expectedEdge: 0 };
            const candidate = {
                key,
                position,
                delta,
                weight,
                context,
                reason: orderReason(position, targetSize)
            };
            if (isExposureReduction(position.size, position.size + delta)) {
                reductions.push(candidate);
            }
            else {
                openings.push(candidate);
            }
        }
        if (reductions.length > 0)
            return this.materializeRebalanceOrders(reductions, now);
        return this.materializeRebalanceOrders(openings, now, new Map());
    }
    allocateTargetSizes(keys, weights, sides, contexts) {
        const targets = new Map();
        const portfolioBudget = this.maxPortfolioMarginBudget();
        if (portfolioBudget <= 0)
            return targets;
        const active = new Set(keys.filter((key) => (weights.get(key) ?? 0) > 1e-9 && (sides.get(key) ?? 0) !== 0));
        while (active.size > 0) {
            const totalWeight = [...active].reduce((sum, key) => sum + (weights.get(key) ?? 0), 0);
            if (totalWeight <= 1e-9)
                break;
            let dropped = "";
            let droppedWeight = Number.POSITIVE_INFINITY;
            for (const key of keys) {
                if (!active.has(key))
                    continue;
                const position = this.positionsByKey.get(key);
                if (!position)
                    continue;
                const desiredBudget = portfolioBudget * (weights.get(key) ?? 0) / totalWeight;
                if (this.executableAllocationForBudget(key, position, desiredBudget, contexts[key]).margin > 1e-9)
                    continue;
                const weight = weights.get(key) ?? 0;
                if (weight < droppedWeight || (Math.abs(weight - droppedWeight) <= 1e-9 && (dropped === "" || key < dropped))) {
                    dropped = key;
                    droppedWeight = weight;
                }
            }
            if (!dropped)
                break;
            active.delete(dropped);
        }
        if (active.size === 0)
            return targets;
        const totalWeight = [...active].reduce((sum, key) => sum + (weights.get(key) ?? 0), 0);
        if (totalWeight <= 1e-9)
            return targets;
        let allocated = 0;
        for (const key of keys) {
            if (!active.has(key))
                continue;
            const position = this.positionsByKey.get(key);
            if (!position)
                continue;
            const desiredBudget = portfolioBudget * (weights.get(key) ?? 0) / totalWeight;
            const executable = this.executableAllocationForBudget(key, position, desiredBudget, contexts[key]);
            if (executable.margin <= 1e-9)
                continue;
            if (!this.meetsMinimumPositionSize(executable.margin))
                continue;
            targets.set(key, (sides.get(key) ?? 0) * executable.quantity);
            allocated += executable.margin + executable.fee;
        }
        let free = portfolioBudget - allocated;
        if (free <= 1e-9)
            return targets;
        const priority = [...keys].sort((a, b) => {
            const diff = (weights.get(b) ?? 0) - (weights.get(a) ?? 0);
            return Math.abs(diff) <= 1e-9 ? a.localeCompare(b) : diff;
        });
        for (const key of priority) {
            if (!active.has(key) || free <= 1e-9)
                continue;
            const position = this.positionsByKey.get(key);
            if (!position)
                continue;
            const step = this.executableLotStepCost(key, position, contexts[key]);
            const stepCost = step.margin + step.fee;
            if (stepCost <= 1e-9) {
                const executable = this.executableAllocationForBudget(key, position, free, contexts[key]);
                if (executable.quantity > 1e-9 && this.meetsMinimumPositionSize(executable.margin)) {
                    targets.set(key, (targets.get(key) ?? 0) + (sides.get(key) ?? 0) * executable.quantity);
                }
                break;
            }
            const steps = Math.floor((free + 1e-9) / stepCost);
            if (steps <= 0)
                continue;
            const next = (targets.get(key) ?? 0) + (sides.get(key) ?? 0) * steps * step.quantity;
            const nextMargin = Math.abs(next) * step.margin / step.quantity;
            if (!this.meetsMinimumPositionSize(nextMargin))
                continue;
            targets.set(key, next);
            free -= steps * stepCost;
        }
        return targets;
    }
    materializeRebalanceOrders(candidates, now, openingExposureByCurrency) {
        const orders = [];
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
                delta = this.capOpeningDeltaToBudget(candidate.key, candidate.position, delta, candidate.context, available);
                if (Math.abs(delta) <= 1e-9) {
                    candidate.position.confidence = candidate.weight;
                    continue;
                }
            }
            const order = this.orderForDelta(candidate.key, candidate.position, delta, candidate.context.expectedEdge, candidate.context.score, candidate.reason, now, candidate.context.confidence);
            order.takeProfit = candidate.context.takeProfit;
            order.stopLoss = candidate.context.stopLoss;
            order.trailingStopActivation = candidate.context.trailingStopActivation;
            order.trailingStopDistance = candidate.context.trailingStopDistance;
            order.trailingStopMinProfit = candidate.context.trailingStopMinProfit;
            if (!this.orderMeetsInstrumentMinimum(order)) {
                candidate.position.confidence = candidate.weight;
                continue;
            }
            orders.push(order);
            if (openingExposureByCurrency && !isExposureReduction(order.previousSize, order.targetSize)) {
                openingExposureByCurrency.set(order.settlementCurrency, (openingExposureByCurrency.get(order.settlementCurrency) ?? 0) + orderBudgetCost(order));
            }
            this.applyDelta(candidate.key, candidate.position, order.sizeDelta, candidate.position.lastPrice ?? candidate.position.entryPrice, this.takerFeeRate(candidate.key), now, candidate.reason);
            const current = this.positionsByKey.get(candidate.key);
            if (current) {
                current.confidence = candidate.weight;
                if ((order.trailingStopActivation ?? 0) > 0 && (order.trailingStopDistance ?? 0) > 0) {
                    current.trailingStopActivation = order.trailingStopActivation;
                    current.trailingStopDistance = order.trailingStopDistance;
                    current.trailingStopMinProfit = order.trailingStopMinProfit;
                }
            }
        }
        return orders;
    }
    availableExposureBudget(currency) {
        const portfolioBudget = this.availablePortfolioBudget();
        const asset = this.assets.asset(currency);
        if (!asset)
            return portfolioBudget;
        if (asset.available <= 0)
            return 0;
        let budget = Math.max(0, asset.available);
        if (this.config.availableMarginBuffer > 0)
            budget *= 1 - this.config.availableMarginBuffer;
        return Math.min(budget, portfolioBudget);
    }
    availablePortfolioBudget() {
        const maxBudget = this.maxPortfolioMarginBudget();
        let used = 0;
        for (const [key, position] of this.positionsByKey) {
            used += this.positionMargin(key, position);
        }
        return Math.max(0, maxBudget - used);
    }
    maxPortfolioMarginBudget() {
        const capital = this.portfolioCapital();
        if (capital <= 0 || this.config.maxMarginRatio <= 0)
            return 0;
        return capital * this.config.maxMarginRatio;
    }
    portfolioCapital() {
        const capital = this.assets.assets().reduce((sum, asset) => sum + positiveOr(asset.equity, asset.cash + asset.used, asset.cash), 0);
        return capital > 0 ? capital : 1;
    }
    positionMargin(key, position) {
        if (Math.abs(position.size) <= 1e-9)
            return 0;
        return this.marginForQuantity(key, position, position.size);
    }
    marginForQuantity(key, position, quantity) {
        if (Math.abs(quantity) <= 1e-9)
            return 0;
        const metadata = this.instrumentFor(position.venue, position.instrument);
        const price = roundToTick(position.lastPrice ?? position.entryPrice ?? 0, metadata.tickSize);
        const contractNotional = instrumentContractNotional(price, metadata);
        const leverage = positiveOr(position.leverage, this.minLeverage(key), 1);
        if (contractNotional <= 0 || leverage <= 0)
            return 0;
        return Math.abs(quantity) * contractNotional / leverage;
    }
    positionUnrealizedPnl(key, position) {
        if (Math.abs(position.size) <= 1e-9 || !position.entryPrice || !position.lastPrice)
            return 0;
        return this.realizedGrossForQuantity(key, position, Math.abs(position.size), position.lastPrice);
    }
    realizedGrossForQuantity(key, position, quantity, exitPrice) {
        if (quantity <= 1e-9 || !position.entryPrice || !exitPrice || exitPrice <= 0)
            return 0;
        const metadata = this.instrumentFor(position.venue, position.instrument);
        const contractValue = positiveOr(metadata.contractValue, 1);
        const contractMultiplier = positiveOr(metadata.contractMultiplier, 1);
        const priceMove = position.size < 0 ? position.entryPrice - exitPrice : exitPrice - position.entryPrice;
        return priceMove * quantity * contractValue * contractMultiplier;
    }
    feeForQuantity(key, position, quantity, price, feeRate) {
        if (quantity <= 1e-9 || !price || price <= 0 || feeRate <= 0)
            return 0;
        const metadata = this.instrumentFor(position.venue, position.instrument);
        return quantity * instrumentContractNotional(price, metadata) * feeRate;
    }
    executableAllocationForBudget(key, position, budget, context) {
        if (budget <= 1e-9)
            return { quantity: 0, margin: 0, fee: 0 };
        const metadata = this.instrumentFor(position.venue, position.instrument);
        const price = roundToTick(position.lastPrice ?? position.entryPrice ?? 0, metadata.tickSize);
        const leverage = this.selectLeverage(key, context?.confidence ?? position.confidence, context?.expectedEdge ?? 0, context?.score);
        const contractNotional = instrumentContractNotional(price, metadata);
        if (contractNotional <= 0 || leverage <= 0)
            return { quantity: 0, margin: 0, fee: 0 };
        const feeRate = this.takerFeeRate(key);
        let maxMargin = budget;
        if (metadata.lotSize <= 0)
            maxMargin = budget / Math.max(1 + leverage * feeRate, 1);
        let quantity = roundDownToStep(maxMargin * leverage / contractNotional, metadata.lotSize);
        while (quantity > 1e-9) {
            if (metadata.minSize > 0 && quantity < metadata.minSize)
                return { quantity: 0, margin: 0, fee: 0 };
            const margin = quantity * contractNotional / leverage;
            const fee = quantity * contractNotional * feeRate;
            if (margin + fee <= budget + 1e-9)
                return { quantity, margin, fee };
            if (metadata.lotSize <= 0)
                return { quantity: 0, margin: 0, fee: 0 };
            quantity = roundDownToStep(quantity - metadata.lotSize, metadata.lotSize);
        }
        return { quantity: 0, margin: 0, fee: 0 };
    }
    executableLotStepCost(key, position, context) {
        const metadata = this.instrumentFor(position.venue, position.instrument);
        if (metadata.lotSize <= 0)
            return { quantity: 0, margin: 0, fee: 0 };
        const price = roundToTick(position.lastPrice ?? position.entryPrice ?? 0, metadata.tickSize);
        const leverage = this.selectLeverage(key, context?.confidence ?? position.confidence, context?.expectedEdge ?? 0, context?.score);
        const contractNotional = instrumentContractNotional(price, metadata);
        if (contractNotional <= 0 || leverage <= 0)
            return { quantity: 0, margin: 0, fee: 0 };
        return {
            quantity: metadata.lotSize,
            margin: metadata.lotSize * contractNotional / leverage,
            fee: metadata.lotSize * contractNotional * this.takerFeeRate(key)
        };
    }
    capOpeningDeltaToBudget(key, position, delta, context, budget) {
        if (Math.abs(delta) <= 1e-9 || budget <= 1e-9)
            return 0;
        const absDelta = Math.abs(delta);
        const executable = this.executableAllocationForBudget(key, position, budget, context);
        if (executable.margin <= 1e-9)
            return 0;
        if (!this.meetsMinimumPositionSize(executable.margin))
            return 0;
        if (executable.quantity < absDelta)
            return this.capExecutableDeltaWithBufferedCost(key, position, sign(delta) * executable.quantity, context, budget);
        const order = this.orderForDelta(key, position, delta, context.expectedEdge, context.score, "budget-check", new Date(), context.confidence);
        if (orderBudgetCost(order) > budget + 1e-9)
            return this.capExecutableDeltaWithBufferedCost(key, position, sign(delta) * executable.quantity, context, budget);
        return delta;
    }
    capExecutableDeltaWithBufferedCost(key, position, delta, context, budget) {
        if (Math.abs(delta) <= 1e-9 || budget <= 1e-9)
            return 0;
        const metadata = this.instrumentFor(position.venue, position.instrument);
        const quantityStep = metadata.lotSize > 0 ? metadata.lotSize : 0;
        let candidate = Math.abs(delta);
        while (candidate > 1e-9) {
            const order = this.orderForDelta(key, position, sign(delta) * candidate, context.expectedEdge, context.score, "budget-check", new Date(), context.confidence);
            if (orderBudgetCost(order) <= budget + 1e-9)
                return sign(delta) * candidate;
            if (quantityStep <= 1e-9)
                return this.capContinuousOpeningDeltaToBudget(key, position, delta, context, budget);
            candidate -= quantityStep;
        }
        return 0;
    }
    capContinuousOpeningDeltaToBudget(key, position, delta, context, budget) {
        if (Math.abs(delta) <= 1e-9 || budget <= 1e-9)
            return 0;
        let low = 0;
        let high = Math.abs(delta);
        for (let i = 0; i < 64; i++) {
            const mid = (low + high) / 2;
            if (mid <= 1e-9)
                break;
            const order = this.orderForDelta(key, position, sign(delta) * mid, context.expectedEdge, context.score, "budget-check", new Date(), context.confidence);
            if (orderBudgetCost(order) <= budget + 1e-9)
                low = mid;
            else
                high = mid;
        }
        return low <= 1e-9 ? 0 : sign(delta) * low;
    }
    shouldSkipRebalanceDelta(key, position, targetSize, delta, now, hasOverride) {
        const isClosing = Math.abs(targetSize) <= 1e-9 && Math.abs(position.size) > 1e-9;
        const isOpening = Math.abs(position.size) <= 1e-9 && Math.abs(targetSize) > 1e-9;
        const isFlip = Math.abs(position.size) > 1e-9 && Math.abs(targetSize) > 1e-9 && !sameSign(position.size, targetSize);
        if (isClosing || isOpening || isFlip)
            return false;
        if (this.effectiveMinOrderDelta() > 0 && this.marginForQuantity(key, position, delta) < this.effectiveMinOrderDelta())
            return true;
        if (!hasOverride && this.config.rebalanceIntervalMs > 0 && position.lastSignalAt) {
            return now.getTime() < position.lastSignalAt.getTime() + this.config.rebalanceIntervalMs;
        }
        return false;
    }
    orderForDelta(key, position, delta, edge, score, reason, now, confidence) {
        const feeRate = this.takerFeeRate(key);
        const leverage = this.selectLeverage(key, confidence, edge, score);
        position.leverage = leverage;
        const metadata = this.instrumentFor(position.venue, position.instrument);
        const price = roundToTick(position.lastPrice ?? position.entryPrice ?? 0, metadata.tickSize);
        const requestedAbsDelta = Math.abs(delta);
        const contractNotional = instrumentContractNotional(price, metadata);
        const closesToZero = Math.abs(position.size) > 1e-9 && Math.abs(position.size + delta) <= 1e-9;
        const quantity = contractNotional > 0 && !closesToZero ? roundDownToStep(requestedAbsDelta, metadata.lotSize) : requestedAbsDelta;
        const notional = quantity * contractNotional;
        const margin = leverage > 0 ? notional / leverage : 0;
        const executableDelta = sign(delta) * quantity;
        const reduceOnly = isExposureReduction(position.size, position.size + executableDelta);
        return {
            venue: position.venue,
            instrument: position.instrument,
            side: delta < 0 ? "sell" : "buy",
            reason,
            sizeDelta: executableDelta,
            previousSize: position.size,
            targetSize: position.size + executableDelta,
            price,
            confidence,
            score,
            expectedEdge: edge,
            feeRate,
            estimatedFee: feeValueForNotional(notional, feeRate),
            estimatedFeeValue: notional * feeRate,
            margin,
            quantity,
            notional,
            settlementCurrency: metadata.settlementCurrency,
            minSize: metadata.minSize,
            lotSize: metadata.lotSize,
            tickSize: metadata.tickSize,
            leverage,
            reduceOnly,
            timestamp: now
        };
    }
    applyDelta(key, position, delta, price, feeRate, now, reason = "") {
        if (Math.abs(delta) <= 1e-9)
            return;
        if (position.size === 0 || sameSign(position.size, delta)) {
            const opened = Math.abs(position.size) <= 1e-9;
            const nextAbs = Math.abs(position.size) + Math.abs(delta);
            if (price && price > 0) {
                position.entryPrice = nextAbs > 0 && Math.abs(position.size) > 1e-9 && position.entryPrice
                    ? (position.entryPrice * Math.abs(position.size) + price * Math.abs(delta)) / nextAbs
                    : price;
                position.lastPrice = price;
            }
            if (opened)
                position.openedAt = now;
            const fee = this.feeForQuantity(key, position, Math.abs(delta), price, feeRate);
            position.fees = (position.fees ?? 0) + fee;
            position.realizedPnl = (position.realizedPnl ?? 0) - fee;
            position.size += delta;
            resetExcursion(position);
            return;
        }
        if (price && price > 0)
            position.lastPrice = price;
        updateExcursion(position);
        const closing = Math.min(Math.abs(position.size), Math.abs(delta));
        const gross = this.realizedGrossForQuantity(key, position, closing, price);
        const fee = this.feeForQuantity(key, position, closing, price, feeRate);
        position.realizedGross = (position.realizedGross ?? 0) + gross;
        position.fees = (position.fees ?? 0) + fee;
        position.realizedPnl = (position.realizedPnl ?? 0) + gross - fee;
        const closed = {
            venue: position.venue,
            instrument: position.instrument,
            side: positionSide(position),
            size: closing,
            entryPrice: position.entryPrice,
            exitPrice: price,
            exitMove: move(position),
            realizedGross: position.realizedGross,
            fees: position.fees,
            realizedPnl: position.realizedPnl,
            mfe: position.mfe,
            mae: position.mae,
            exitReason: reason,
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
        position.fees = this.feeForQuantity(key, position, remaining, price, this.takerFeeRate(key));
        position.realizedPnl = -position.fees;
        resetExcursion(position);
    }
    effectiveMinOrderDelta() {
        return this.config.minOrderDelta <= 0 ? 0 : this.config.minOrderDelta * this.maxPortfolioMarginBudget();
    }
    minimumPositionSize() {
        return this.config.minPositionSizeRatio <= 0 ? 0 : this.config.minPositionSizeRatio * this.portfolioCapital();
    }
    meetsMinimumPositionSize(size) {
        const minimum = this.minimumPositionSize();
        return minimum <= 0 || Math.abs(size) + 1e-9 >= minimum;
    }
    selectLeverage(key, confidence, edge, score = 0) {
        const minLev = this.minLeverage(key);
        const maxLev = Math.max(this.maxLeverage(key), minLev);
        if (maxLev === minLev)
            return minLev;
        const edgeScore = clamp01(edge / Math.max(this.config.minExpectedEdge * 3, 0.001));
        const quality = clamp01(clamp01(confidence) * 0.65 + edgeScore * 0.25 + clamp01(Math.abs(score)) * 0.1);
        return minLev + (maxLev - minLev) * quality;
    }
    makerFeeRate(key) {
        return this.config.instruments[key]?.makerFeeRate ?? this.config.makerFeeRate;
    }
    takerFeeRate(key) {
        return this.config.instruments[key]?.takerFeeRate ?? this.config.takerFeeRate;
    }
    minLeverage(key) {
        return this.config.instruments[key]?.minLeverage ?? this.config.minLeverage;
    }
    maxLeverage(key) {
        const configured = this.config.instruments[key]?.maxLeverage ?? this.config.maxLeverage;
        const [venue, instrument] = splitKey(key);
        const metadataMax = this.instrumentMetadata.instrument(venue, instrument)?.maxLeverage ?? 0;
        return metadataMax > 0 ? Math.min(configured || metadataMax, metadataMax) : configured;
    }
    trailingConfigForSignal(key, signal) {
        const override = this.config.instruments[key];
        let activation = signal.trailingStopActivation ?? override?.trailingStopActivation ?? 0;
        let distance = signal.trailingStopDistance ?? override?.trailingStopDistance ?? 0;
        let minProfit = signal.trailingStopMinProfit ?? override?.trailingStopMinProfit ?? 0;
        if (activation <= 0 || distance <= 0)
            return { activation: 0, distance: 0, minProfit: 0 };
        activation = Math.max(0, activation);
        distance = Math.max(0, distance);
        minProfit = Math.max(0, minProfit);
        const feeFloor = 2 * this.takerFeeRate(key);
        if (minProfit < feeFloor)
            minProfit = feeFloor;
        if (activation < minProfit + 1e-9)
            activation = minProfit + Math.min(distance, feeFloor);
        return { activation, distance, minProfit };
    }
    instrumentFor(venue, instrument) {
        return this.instrumentMetadata.instrument(venue, instrument) ?? {
            venue,
            instrument,
            settlementCurrency: "USDT",
            lotSize: 0,
            minSize: 0,
            tickSize: 0,
            contractValue: 0,
            contractMultiplier: 0,
            maxLeverage: 0
        };
    }
    orderMeetsInstrumentMinimum(order) {
        if (order.quantity <= 0)
            return false;
        if (order.reason === "closing" || order.reason === "flip")
            return true;
        if (order.minSize > 0 && order.quantity > 0 && order.quantity < order.minSize)
            return false;
        return true;
    }
}
function websocketUrlFromBase(baseUrl) {
    if (!baseUrl)
        return undefined;
    const url = new URL(baseUrl);
    url.protocol = url.protocol === "http:" ? "ws:" : "wss:";
    url.pathname = "/ws";
    url.search = "";
    return url.toString();
}
function normalizeConfig(config) {
    let maxMarginRatio = config.maxMarginRatio ?? 0;
    if (maxMarginRatio <= 0) {
        const legacy = config.positionSize ?? productionDefaults.positionSize;
        maxMarginRatio = legacy > 0 && legacy <= 1 ? legacy : productionDefaults.maxMarginRatio;
    }
    return {
        maxMarginRatio: Math.min(Math.max(maxMarginRatio, 0), 1),
        positionSize: Math.max(config.positionSize ?? productionDefaults.positionSize, 0),
        minExpectedEdge: Math.max(config.minExpectedEdge ?? productionDefaults.minExpectedEdge, 0),
        minOrderDelta: Math.min(Math.max(config.minOrderDelta ?? productionDefaults.minOrderDelta, 0), 1),
        minPositionSizeRatio: Math.min(Math.max(config.minPositionSizeRatio ?? productionDefaults.minPositionSizeRatio, 0), 1),
        rebalanceIntervalMs: Math.max(config.rebalanceIntervalMs ?? productionDefaults.rebalanceIntervalMs, 0),
        makerFeeRate: Math.max(config.makerFeeRate ?? productionDefaults.makerFeeRate, 0),
        takerFeeRate: Math.max(config.takerFeeRate ?? productionDefaults.takerFeeRate, 0),
        minLeverage: Math.max(config.minLeverage ?? productionDefaults.minLeverage, 0),
        maxLeverage: Math.max(config.maxLeverage ?? productionDefaults.maxLeverage, 0),
        availableMarginBuffer: Math.min(Math.max(config.availableMarginBuffer ?? productionDefaults.availableMarginBuffer, 0), 0.95),
        executableMarginBuffer: Math.min(Math.max(config.executableMarginBuffer ?? productionDefaults.executableMarginBuffer, 0), 0.05),
        instruments: config.instruments ?? {},
        assetManager: config.assetManager ?? new AssetManager(),
        instrumentManager: config.instrumentManager ?? new InstrumentManager(),
        initialState: config.initialState,
        persist: config.persist
    };
}
function feeAdjustedExpectedEdge(signal, takerFeeRate) {
    return expectedEdge(signal) - 2 * takerFeeRate;
}
function expectedEdge(signal) {
    const confidence = clamp01(signal.confidence);
    return confidence * Math.max(signal.takeProfit, 0) - (1 - confidence) * Math.max(signal.stopLoss, 0);
}
function orderBudgetCost(order) {
    return Math.max(0, order.margin) + Math.max(0, order.estimatedFee);
}
function feeValueForNotional(notional, feeRate) {
    if (notional <= 0 || feeRate <= 0)
        return 0;
    return notional * feeRate;
}
function instrumentContractNotional(price, metadata) {
    if (price <= 0)
        return 0;
    return price * positiveOr(metadata.contractValue, 1) * positiveOr(metadata.contractMultiplier, 1);
}
function ratioOrZero(numerator, denominator) {
    return denominator > 0 ? numerator / denominator : 0;
}
function feeExposureForMargin(margin, leverage, feeRate) {
    if (margin <= 0 || leverage <= 0 || feeRate <= 0)
        return 0;
    return margin * leverage * feeRate;
}
function signalDate(timestamp) {
    if (timestamp instanceof Date)
        return timestamp;
    if (timestamp)
        return new Date(timestamp);
    return new Date();
}
function positionKey(venue, instrument) {
    return `${venue}:${instrument}`;
}
function splitKey(key) {
    const separator = key.indexOf(":");
    return separator === -1 ? ["", key] : [key.slice(0, separator), key.slice(separator + 1)];
}
function sideSign(side) {
    if (side === "buy")
        return 1;
    if (side === "sell")
        return -1;
    return 0;
}
function sign(value) {
    if (value < 0)
        return -1;
    if (value > 0)
        return 1;
    return 0;
}
function sameSign(a, b) {
    return sign(a) === sign(b);
}
function clamp01(value) {
    return Math.min(Math.max(value, 0), 1);
}
function positiveOr(...values) {
    for (const value of values) {
        if (value !== undefined && value > 0)
            return value;
    }
    return 0;
}
function roundDownToStep(value, step) {
    if (value <= 0 || !step || step <= 0)
        return value;
    return Math.floor(value / step) * step;
}
function roundToTick(value, tick) {
    if (value <= 0 || !tick || tick <= 0)
        return value;
    return Math.round(value / tick) * tick;
}
function positionSide(position) {
    if (position.size < 0)
        return "sell";
    if (position.size > 0)
        return "buy";
    return "";
}
function confidenceFromSize(position, positionSize) {
    return positionSize <= 0 ? clamp01(Math.abs(position.size)) : clamp01(Math.abs(position.size) / positionSize);
}
function move(position) {
    if (!position.entryPrice || !position.lastPrice)
        return 0;
    if (position.size < 0)
        return (position.entryPrice - position.lastPrice) / position.entryPrice;
    return (position.lastPrice - position.entryPrice) / position.entryPrice;
}
function takeProfitPrice(position) {
    if (!position.entryPrice || !position.takeProfit)
        return undefined;
    return position.size < 0
        ? position.entryPrice * (1 - position.takeProfit)
        : position.entryPrice * (1 + position.takeProfit);
}
function stopLossPrice(position) {
    if (!position.entryPrice || !position.stopLoss)
        return undefined;
    return position.size < 0
        ? position.entryPrice * (1 + position.stopLoss)
        : position.entryPrice * (1 - position.stopLoss);
}
function takeProfitTriggered(position, price) {
    const target = takeProfitPrice(position);
    if (!target)
        return false;
    return position.size < 0 ? price <= target : price >= target;
}
function stopLossTriggered(position, price) {
    const target = stopLossPrice(position);
    if (!target)
        return false;
    return position.size < 0 ? price >= target : price <= target;
}
function exitTriggered(position, price) {
    return takeProfitTriggered(position, price) || stopLossTriggered(position, price);
}
function exitReason(position, price) {
    if (takeProfitTriggered(position, price))
        return "take_profit";
    if (stopLossTriggered(position, price))
        return "stop_loss";
    if (trailingStopTriggered(position))
        return "trailing_stop";
    return undefined;
}
function trailingStopTriggered(position) {
    if (!position.trailingStopActivation || !position.trailingStopDistance)
        return false;
    if ((position.mfe ?? 0) + 1e-9 < position.trailingStopActivation)
        return false;
    let floor = (position.mfe ?? 0) - position.trailingStopDistance;
    if ((position.trailingStopMinProfit ?? 0) > floor)
        floor = position.trailingStopMinProfit ?? 0;
    return move(position) <= floor + 1e-9;
}
function resetExcursion(position) {
    const currentMove = move(position);
    position.mfe = Math.max(currentMove, 0);
    position.mae = Math.min(currentMove, 0);
}
function updateExcursion(position) {
    const currentMove = move(position);
    position.mfe = Math.max(position.mfe ?? 0, currentMove);
    position.mae = Math.min(position.mae ?? 0, currentMove);
}
function orderReason(position, targetSize) {
    if (Math.abs(position.size) <= 1e-9)
        return "opening";
    if (Math.abs(targetSize) <= 1e-9)
        return "closing";
    if (!sameSign(position.size, targetSize))
        return "flip";
    return "rebalance";
}
function isFlipTarget(previousSize, targetSize) {
    return Math.abs(previousSize) > 1e-9 && Math.abs(targetSize) > 1e-9 && !sameSign(previousSize, targetSize);
}
function isExposureReduction(previousSize, targetSize) {
    if (Math.abs(previousSize) <= 1e-9)
        return false;
    if (Math.abs(targetSize) <= 1e-9)
        return true;
    if (!sameSign(previousSize, targetSize))
        return true;
    return Math.abs(targetSize) < Math.abs(previousSize) - 1e-9;
}
function blendRisk(current, incoming, gate) {
    if (current <= 0)
        return incoming;
    if (incoming <= 0)
        return current;
    const clamped = clamp01(gate);
    return current * (1 - clamped) + incoming * clamped;
}
//# sourceMappingURL=index.js.map