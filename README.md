# Grexie Signals Node Client

Typed TypeScript client for Grexie Signals websocket subscriptions and in-memory position management.

```sh
npm install @grexie/signals-client
```

## Websocket Client

`SignalsClient` manages the authenticated websocket lifecycle using a `SignalsWebSocketToken`.

```ts
import { SignalsClient } from "@grexie/signals-client";

const client = new SignalsClient("ws_your_token", {
  baseUrl: "https://signals.grexie.com"
});

await client.connect();
client.subscribe("okx", "BTC-USDT-SWAP");

for await (const event of client) {
  if (event.type === "signal") {
    console.log(event.signal.instrument, event.signal.side, event.signal.confidence);
  }
  if (event.type === "info") {
    console.log(event.stage, event.message);
  }
}
```

The event union is fully typed:

- `ReadyEvent`
- `SubscribedEvent`
- `UnsubscribedEvent`
- `InfoEvent`
- `SignalEvent`
- `ErrorEvent`

Replay messages sent by the server expose `replay` and `replayedAt`.

## Position Manager

`PositionManager` can be attached to a `SignalsClient` or fed manually with `handleSignal`. It keeps a complete in-memory list of positions and produces target order recommendations.

```ts
import {
  PositionManager,
  productionPositionManagerConfig
} from "@grexie/signals-client";

const manager = new PositionManager(client, productionPositionManagerConfig({
  positionSize: 0.10,
  minExpectedEdge: 0.0045,
  minOrderDelta: 0.20,
  rebalanceIntervalMs: 6 * 60 * 60 * 1000,
  makerFeeRate: 0.0002,
  takerFeeRate: 0.0005,
  minLeverage: 1,
  maxLeverage: 3,
  instruments: {
    "okx:BTC-USDT-SWAP": { takerFeeRate: 0.00045, maxLeverage: 5 }
  }
}));
manager.instrumentManager().updateInstrument({
  venue: "okx",
  instrument: "BTC-USDT-SWAP",
  settlementCurrency: "USDT"
});

for await (const order of manager.run()) {
  console.log(order.instrument, order.side, order.sizeDelta, order.targetSize, order.leverage);
}
```

The sizing model follows the production Grexie Signals server:

- `positionSize` is the total portfolio budget across all active positions;
- confidence is stored separately from position size;
- positions are rebalanced by confidence weight;
- reductions, closes, and first-phase flips are emitted before openings or increases;
- openings and increases are capped by live `AssetManager` available exposure when asset snapshots are attached;
- `minOrderDelta` is scaled by `positionSize`, so a `0.20` order threshold with a `0.10` portfolio budget becomes `0.02`;
- same-side churn can be suppressed with `rebalanceIntervalMs`, while opposite-side signals can still flip positions;
- maker/taker fees and per-instrument overrides feed estimated fees and realized PnL;
- leverage is selected inside the configured min/max range using confidence, fee-adjusted expected edge, and signal score.

`PositionManager` ignores replay signal events and ignores live signals whose venue/instrument pair has not been configured in its `InstrumentManager`. `run` uses an independent event stream, so multiple position managers can share one `SignalsClient`.

Use `addPosition`, `updatePosition`, and `closePosition` to hydrate or mutate the runtime from your exchange account. Use `updatePrice` with exchange mark prices to evaluate take-profit and stop-loss exits between websocket signals.

## Assets, Instruments, And Stats

Use `AssetManager` for cash, available balance, used margin, and equity updates. Use `InstrumentManager` for settlement currency, lot size, minimum size, tick size, and max leverage. `PositionManager` uses both managers to emit concrete `quantity`, `notional`, `settlementCurrency`, and fee-value estimates.

Call `manager.stats()` for realized and unrealized PnL in account value and percent, grouped by instrument and settlement currency.

## Price Data

Current Grexie Signals websocket messages expose strategy direction, confidence, risk levels, and component diagnostics. If your subscribed payload does not include signal prices, provide exchange marks through `updatePosition` or `updatePrice` before relying on realized PnL. Order sizing recommendations are still returned as portfolio percentages.

## Development

```sh
npm install
npm run build
npm test
```
