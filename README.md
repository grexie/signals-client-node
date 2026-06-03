# Grexie Signals Node Client

Typed Node.js client for the Grexie Signals router websocket protocol.

```sh
npm install @grexie/signals-client
```

## SignalsManager

`SignalsManager` owns one router basket subscription. It sends your asset and venue-position snapshots to the server, then emits server-created router intents from the websocket. It does not calculate order management locally.

```ts
import { SignalsClient, SignalsManager } from "@grexie/signals-client";

const client = new SignalsClient("ws_your_token", {
  baseUrl: "https://signals.grexie.com"
});
await client.connect();

const manager = new SignalsManager(client, {
  assets: [{ venue: "okx", currency: "USDT", cash: 1000, available: 1000, equity: 1000, maxUsage: 1 }]
}, {
  venue: "okx",
  instruments: ["BTC-USDT-SWAP", "ETH-USDT-SWAP"],
  risk: { maxMarginRatio: 1, maxConcurrentPositions: 1, minLeverage: 1, maxLeverage: 1 }
});

manager.on("intent", (intent) => {
  console.log(intent.instrument, intent.side, intent.contractSize, intent.reduceOnly);
  // Execute on your venue, then call manager.updatePosition(...)
});

await manager.run();
```

Client-to-server updates:

- `updateAsset`: currency, cash, available, used, equity, maxUsage.
- `updatePosition`: instrument, signed size, status, entry/mark, leverage, TP/SL.
- `addInstrument` / `removeInstrument`: edit a basket while it is running.
- `updateConfig`: runtime settings such as `profitWithdrawRatio`.
- `scheduleWithdrawal`: ask the router to free profitable/breakeven equity.

Server-to-client events:

- `create-market-order`: open, increase, reduce, close, or preempt with contract size, leverage, reduce-only, TP, and SL metadata.
- `update-tpsl`: update protection for an existing position.
- `withdraw`: withdraw an amount after the router has made room.

## Development

```sh
npm test
npm run build
```
