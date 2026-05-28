# Node Signalsbot Example

Paper-trading command line bot for Grexie Signals. It subscribes to `SIGNALS_INSTRUMENTS`, reads OKX candle prices, feeds the Node client `PositionManager`, and persists positions, closed trades, orders, and snapshots in a local JSON file database.

## Run

```sh
cd examples/signalsbot
cp .env.example .env
$EDITOR .env
npm install
npm start -- papertrader
```

The bot logs `Position Opened`, `Position Closed`, `Added margin to position`, and `Removed margin from position` lines with sizing, fees, risk, and PnL details. Every five minutes it prints position-manager stats and current PnL.

Clean the local JSON database with:

```sh
npm start -- clean
```

## Docker

```sh
cd examples/signalsbot
cp .env.example .env
docker compose up --build
```

The compose file stores the local database in the `signalsbot-data` volume.
