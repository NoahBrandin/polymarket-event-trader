# Polymarket Market WebSocket – Python

Dieses Modul ergänzt den asynchronen Gamma-API-Client um Echtzeitdaten aus dem
öffentlichen Polymarket-Market-Channel.

## Ziel der Architektur

Der WebSocket ist absichtlich nicht als einzelnes Endlosskript aufgebaut. Die
Verantwortlichkeiten sind getrennt, damit Netzwerklogik, Datenmodell und
Orderbook unabhängig getestet oder ausgetauscht werden können.

```text
polymarket_websocket/
├── client.py       Verbindung, Heartbeat, Subscription, Reconnect, Callbacks
├── parser.py       JSON-Nachrichten -> typisierte Events
├── models.py       Enums und Dataclasses aller Market-Events
├── orderbook.py    lokaler Level-2-Orderbook-Zustand
├── errors.py       einheitliche Fehlercodes und Exceptions
└── __init__.py     öffentliche Paket-API
```

Datenfluss:

```text
Gamma API
   │ Markt-Slug -> clob_token_ids
   ▼
MarketWebSocketClient
   │ rohe JSON-/Kontrollnachrichten
   ▼
MarketEventParser
   │ BookEvent, PriceChangeEvent, ...
   ├──────────────► Event-Queue / async for
   ├──────────────► Callback-Handler
   └──────────────► OrderBookStore
```

## Unterstützte Ereignisse

- `book`: vollständiger Level-2-Orderbook-Snapshot
- `price_change`: inkrementelle Änderung einzelner Preisstufen
- `tick_size_change`: Änderung der minimalen Preisschrittweite
- `last_trade_price`: ausgeführter Trade
- `best_bid_ask`: aktueller bester Bid und Ask
- `new_market`: neu angelegter Markt
- `market_resolved`: aufgelöster Markt
- unbekannte zukünftige Eventtypen als `UnknownMarketEvent`

`best_bid_ask`, `new_market` und `market_resolved` werden durch
`custom_feature_enabled=True` aktiviert.

## Installation

```bash
python -m pip install -r requirements.txt
```

Benötigt werden:

- Python 3.10 oder neuer
- `httpx` für die Gamma API
- `websockets` für den Echtzeitkanal

## Schnellstart über einen Markt-Slug

```bash
python example_websocket.py --slug DEIN-MARKT-SLUG
```

Der Ablauf ist:

1. Die Gamma API sucht den Markt anhand des Slugs.
2. `clobTokenIds` wird in `GammaMarket.clob_token_ids` normalisiert.
3. Beide Outcome-Token werden beim Market-WebSocket abonniert.
4. Eingehende Snapshots und Deltas aktualisieren den lokalen Orderbook-Speicher.

## Direkte Subscription über Token-IDs

```bash
python example_websocket.py \
  --asset-id TOKEN_ID_YES \
  --asset-id TOKEN_ID_NO
```

## Verwendung als Bibliothek

```python
import asyncio

from src import (
    LastTradePriceEvent,
    MarketWebSocketClient,
)


async def main() -> None:
    token_ids = ["TOKEN_ID_YES", "TOKEN_ID_NO"]

    async with MarketWebSocketClient(token_ids) as client:
        async for event in client:
            if isinstance(event, LastTradePriceEvent):
                print(event.asset_id, event.price, event.size)


asyncio.run(main())
```

## Gamma API und WebSocket gemeinsam verwenden

```python
import asyncio

from gamma_api import GammaApiClient
from src import MarketWebSocketClient


async def main() -> None:
    async with GammaApiClient() as gamma:
        market = await gamma.get_market_by_slug("DEIN-MARKT-SLUG")

    if market is None or not market.clob_token_ids:
        raise RuntimeError("Markt oder Token-IDs nicht gefunden")

    async with MarketWebSocketClient(market.clob_token_ids) as websocket:
        async for event in websocket:
            print(event)


asyncio.run(main())
```

## Lokales Orderbook

Der Client aktualisiert `client.order_books` automatisch.

```python
book = client.order_books.get(token_id, depth=10)

if book is not None:
    print(book.best_bid)
    print(book.best_ask)
    print(book.spread)
    print(book.midpoint)
    print(book.bids)
    print(book.asks)
```

Ein `book`-Event ersetzt den vollständigen Zustand eines Assets. Ein
`price_change`-Event ändert einzelne Preisstufen. Bei `size == 0` wird die
betroffene Preisstufe gelöscht.

Für Preise und Größen wird `Decimal` statt `float` verwendet. Dadurch entstehen
bei Geldwerten keine unnötigen binären Rundungsfehler.

## Dynamische Subscriptions

```python
await client.subscribe([weitere_token_id])
await client.unsubscribe([alte_token_id])
await client.replace_subscriptions([token_a, token_b])
```

Die gewünschte Subscription wird intern gespeichert. Nach einem Reconnect sendet
der Client automatisch den vollständigen aktuellen Satz von Asset-IDs.

## Callback-API

```python
from src import MarketEventType


def on_trade(event):
    print("Trade:", event)


client.add_handler(on_trade, MarketEventType.LAST_TRADE_PRICE)
client.add_handler(lambda event: print("Alle Events:", event))
client.add_error_handler(lambda error: print(error.code, error))
client.add_state_handler(lambda state: print("Status:", state.value))
```

`add_handler(handler)` ohne Eventtyp registriert einen globalen Handler.

## Verbindung und Heartbeat

Der Market-Channel erwartet einen textuellen `PING`-Heartbeat. Der Server
antwortet mit `PONG`. Der Client:

1. deaktiviert das automatische WebSocket-Protokoll-Ping der Bibliothek,
2. sendet standardmäßig alle 10 Sekunden `PING`,
3. überwacht den letzten `PONG`,
4. trennt und reconnectet bei einem Heartbeat-Timeout.

## Reconnect-Konfiguration

```python
from src import (
    MarketWebSocketClient,
    MarketWebSocketConfig,
    ReconnectPolicy,
)

config = MarketWebSocketConfig(
    heartbeat_interval=10,
    pong_timeout=25,
    reconnect=ReconnectPolicy(
        enabled=True,
        initial_delay=1,
        maximum_delay=30,
        multiplier=2,
        jitter_ratio=0.20,
        maximum_attempts=None,
    ),
)

client = MarketWebSocketClient(token_ids, config=config)
```

Der Jitter verhindert, dass viele Clients nach einem Serverausfall exakt zur
gleichen Zeit reconnecten.

## Fehlerbehandlung

```python
from src import PolymarketWebSocketError

try:
    async with MarketWebSocketClient(token_ids) as client:
        async for event in client:
            ...
except PolymarketWebSocketError as error:
    print(error.code)
    print(error.retryable)
    print(error.original_error)
```

Wichtige Fehlercodes:

- `INVALID_SUBSCRIPTION`
- `CONNECTION_FAILED`
- `CONNECTION_CLOSED`
- `HEARTBEAT_TIMEOUT`
- `INVALID_MESSAGE`
- `SEND_FAILED`
- `CALLBACK_FAILED`

Ungültige einzelne Datennachrichten werden gemeldet und übersprungen. Sie lösen
nicht automatisch einen kompletten Reconnect aus.

## Tests

```bash
python -m unittest discover -s tests -v
```

Die Tests verwenden einen lokalen simulierten WebSocket-Server und prüfen:

- initiale Subscription,
- Batch-Nachrichten,
- Heartbeat und `PONG`,
- dynamische Subscription,
- Parsing mit `Decimal`,
- Snapshot- und Delta-Aktualisierung des Orderbooks.

## Abgrenzung zum User-Channel

Dieses Paket implementiert bewusst den öffentlichen Market-Channel. Der
Authentifizierungsdaten enthaltende User-Channel für eigene Orders und Trades
sollte als separater Client implementiert werden, damit öffentliche Marktdaten
nicht unnötig mit API-Schlüsseln gekoppelt werden.
