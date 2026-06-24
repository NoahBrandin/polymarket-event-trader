# Modul `polymarket_interfaces`

Dieses Paket kapselt die öffentlichen Polymarket-Datenschnittstellen und ist vom eigentlichen Bot getrennt.

## Gamma API

`gamma_api.py` liefert Markt- und Event-Metadaten:

- Märkte und Events suchen,
- Slugs und Condition-IDs auflösen,
- CLOB-Token-IDs ermitteln,
- Zahlen, Datumswerte und JSON-Stringfelder normalisieren,
- Rate-Limiting und einheitliche HTTP-Fehler.

## CLOB API

`clob_api.py` liefert öffentliche Handelsplatzdaten, versendet jedoch keine Orders:

- CLOB-Märkte über Condition-IDs und Cursor laden,
- einzelne und gebündelte Orderbücher abrufen,
- beste Kauf-/Verkaufspreise, Midpoints und Spreads laden,
- letzte Trades sowie einzelne und gebündelte Preisverläufe abrufen,
- Tick-Size, Neg-Risk-Status und Basisgebühr ermitteln,
- Preise und Größen mit `Decimal` statt binären Fließkommazahlen verarbeiten,
- fehlerhafte oder unvollständige Antworten fail-closed als `PolymarketError` ablehnen.

Beispiel:

```python
from polymarket_interfaces import ClobMarketAPI, ClobSide

async with ClobMarketAPI() as clob:
    book = await clob.get_order_book(token_id)
    buy_price = await clob.get_price(token_id, ClobSide.BUY)
```

Für signierte Orders, API-Key-Ableitung und L2-Authentifizierung ist das offizielle Polymarket-SDK zu verwenden. Diese Schnittstelle speichert keine privaten Schlüssel und sendet keine Handelsaufträge.

## Market WebSocket

`polymarket_websocket/` verarbeitet öffentliche Echtzeit-Marktdaten:

- Subscription über Asset-IDs,
- Heartbeat,
- Reconnect,
- Parsing typisierter Events,
- lokaler Orderbook-Speicher,
- dynamisches Subscribe/Unsubscribe.

Dieses Paket enthält keine Trading-Strategie und sendet keine Orders.
