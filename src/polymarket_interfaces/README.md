# Modul `polymarket_interfaces`

Dieses Paket kapselt die öffentlichen Polymarket-Datenschnittstellen und ist vom eigentlichen Bot getrennt.

## Gamma API

`gamma_api.py` liefert Markt- und Event-Metadaten:

- Märkte und Events suchen,
- Slugs und Condition-IDs auflösen,
- CLOB-Token-IDs ermitteln,
- Zahlen, Datumswerte und JSON-Stringfelder normalisieren,
- Rate-Limiting und einheitliche HTTP-Fehler.

## Market WebSocket

`polymarket_websocket/` verarbeitet öffentliche Echtzeit-Marktdaten:

- Subscription über Asset-IDs,
- Heartbeat,
- Reconnect,
- Parsing typisierter Events,
- lokaler Orderbook-Speicher,
- dynamisches Subscribe/Unsubscribe.

Dieses Paket enthält keine Trading-Strategie und sendet keine Orders.
