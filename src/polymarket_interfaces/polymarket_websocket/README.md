# Modul `polymarket_interfaces.polymarket_websocket`

Der öffentliche Market-WebSocket-Client stellt normalisierte, typisierte Polymarket-Events bereit.

## Dateien

| Datei | Aufgabe |
|---|---|
| `client.py` | Verbindung, Reconnect, Heartbeat, Subscription und Event-Dispatch |
| `models.py` | Eventtypen, Enums und Dataclasses |
| `parser.py` | JSON-Nachrichten in Eventobjekte übersetzen |
| `orderbook.py` | Snapshots und Deltas zu einem lokalen L2-Orderbook zusammenführen |
| `errors.py` | normalisierte WebSocket-Fehler |

## Datenfluss

```text
WebSocket-Text
    │
    ▼
MarketEventParser
    │
    ├── BookEvent ─────────► OrderBookStore ersetzt Snapshot
    ├── PriceChangeEvent ──► OrderBookStore ändert Levels
    └── übrige Events ─────► Handler und Event-Queue
```

Der Client kann als asynchroner Iterator oder über Callback-Handler verwendet werden.
