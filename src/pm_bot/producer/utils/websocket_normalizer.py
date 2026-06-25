from __future__ import annotations

from typing import Any

from pm_bot.configuration.logger_config import get_logger
from pm_bot.locel_types import get_str_enum_from_value, get_unix_time_millis_to_datetime
from pm_bot.pipeline.events import AssetUpdatePayload, ErrorPayload, EventType, MarketUpdatePayload

logger = get_logger()


def parse_market_ws_message(data: dict[str, Any]) -> Any:
    """
    Parse a raw WS JSON message into a normalized structure for the bot.

    Returns:
      dict with keys:
        - "type": "ORDERBOOK_SNAPSHOT" | "ORDERBOOK_DELTA" | "TRADE" | "HEARTBEAT"
        - "market_id"
        - "payload" (already normalized where possible)
    or None if message cannot be interpreted.
    """
    # Common field candidates
    event_type = get_str_enum_from_value(str(data.get("event_type")), EventType)
    market_id = data.get("market")
    timestamp = get_unix_time_millis_to_datetime(float(data.get("timestamp")))

    bids = data.get("bids")  # Type -> [{"price":"float","size":"float"}, ....]
    asks = data.get("asks")

    price_changes = data.get("price_changes")
    if price_changes is not None:
        try:
            asset_event = [AssetUpdatePayload(
                asset_id=change.get("asset_id"),
                side=change.get("side"),
                best_bid=change.get("best_bid"),
                best_ask=change.get("best_ask"),)
                for change in price_changes
            ]
            return MarketUpdatePayload(market_id = market_id, event_type=event_type,
                                       timestamp=timestamp, asset_events = asset_event)
        except Exception as e:
            logger.error(f"Initialization of price_change asset_events failed: {e}")
            return ErrorPayload(message="Initialization of price_change asset_events failed", details=e)

    price = data.get("price")
    if price is not None:
        side = data.get("side")
        if side == "BUY":
            bids = [{"price": price, "size": data.get("size")}]
        else:
            asks = [{"price": price, "size": data.get("size")}]

    return MarketUpdatePayload(market_id=market_id, event_type=event_type, timestamp=timestamp,
                                 asset_events=[AssetUpdatePayload(
                                    asset_id=str(data.get("asset_id")),
                                    side=data.get("side"),
                                    old_tick_size= data.get("old_tick_size"),
                                    new_tick_size= data.get("new_tick_size"),
                                    fee_rate_bps= data.get("fee_rate_bps"),
                                    best_bid= data.get("best_bid"),
                                    best_ask= data.get("best_ask"),
                                    bids=bids,
                                    asks=asks,
                                 )]
                             )
