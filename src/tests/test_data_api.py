from __future__ import annotations

import unittest
from decimal import Decimal

import httpx

from polymarket_interfaces.data_api import (
    ActivityQuery,
    DataAPI,
    PositionQuery,
    TradeFilterType,
    TradeQuery,
)
from polymarket_interfaces.utils.utils import ErrorCode, PolymarketError

USER = "0x56687bf447db6ffa42ffe2204a05edaa20f55839"
MARKET = "0x" + "a" * 64
ASSET = "123456789"


def position_payload(asset: str = ASSET, avg_price: str = "0.4200000001") -> dict:
    return {
        "proxyWallet": USER,
        "asset": asset,
        "conditionId": MARKET,
        "size": "12.5",
        "avgPrice": avg_price,
        "initialValue": "5.25",
        "currentValue": "6.25",
        "cashPnl": "1",
        "percentPnl": "19.047619",
        "totalBought": "12.5",
        "realizedPnl": "0",
        "percentRealizedPnl": "0",
        "curPrice": "0.5",
        "redeemable": False,
        "mergeable": False,
        "title": "Test market",
        "slug": "test-market",
        "icon": None,
        "eventSlug": "test-event",
        "outcome": "Yes",
        "outcomeIndex": 0,
        "oppositeOutcome": "No",
        "oppositeAsset": "987",
        "endDate": "2026-12-31T00:00:00Z",
        "negativeRisk": False,
    }


class DataAPITest(unittest.IsolatedAsyncioTestCase):
    async def test_get_asset_average_price_is_exact_decimal(self) -> None:
        seen_queries: list[dict[str, str]] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen_queries.append(dict(request.url.params))
            return httpx.Response(200, json=[position_payload()])

        client = httpx.AsyncClient(
            base_url="https://data-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        api = DataAPI(http_client=client)
        try:
            value = await api.get_asset_average_price(USER, ASSET)
        finally:
            await client.aclose()

        self.assertEqual(value, Decimal("0.4200000001"))
        self.assertEqual(seen_queries[0]["sizeThreshold"], "0")
        self.assertEqual(seen_queries[0]["limit"], "500")

    async def test_position_query_serialization(self) -> None:
        captured: dict[str, str] = {}

        async def handler(request: httpx.Request) -> httpx.Response:
            captured.update(dict(request.url.params))
            return httpx.Response(200, json=[])

        client = httpx.AsyncClient(
            base_url="https://data-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        api = DataAPI(http_client=client)
        try:
            await api.get_positions(
                USER,
                PositionQuery(
                    markets=(MARKET,),
                    size_threshold=Decimal("0.01"),
                    redeemable=True,
                    limit=25,
                ),
            )
        finally:
            await client.aclose()

        self.assertEqual(captured["market"], MARKET)
        self.assertEqual(captured["sizeThreshold"], "0.01")
        self.assertEqual(captured["redeemable"], "true")
        self.assertEqual(captured["sortBy"], "TOKENS")

    async def test_total_position_value(self) -> None:
        async def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=[
                    {"user": USER, "value": "10.125"},
                    {"user": USER.upper(), "value": "2.375"},
                ],
            )

        client = httpx.AsyncClient(
            base_url="https://data-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        api = DataAPI(http_client=client)
        try:
            value = await api.get_total_position_value(USER)
        finally:
            await client.aclose()

        self.assertEqual(value, Decimal("12.500"))

    async def test_http_errors_use_shared_polymarket_error(self) -> None:
        async def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(429, json={"message": "slow down"})

        client = httpx.AsyncClient(
            base_url="https://data-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        api = DataAPI(http_client=client)
        try:
            with self.assertRaises(PolymarketError) as raised:
                await api.get_positions(USER)
        finally:
            await client.aclose()

        self.assertEqual(raised.exception.code, ErrorCode.RATE_LIMITED)
        self.assertTrue(raised.exception.retryable)

    async def test_invalid_filter_combinations_fail_before_request(self) -> None:
        async def handler(_: httpx.Request) -> httpx.Response:
            self.fail("HTTP request should not be sent")

        client = httpx.AsyncClient(
            base_url="https://data-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        api = DataAPI(http_client=client)
        try:
            with self.assertRaises(ValueError):
                await api.get_trades(
                    TradeQuery(
                        filter_type=TradeFilterType.CASH,
                        filter_amount=None,
                    )
                )

            with self.assertRaises(ValueError):
                await api.get_activity(
                    USER,
                    ActivityQuery(start=200, end=100),
                )
        finally:
            await client.aclose()


if __name__ == "__main__":
    unittest.main()
