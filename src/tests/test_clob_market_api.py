from __future__ import annotations

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

from polymarket_interfaces.clob_market_api import (
    BatchPriceHistoryParams,
    BookRequest,
    ClobMarketAPI,
    ClobSide,
    PriceHistoryInterval,
    PriceHistoryParams,
)
from polymarket_interfaces.utils.utils import ErrorCode, PolymarketError


def run(coro):
    return asyncio.run(coro)


def make_client(handler):
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(
        base_url="https://clob.polymarket.com",
        transport=transport,
    )
    return ClobMarketAPI(http_client=http_client), http_client


def test_get_order_book_normalizes_decimal_and_top_of_book():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/book"
        assert request.url.params["token_id"] == "token-1"
        return httpx.Response(
            200,
            json={
                "market": "condition-1",
                "asset_id": "token-1",
                "timestamp": "1710000000",
                "bids": [
                    {"price": "0.40", "size": "10.5"},
                    {"price": "0.42", "size": "4"},
                ],
                "asks": [
                    {"price": "0.45", "size": "2"},
                    {"price": "0.44", "size": "3"},
                ],
                "min_order_size": "1",
                "tick_size": "0.01",
                "neg_risk": False,
                "last_trade_price": "0.43",
                "hash": "book-hash",
            },
        )

    client, http_client = make_client(handler)
    try:
        book = run(client.get_order_book("token-1"))
    finally:
        run(http_client.aclose())

    assert book.best_bid == Decimal("0.42")
    assert book.best_ask == Decimal("0.44")
    assert book.midpoint == Decimal("0.43")
    assert book.spread == Decimal("0.02")
    assert book.last_trade_price == Decimal("0.43")
    assert book.bids[0].size == Decimal("10.5")


def test_single_and_batch_prices_use_documented_payloads():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/price":
            assert dict(request.url.params) == {
                "token_id": "token-1",
                "side": "BUY",
            }
            return httpx.Response(200, json={"price": "0.51"})

        assert request.url.path == "/prices"
        assert json.loads(request.read()) == [
            {"token_id": "token-1", "side": "BUY"},
            {"token_id": "token-2", "side": "SELL"},
        ]
        return httpx.Response(
            200,
            json={
                "token-1": {"BUY": "0.51"},
                "token-2": {"SELL": "0.48"},
            },
        )

    client, http_client = make_client(handler)
    try:
        price = run(client.get_price("token-1", ClobSide.BUY))
        prices = run(
            client.get_prices(
                [
                    BookRequest("token-1", ClobSide.BUY),
                    BookRequest("token-2", ClobSide.SELL),
                ]
            )
        )
    finally:
        run(http_client.aclose())

    assert len(calls) == 2
    assert price == Decimal("0.51")
    assert prices["token-1"].buy == Decimal("0.51")
    assert prices["token-1"].sell is None
    assert prices["token-2"].sell == Decimal("0.48")


def test_price_history_requires_bounded_range_or_interval():
    with pytest.raises(ValueError):
        PriceHistoryParams(market="token-1")

    params = PriceHistoryParams(
        market="token-1",
        interval=PriceHistoryInterval.ONE_DAY,
        fidelity=60,
    )
    assert params.to_query() == {
        "market": "token-1",
        "fidelity": 60,
        "interval": "1d",
    }


def test_invalid_book_numbers_fail_closed():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "market": "condition-1",
                "asset_id": "token-1",
                "bids": [{"price": "not-a-number", "size": "2"}],
                "asks": [],
            },
        )

    client, http_client = make_client(handler)
    try:
        with pytest.raises(PolymarketError) as exc_info:
            run(client.get_order_book("token-1"))
    finally:
        run(http_client.aclose())

    assert exc_info.value.code is ErrorCode.INVALID_RESPONSE


def test_http_error_preserves_clob_error_message():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "No orderbook exists"})

    client, http_client = make_client(handler)
    try:
        with pytest.raises(PolymarketError) as exc_info:
            run(client.get_order_book("token-1"))
    finally:
        run(http_client.aclose())

    assert exc_info.value.code is ErrorCode.MARKET_NOT_FOUND
    assert str(exc_info.value) == "No orderbook exists"


def test_batch_price_history_uses_current_rest_payload_and_normalizes_points():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/batch-prices-history"
        assert json.loads(request.read()) == {
            "markets": ["token-1", "token-2"],
            "interval": "1h",
            "fidelity": 5,
        }
        return httpx.Response(
            200,
            json={
                "history": {
                    "token-1": [{"t": 1710000000, "p": "0.51"}],
                    "token-2": [{"t": 1710000000, "p": 0.49}],
                }
            },
        )

    client, http_client = make_client(handler)
    try:
        history = run(
            client.get_batch_price_history(
                BatchPriceHistoryParams(
                    markets=("token-1", "token-2"),
                    interval=PriceHistoryInterval.ONE_HOUR,
                    fidelity=5,
                )
            )
        )
    finally:
        run(http_client.aclose())

    assert history["token-1"][0].price == Decimal("0.51")
    assert history["token-2"][0].price == Decimal("0.49")


def test_batch_endpoints_reject_malformed_items_instead_of_skipping_them():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[None])

    client, http_client = make_client(handler)
    try:
        with pytest.raises(PolymarketError) as exc_info:
            run(client.get_order_books(["token-1"]))
    finally:
        run(http_client.aclose())

    assert exc_info.value.code is ErrorCode.INVALID_RESPONSE


def test_midpoint_accepts_current_mid_price_response_key():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"mid_price": "0.505"})

    client, http_client = make_client(handler)
    try:
        midpoint = run(client.get_midpoint("token-1"))
    finally:
        run(http_client.aclose())

    assert midpoint == Decimal("0.505")


def test_batch_price_requires_side_for_every_request():
    client, http_client = make_client(
        lambda _: pytest.fail("Bei ungültigem Payload darf kein HTTP-Request erfolgen")
    )
    try:
        with pytest.raises(ValueError):
            run(client.get_prices([BookRequest("token-1")]))
    finally:
        run(http_client.aclose())
