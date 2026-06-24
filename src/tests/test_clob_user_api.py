from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polymarket_interfaces.clob_market_api import ClobSide
from polymarket_interfaces.clob_user_api import (
    CLOBApiCredentials,
    ClobUserAPI,
    ClobAssetType,
    ClobOrderType,
    LimitOrderRequest,
    MarketOrderRequest,
)
from polymarket_interfaces.utils.utils import ErrorCode, PolymarketError


@dataclass
class FakeApiCreds:
    api_key: str
    api_secret: str
    api_passphrase: str


class FakeParams:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class FakeOrderType:
    GTC = "GTC"
    GTD = "GTD"
    FOK = "FOK"
    FAK = "FAK"


class FakeSide:
    BUY = 0
    SELL = 1


class FakeClient:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.creds = kwargs.get("creds")
        self.calls = []
        self.__class__.instances.append(self)

    def create_or_derive_api_key(self, nonce=None):
        self.calls.append(("authenticate", nonce))
        return FakeApiCreds("key", "secret", "passphrase")

    def set_api_creds(self, creds):
        self.creds = creds

    def get_address(self):
        return "0xabc"

    def get_open_orders(self, **kwargs):
        self.calls.append(("get_open_orders", kwargs))
        return [{"id": "open-1"}]

    def get_trades(self, **kwargs):
        self.calls.append(("get_trades", kwargs))
        return [{"id": "trade-1"}]

    def get_order(self, order_id):
        return {"id": order_id}

    def get_balance_allowance(self, params):
        return {"balance": "10", "allowance": "5", "asset": params.asset_type}

    def update_balance_allowance(self, params):
        return {"updated": True, "asset": params.asset_type}

    def get_notifications(self):
        return [{"id": "notification-1"}]

    def create_and_post_order(self, **kwargs):
        self.calls.append(("limit", kwargs))
        return {
            "success": True,
            "orderID": "order-1",
            "status": "live",
            "makingAmount": "2",
            "takingAmount": "1",
        }

    def create_and_post_market_order(self, **kwargs):
        self.calls.append(("market", kwargs))
        return {"success": True, "orderID": "order-2", "status": "matched"}

    def cancel_order(self, payload):
        return {"canceled": [payload.orderID]}

    def cancel_orders(self, order_ids):
        return {"canceled": order_ids}

    def cancel_all(self):
        return {"canceled": "all"}

    def cancel_market_orders(self, payload):
        return {"market": payload.market, "asset_id": payload.asset_id}


def fake_sdk():
    return SimpleNamespace(
        ApiCreds=FakeApiCreds,
        BalanceAllowanceParams=FakeParams,
        ClobClient=FakeClient,
        MarketOrderArgs=FakeParams,
        OpenOrderParams=FakeParams,
        OrderArgs=FakeParams,
        OrderMarketCancelParams=FakeParams,
        OrderPayload=FakeParams,
        OrderType=FakeOrderType,
        PartialCreateOrderOptions=FakeParams,
        Side=FakeSide,
        TradeParams=FakeParams,
    )


@pytest.fixture(autouse=True)
def clear_clients():
    FakeClient.instances.clear()


@pytest.mark.asyncio
async def test_auto_authenticates_before_user_data_request():
    api = ClobUserAPI("0xprivate", sdk_loader=fake_sdk)

    orders = await api.get_open_orders(market="condition")

    assert orders == [{"id": "open-1"}]
    assert api.is_authenticated
    assert api.credentials.api_key == "key"
    assert FakeClient.instances[0].creds.api_key == "key"


@pytest.mark.asyncio
async def test_existing_credentials_create_l2_client_without_derivation():
    creds = CLOBApiCredentials("key", "secret", "passphrase")
    api = ClobUserAPI("0xprivate", credentials=creds, sdk_loader=fake_sdk)

    await api.get_trades(asset_id="token")

    client = FakeClient.instances[0]
    assert client.kwargs["creds"].api_key == "key"
    assert not any(call[0] == "authenticate" for call in client.calls)


@pytest.mark.asyncio
async def test_live_order_requires_explicit_opt_in():
    api = ClobUserAPI(
        "0xprivate",
        credentials=CLOBApiCredentials("key", "secret", "passphrase"),
        sdk_loader=fake_sdk,
    )
    request = LimitOrderRequest(
        token_id="token",
        side=ClobSide.BUY,
        price=Decimal("0.40"),
        size=Decimal("10"),
    )

    with pytest.raises(PolymarketError) as exc:
        await api.place_limit_order(request)

    assert exc.value.code is ErrorCode.AUTH_FAILED


@pytest.mark.asyncio
async def test_places_limit_order_and_normalizes_response():
    api = ClobUserAPI(
        "0xprivate",
        credentials=CLOBApiCredentials("key", "secret", "passphrase"),
        allow_live_trading=True,
        sdk_loader=fake_sdk,
    )
    request = LimitOrderRequest(
        token_id="token",
        side=ClobSide.BUY,
        price=Decimal("0.40"),
        size=Decimal("10"),
        order_type=ClobOrderType.GTC,
        tick_size=Decimal("0.01"),
        post_only=True,
    )

    result = await api.place_limit_order(request)

    assert result.success is True
    assert result.order_id == "order-1"
    assert result.making_amount == Decimal("2")
    call = FakeClient.instances[0].calls[-1]
    assert call[0] == "limit"
    assert call[1]["post_only"] is True
    assert call[1]["order_args"].side == FakeSide.BUY
    assert call[1]["options"].tick_size == "0.01"


@pytest.mark.asyncio
async def test_places_market_sell_and_cancels_order():
    api = ClobUserAPI(
        "0xprivate",
        credentials=CLOBApiCredentials("key", "secret", "passphrase"),
        allow_live_trading=True,
        sdk_loader=fake_sdk,
    )
    result = await api.place_market_order(
        MarketOrderRequest(
            token_id="token",
            side=ClobSide.SELL,
            amount=Decimal("3.5"),
            order_type=ClobOrderType.FAK,
        )
    )
    canceled = await api.cancel_order("order-2")

    assert result.order_id == "order-2"
    assert canceled == {"canceled": ["order-2"]}


@pytest.mark.asyncio
async def test_balance_allowance_for_conditional_requires_token():
    api = ClobUserAPI(
        "0xprivate",
        credentials=CLOBApiCredentials("key", "secret", "passphrase"),
        sdk_loader=fake_sdk,
    )

    with pytest.raises(ValueError):
        await api.get_balance_allowance(ClobAssetType.CONDITIONAL)
