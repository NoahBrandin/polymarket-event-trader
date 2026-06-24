import time
import uuid
import requests
from typing import Dict, List, Optional


class PaperClobClient:
    def __init__(self, initial_usdc: float = 10000.0, clob_url: str = "https://clob.polymarket.com"):
        self.clob_url = clob_url
        self.balance = float(initial_usdc)
        self.positions: Dict[str, float] = {}  # token_id -> Anzahl Shares
        self.orders: Dict[str, dict] = {}  # order_id -> Order-Details
        self.trade_history: List[dict] = []
        print(f"[PaperExecution] Initialisiert mit {self.balance} USDC.")

    def get_balance(self) -> float:
        return self.balance

    def get_positions(self) -> Dict[str, float]:
        return {k: v for k, v in self.positions.items() if v > 0}

    def get_order_book(self, token_id: str) -> dict:
        """Holt das echte Live-Orderbuch von Polymarket für akkurates Matching"""
        try:
            resp = requests.get(f"{self.clob_url}/book", params={"token_id": token_id})
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            print(f"[PaperExecution] Fehler beim Abrufen des Live-Orderbuchs: {e}")
        return {"bids": [], "asks": []}

    def create_order(self, token_id: str, price: float, size: float, side: str) -> dict:
        """
        Erstellt eine simulierte Order.
        side: 'BUY' oder 'SELL'
        price: Limitpreis zwischen 0.01 und 0.99
        size: Anzahl der Shares
        """
        side = side.upper()
        if side not in ["BUY", "SELL"]:
            raise ValueError("Side muss 'BUY' oder 'SELL' sein.")
        if not (0.0 < price < 1.0):
            raise ValueError("Preis muss bei Polymarket zwischen 0.0 und 1.0 liegen.")

        order_id = f"paper_{uuid.uuid4().hex[:12]}"

        order = {
            "order_id": order_id,
            "client_order_id": order_id,
            "token_id": token_id,
            "price": float(price),
            "original_size": float(size),
            "size": float(size),  # Verbleibende Size, die noch gefüllt werden muss
            "side": side,
            "status": "OPEN",
            "created_at": time.time()
        }

        # Vorab-Check des Guthabens/Bestands
        if side == "BUY" and (price * size) > self.balance:
            print(
                f"[PaperExecution] Order abgelehnt: Nicht genug USDC ({self.balance} vorhanden, benötigt {price * size})")
            order["status"] = "REJECTED"
            self.orders[order_id] = order
            return order

        if side == "SELL" and self.positions.get(token_id, 0.0) < size:
            print(f"[PaperExecution] Order abgelehnt: Nicht genügend Shares von {token_id}")
            order["status"] = "REJECTED"
            self.orders[order_id] = order
            return order

        self.orders[order_id] = order

        # Sofort versuchen gegen das aktuelle Orderbuch zu matchen
        self._match_order(order_id)
        return order

    def cancel_order(self, order_id: str) -> dict:
        if order_id in self.orders:
            if self.orders[order_id]["status"] in ["OPEN", "PARTIALLY_FILLED"]:
                self.orders[order_id]["status"] = "CANCELED"
                return {"success": True, "order_id": order_id, "status": "CANCELED"}
        return {"success": False, "error": "Order nicht gefunden oder bereits geschlossen"}

    def get_order(self, order_id: str) -> Optional[dict]:
        return self.orders.get(order_id)

    def _match_order(self, order_id: str):
        order = self.orders.get(order_id)
        if not order or order["status"] not in ["OPEN", "PARTIALLY_FILLED"]:
            return

        token_id = order["token_id"]
        side = order["side"]
        limit_price = order["price"]
        remaining_size = order["size"]

        book = self.get_order_book(token_id)

        if side == "BUY":
            # Kauf-Order matcht gegen Verkaufs-Angebote (asks)
            asks = book.get("asks", [])
            for ask in asks:
                ask_price = float(ask["price"])
                ask_size = float(ask["size"])

                if limit_price >= ask_price and remaining_size > 0:
                    match_size = min(remaining_size, ask_size)
                    self._execute_fill(order_id, ask_price, match_size)
                    remaining_size -= match_size

        elif side == "SELL":
            # Verkaufs-Order matcht gegen Kauf-Angebote (bids)
            bids = book.get("bids", [])
            for bid in bids:
                bid_price = float(bid["price"])
                bid_size = float(bid["size"])

                if limit_price <= bid_price and remaining_size > 0:
                    match_size = min(remaining_size, bid_size)
                    self._execute_fill(order_id, bid_price, match_size)
                    remaining_size -= match_size

    def _execute_fill(self, order_id: str, execution_price: float, size: float):
        order = self.orders[order_id]
        token_id = order["token_id"]
        side = order["side"]
        cost = execution_price * size

        if side == "BUY":
            if self.balance < cost:
                size = self.balance / execution_price
                cost = self.balance
                if size <= 0: return

            self.balance -= cost
            self.positions[token_id] = self.positions.get(token_id, 0.0) + size

        elif side == "SELL":
            current_shares = self.positions.get(token_id, 0.0)
            if current_shares < size:
                size = current_shares
                cost = execution_price * size
                if size <= 0: return

            self.balance += cost
            self.positions[token_id] = current_shares - size

        order["size"] -= size
        if order["size"] <= 1e-5:
            order["status"] = "FILLED"
            order["size"] = 0.0
        else:
            order["status"] = "PARTIALLY_FILLED"

        self.trade_history.append({
            "order_id": order_id,
            "token_id": token_id,
            "side": side,
            "execution_price": execution_price,
            "size": size,
            "timestamp": time.time()
        })
        print(f"**[Paper Fill]** {side} {size:.2f} Shares von {token_id} zu {execution_price} USDC")

    def tick(self):
        """
        Sollte periodisch in deinem Haupt-Loop aufgerufen werden,
        um offene Limit-Orders mit veränderten Marktpreisen zu matchen.
        """
        open_orders = [oid for oid, o in self.orders.items() if o["status"] in ["OPEN", "PARTIALLY_FILLED"]]
        for oid in open_orders:
            self._match_order(oid)