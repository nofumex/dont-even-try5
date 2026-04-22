import asyncio
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

import requests
from aiogram import Bot

from config import CRYPTOBOT_API_TOKEN

CRYPTO_TOKEN = CRYPTOBOT_API_TOKEN
CRYPTO_API_BASE = "https://pay.crypt.bot/api"
SUPPORTED_PAYMENT_ASSETS = ["USDT", "TON", "BTC", "ETH", "LTC", "BNB", "TRX", "USDC"]

# Active invoices stored in memory until they are paid.
active_invoices: Dict[str, Dict[str, Any]] = {}


def create_crypto_invoice(user_id: int, amount: int) -> Optional[str]:
    """Create a USD invoice in Crypto Bot that can be paid with multiple assets."""
    headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
    payload = {
        "currency_type": "fiat",
        "fiat": "USD",
        "accepted_assets": ",".join(SUPPORTED_PAYMENT_ASSETS),
        "amount": str(amount),
        "description": f"Top up balance by {amount}$",
        "hidden_message": "Thanks for your payment! Balance will be credited automatically.",
        "payload": f"{user_id}:{amount}",
        "allow_comments": True,
        "allow_anonymous": True,
    }

    try:
        response = requests.post(
            f"{CRYPTO_API_BASE}/createInvoice",
            headers=headers,
            json=payload,
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()

        if data.get("ok"):
            invoice = data["result"]
            invoice_id = str(invoice["invoice_id"])
            active_invoices[invoice_id] = {
                "user_id": user_id,
                "amount": amount,
                "paid": False,
            }
            return invoice.get("bot_invoice_url") or invoice.get("pay_url")

        print(f"Invoice creation failed: {data}")
    except Exception as e:
        print(f"Invoice creation error: {e}")

    return None


async def check_invoices(bot: Bot) -> None:
    """Poll invoice statuses and credit user balances after payment."""
    from database import update_balance

    while True:
        await asyncio.sleep(10)
        if not active_invoices:
            continue

        invoice_ids = [invoice_id for invoice_id, meta in active_invoices.items() if not meta.get("paid")]
        if not invoice_ids:
            continue

        headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
        try:
            response = requests.get(
                f"{CRYPTO_API_BASE}/getInvoices",
                headers=headers,
                params={
                    "invoice_ids": ",".join(invoice_ids),
                    "status": "paid",
                    "count": len(invoice_ids),
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Invoice request error: {e}")
            continue

        if not data.get("ok"):
            print(f"Invoice fetch failed: {data}")
            continue

        result = data.get("result")
        if not isinstance(result, dict) or "items" not in result:
            print(f"Unexpected result structure: {result}")
            continue

        invoices = result["items"]
        if not isinstance(invoices, list):
            print(f"Unexpected invoices type: {type(invoices)}, content: {invoices}")
            continue

        for invoice in invoices:
            if not isinstance(invoice, dict):
                print(f"Unexpected invoice type: {type(invoice)}, content: {invoice}")
                continue

            if invoice.get("status") != "paid":
                continue

            inv_id = str(invoice.get("invoice_id"))
            if inv_id not in active_invoices or active_invoices[inv_id]["paid"]:
                continue

            user_id = active_invoices[inv_id]["user_id"]
            amount = active_invoices[inv_id]["amount"]
            update_balance(user_id, amount)
            active_invoices[inv_id]["paid"] = True

            paid_asset = invoice.get("paid_asset", "crypto")
            paid_amount_raw = invoice.get("paid_amount")
            try:
                paid_amount = str(Decimal(str(paid_amount_raw)).normalize()) if paid_amount_raw is not None else None
            except (InvalidOperation, TypeError):
                paid_amount = str(paid_amount_raw) if paid_amount_raw is not None else None

            payment_details = f"{paid_amount} {paid_asset}" if paid_amount else paid_asset

            try:
                await bot.send_message(
                    user_id,
                    f"Payment received: {payment_details}. {amount}$ has been credited to your balance.",
                )
            except Exception as e:
                print(f"Message send error: {e}")
