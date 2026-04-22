import asyncio
import requests
from typing import Dict, Any, Optional
from aiogram import Bot
from config import CRYPTOBOT_API_TOKEN

CRYPTO_TOKEN = CRYPTOBOT_API_TOKEN
CRYPTO_API_BASE = "https://pay.crypt.bot/api"

# Словарь активных инвойсов
active_invoices: Dict[str, Dict[str, Any]] = {}

def create_crypto_invoice(user_id: int, amount: int) -> Optional[str]:
    """Создает инвойс в CryptoBot"""
    headers = {
        "Crypto-Pay-API-Token": CRYPTO_TOKEN
    }
    payload = {
        "asset": "USDT",
        "amount": amount,
        "description": f"Top up balance by {amount}$",
        "hidden_message": "Thanks for your payment! Balance will be credited automatically.",
        "payload": f"{user_id}:{amount}",
        "allow_comments": False
    }
    
    try:
        response = requests.post(f"{CRYPTO_API_BASE}/createInvoice", headers=headers, json=payload)
        data = response.json()
        
        if data.get("ok"):
            invoice = data["result"]
            active_invoices[invoice["invoice_id"]] = {
                "user_id": user_id,
                "amount": amount,
                "paid": False
            }
            return invoice["pay_url"]
    except Exception as e:
        print(f"Invoice creation error: {e}")
    
    return None

async def check_invoices(bot: Bot) -> None:
    """Checks invoice status and credits funds"""
    from database import update_balance
    
    while True:
        await asyncio.sleep(10)
        if not active_invoices:
            continue

        headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
        try:
            response = requests.get(f"{CRYPTO_API_BASE}/getInvoices", headers=headers)
            data = response.json()
        except Exception as e:
            print(f"Invoice request error: {e}")
            continue

        if not data.get("ok"):
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

            if invoice.get("status") == "paid":
                inv_id = invoice.get("invoice_id")
                if inv_id in active_invoices and not active_invoices[inv_id]["paid"]:
                    user_id = active_invoices[inv_id]["user_id"]
                    amount = active_invoices[inv_id]["amount"]
                    update_balance(user_id, amount)
                    active_invoices[inv_id]["paid"] = True
                    
                    try:
                        await bot.send_message(user_id, f"✅ Payment of {amount}$ received. Balance credited.")
                    except Exception as e:
                        print(f"Message send error: {e}")
