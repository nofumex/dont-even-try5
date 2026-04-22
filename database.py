import os
import json
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from collections import defaultdict
from config import ADMIN_IDS

# Работа с пользователями
USER_FILE = "users.json"
SALES_FILE = "sales.json"
CATALOG_FILE = "catalog.json"
SETTINGS_FILE = "settings.json"

def load_users() -> Dict[str, Any]:
    """Загружает данные пользователей из файла"""
    try:
        with open(USER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        with open(USER_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f)
        return {}

def save_users(users: Dict[str, Any]) -> None:
    """Сохраняет данные пользователей в файл"""
    with open(USER_FILE, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=4, ensure_ascii=False)

def get_balance(user_id: int) -> int:
    """Получает баланс пользователя"""
    users = load_users()
    return users.get(str(user_id), {}).get("balance", 0)

def update_balance(user_id: int, amount: int) -> None:
    """Обновляет баланс пользователя"""
    users = load_users()
    user = users.setdefault(str(user_id), {"balance": 0, "username": ""})
    user["balance"] += amount
    save_users(users)

def get_user_id_by_username(username: str) -> Optional[int]:
    """Находит user_id по username"""
    users = load_users()
    username = username.lstrip("@").lower()
    for uid, data in users.items():
        if data.get("username", "").lower() == username:
            return int(uid)
    return None

def add_user(user_id: int, username: str = "") -> None:
    """Добавляет нового пользователя или обновляет username"""
    users = load_users()
    user_id_str = str(user_id)
    if user_id_str not in users:
        users[user_id_str] = {"balance": 0, "username": username}
    else:
        if users[user_id_str].get("username", "") != username:
            users[user_id_str]["username"] = username
    save_users(users)

# -------------------- Продажи и статистика --------------------

def _ensure_sales_file() -> None:
    if not os.path.exists(SALES_FILE):
        with open(SALES_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)

def load_sales() -> List[Dict[str, Any]]:
    """Загружает список продаж"""
    try:
        with open(SALES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        _ensure_sales_file()
        return []

def save_sales(sales: List[Dict[str, Any]]) -> None:
    with open(SALES_FILE, "w", encoding="utf-8") as f:
        json.dump(sales, f, indent=4, ensure_ascii=False)

def add_sale(user_id: int, total_price: int, quantity: int, folder: str, item_type: str) -> None:
    """Добавляет запись о продаже"""
    # Не учитываем покупки администраторов в статистике
    try:
        if int(user_id) in [int(x) for x in ADMIN_IDS]:
            return
    except Exception:
        pass
    sales = load_sales()
    sales.append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "user_id": int(user_id),
        "total_price": int(total_price),
        "quantity": int(quantity),
        "folder": folder,
        "item_type": item_type,
    })
    save_sales(sales)

def _is_same_day(ts_iso: str, ref: datetime) -> bool:
    try:
        ts = datetime.fromisoformat(ts_iso)
    except Exception:
        return False
    return ts.date() == ref.date()

def _is_same_month(ts_iso: str, ref: datetime) -> bool:
    try:
        ts = datetime.fromisoformat(ts_iso)
    except Exception:
        return False
    return ts.year == ref.year and ts.month == ref.month

def get_unique_buyers_count() -> int:
    sales = load_sales()
    admin_set = {int(x) for x in ADMIN_IDS}
    return len({str(s.get("user_id")) for s in sales if int(s.get("user_id", 0)) not in admin_set})

def get_sales_sum_day() -> int:
    sales = load_sales()
    now = datetime.now(timezone.utc)
    admin_set = {int(x) for x in ADMIN_IDS}
    return sum(int(s.get("total_price", 0)) for s in sales if int(s.get("user_id", 0)) not in admin_set and _is_same_day(s.get("ts", ""), now))

def get_sales_sum_month() -> int:
    sales = load_sales()
    now = datetime.now(timezone.utc)
    admin_set = {int(x) for x in ADMIN_IDS}
    return sum(int(s.get("total_price", 0)) for s in sales if int(s.get("user_id", 0)) not in admin_set and _is_same_month(s.get("ts", ""), now))

def get_total_orders_count() -> int:
    admin_set = {int(x) for x in ADMIN_IDS}
    return len([s for s in load_sales() if int(s.get("user_id", 0)) not in admin_set])

def get_avg_ticket_today() -> float:
    sales = load_sales()
    now = datetime.now(timezone.utc)
    admin_set = {int(x) for x in ADMIN_IDS}
    today_sales = [int(s.get("total_price", 0)) for s in sales if int(s.get("user_id", 0)) not in admin_set and _is_same_day(s.get("ts", ""), now)]
    if not today_sales:
        return 0.0
    return sum(today_sales) / len(today_sales)

def get_top_buyers(limit: int = 5) -> List[Tuple[int, int]]:
    """Возвращает список (user_id, total_spent) отсортированный по сумме, ограничение limit"""
    sales = load_sales()
    spent_by_user: Dict[int, int] = defaultdict(int)
    admin_set = {int(x) for x in ADMIN_IDS}
    for s in sales:
        if int(s.get("user_id", 0)) in admin_set:
            continue
        spent_by_user[int(s.get("user_id", 0))] += int(s.get("total_price", 0))
    items = sorted(spent_by_user.items(), key=lambda kv: kv[1], reverse=True)
    return items[:limit]

def get_username_by_user_id(user_id: int) -> str:
    users = load_users()
    return users.get(str(user_id), {}).get("username", "")

# -------------------- Каталог (категории/прокси) --------------------

def _default_catalog() -> Dict[str, Any]:
    return {
        "categories": {
            # name -> {folder, price}
            "FB Marketplace": {"folder": "fb_marketplace", "price": 5},
            "eBay": {"folder": "ebay", "price": 20},
            "Kleinanzeigen": {"folder": "kleinanzeigen", "price": 20},
            "Etsy": {"folder": "etsy", "price": 10},
            "Vinted": {"folder": "vinted", "price": 20},
            "Wallapop": {"folder": "wallapop", "price": 20},
        },
        "proxies": {
            # name -> {folder, price, flag}
            "SOCKS5 Germany": {"folder": "proxy_de", "price": 3, "flag": "🇩🇪"},
            "SOCKS5 Canada": {"folder": "proxy_ca", "price": 3, "flag": "🇨🇦"},
            "SOCKS5 Hungary": {"folder": "proxy_hu", "price": 3, "flag": "🇭🇺"},
            "SOCKS5 USA": {"folder": "proxy_us", "price": 3, "flag": "🇺🇸"},
            "SOCKS5 Singapore": {"folder": "proxy_sg", "price": 3, "flag": "🇸🇬"},
        },
        "product_categories": {
            "Accounts": {
                "icon": "🧾",
                "items": {
                    "FB Marketplace": {"folder": "fb_marketplace", "price": 5},
                    "eBay": {"folder": "ebay", "price": 20},
                    "Kleinanzeigen": {"folder": "kleinanzeigen", "price": 20},
                    "Etsy": {"folder": "etsy", "price": 10},
                    "Vinted": {"folder": "vinted", "price": 20},
                    "Wallapop": {"folder": "wallapop", "price": 20},
                }
            },
            "Proxies": {
                "icon": "🧰",
                "items": {
                    "SOCKS5 Germany": {"folder": "proxy_de", "price": 3, "flag": "🇩🇪"},
                    "SOCKS5 Canada": {"folder": "proxy_ca", "price": 3, "flag": "🇨🇦"},
                    "SOCKS5 Hungary": {"folder": "proxy_hu", "price": 3, "flag": "🇭🇺"},
                    "SOCKS5 USA": {"folder": "proxy_us", "price": 3, "flag": "🇺🇸"},
                    "SOCKS5 Singapore": {"folder": "proxy_sg", "price": 3, "flag": "🇸🇬"},
                }
            }
        }
    }

def load_catalog() -> Dict[str, Any]:
    try:
        with open(CATALOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            # simple validation
            if not isinstance(data, dict):
                raise ValueError("catalog format invalid")
            data.setdefault("categories", {})
            data.setdefault("proxies", {})
            return data
    except (json.JSONDecodeError, FileNotFoundError, ValueError):
        data = _default_catalog()
        save_catalog(data)
        return data

def save_catalog(catalog: Dict[str, Any]) -> None:
    with open(CATALOG_FILE, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=4, ensure_ascii=False)

def get_categories() -> Dict[str, Dict[str, Any]]:
    return load_catalog().get("categories", {})

def get_proxies() -> Dict[str, Dict[str, Any]]:
    return load_catalog().get("proxies", {})

def add_category(name: str, folder: str, price: int) -> None:
    catalog = load_catalog()
    catalog.setdefault("categories", {})[name] = {"folder": folder, "price": int(price)}
    save_catalog(catalog)

def remove_category(name: str) -> bool:
    catalog = load_catalog()
    if name in catalog.get("categories", {}):
        del catalog["categories"][name]
        save_catalog(catalog)
        return True
    return False

def update_category_price(name: str, price: int) -> bool:
    catalog = load_catalog()
    if name in catalog.get("categories", {}):
        catalog["categories"][name]["price"] = int(price)
        save_catalog(catalog)
        return True
    return False

def add_proxy(name: str, folder: str, price: int, flag: str = "") -> None:
    catalog = load_catalog()
    catalog.setdefault("proxies", {})[name] = {"folder": folder, "price": int(price), "flag": flag or ""}
    save_catalog(catalog)

def remove_proxy(name: str) -> bool:
    catalog = load_catalog()
    if name in catalog.get("proxies", {}):
        del catalog["proxies"][name]
        save_catalog(catalog)
        return True
    return False

def update_proxy_price(name: str, price: int) -> bool:
    catalog = load_catalog()
    if name in catalog.get("proxies", {}):
        catalog["proxies"][name]["price"] = int(price)
        save_catalog(catalog)
        return True
    return False

# -------------------- Настройки бота --------------------

def _default_settings() -> Dict[str, Any]:
    return {
        "require_subscription": True,
    }

def load_settings() -> Dict[str, Any]:
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("settings format invalid")
            data.setdefault("require_subscription", True)
            return data
    except (json.JSONDecodeError, FileNotFoundError, ValueError):
        data = _default_settings()
        save_settings(data)
        return data

def save_settings(settings: Dict[str, Any]) -> None:
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=4, ensure_ascii=False)

def get_require_subscription() -> bool:
    """Получает текущее значение требования подписки"""
    return load_settings().get("require_subscription", True)

def toggle_require_subscription() -> bool:
    """Переключает требование подписки и возвращает новое значение"""
    settings = load_settings()
    new_value = not settings.get("require_subscription", True)
    settings["require_subscription"] = new_value
    save_settings(settings)
    return new_value

# -------------------- Новая система: Динамические категории товаров --------------------

def get_product_categories() -> Dict[str, Dict[str, Any]]:
    """Получает все основные категории товаров (Accounts, Proxies, Custom categories)"""
    return load_catalog().get("product_categories", {})

def add_product_category(category_name: str, icon: str = "📦") -> bool:
    """Добавляет новую основную категорию товаров"""
    catalog = load_catalog()
    catalog.setdefault("product_categories", {})
    if category_name in catalog["product_categories"]:
        return False  # Категория уже существует
    catalog["product_categories"][category_name] = {
        "icon": icon,
        "items": {}
    }
    save_catalog(catalog)
    return True

def remove_product_category(category_name: str) -> bool:
    """Удаляет основную категорию товаров"""
    catalog = load_catalog()
    if category_name in catalog.get("product_categories", {}):
        del catalog["product_categories"][category_name]
        save_catalog(catalog)
        return True
    return False

def add_item_to_category(category_name: str, item_name: str, folder: str, price: int, flag: str = "") -> bool:
    """Добавляет товар (подкатегорию) в категорию"""
    catalog = load_catalog()
    if category_name not in catalog.get("product_categories", {}):
        return False
    
    items = catalog["product_categories"][category_name].get("items", {})
    items[item_name] = {
        "folder": folder,
        "price": int(price)
    }
    if flag:
        items[item_name]["flag"] = flag
    
    catalog["product_categories"][category_name]["items"] = items
    save_catalog(catalog)
    return True

def remove_item_from_category(category_name: str, item_name: str) -> bool:
    """Удаляет товар (подкатегорию) из категории"""
    catalog = load_catalog()
    if category_name not in catalog.get("product_categories", {}):
        return False
    
    items = catalog["product_categories"][category_name].get("items", {})
    if item_name in items:
        del items[item_name]
        save_catalog(catalog)
        return True
    return False

def update_item_price(category_name: str, item_name: str, price: int) -> bool:
    """Изменяет цену товара"""
    catalog = load_catalog()
    if category_name not in catalog.get("product_categories", {}):
        return False
    
    items = catalog["product_categories"][category_name].get("items", {})
    if item_name in items:
        items[item_name]["price"] = int(price)
        save_catalog(catalog)
        return True
    return False

def get_items_by_category(category_name: str) -> Dict[str, Dict[str, Any]]:
    """Получает все товары в категории"""
    categories = get_product_categories()
    if category_name not in categories:
        return {}
    return categories[category_name].get("items", {})

def get_item_info(category_name: str, item_name: str) -> Optional[Dict[str, Any]]:
    """Получает информацию о конкретном товаре"""
    items = get_items_by_category(category_name)
    return items.get(item_name)

def get_item_info_by_folder_new(folder: str) -> Tuple[Optional[str], Optional[str], Optional[Dict[str, Any]]]:
    """Получает категорию и название товара по папке (для новой системы)"""
    categories = get_product_categories()
    for cat_name, cat_data in categories.items():
        for item_name, item_data in cat_data.get("items", {}).items():
            if item_data.get("folder") == folder:
                return (cat_name, item_name, item_data)
    return (None, None, None)

