import os
from aiogram import Bot, Dispatcher, F, types
from aiogram.types import (
    Message, FSInputFile, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardButton, InlineKeyboardMarkup
)
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import ADMIN_IDS, CHANNEL_ID, CHANNEL_USERNAME
from database import (
    load_users,
    save_users,
    get_balance,
    update_balance,
    get_user_id_by_username,
    add_user,
    add_sale,
    get_unique_buyers_count,
    get_sales_sum_day,
    get_sales_sum_month,
    get_total_orders_count,
    get_avg_ticket_today,
    get_top_buyers,
    get_username_by_user_id,
    load_sales,
    get_categories,
    get_proxies,
    add_category,
    remove_category,
    update_category_price,
    add_proxy,
    remove_proxy,
    update_proxy_price,
    get_require_subscription,
    toggle_require_subscription,
    get_product_categories,
    add_product_category,
    remove_product_category,
    add_item_to_category,
    remove_item_from_category,
    update_item_price,
    get_items_by_category,
    get_item_info,
    get_item_info_by_folder_new,
)
from payments import create_crypto_invoice

NAV_COMMANDS = {
    "🛍️ Products",
    "📦 Stock",
    "👤 Profile",
    "◀ Back",
    "Back",
    "/cancel",
    "/start",
    "/admin",
}

# FSM для админки
class AdminStates(StatesGroup):
    wait_user_id = State()
    wait_amount = State()
    wait_user_line = State()
    # catalog management
    wait_cat_add_line = State()        # "Name | folder | price"
    wait_cat_price_value = State()     # price after selecting a category
    wait_proxy_add_line = State()      # "Name | folder | price | flag"
    wait_proxy_price_value = State()
    # new product categories
    wait_product_cat_name = State()    # name of new product category
    wait_product_cat_icon = State()    # icon for new product category
    wait_product_item_line = State()   # "Name | folder | price [| flag]" for adding item to category
    wait_product_item_price = State()  # new price for item

# All admin states for filtering
_ADMIN_STATES = [
    AdminStates.wait_user_id,
    AdminStates.wait_amount,
    AdminStates.wait_user_line,
    AdminStates.wait_cat_add_line,
    AdminStates.wait_cat_price_value,
    AdminStates.wait_proxy_add_line,
    AdminStates.wait_proxy_price_value,
    AdminStates.wait_product_cat_name,
    AdminStates.wait_product_cat_icon,
    AdminStates.wait_product_item_line,
    AdminStates.wait_product_item_price,
]

os.makedirs("data", exist_ok=True)

def _ensure_category_dirs():
    # Старая система совместимости
    for cat in get_categories().values():
        os.makedirs(f"data/{cat['folder']}", exist_ok=True)
    for p in get_proxies().values():
        os.makedirs(f"data/{p['folder']}", exist_ok=True)
    # Новая система
    for cat_name, cat_data in get_product_categories().items():
        for item_name, item_data in cat_data.get("items", {}).items():
            folder = item_data.get("folder")
            if folder:
                os.makedirs(f"data/{folder}", exist_ok=True)

def safe_callback_answer(callback: types.CallbackQuery, *args, **kwargs):
    try:
        return callback.answer(*args, **kwargs)
    except TelegramBadRequest:
        return None


def get_item_info_by_folder(folder: str):
    # Сначала пробуем новую систему
    cat_name, item_name, info = get_item_info_by_folder_new(folder)
    if cat_name:
        return ("product", cat_name, item_name, info)
    
    # Затем старая система совместимости
    for name, info in get_categories().items():
        if info.get("folder") == folder:
            return ("account", name, None, info)
    for name, info in get_proxies().items():
        if info.get("folder") == folder:
            return ("proxy", name, None, info)
    return (None, None, None, None)

async def require_subscription_for_message(bot: Bot, message: Message) -> bool:
    user_id = message.from_user.id
    if user_id in ADMIN_IDS:
        return True
    # Проверяем включена ли обязанность подписки
    if not get_require_subscription():
        return True
    subscribed = await is_user_subscribed(bot, user_id)
    if not subscribed:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Subscribe", url=f"https://t.me/{CHANNEL_USERNAME}")],
            [InlineKeyboardButton(text="Check subscription", callback_data="check_sub")]
        ])
        await message.answer(
            "❗ To use this bot, please subscribe to @{}\n\nAfter subscribing, tap \"Check subscription\".".format(CHANNEL_USERNAME),
            reply_markup=kb
        )
        return False
    return True

async def require_subscription_for_callback(bot: Bot, callback: types.CallbackQuery) -> bool:
    user_id = callback.from_user.id
    if user_id in ADMIN_IDS:
        return True
    # Проверяем включена ли обязанность подписки
    if not get_require_subscription():
        return True
    subscribed = await is_user_subscribed(bot, user_id)
    if not subscribed:
        await callback.answer("❌ Subscribe to use the bot.", show_alert=True)
        return False
    return True

async def is_user_subscribed(bot: Bot, user_id: int) -> bool:
    """Check whether the user is subscribed to the channel"""
    chat_id = CHANNEL_ID
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        status = member.status
        print(f"User {user_id} status in {chat_id}: {status}")

        # Ensure the user hasn't left or been kicked
        if status in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
            return False

        # Subscribed: MEMBER, ADMINISTRATOR, CREATOR/OWNER, RESTRICTED
        if status in [
            ChatMemberStatus.MEMBER,
            ChatMemberStatus.ADMINISTRATOR,
            ChatMemberStatus.CREATOR,
            ChatMemberStatus.RESTRICTED
        ]:
            return True

        return False
    except Exception as e:
        print(f"Error checking subscription for user {user_id}: {repr(e)}")
        return False

async def send_main_menu(bot: Bot, user_id: int, as_admin: bool = False):
    """Send main menu to the user (only for admins or после проверки подписки)"""
    if not as_admin:
        # Проверяем подписку только если она включена
        if user_id not in ADMIN_IDS and get_require_subscription():
            subscribed = await is_user_subscribed(bot, user_id)
            if not subscribed:
                return # Не слать меню вовсе!
    kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🛍️ Products"), KeyboardButton(text="📦 Stock")],
        [KeyboardButton(text="👤 Profile")]
    ], resize_keyboard=True)

    await bot.send_photo(
        user_id,
        photo=FSInputFile("shopheader16.jpg"),
        caption=(
            "<b>👋 Welcome to ONION Shop!</b>\n\n"
            "Use the buttons below to navigate ⬇️"
        ),
        reply_markup=kb
    )

def register_handlers(dp: Dispatcher, bot: Bot):
    """Register all handlers"""
    
    # /start with subscription check
    @dp.message(CommandStart())
    async def cmd_start(message: Message):
        user_id = message.from_user.id
        username = message.from_user.username or ""
        add_user(user_id, username)

        # Always show menu to admin without subscription check
        if user_id in ADMIN_IDS:
            await send_main_menu(bot, user_id, as_admin=True)
            return

        # Если подписка не требуется, сразу показываем меню
        if not get_require_subscription():
            await send_main_menu(bot, user_id)
            return

        subscribed = await is_user_subscribed(bot, user_id)
        if not subscribed:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Subscribe", url=f"https://t.me/{CHANNEL_USERNAME}")],
                [InlineKeyboardButton(text="Check subscription", callback_data="check_sub")]
            ])
            await message.answer(
                f"❗ To use this bot, please subscribe to @{CHANNEL_USERNAME}\n\nAfter subscribing, tap \"Check subscription\".",
                reply_markup=kb
            )
            return
        # Пользователь подписан, показываем стартовое меню
        await send_main_menu(bot, user_id)

    @dp.message(F.state.in_(_ADMIN_STATES), lambda m: (m.text or "") in NAV_COMMANDS)
    async def admin_state_navigation(message: Message, state: FSMContext):
        await state.clear()
        await message.answer("✅ Operation canceled. Use menu buttons again.")

    # Subscription check button
    @dp.callback_query(F.data == "check_sub")
    async def check_subscription(callback: types.CallbackQuery):
        user_id = callback.from_user.id
        chat_id = CHANNEL_ID

        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            status = member.status
            print(f"User {user_id} status in {chat_id}: {status}")

            if status not in [ChatMemberStatus.LEFT, ChatMemberStatus.KICKED]:
                await callback.message.edit_text(
                    "✅ You are subscribed! You can now use the bot.",
                    reply_markup=None
                )
                await send_main_menu(bot, user_id)
            else:
                await callback.answer("❌ You are not subscribed. Please subscribe.", show_alert=True)
        except Exception as e:
            print(f"Subscription check error: {repr(e)}")
            await callback.answer("⚠️ Failed to check subscription. Try again later.", show_alert=True)

    # Admin panel
    @dp.message(Command("admin"))
    async def admin_panel(message: Message, state: FSMContext):
        if message.from_user.id not in ADMIN_IDS:
            # Разрешаем также администраторам канала
            try:
                member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=message.from_user.id)
                if member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
                    return
            except Exception:
                return
        
        # Получаем текущее значение требования подписки
        require_sub = get_require_subscription()
        sub_status = "🔓 Subscription OFF" if not require_sub else "🔒 Subscription ON"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton(text="💰 Adjust balance", callback_data="admin_adjust_balance")],
            [InlineKeyboardButton(text="🗂 Manage catalog", callback_data="admin_catalog")],
            [InlineKeyboardButton(text="🏆 Top buyers", callback_data="admin_top_buyers")],
            [InlineKeyboardButton(text=sub_status, callback_data="admin_toggle_subscription")],
        ])
        await message.answer("🔐 Admin panel:", reply_markup=kb)

    # Toggle subscription requirement
    @dp.callback_query(F.data == "admin_toggle_subscription")
    async def admin_toggle_subscription(callback: types.CallbackQuery):
        if callback.from_user.id not in ADMIN_IDS:
            await safe_callback_answer(callback)
            return
        
        new_value = toggle_require_subscription()
        status_text = "✅ ON" if new_value else "❌ OFF"
        await callback.answer(f"Subscription requirement: {status_text}", show_alert=True)
        
        # Обновляем кнопку в меню
        require_sub = get_require_subscription()
        sub_status = "🔓 Subscription OFF" if not require_sub else "🔒 Subscription ON"
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📊 Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton(text="💰 Adjust balance", callback_data="admin_adjust_balance")],
            [InlineKeyboardButton(text="🗂 Manage catalog", callback_data="admin_catalog")],
            [InlineKeyboardButton(text="🏆 Top buyers", callback_data="admin_top_buyers")],
            [InlineKeyboardButton(text=sub_status, callback_data="admin_toggle_subscription")],
        ])
        await callback.message.edit_reply_markup(reply_markup=kb)

    # -------- Admin: catalog management --------
    @dp.callback_query(F.data == "admin_catalog")
    async def admin_catalog_menu(callback: types.CallbackQuery):
        if callback.from_user.id not in ADMIN_IDS:
            await safe_callback_answer(callback)
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Add account category", callback_data="adm_cat_add")],
            [InlineKeyboardButton(text="🗑 Remove account category", callback_data="adm_cat_del")],
            [InlineKeyboardButton(text="💲 Change account price", callback_data="adm_cat_price")],
            [InlineKeyboardButton(text="➕ Add proxy", callback_data="adm_prx_add")],
            [InlineKeyboardButton(text="🗑 Remove proxy", callback_data="adm_prx_del")],
            [InlineKeyboardButton(text="💲 Change proxy price", callback_data="adm_prx_price")],
            [InlineKeyboardButton(text="📦 Manage products", callback_data="adm_product_cat_menu")],
        ])
        await callback.message.answer("🗂 Catalog management:", reply_markup=kb)
        await safe_callback_answer(callback)

    # -------- Product Categories Management --------
    @dp.callback_query(F.data == "adm_product_cat_menu")
    async def adm_product_cat_menu(callback: types.CallbackQuery):
        if callback.from_user.id not in ADMIN_IDS:
            await safe_callback_answer(callback)
            return
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Add product category", callback_data="adm_prod_cat_add")],
            [InlineKeyboardButton(text="🗑 Remove product category", callback_data="adm_prod_cat_del")],
            [InlineKeyboardButton(text="✏️ Edit category items", callback_data="adm_prod_cat_edit")],
        ])
        await callback.message.answer("📦 Product Categories:", reply_markup=kb)
        await safe_callback_answer(callback)

    # Add product category
    @dp.callback_query(F.data == "adm_prod_cat_add")
    async def adm_prod_cat_add(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.answer("Enter product category name (e.g., Candies):")
        await state.set_state(AdminStates.wait_product_cat_name)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_product_cat_name)
    async def adm_prod_cat_name(message: Message, state: FSMContext):
        category_name = (message.text or "").strip()
        if not category_name:
            await message.answer("❌ Category name cannot be empty.")
            return
        await state.update_data(category_name=category_name)
        await message.answer("Enter category icon (emoji, e.g., 🍬):")
        await state.set_state(AdminStates.wait_product_cat_icon)

    @dp.message(AdminStates.wait_product_cat_icon)
    async def adm_prod_cat_icon(message: Message, state: FSMContext):
        icon = (message.text or "").strip()
        if not icon:
            icon = "📦"
        data = await state.get_data()
        category_name = data.get("category_name")
        
        ok = add_product_category(category_name, icon)
        if ok:
            await message.answer(f"✅ Product category added: {icon} {category_name}")
            _ensure_category_dirs()
        else:
            await message.answer("❌ Category already exists.")
        await state.clear()

    # Remove product category
    @dp.callback_query(F.data == "adm_prod_cat_del")
    async def adm_prod_cat_del(callback: types.CallbackQuery):
        categories = get_product_categories()
        if not categories:
            await callback.message.answer("❌ No product categories.")
            await safe_callback_answer(callback)
            return
        kb = InlineKeyboardBuilder()
        for cat_name, cat_data in categories.items():
            icon = cat_data.get("icon", "📦")
            kb.button(text=f"🗑 {icon} {cat_name}", callback_data=f"adm_del_prod_cat:{cat_name}")
        kb.adjust(1)
        await callback.message.answer("Choose a category to remove:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("adm_del_prod_cat:"))
    async def adm_del_prod_cat(callback: types.CallbackQuery):
        category_name = callback.data.split(":", 1)[1]
        ok = remove_product_category(category_name)
        if ok:
            await callback.message.answer(f"✅ Category {category_name} removed.")
        else:
            await callback.message.answer("❌ Category not found.")
        await safe_callback_answer(callback)

    # Edit category items (add/remove/change price)
    @dp.callback_query(F.data == "adm_prod_cat_edit")
    async def adm_prod_cat_edit(callback: types.CallbackQuery):
        categories = get_product_categories()
        if not categories:
            await callback.message.answer("❌ No product categories.")
            await safe_callback_answer(callback)
            return
        kb = InlineKeyboardBuilder()
        for cat_name, cat_data in categories.items():
            icon = cat_data.get("icon", "📦")
            kb.button(text=f"{icon} {cat_name}", callback_data=f"adm_edit_prod_cat:{cat_name}")
        kb.adjust(1)
        await callback.message.answer("Choose a category to edit:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("adm_edit_prod_cat:"))
    async def adm_edit_prod_cat(callback: types.CallbackQuery):
        category_name = callback.data.split(":", 1)[1]
        items = get_items_by_category(category_name)
        
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Add item", callback_data=f"adm_add_prod_item:{category_name}")
        
        if items:
            for item_name in items.keys():
                kb.button(text=f"✏️ {item_name}", callback_data=f"adm_edit_prod_item:{category_name}:{item_name}")
        
        kb.adjust(1)
        await callback.message.answer(f"Edit items in {category_name}:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    # Add item to category
    @dp.callback_query(F.data.startswith("adm_add_prod_item:"))
    async def adm_add_prod_item(callback: types.CallbackQuery, state: FSMContext):
        category_name = callback.data.split(":", 1)[1]
        await state.update_data(category_name=category_name, edit_item_name=None)
        await callback.message.answer("Enter item details on one line:\nName | folder | price [| flag (optional)]\n\nExample: Chocolate | candies_chocolate | 10")
        await state.set_state(AdminStates.wait_product_item_line)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_product_item_line)
    async def adm_product_item_line(message: Message, state: FSMContext):
        line = (message.text or "").strip()
        parts = [p.strip() for p in line.split("|")]
        
        if len(parts) < 3 or len(parts) > 4:
            await message.answer("Format: Name | folder | price [| flag]\nExample: Chocolate | candies_chocolate | 10 | 🍫")
            return
        
        item_name = parts[0]
        folder = parts[1]
        price_str = parts[2]
        flag = parts[3] if len(parts) == 4 else ""
        
        try:
            price = int(price_str)
        except ValueError:
            await message.answer("❌ Price must be an integer")
            return
        
        data = await state.get_data()
        category_name = data.get("category_name")
        
        ok = add_item_to_category(category_name, item_name, folder, price, flag)
        if ok:
            _ensure_category_dirs()
            await message.answer(f"✅ Item added: {item_name} ({folder}) — {price}$")
        else:
            await message.answer("❌ Could not add item. Category might not exist.")
        
        await state.clear()

    # Edit item (view options)
    @dp.callback_query(F.data.startswith("adm_edit_prod_item:"))
    async def adm_edit_prod_item(callback: types.CallbackQuery):
        parts = callback.data.split(":")
        category_name = parts[1]
        item_name = parts[2]
        
        item_info = get_item_info(category_name, item_name)
        if not item_info:
            await callback.message.answer("❌ Item not found.")
            await safe_callback_answer(callback)
            return
        
        price = item_info.get("price", 0)
        folder = item_info.get("folder", "")
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💲 Change price", callback_data=f"adm_chg_item_price:{category_name}:{item_name}")],
            [InlineKeyboardButton(text="🗑 Remove item", callback_data=f"adm_del_prod_item:{category_name}:{item_name}")],
            [InlineKeyboardButton(text="◀ Back", callback_data=f"adm_edit_prod_cat:{category_name}")],
        ])
        
        info_text = f"Item: {item_name}\nFolder: {folder}\nPrice: {price}$"
        await callback.message.answer(info_text, reply_markup=kb)
        await safe_callback_answer(callback)

    # Change item price
    @dp.callback_query(F.data.startswith("adm_chg_item_price:"))
    async def adm_chg_item_price(callback: types.CallbackQuery, state: FSMContext):
        parts = callback.data.split(":")
        category_name = parts[1]
        item_name = parts[2]
        
        await state.update_data(category_name=category_name, item_name=item_name)
        await callback.message.answer(f"Enter new price for {item_name}:")
        await state.set_state(AdminStates.wait_product_item_price)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_product_item_price)
    async def adm_product_item_price(message: Message, state: FSMContext):
        try:
            price = int((message.text or "").strip())
        except ValueError:
            await message.answer("❌ Price must be an integer")
            return
        
        data = await state.get_data()
        category_name = data.get("category_name")
        item_name = data.get("item_name")
        
        ok = update_item_price(category_name, item_name, price)
        if ok:
            await message.answer(f"✅ Price updated: {item_name} — {price}$")
        else:
            await message.answer("❌ Could not update price.")
        
        await state.clear()

    # Delete item
    @dp.callback_query(F.data.startswith("adm_del_prod_item:"))
    async def adm_del_prod_item(callback: types.CallbackQuery):
        parts = callback.data.split(":")
        category_name = parts[1]
        item_name = parts[2]
        
        ok = remove_item_from_category(category_name, item_name)
        if ok:
            await callback.message.answer(f"✅ Item {item_name} removed.")
        else:
            await callback.message.answer("❌ Item not found.")
        
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "adm_cat_add")
    async def adm_cat_add(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.answer("Send on one line: Name | folder | price")
        await state.set_state(AdminStates.wait_cat_add_line)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_cat_add_line)
    async def adm_cat_add_line(message: Message, state: FSMContext):
        line = (message.text or "").strip()
        parts = [p.strip() for p in line.split("|")]
        if len(parts) != 3:
            await message.answer("Format: Name | folder | price")
            return
        name, folder, price_str = parts
        try:
            price = int(price_str)
        except ValueError:
            await message.answer("Price must be an integer")
            return
        add_category(name, folder, price)
        _ensure_category_dirs()
        await message.answer(f"✅ Category added: {name} ({folder}) — {price}$")
        await state.clear()

    @dp.callback_query(F.data == "adm_cat_del")
    async def adm_cat_del(callback: types.CallbackQuery):
        cats = list(get_categories().keys())
        if not cats:
            await callback.message.answer("No account categories.")
            await safe_callback_answer(callback)
            return
        kb = InlineKeyboardBuilder()
        for name in cats:
            kb.button(text=f"🗑 {name}", callback_data=f"adm_del_cat:{name}")
        kb.adjust(1)
        await callback.message.answer("Choose a category to remove:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("adm_del_cat:"))
    async def adm_del_cat(callback: types.CallbackQuery):
        name = callback.data.split(":", 1)[1]
        ok = remove_category(name)
        await callback.message.answer("✅ Removed" if ok else "❌ Not found")
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "adm_cat_price")
    async def adm_cat_price(callback: types.CallbackQuery, state: FSMContext):
        kb = InlineKeyboardBuilder()
        for name in get_categories().keys():
            kb.button(text=f"💲 {name}", callback_data=f"adm_price_cat:{name}")
        kb.adjust(1)
        await callback.message.answer("Choose a category to change price:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("adm_price_cat:"))
    async def adm_price_cat_select(callback: types.CallbackQuery, state: FSMContext):
        name = callback.data.split(":", 1)[1]
        await state.update_data(target_cat=name)
        await callback.message.answer(f"Enter new price for: {name}")
        await state.set_state(AdminStates.wait_cat_price_value)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_cat_price_value)
    async def adm_cat_price_value(message: Message, state: FSMContext):
        data = await state.get_data()
        name = data.get("target_cat")
        try:
            price = int((message.text or "").strip())
        except ValueError:
            await message.answer("Enter integer price")
            return
        ok = update_category_price(name, price)
        await message.answer("✅ Price updated" if ok else "❌ Category not found")
        await state.clear()

    @dp.callback_query(F.data == "adm_prx_add")
    async def adm_prx_add(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.answer("Send on one line: Name | folder | price | flag (emoji)")
        await state.set_state(AdminStates.wait_proxy_add_line)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_proxy_add_line)
    async def adm_prx_add_line(message: Message, state: FSMContext):
        line = (message.text or "").strip()
        parts = [p.strip() for p in line.split("|")]
        if len(parts) != 4:
            await message.answer("Format: Name | folder | price | flag")
            return
        name, folder, price_str, flag = parts
        try:
            price = int(price_str)
        except ValueError:
            await message.answer("Price must be an integer")
            return
        add_proxy(name, folder, price, flag)
        _ensure_category_dirs()
        await message.answer(f"✅ Proxy added: {name} ({folder}) — {price}$ {flag}")
        await state.clear()

    @dp.callback_query(F.data == "adm_prx_del")
    async def adm_prx_del(callback: types.CallbackQuery):
        prx = list(get_proxies().keys())
        if not prx:
            await callback.message.answer("No proxies.")
            await safe_callback_answer(callback)
            return
        kb = InlineKeyboardBuilder()
        for name in prx:
            kb.button(text=f"🗑 {name}", callback_data=f"adm_del_prx:{name}")
        kb.adjust(1)
        await callback.message.answer("Choose a proxy to remove:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("adm_del_prx:"))
    async def adm_del_prx(callback: types.CallbackQuery):
        name = callback.data.split(":", 1)[1]
        ok = remove_proxy(name)
        await callback.message.answer("✅ Removed" if ok else "❌ Not found")
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "adm_prx_price")
    async def adm_prx_price(callback: types.CallbackQuery, state: FSMContext):
        kb = InlineKeyboardBuilder()
        for name in get_proxies().keys():
            kb.button(text=f"💲 {name}", callback_data=f"adm_price_prx:{name}")
        kb.adjust(1)
        await callback.message.answer("Choose a proxy to change price:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("adm_price_prx:"))
    async def adm_price_prx_select(callback: types.CallbackQuery, state: FSMContext):
        name = callback.data.split(":", 1)[1]
        await state.update_data(target_prx=name)
        await callback.message.answer(f"Enter new price for: {name}")
        await state.set_state(AdminStates.wait_proxy_price_value)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_proxy_price_value)
    async def adm_prx_price_value(message: Message, state: FSMContext):
        data = await state.get_data()
        name = data.get("target_prx")
        try:
            price = int((message.text or "").strip())
        except ValueError:
            await message.answer("Enter integer price")
            return
        ok = update_proxy_price(name, price)
        await message.answer("✅ Price updated" if ok else "❌ Proxy not found")
        await state.clear()

    @dp.message(AdminStates.wait_user_id)
    async def process_user_id(message: Message, state: FSMContext):
        text = message.text.strip()
        if text.startswith("@"):
            user_id = get_user_id_by_username(text)
            if user_id is None:
                await message.answer("❌ Username not found.")
                return
        elif text.isdigit():
            user_id = int(text)
        else:
            await message.answer("❌ Enter a valid @username or numeric user ID.")
            return

        await state.update_data(user_id=user_id)
        await message.answer("💰 Enter amount to adjust:")
        await state.set_state(AdminStates.wait_amount)

    @dp.message(AdminStates.wait_amount)
    async def process_amount(message: Message, state: FSMContext):
        text = message.text.strip()

        # Validate number (can be signed)
        try:
            amount = int(text)
        except ValueError:
            await message.answer("❌ Enter a valid number (e.g., 100 or -50).")
            return

        data = await state.get_data()
        user_id = data["user_id"]

        # Update balance
        update_balance(user_id, amount)

        # Operation type
        if amount > 0:
            operation_text = f"credited {amount}$"
            user_text = f"💰 Your balance was credited by {amount}$ by admin."
        elif amount < 0:
            operation_text = f"debited {-amount}$"
            user_text = f"⚠️ {-amount}$ was debited from your balance by admin."
        else:
            await message.answer("❌ Amount cannot be zero.")
            return

        await message.answer(f"✅ User with ID {user_id} {operation_text}.")

        # Отправляем уведомление пользователю
        try:
            await bot.send_message(user_id, user_text)
        except Exception as e:
            print(f"Error sending message to user {user_id}: {e}")

        await state.clear()

    # Новый упрощенный ввод: "@username 100" или "@username -100"
    @dp.callback_query(F.data == "admin_adjust_balance")
    async def admin_adjust_balance_start(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.answer("Enter on one line: @username amount (e.g., @user 100 or @user -50)")
        await state.set_state(AdminStates.wait_user_line)
        await safe_callback_answer(callback)

    @dp.message(AdminStates.wait_user_line)
    async def admin_adjust_balance_process(message: Message, state: FSMContext):
        text = (message.text or "").strip()
        parts = text.split()
        if len(parts) != 2 or not parts[0].startswith("@"):
            await message.answer("Format: @username amount. Example: @user 100")
            return
        username, amount_str = parts
        try:
            amount = int(amount_str)
        except ValueError:
            await message.answer("Amount must be a number. Example: @user 100")
            return
        user_id = get_user_id_by_username(username)
        if user_id is None:
            await message.answer("❌ This @username not found in DB. The user must write to the bot once.")
            return
        update_balance(user_id, amount)
        # Сообщение пользователю
        try:
            if amount > 0:
                await message.bot.send_message(user_id, f"💰 Your balance was credited by {amount}$ by admin.")
            else:
                await message.bot.send_message(user_id, f"⚠️ {-amount}$ was debited from your balance by admin.")
        except Exception:
            pass
        sign = "+" if amount > 0 else ""
        await message.answer(f"✅ Balance of {username} changed by {sign}{amount}$")
        await state.clear()

    @dp.callback_query(F.data == "admin_stats")
    async def admin_stats(callback: types.CallbackQuery):
        users = load_users()
        total_users = len(users)
        unique_buyers = get_unique_buyers_count()
        sales_day = get_sales_sum_day()
        sales_month = get_sales_sum_month()
        orders_total = get_total_orders_count()
        avg_ticket = get_avg_ticket_today()
        sales_all = sum(int(s.get("total_price", 0)) for s in load_sales())
        conversion = (unique_buyers / total_users * 100) if total_users else 0
        text = (
            "📊 Statistics:\n"
            f"👥 Total users: {total_users}\n"
            f"🛒 Unique buyers: {unique_buyers}\n"
            f"📈 Conversion: {conversion:.1f}%\n"
            f"💵 Sales today: {sales_day}$\n"
            f"💵 Sales this month: {sales_month}$\n"
            f"💳 Avg ticket today: {avg_ticket:.2f}$\n"
            f"🧾 Total orders: {orders_total}\n"
            f"💰 Revenue total: {sales_all}$\n"
        )
        await callback.message.answer(text)
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "admin_top_buyers")
    async def admin_top_buyers(callback: types.CallbackQuery):
        top = get_top_buyers(limit=5)
        if not top:
            await callback.message.answer("No purchases yet.")
            await safe_callback_answer(callback)
            return
        lines = ["🏆 Top buyers:"]
        for idx, (uid, spent) in enumerate(top, start=1):
            uname = get_username_by_user_id(uid)
            display = f"@{uname}" if uname else str(uid)
            lines.append(f"{idx}. {display} — {spent}$")
        await callback.message.answer("\n".join(lines))
        await safe_callback_answer(callback)

    # Категории товаров
    @dp.message(F.text == "🛍️ Products")
    async def show_categories(message: Message):
        if not await require_subscription_for_message(bot, message):
            return
        kb = InlineKeyboardBuilder()
        kb.button(text="🧾 Accounts", callback_data="cat_accounts")
        kb.button(text="🧰 Proxies", callback_data="cat_proxies")

        product_categories = get_product_categories()
        for cat_name, cat_data in product_categories.items():
            if cat_name in ["Accounts", "Proxies"]:
                continue
            icon = cat_data.get("icon", "📦")
            kb.button(text=f"{icon} {cat_name}", callback_data=f"cat_prod:{cat_name}")

        kb.button(text="◀ Back", callback_data="back_main")
        kb.adjust(2, 1)
        await message.answer("Choose a category:", reply_markup=kb.as_markup())

    @dp.callback_query(F.data == "back_main")
    async def back_to_main(callback: types.CallbackQuery):
        await send_main_menu(bot, callback.from_user.id)
        await safe_callback_answer(callback)

    # Обработчик для новой системы категорий товаров
    @dp.callback_query(F.data.startswith("cat_prod:"))
    async def show_product_category_items(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        category_name = callback.data.split(":", 1)[1]
        items = get_items_by_category(category_name)
        
        if not items:
            kb = InlineKeyboardBuilder()
            kb.button(text="◀ Back", callback_data="cat_root")
            kb.adjust(1)
            await callback.message.answer(f"❌ No items in <b>{category_name}</b>.", reply_markup=kb.as_markup())
            await safe_callback_answer(callback)
            return
        
        kb = InlineKeyboardBuilder()
        for item_name, item_data in items.items():
            price = item_data.get("price", 0)
            kb.button(text=f"{item_name} | {price}$", callback_data=f"cat_prod_item:{category_name}:{item_name}")
        kb.button(text="◀ Back", callback_data="cat_root")
        kb.adjust(1)
        
        cat_data = get_product_categories().get(category_name, {})
        icon = cat_data.get("icon", "📦")
        await callback.message.answer(f"{icon} <b>{category_name}</b>", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    # Обработчик для выбора конкретного товара в категории
    @dp.callback_query(F.data.startswith("cat_prod_item:"))
    async def select_product_item(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        parts = callback.data.split(":")
        category_name = parts[1]
        item_name = parts[2]
        
        item_info = get_item_info(category_name, item_name)
        if not item_info:
            await callback.message.answer("❌ Item not found.")
            await safe_callback_answer(callback)
            return
        
        folder_path = f"data/{item_info['folder']}"
        _ensure_category_dirs()
        
        try:
            files = os.listdir(folder_path)
        except FileNotFoundError:
            files = []
        
        kb = InlineKeyboardBuilder()
        if files:
            price = item_info.get("price", 0)
            kb.button(text=f"Buy | {price}$", callback_data=f"buy:{item_info['folder']}")
        kb.button(text="◀ Back", callback_data=f"cat_prod:{category_name}")
        kb.adjust(1)
        
        if not files:
            await callback.message.answer(f"❌ <b>{item_name}</b> is out of stock.", reply_markup=kb.as_markup())
        else:
            await callback.message.answer(f"📦 <b>{item_name}</b> | {len(files)} available", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "cat_root")
    async def show_root(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        kb = InlineKeyboardBuilder()
        kb.button(text="🧾 Accounts", callback_data="cat_accounts")
        kb.button(text="🧰 Proxies", callback_data="cat_proxies")

        product_categories = get_product_categories()
        for cat_name, cat_data in product_categories.items():
            if cat_name in ["Accounts", "Proxies"]:
                continue
            icon = cat_data.get("icon", "📦")
            kb.button(text=f"{icon} {cat_name}", callback_data=f"cat_prod:{cat_name}")

        kb.button(text="◀ Back", callback_data="back_main")
        kb.adjust(2, 1)
        await callback.message.answer("Choose a section:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "cat_accounts")
    async def show_accounts_categories(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        kb = InlineKeyboardBuilder()
        for name in get_categories().keys():
            kb.button(text=name, callback_data=name)
        kb.button(text="◀ Back", callback_data="cat_root")
        kb.adjust(2, 1)
        await callback.message.answer("Choose an account category:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.func(lambda d: d in get_categories().keys()))
    async def show_items(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        cat_name = callback.data
        info = get_categories().get(cat_name)
        folder_path = f"data/{info['folder']}"
        _ensure_category_dirs()
        files = os.listdir(folder_path)
        kb = InlineKeyboardBuilder()
        if files:
            kb.button(text=f"Account | {info['price']}$", callback_data=f"buy:{info['folder']}")
        kb.button(text="◀ Back", callback_data="cat_accounts")
        kb.adjust(1)
        if not files:
            await callback.message.answer(f"❌ No items in <b>{cat_name}</b> category.", reply_markup=kb.as_markup())
        else:
            await callback.message.answer(
                f"📃 Category: <b>{cat_name}</b>",
                reply_markup=kb.as_markup()
            )
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "cat_proxies")
    async def show_proxies(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        kb = InlineKeyboardBuilder()
        for name, p in get_proxies().items():
            kb.button(text=f"{name} {p['flag']}", callback_data=name)
        kb.button(text="◀ Back", callback_data="cat_root")
        kb.adjust(1)
        await callback.message.answer("Choose a SOCKS5 option:", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.func(lambda d: d in get_proxies().keys()))
    async def show_proxy_item(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        name = callback.data
        info = get_proxies().get(name)
        folder_path = f"data/{info['folder']}"
        _ensure_category_dirs()
        files = os.listdir(folder_path)
        kb = InlineKeyboardBuilder()
        if files:
            kb.button(text=f"SOCKS5 | {name.split(' ', 1)[1]} | {info['price']}$", callback_data=f"buy:{info['folder']}")
        kb.button(text="◀ Back", callback_data="cat_proxies")
        kb.adjust(1)
        if not files:
            await callback.message.answer(f"❌ Option <b>{name}</b> is out of stock.", reply_markup=kb.as_markup())
        else:
            await callback.message.answer(f"📡 Proxy: <b>{name}</b>", reply_markup=kb.as_markup())
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("buy:"))
    async def choose_quantity(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        folder = callback.data.split(":")[1]
        result = get_item_info_by_folder(folder)
        _type = result[0]
        info = result[-1]
        price = info["price"] if info else None

        kb = InlineKeyboardBuilder()
        for qty in range(1, 6):  # from 1 to 5
            kb.button(text=str(qty), callback_data=f"buy_qty:{folder}:{qty}")
        # Back to item view depending on type
        if _type == "account":
            kb.button(text="◀ Back", callback_data="cat_accounts")
        elif _type == "proxy":
            kb.button(text="◀ Back", callback_data="cat_proxies")
        elif _type == "product":
            cat_name = result[1]
            kb.button(text="◀ Back", callback_data=f"cat_prod:{cat_name}")

        kb.adjust(5, 1)

        if _type == "product":
            item_name = result[2]
            title = item_name
        elif _type == "account":
            title = "accounts"
        else:
            title = "proxies"
        
        await callback.message.answer(
            f"Choose quantity of {title} at {price}$ each:",
            reply_markup=kb.as_markup()
        )
        await safe_callback_answer(callback)

    @dp.callback_query(F.data.startswith("buy_qty:"))
    async def process_purchase(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        _, folder, qty_str = callback.data.split(":")
        quantity = int(qty_str)
        user_id = str(callback.from_user.id)
        users = load_users()
        result = get_item_info_by_folder(folder)
        _type = result[0]
        info = result[-1]
        price = info["price"] if info else None
        total_price = price * quantity

        if not os.path.exists(f"data/{folder}"):
            await callback.message.answer("❌ Category not found.")
            return

        files = os.listdir(f"data/{folder}")
        if len(files) < quantity:
            await callback.message.answer(f"❌ Not enough items in stock. Only {len(files)} available.")
            return

        if users.get(user_id, {}).get("balance", 0) < total_price:
            await callback.message.answer(
                f"❌ Insufficient funds. Your balance: {users.get(user_id, {}).get('balance', 0)}$, required {total_price}$.")
            return

        try:
            update_balance(callback.from_user.id, -total_price)
            for i in range(quantity):
                filename = files[i]
                path = f"data/{folder}/{filename}"
                await callback.message.answer_document(document=FSInputFile(path),
                                                       caption=f"Your item 🍪 ({i + 1}/{quantity})")
                os.remove(path)
            # Логируем продажу
            add_sale(callback.from_user.id, total_price, quantity, folder, _type or "unknown")
        except Exception as e:
            await callback.message.answer(f"❌ Error while delivering item: {str(e)}")
            return

        if _type == "product":
            noun = result[2]  # item_name
        elif _type == "account":
            noun = "accounts"
        else:
            noun = "proxies"
        
        await callback.answer(f"✅ You purchased {quantity} {noun} for {total_price}$.")

    # Проверка наличия
    @dp.message(F.text == "📦 Stock")
    async def check_stock(message: Message):
        if not await require_subscription_for_message(bot, message):
            return
        text = ""
        
        # Старая система (совместимость)
        cats = get_categories()
        if cats:
            text += "➖➖➖ Accounts ➖➖➖\n"
            for name, info in cats.items():
                folder = f"data/{info['folder']}"
                try:
                    count = len(os.listdir(folder))
                except:
                    count = 0
                text += f"{name} | {info['price']}$ | {count} pcs\n"
        
        prx = get_proxies()
        if prx:
            text += "\n➖➖➖ SOCKS5 Proxies ➖➖➖\n"
            for name, info in prx.items():
                folder = f"data/{info['folder']}"
                try:
                    count = len(os.listdir(folder))
                except:
                    count = 0
                country = name.split(' ', 1)[1]
                text += f"{country} | {info.get('flag','')} | {info['price']}$ | {count} pcs\n"
        
        # Новая система
        product_categories = get_product_categories()
        for cat_name, cat_data in product_categories.items():
            if cat_name not in ["Accounts", "Proxies"]:  # Не дублировать старые категории
                icon = cat_data.get("icon", "📦")
                text += f"\n➖➖➖ {icon} {cat_name} ➖➖➖\n"
                for item_name, item_data in cat_data.get("items", {}).items():
                    folder = item_data.get("folder")
                    price = item_data.get("price", 0)
                    try:
                        count = len(os.listdir(f"data/{folder}"))
                    except:
                        count = 0
                    text += f"{item_name} | {price}$ | {count} pcs\n"
        
        await message.answer(text if text else "❌ No stock available.")

    # Профиль
    @dp.message(F.text == "👤 Profile")
    async def profile(message: Message):
        if not await require_subscription_for_message(bot, message):
            return
        balance = get_balance(message.from_user.id)
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Top up", callback_data="topup")],
            [InlineKeyboardButton(text="Rules", callback_data="rules")],
            [InlineKeyboardButton(text="Help", callback_data="help")]
        ])
        await message.answer(f"Name: {message.from_user.full_name}\n💰 Balance: {balance}$", reply_markup=kb)

    @dp.callback_query(F.data == "topup")
    async def topup_start(callback: types.CallbackQuery):
        if not await require_subscription_for_callback(bot, callback):
            return
        await callback.message.answer("💸 Send the top-up amount:")
        await safe_callback_answer(callback)

    @dp.message(lambda m: m.text and m.text.isdigit())
    async def handle_amount(message: Message):
        if not await require_subscription_for_message(bot, message):
            return
        amount = int(message.text)
        if amount <= 0:
            await message.answer("❌ Amount must be positive.")
            return
        url = create_crypto_invoice(message.from_user.id, amount)
        if url:
            btn = InlineKeyboardButton(text="💳 Proceed to payment", url=url)
            markup = InlineKeyboardMarkup(inline_keyboard=[[btn]])
            await message.answer(f"Amount: {amount}$\nClick the button below to pay via CryptoBot:", reply_markup=markup)
        else:
            await message.answer("❌ Failed to create invoice. Try again later.")

    @dp.callback_query(F.data == "rules")
    async def rules(callback: types.CallbackQuery):
        await callback.message.answer(
            "📜 Rules / Правила:\n\n"
            "EN:\n"
            "1) Do not use items from this shop for actions that violate the laws of your country.\n"
            "2) By purchasing, you automatically accept all rules and take full responsibility for your use.\n"
            "3) Replacement or refund to balance is possible only if support confirms the item is invalid. Evidence is required (screenshots/video). Any fraud attempt leads to denial and possible ban.\n"
            "4) No refunds for misuse, lack of skills, service/proxy blocks or limits, changes in service rules/policies, or if the item was partially used or shared with third parties.\n"
            "5) Check the item immediately after purchase — validity and operability are time‑limited.\n\n"
            "RU:\n"
            "1) Запрещено использовать товары из этого магазина для действий, противоречащих законам вашей страны.\n"
            "2) Покупая товар, вы автоматически соглашаетесь с правилами и берёте полную ответственность на себя.\n"
            "3) Замена или возврат на баланс возможны только при подтверждённой саппортом недействительности товара. Нужны доказательства (скриншоты/видео). Попытка обмана ведёт к отказу и блокировке.\n"
            "4) Возврат не делается из‑за неправильного использования, отсутствия навыков, блокировок/лимитов со стороны сервисов и прокси, изменений их правил/политик, а также если товар частично использован или передан третьим лицам.\n"
            "5) Проверяйте товар сразу после покупки — актуальность и работоспособность ограничены временем.\n"
        )
        await safe_callback_answer(callback)

    @dp.callback_query(F.data == "help")
    async def help_msg(callback: types.CallbackQuery):
        await callback.message.answer("🔧 Support: @OnionSupport1\n📬 For any questions — write to us.")
        await safe_callback_answer(callback)

    # Загрузка товаров админом
    @dp.message(F.document)
    async def handle_cookie_upload(message: Message):
        # Allow upload for global ADMIN_IDS or channel admins/owner
        is_admin = (message.from_user.id in ADMIN_IDS)
        if not is_admin:
            try:
                member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=message.from_user.id)
                if member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]:
                    is_admin = True
            except Exception:
                pass
        if not is_admin:
            return
        file = message.document
        filename = (file.file_name or "").lower()

        if not filename.endswith(".txt"):
            await message.answer("❌ Only .txt files are allowed.")
            return

        _ensure_category_dirs()
        # Сначала проверяем старую систему (аккаунты)
        for name, cat in get_categories().items():
            if cat['folder'] in filename:
                path = f"data/{cat['folder']}/{filename}"
                await bot.download(file=file.file_id, destination=path)
                await message.answer(f"✅ File added to category: {name}")
                return

        # Затем проверяем старую систему (прокси)
        for name, p in get_proxies().items():
            if p['folder'] in filename:
                path = f"data/{p['folder']}/{filename}"
                await bot.download(file=file.file_id, destination=path)
                await message.answer(f"✅ File added to category: {name}")
                return

        # Проверяем новую систему (динамические категории)
        product_categories = get_product_categories()
        for cat_name, cat_data in product_categories.items():
            for item_name, item_data in cat_data.get("items", {}).items():
                folder = item_data.get("folder")
                if folder and folder in filename:
                    path = f"data/{folder}/{filename}"
                    await bot.download(file=file.file_id, destination=path)
                    await message.answer(f"✅ File added to {cat_name} → {item_name}")
                    return

        await message.answer("❌ Could not determine category from filename.")
