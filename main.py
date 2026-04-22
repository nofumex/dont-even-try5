import asyncio
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties

from config import BOT_TOKEN
from handlers import register_handlers
from payments import check_invoices

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

# Регистрация обработчиков
register_handlers(dp, bot)

# Запуск
if __name__ == "__main__":
    async def main():
        # Запускаем background task для проверки инвойсов
        asyncio.create_task(check_invoices(bot))
        # Запускаем polling
        await dp.start_polling(bot)

    asyncio.run(main())
