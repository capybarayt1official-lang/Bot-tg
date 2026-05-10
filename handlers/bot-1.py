import asyncio
import logging
from aiogram import Bot, Dispatcher, Router
from aiogram.types import Message
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN
from database import init_db
from handlers import start, profile, referrals, clans, work_shop, cases, battle
from profanity import contains_bad_words

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

dp.include_router(start.router)
dp.include_router(profile.router)
dp.include_router(referrals.router)
dp.include_router(clans.router)
dp.include_router(work_shop.router)
dp.include_router(cases.router)
dp.include_router(battle.router)

profanity_router = Router()

@profanity_router.message()
async def profanity_filter(message: Message):
    if message.text and contains_bad_words(message.text):
        try:
            await message.delete()
        except:
            pass
        await message.answer("🚫 Пожалуйста, не используй нецензурные слова!")

dp.include_router(profanity_router)

async def main():
    init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
