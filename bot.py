import asyncio
import csv
import os

import psycopg2
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from telegram import Bot

load_dotenv()

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHANNEL_ID = os.environ["TELEGRAM_CHANNEL_ID"]
THREAD_ID = int(os.environ["TELEGRAM_THREAD_ID"]) if os.environ.get("TELEGRAM_THREAD_ID") else None
DATABASE_URL = os.environ["DATABASE_URL"]
MESSAGES_FILE = os.path.join(os.path.dirname(__file__), "messages.csv")


def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_state (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    current_index INTEGER NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                INSERT INTO bot_state (id, current_index) VALUES (1, 0)
                ON CONFLICT (id) DO NOTHING
            """)
        conn.commit()


def get_index() -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT current_index FROM bot_state WHERE id = 1")
            return cur.fetchone()[0]


def set_index(index: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE bot_state SET current_index = %s WHERE id = 1", (index,))
        conn.commit()


def load_messages() -> list[str]:
    with open(MESSAGES_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row["message"].strip() for row in reader if row.get("message", "").strip()]


async def post_next():
    messages = load_messages()
    if not messages:
        print("No messages found in messages.csv")
        return

    index = get_index()
    message = messages[index % len(messages)]

    bot = Bot(token=BOT_TOKEN)
    async with bot:
        await bot.send_message(chat_id=CHANNEL_ID, text=message, message_thread_id=THREAD_ID)

    print(f"[{index + 1}/{len(messages)}] Posted: {message[:60]}...")
    set_index(index + 1)


async def main():
    init_db()

    # Post immediately on startup, then every hour
    await post_next()

    scheduler = AsyncIOScheduler()
    scheduler.add_job(post_next, "interval", hours=3)
    scheduler.start()

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
