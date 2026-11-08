"""Одноразовый вход: печатает SESSION_STRING. Запускать в GitHub Codespaces."""
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession


async def main():
    api_id = int(input("API_ID: ").strip())
    api_hash = input("API_HASH: ").strip()
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        # Telethon сам спросит номер, код из Telegram и пароль 2FA
        print("\n=== SESSION_STRING (никому не показывай!) ===\n")
        print(client.session.save())
        print("\n=============================================")


asyncio.run(main())
