import asyncio
from pyrogram import Client


async def main():
    async with Client(
        "user_session",
        api_id=31304283,
        api_hash="5743a8fa4aceed8d12c89fb5d061a09d",
        phone_number="+79165382059",
    ) as app:
        await app.send_message("me", "Pyrogram works!")


asyncio.run(main())
