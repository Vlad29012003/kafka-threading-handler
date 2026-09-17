from .models import Message, BlockCommand, BannedWordCommand

import time
from .app import app
from .agents import messages_topic, blocked_users_topic, banned_words_topic
import asyncio


@app.command()
async def main():
    # 1. user_2 блокирует user_1
    await blocked_users_topic.send(
        value=BlockCommand(blocker_id="user_2", blocked_id="user_1", is_blocked=True)
    )

    await banned_words_topic.send(
        value=BannedWordCommand(
            word="урод",
            is_banned=True,
        )
    )

    await asyncio.sleep(2)

    # 2. user_1 пишет user_2 — должно быть отфильтровано (не попасть в filtered_messages)
    await messages_topic.send(
        value=Message(
            user_id="user_1",
            recipient_id="user_2",
            message="Привет",
            timestamp=time.time(),
        )
    )

    await messages_topic.send(
        value=Message(
            user_id="user_3",
            recipient_id="user_4",
            message="Ты урод",
            timestamp=time.time(),
        )
    )

    # 3. user_3 пишет user_4 (блокировки нет) — должно нормально дойти до filtered_messages
    await messages_topic.send(
        value=Message(
            user_id="user_3",
            recipient_id="user_4",
            message="Привет, это чистое сообщение",
            timestamp=time.time(),
        )
    )
