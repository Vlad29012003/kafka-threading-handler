from .app import app
from .config import (
    TOPIC_BLOCKED_USERS,
    TOPIC_MESSAGES,
    TOPIC_FILTERED_MESSAGES,
    TOPIC_BANNED_WORDS,
)
from .models import BlockCommand, Message, BannedWordCommand
from .tables import blocked_users_table, banned_words_table

# Топик с типизацией: Faust будет автоматически десериализовать
# каждое сообщение из JSON в объект BlockCommand.
blocked_users_topic = app.topic(TOPIC_BLOCKED_USERS, value_type=BlockCommand)
messages_topic = app.topic(TOPIC_MESSAGES, value_type=Message)
filtered_messages_topic = app.topic(TOPIC_FILTERED_MESSAGES, value_type=Message)
banned_words_topic = app.topic(TOPIC_BANNED_WORDS, value_type=BannedWordCommand)


@app.agent(blocked_users_topic)
async def process_blocked_user(commands):
    # это цикл, который получает из Kafka каждое новое событие BlockCommand по мере поступления
    # потоковая обработка — реакция на каждое событие сразу, а не пачкой
    async for cmd in commands:
        # флаг из модели, который мы сами придумали в models.py
        if cmd.is_blocked:
            # если True — добавляем в множество (.add),
            blocked_users_table[cmd.blocker_id].add(cmd.blocked_id)
        else:
            # если False (разблокировка) — убираем (.discard, он безопасен даже если элемента там не было, в отличие от .remove).
            blocked_users_table[cmd.blocker_id].discard(cmd.blocked_id)


@app.agent(messages_topic)
async def check_message(messaging):
    async for msg in messaging:
        # получатель заблокировал отправителя?
        if msg.user_id in blocked_users_table[msg.recipient_id]:
            continue  # молча дропаем — сообщение не идёт дальше

        # цензура: проходим по каждому слову сообщения
        banned = banned_words_table["global"]
        words = msg.message.split(" ")
        masked_words = []
        for word in words:
            if word.lower() in banned:
                masked_words.append("*" * len(word))
            else:
                masked_words.append(word)
        clean_text = " ".join(masked_words)

        # создаём новый Message с очищенным текстом, остальные поля те же
        clean_message = Message(
            user_id=msg.user_id,
            recipient_id=msg.recipient_id,
            message=clean_text,
            timestamp=msg.timestamp,
        )

        await filtered_messages_topic.send(value=clean_message)


@app.agent(banned_words_topic)
async def ban_words(commands):
    async for cmd in commands:
        if cmd.is_banned:
            banned_words_table["global"].add(cmd.word)
        else:
            banned_words_table["global"].discard(cmd.word)
