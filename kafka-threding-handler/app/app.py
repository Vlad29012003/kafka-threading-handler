import faust
from .config import (
    KAFKA_BROKER_URL,
    TOPIC_MESSAGES,
    TOPIC_FILTERED_MESSAGES,
    TOPIC_BLOCKED_USERS,
    APP_ID,
)

app = faust.App(
    APP_ID,
    broker=KAFKA_BROKER_URL,
    store="rocksdb://",
    topic_partitions="json",
    partitions=1,
)

topic1 = app.topic(TOPIC_MESSAGES, value_type=str)
topic2 = app.topic(TOPIC_FILTERED_MESSAGES, value_type=str)
topic3 = app.topic(TOPIC_BLOCKED_USERS, value_type=str)


table_topic = app.Table("blocked_users", default=int, partitions=1)


@app.agent(topic1, topic2, topic3)
async def count_topics(topics):
    async for page in topics:
        table_topic[page] += 1


if __name__ == "__main__":
    app.main()
