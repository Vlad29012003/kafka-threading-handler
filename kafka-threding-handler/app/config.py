"""
Конфигурация приложения: адрес Kafka-брокера и имена топиков.
Все настройки читаются из переменных окружения с разумными значениями
по умолчанию — это позволяет запускать приложение как локально (Faust
работает на хосте и стучится в Kafka через проброшенный порт), так и
внутри docker-compose (тогда Faust обращается к брокеру по имени сервиса
"kafka" и внутреннему порту 9092).
"""

import os

# Адрес Kafka-брокера.
# - Если Faust запускается на хосте (например, через `faust -A app.app worker`
#   прямо в терминале, вне docker-compose), используем внешний листенер,
#   который проброшен наружу в docker-compose.yml: localhost:9094.
# - Если Faust будет запускаться внутри той же docker-сети (отдельным
#   сервисом в compose), нужно поменять значение по умолчанию на "kafka:9092"
#   или задать переменную окружения KAFKA_BROKER_URL при запуске контейнера.

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka://localhost:9094")

# Имена топиков — соответствуют тем, что создаются сервисом kafka-init
# в docker-compose.yml.

TOPIC_MESSAGES = "messages"
TOPIC_FILTERED_MESSAGES = "filtered_messages"
TOPIC_BLOCKED_USERS = "blocked_users"

TOPIC_BANNED_WORDS = "banned_words"


# Имя Faust-приложения. Faust использует его как префикс для служебных
# топиков (changelog-топики table, топики repartition и т.д.), поэтому
# должно быть стабильным и уникальным для проекта.
APP_ID = "message-moderation-app"
