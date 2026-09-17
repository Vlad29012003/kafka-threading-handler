import faust
from .config import KAFKA_BROKER_URL, APP_ID

app = faust.App(
    APP_ID,
    broker=KAFKA_BROKER_URL,
    store="memory://",  # персистентное хранилище — данные таблиц пишутся на диск
)

# Импортируем после создания app, а не в начале файла — иначе
# получим циклический импорт (tables.py и agents.py делают
# `from .app import app`, а сам app.py всё ещё в процессе загрузки).


from . import tables, agents, producer  # noqa: E402,F401

if __name__ == "__main__":
    app.main()
