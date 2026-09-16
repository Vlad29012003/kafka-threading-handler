"""
Модели данных (Faust Records) — аналог Pydantic-схем, только со встроенной
сериализацией в JSON "из коробки": Faust сам превращает Record в JSON при
записи в топик и обратно в объект при чтении, ключи совпадают с именами
полей.
"""

import faust

class Message(faust.Record, serializer="json"):
     """
    Сообщение пользователя — то, что летает в топиках `messages`
    и `filtered_messages`. Структура специально совпадает с форматом,
    который описан в задании 2 для ksqlDB (user_id, recipient_id,
    message, timestamp), чтобы один и тот же продюсер тестовых данных
    годился для обеих частей задания.
    """
    
    user_id: int
    recipient_id: int
    message:str
    timestamp: float # unix-время отправки в секундах
    

class BlockCommand(faust.Record , serializer="json"):
     """
    Событие блокировки/разблокировки — то, что летает в топике
    `blocked_users`. Каждое такое событие говорит: пользователь
    `blocker_id` блокирует (или разблокирует) пользователя `blocked_id`.
 
    is_blocked=True  -> добавить blocked_id в список заблокированных blocker_id
    is_blocked=False -> убрать blocked_id из списка (разблокировать)
    """
    
    blocker_id : str
    blocked_id : str
    is_blocked : bool = True