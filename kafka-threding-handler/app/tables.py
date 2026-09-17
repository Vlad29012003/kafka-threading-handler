from .app import app


blocked_users_table = app.Table("blocked_user", default=set, partitions=1)
banned_words_table = app.Table("banned-words-table", default=set, partitions=1)
