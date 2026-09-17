from .app import app


blocked_users_table = app.SetTable("blocked_user", partitions=1)
banned_words_table = app.SetTable("banned-words-table", partitions=1)
