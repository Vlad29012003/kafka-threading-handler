from .app import app


blocked_users_table = app.Table("blocked_user", default=set, partitions=1)
