import logging
from typing import Optional
from datetime import datetime
from stg_order_system_users_dag.user_reader import UserReader
from stg_order_system_users_dag.pg_saver import PgSaver

log = logging.getLogger(__name__)

class UserLoader:
    def __init__(self, reader: UserReader, pg_saver: PgSaver, log):
        self.reader = reader
        self.pg_saver = pg_saver
        self.log = log
        
    def run_copy(self) -> Optional[datetime]:
        try:
            users = self.reader.get_users(limit=1000)
            
            if not users:
                self.log.info("Нет новых пользователей для загрузки")
                return None
                
            self.pg_saver.save_users(users)
            
            max_update_ts = max(user.get('update_ts') for user in users)
            self.log.info(f"Максимальный update_ts: {max_update_ts}")
            
            return max_update_ts
            
        except Exception as e:
            self.log.error(f"Ошибка при загрузке пользователей: {e}")
            raise