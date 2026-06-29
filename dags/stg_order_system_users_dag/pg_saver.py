import json
import logging
from typing import Dict, Any, List

log = logging.getLogger(__name__)

class PgSaver:
    def __init__(self, pg_connect):
        """
        Инициализация сохранения в PostgreSQL
        :param pg_connect: подключение к PostgreSQL (объект PgConnect)
        """
        self.pg_connect = pg_connect
        
    def save_users(self, users: List[Dict[str, Any]]) -> None:
        """
        Сохранение пользователей в таблицу stg.ordersystem_users
        :param users: список пользователей для сохранения
        """
        if not users:
            log.info("Нет данных для сохранения")
            return
        
        # Получаем клиент для работы с БД
        conn = self.pg_connect.client()
        
        # Создаем таблицу, если ее нет
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
                    id SERIAL PRIMARY KEY,
                    object_id VARCHAR(255) UNIQUE NOT NULL,
                    object_value JSONB NOT NULL,
                    update_ts TIMESTAMP NOT NULL
                );
            """)
            conn.commit()
        
        # Сохраняем данные
        for user in users:
            object_id = str(user.get('_id'))
            # Преобразуем объект в JSON с обработкой datetime
            object_value = json.dumps(user, default=str)
            update_ts = user.get('update_ts')
            
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """, (object_id, object_value, update_ts))
                conn.commit()
                
        log.info(f"Сохранено {len(users)} записей пользователей")