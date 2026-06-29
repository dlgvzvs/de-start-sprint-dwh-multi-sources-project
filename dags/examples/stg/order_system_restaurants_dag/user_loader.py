import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
import json
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg.order_system_restaurants_dag.user_reader import UserReader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


class UserLoader:
    """
    Класс для загрузки данных о пользователях из MongoDB в PostgreSQL
    """
    
    def __init__(
        self,
        collection_reader: UserReader,
        dwh_pg_connect: ConnectionBuilder,
        pg_saver: PgSaver,
        log: logging.Logger
    ):
        """
        Инициализация загрузчика пользователей
        
        Args:
            collection_reader: Читатель данных из MongoDB
            dwh_pg_connect: Подключение к PostgreSQL
            pg_saver: Сохранятель данных в PostgreSQL
            log: Логгер
        """
        self.collection_reader = collection_reader
        self.dwh_pg_connect = dwh_pg_connect
        self.pg_saver = pg_saver
        self.log = log
        self.entity_name = "ordersystem_users"  # Имя сущности для таблицы настроек
        
    def get_last_update_ts(self) -> Optional[datetime]:
        """
        Получает последнюю отметку времени из таблицы настроек
        
        Returns:
            Optional[datetime]: Последняя отметка времени или None
        """
        conn = self.dwh_pg_connect.get_connection()
        cursor = conn.cursor()
        
        try:
            query = """
                SELECT last_update_ts 
                FROM stg.srv_wf_settings 
                WHERE entity_name = %s
            """
            cursor.execute(query, (self.entity_name,))
            result = cursor.fetchone()
            
            if result:
                self.log.info(f"Найдена последняя отметка для '{self.entity_name}': {result[0]}")
                return result[0]
            else:
                self.log.info(f"Запись для '{self.entity_name}' не найдена, загружаем все данные")
                return None
                
        except Exception as e:
            self.log.error(f"Ошибка при получении последней отметки: {e}")
            raise
        finally:
            cursor.close()
    
    def save_users(self, users_data: List[Dict[str, Any]]) -> Optional[datetime]:
        """
        Сохраняет данные о пользователях в PostgreSQL
        
        Args:
            users_data: Список документов пользователей
            
        Returns:
            Optional[datetime]: Максимальный update_ts из сохраненных данных
        """
        if not users_data:
            self.log.info("Нет данных для сохранения")
            return None
            
        conn = self.dwh_pg_connect.get_connection()
        cursor = conn.cursor()
        max_update_ts = None
        
        try:
            # Начинаем транзакцию
            cursor.execute("BEGIN")
            
            # Сохраняем каждого пользователя
            for user in users_data:
                # Извлекаем ID пользователя
                user_id = str(user.get('_id', ''))
                if not user_id:
                    self.log.warning(f"Пропущен документ без _id: {user}")
                    continue
                
                # Преобразуем данные в JSON
                user_json = json.dumps(user, default=str, ensure_ascii=False)
                
                # Получаем update_ts
                update_ts = user.get('update_ts')
                
                # SQL-запрос для вставки с обновлением
                sql = """
                    INSERT INTO stg.ordersystem_users (user_id, user_data, update_ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id) 
                    DO UPDATE SET 
                        user_data = EXCLUDED.user_data,
                        update_ts = EXCLUDED.update_ts
                """
                
                cursor.execute(sql, (user_id, user_json, update_ts))
                
                # Обновляем максимальный update_ts
                if update_ts:
                    if max_update_ts is None or update_ts > max_update_ts:
                        max_update_ts = update_ts
            
            # Сохраняем последний update_ts в таблицу настроек
            if max_update_ts:
                settings_sql = """
                    INSERT INTO stg.srv_wf_settings (entity_name, last_update_ts)
                    VALUES (%s, %s)
                    ON CONFLICT (entity_name) 
                    DO UPDATE SET last_update_ts = EXCLUDED.last_update_ts
                """
                cursor.execute(settings_sql, (self.entity_name, max_update_ts))
                self.log.info(f"Обновлена отметка для '{self.entity_name}': {max_update_ts}")
            
            cursor.execute("COMMIT")
            self.log.info(f"Успешно сохранено {len(users_data)} записей пользователей")
            return max_update_ts
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            self.log.error(f"Ошибка при сохранении данных пользователей: {e}")
            raise
        finally:
            cursor.close()
    
    def run_copy(self):
        """
        Выполняет копирование данных о пользователях из MongoDB в PostgreSQL
        """
        try:
            # Получаем последнюю отметку времени
            last_ts = self.get_last_update_ts()
            self.log.info(f"Начало загрузки пользователей. Последняя отметка: {last_ts}")
            
            # Читаем данные из MongoDB
            users_data = self.collection_reader.read_data(
                last_update_ts=last_ts,
                limit=self.collection_reader.batch_size
            )
            
            if not users_data:
                self.log.info("Новых данных о пользователях для загрузки нет")
                return
            
            # Сохраняем данные в PostgreSQL
            max_update_ts = self.save_users(users_data)
            
            self.log.info(
                f"Загрузка пользователей завершена. "
                f"Загружено: {len(users_data)}, "
                f"Последний update_ts: {max_update_ts}"
            )
            
        except Exception as e:
            self.log.error(f"Ошибка при загрузке данных о пользователях: {e}")
            raise