import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from lib import MongoConnect

log = logging.getLogger(__name__)


class UserReader:
    """
    Класс для чтения данных о пользователях из MongoDB
    """
    
    def __init__(self, mongo_connect: MongoConnect):
        """
        Инициализация читателя пользователей
        
        Args:
            mongo_connect: Подключение к MongoDB
        """
        self.mongo_connect = mongo_connect
        self.collection_name = "users"
        self.batch_size = 1000  # Количество записей за один раз
        
    def read_data(
        self, 
        last_update_ts: Optional[datetime] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Читает данные о пользователях из MongoDB
        
        Args:
            last_update_ts: Время последней загрузки (для инкрементальной загрузки)
            limit: Максимальное количество записей для чтения
            
        Returns:
            List[Dict[str, Any]]: Список документов пользователей
        """
        try:
            # Получаем клиент MongoDB
            db = self.mongo_connect.client()
            collection = db.get_collection(self.collection_name)
            
            # Формируем фильтр для инкрементальной загрузки
            filter_query = {}
            if last_update_ts:
                filter_query = {'update_ts': {'$gt': last_update_ts}}
                log.info(f"Фильтр по update_ts > {last_update_ts}")
            
            # Сортировка по update_ts для последовательной обработки
            sort_params = [('update_ts', 1)]
            
            # Выполняем запрос с ограничением
            cursor = collection.find(
                filter=filter_query,
                sort=sort_params,
                limit=limit
            )
            
            # Преобразуем курсор в список
            documents = list(cursor)
            
            # Логируем результат
            log.info(f"Прочитано {len(documents)} документов из коллекции '{self.collection_name}'")
            
            return documents
            
        except Exception as e:
            log.error(f"Ошибка при чтении данных из MongoDB: {e}")
            raise