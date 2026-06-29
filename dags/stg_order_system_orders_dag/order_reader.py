import logging
from typing import List, Dict, Any
from datetime import datetime
from lib import MongoConnect

log = logging.getLogger(__name__)

class OrderReader:
    def __init__(self, mc: MongoConnect, last_load_ts: datetime):
        """
        Инициализация читателя заказов
        :param mc: подключение к MongoDB
        :param last_load_ts: дата последней загрузки
        """
        self.client = mc.client()
        self.last_load_ts = last_load_ts
        self.collection_name = 'orders'
        
    def get_orders(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """
        Получение списка заказов из MongoDB
        :param limit: лимит записей за одну итерацию
        :return: список документов заказов
        """
        filter_query = {
            'update_ts': {'$gt': self.last_load_ts}
        }
        
        sort_query = [('update_ts', 1)]
        
        log.info(f"Чтение заказов с update_ts > {self.last_load_ts}")
        
        collection = self.client.get_collection(self.collection_name)
        cursor = collection.find(
            filter=filter_query,
            sort=sort_query,
            limit=limit
        )
        
        documents = list(cursor)
        log.info(f"Найдено {len(documents)} записей заказов")
        
        return documents