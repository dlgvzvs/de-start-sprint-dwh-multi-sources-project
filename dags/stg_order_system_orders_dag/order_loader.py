import logging
from typing import Optional
from datetime import datetime
from stg_order_system_orders_dag.order_reader import OrderReader
from stg_order_system_orders_dag.pg_saver import PgSaver

log = logging.getLogger(__name__)

class OrderLoader:
    def __init__(self, reader: OrderReader, pg_saver: PgSaver, log):
        self.reader = reader
        self.pg_saver = pg_saver
        self.log = log
        
    def run_copy(self) -> Optional[datetime]:
        try:
            orders = self.reader.get_orders(limit=1000)
            
            if not orders:
                self.log.info("Нет новых заказов для загрузки")
                return None
                
            self.pg_saver.save_orders(orders)
            
            max_update_ts = max(order.get('update_ts') for order in orders)
            self.log.info(f"Максимальный update_ts: {max_update_ts}")
            
            return max_update_ts
            
        except Exception as e:
            self.log.error(f"Ошибка при загрузке заказов: {e}")
            raise