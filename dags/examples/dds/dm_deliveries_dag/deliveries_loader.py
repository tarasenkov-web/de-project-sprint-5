import json
from typing import List, Optional
from datetime import datetime, date, time
from decimal import Decimal
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

# from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from examples.dds import EtlSetting, DdsEtlSettingsRepository
# from examples.dds.dm_restaurants_dag.restaurants_loader import (RestaurantDdsRepository, RestaurantJsonObj,
#                                    RestaurantRawRepository)

# from examples.dds.dm_timestamps_dag.timestamps_loader import (TimestampDdsRepository, TimestampJsonObj,
#                                    TimestampRawRepository)

# from examples.dds.dm_orders_dag.orders_loader import (OrderDdsRepository, OrderJsonObj,
#                                    OrderRawRepository)
# from examples.dds.dm_couriers_dag.couriers_loader import (CourierDdsRepository, CourierJsonObj,
#                                    CourierRawRepository)


class DeliveryJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class DeliveryDdsObj(BaseModel):
    id: int
    delivery_id: str
    courier_id: str
    order_id: int
    rate: int
    tip_sum: int

class DeliveryRawRepository:
    def load_raw_deliveries(self, conn: Connection, last_loaded_record_id: int) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.deliverysystem_deliveries
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs

# class DeliveryDdsRepository:
#     def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
#         with conn.cursor() as cur:
#             cur.execute(
#                 """
#                     INSERT INTO dds.dm_deliveries(delivery_id, courier_id, order_id, rate, tip_sum)
#                     VALUES (%(delivery_id)s, %(courier_id)s, %(order_id)s, %(rate)s, %(tip_sum)s);
#                 """,
#                 {
#                     "delivery_id": delivery.delivery_id,
#                     "courier_id": delivery.courier_id,
#                     "order_id": delivery.order_id,
#                     "rate": delivery.rate,
#                     "tip_sum": delivery.tip_sum,
#                 },
#             )
            

#     def get_delivery(self, conn: Connection, order_id: str) -> Optional[DeliveryDdsObj]:
#         with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
#             cur.execute(
#                 """
#                     SELECT
#                         id,
#                         delivery_id, 
#                         courier_id, 
#                         order_id, 
#                         rate, 
#                         tip_sum
#                     FROM dds.dm_deliveries
#                     WHERE order_id = %(order_id)s;
#                 """,
#                 {"order_id": order_id},
#             )
#             obj = cur.fetchone()
#         return obj
    
#     def list_deliveries(self, conn: Connection) -> List[DeliveryDdsObj]:
#         with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
#             cur.execute(
#                 """
#                     SELECT id, delivery_id, courier_id, order_id, rate, tip_sum
#                     FROM dds.dm_deliveries;
#                 """
#             )
#             obj = cur.fetchall()
#         return obj


# class DeliveryLoader:
#     WF_KEY = "deliveries_raw_to_dds_workflow"
#     LAST_LOADED_ID_KEY = "last_loaded_delivery_id"

#     def __init__(self, pg: PgConnect) -> None:
#         self.dwh = pg
#         self.raw = DeliveryRawRepository()
#         self.dds = DeliveryDdsRepository()
#         self.dds_orders = OrderDdsRepository()
#         self.dds_couriers = CourierDdsRepository()
#         self.settings_repository = DdsEtlSettingsRepository()

#     def parse_deliveries(self, raws: DeliveryJsonObj, order_id: int, courier_id: int) -> DeliveryDdsObj:
#         delivery_json = json.loads(raws.object_value)
#         t = DeliveryDdsObj(id=0,
#                         delivery_id=delivery_json['delivery_id'],
#                         rate=delivery_json['rate'],
#                         tip_sum=delivery_json['tip_sum'],
#                         order_id=order_id,
#                         courier_id = courier_id
#                         )
#         return t
    

#     def load_deliveries(self):
#         with self.dwh.connection() as conn:
#             wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
#             if not wf_setting:
#                 wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

#             last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

#             load_queue = self.raw.load_raw_deliveries(conn, last_loaded_id)
#             load_queue.sort(key=lambda x: x.id)

#             for u in load_queue:
#                 delivery_json = json.loads(u.object_value)
                

#                 order_version = self.dds_orders.get_order(conn, delivery_json['order_id'])
#                 if not order_version:
#                     break

#                 courier_version = self.dds_couriers.get_courier(conn, delivery_json['courier_id'])
#                 if not courier_version:
#                     break
                
#                 dt = datetime.strptime(delivery_json['date'], "%Y-%m-%d %H:%M:%S")
#                 timestamp_version = self.dds_timestamps.get_timestamp(conn, dt)
#                 if not timestamp_version:
#                     break
                
#                 delivery_to_load = self.parse_deliveries(u, order_version.id, courier_version.id,)
#                 # existing = self.dds.get_Delivery(conn, Delivery_to_load.Delivery_key)
#                 # if not existing:
#                 self.dds.insert_Delivery(conn, delivery_to_load)
            
#                 settings_json = json.dumps(wf_setting.workflow_settings)

#                 wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
#                 self.settings_repository.save_setting(conn, wf_setting.workflow_key, settings_json)