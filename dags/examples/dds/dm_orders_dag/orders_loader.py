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
from examples.dds.dm_restaurants_dag.restaurants_loader import (RestaurantDdsRepository, RestaurantJsonObj,
                                   RestaurantRawRepository)

from examples.dds.dm_timestamps_dag.timestamps_loader import (TimestampDdsRepository, TimestampJsonObj,
                                   TimestampRawRepository)

from examples.dds.dm_users_dag.users_loader import (UserDdsRepository, UserJsonObj,
                                   UserRawRepository)
from examples.dds.dm_couriers_dag.couriers_loader import (CourierDdsRepository, CourierJsonObj,
                                   CourierRawRepository)
from examples.dds.dm_deliveries_dag.deliveries_loader import (DeliveryJsonObj,
                                   DeliveryRawRepository)


class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class OrderDdsObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    courier_id: int

class OrderRawRepository:
    def load_raw_orders(self, conn: Connection, last_loaded_record_id: int) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs

class OrderDdsRepository:
    def insert_order(self, conn: Connection, order: OrderDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id, courier_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s, %(courier_id)s);
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "user_id": order.user_id,
                    "courier_id": order.courier_id,
                },
            )
            

    def get_order(self, conn: Connection, order_key: str) -> Optional[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_key, 
                        order_status, 
                        restaurant_id, 
                        timestamp_id, 
                        user_id,
                        courier_id
                    FROM dds.dm_orders
                    WHERE order_key = %(order_key)s;
                """,
                {"order_key": order_key},
            )
            obj = cur.fetchone()
        return obj
    
    def list_orders(self, conn: Connection) -> List[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_key, order_status, restaurant_id, timestamp_id, user_id, courier_id
                    FROM dds.dm_orders;
                """
            )
            obj = cur.fetchall()
        return obj


class OrderLoader:
    WF_KEY = "orders_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_id"

    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg
        self.raw = OrderRawRepository()
        self.dds = OrderDdsRepository()
        self.dds_restaurants = RestaurantDdsRepository()
        self.dds_timestamps = TimestampDdsRepository()
        self.dds_users = UserDdsRepository()
        self.dds_couriers = CourierDdsRepository()
        self.raw_deliveries = DeliveryRawRepository()
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_orders(self, raws: OrderJsonObj, restaurant_id: int, timestamp_id: int, user_id: int, courier_id: int) -> OrderDdsObj:
        order_json = json.loads(raws.object_value)
        t = OrderDdsObj(id=0,
                        order_key=order_json['_id'],
                        order_status=order_json['final_status'],
                        restaurant_id=restaurant_id,
                        timestamp_id=timestamp_id,
                        user_id=user_id,
                        courier_id = courier_id
                        )
        return t
    

    def load_orders(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_orders(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)

            for u in load_queue:
                order_json = json.loads(u.object_value)
                

                restaurant_version = self.dds_restaurants.get_restaurant(conn, order_json['restaurant']['id'])
                if not restaurant_version:
                    break

                deliveries = self.raw_deliveries.load_raw_deliveries(conn, last_loaded_id)
                delivery_version = next((d for d in deliveries if d.object_id == order_json['_id']), None)
                # print(f"Доставка с ID {deliveries} не найдена.") 
                # print(f"Доставка с ID {delivery_version} не найдена.") 
                if not delivery_version:
                    # print(f"Доставка с ID {order_json['_id']} не найдена.") 
                    continue
                else:
                    object_value = delivery_version.object_value
                    parsed_object_value = json.loads(object_value)
                    courier_id = parsed_object_value['courier_id']
                    courier_version = self.dds_couriers.get_courier(conn, courier_id)

                if not courier_version: 
                    print(f"Курьер с ID {courier_id} не найден.") 
                    continue
                
                user_version = self.dds_users.get_user(conn, order_json['user']['id'])
                if not user_version:
                    break
                
                dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
                timestamp_version = self.dds_timestamps.get_timestamp(conn, dt)
                if not timestamp_version:
                    break
                
                order_to_load = self.parse_orders(u, restaurant_version.id, timestamp_version.id, user_version.id, courier_version.id)
                # existing = self.dds.get_order(conn, order_to_load.order_key)
                # if not existing:
                self.dds.insert_order(conn, order_to_load)
            
                settings_json = json.dumps(wf_setting.workflow_settings)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, settings_json)