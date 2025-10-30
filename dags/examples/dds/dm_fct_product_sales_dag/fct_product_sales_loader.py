import json
import logging
from typing import List, Optional
from datetime import datetime, date, time
from typing import Dict, List, Tuple
from decimal import Decimal
from sqlalchemy import Numeric
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from lib.dict_util import json2str

# from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from examples.dds import EtlSetting, DdsEtlSettingsRepository
# from examples.dds.dm_restaurants_dag.restaurants_loader import (RestaurantDdsRepository, RestaurantJsonObj,
                                #    RestaurantRawRepository)

from examples.dds.dm_orders_dag.orders_loader import (OrderDdsRepository, OrderJsonObj,
                                   OrderRawRepository)

from examples.dds.dm_products_dag.products_loader import (ProductDdsObj, ProductDdsRepository, ProductJsonObj,
                                   ProductRawRepository)

from examples.dds.dm_couriers_dag.couriers_loader import (CourierDdsRepository, CourierJsonObj,
                                   CourierRawRepository)
from examples.dds.dm_deliveries_dag.deliveries_loader import (DeliveryJsonObj,
                                   DeliveryRawRepository)

log = logging.getLogger(__name__)

class FctProductSalesJsonObj:
    def __init__(self, d: Dict) -> None:
        self.product_id: str = d["product_id"]
        self.product_name: str = d["product_name"]
        self.price: float = d["price"]
        self.quantity: int = d["quantity"]
        self.product_cost: float = d["product_cost"]
        self.bonus_payment: float = d["bonus_payment"]
        self.bonus_grant: float = d["bonus_grant"]


class FctProductSalesDdsObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float
    rate: float
    tip_sum: float

class BonusPaymentJsonObj:
    EVENT_TYPE = "bonus_transaction"

    def __init__(self, d: Dict) -> None:
        self.user_id: int = d["user_id"]
        self.order_id: str = d["order_id"]
        self.order_date: datetime = datetime.strptime(d["order_date"], "%Y-%m-%d %H:%M:%S")
        self.product_payments = [FctProductSalesJsonObj(it) for it in d["product_payments"]]



class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class BonusEventRepository:

    def load_raw_events(self, conn: Connection, event_type: str, last_loaded_record_id: int) -> List[EventObj]:
        with conn.cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = %(event_type)s AND id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {
                    "event_type": event_type,
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs

class FctProductSalesDdsRepository:
    def insert_facts(self, conn: Connection, facts: List[FctProductSalesDdsObj]) -> None:
        with conn.cursor() as cur:
            for fact in facts:
                cur.execute(
                    """
                        INSERT INTO dds.fct_product_sales(
                            order_id,
                            product_id,
                            count,
                            price,
                            total_sum,
                            bonus_payment,
                            bonus_grant,
                            rate,
                            tip_sum
                        )
                        VALUES (
                            %(order_id)s,
                            %(product_id)s,
                            %(count)s,
                            %(price)s,
                            %(total_sum)s,
                            %(bonus_payment)s,
                            %(bonus_grant)s,
                            %(rate)s,
                            %(tip_sum)s
                        )
                        ON CONFLICT (order_id, product_id) DO UPDATE
                        SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum
                        ;
                    """,
                    {
                        "product_id": fact.product_id,
                        "order_id": fact.order_id,
                        "count": fact.count,
                        "price": fact.price,
                        "total_sum": fact.total_sum,
                        "bonus_payment": fact.bonus_payment,
                        "bonus_grant": fact.bonus_grant,
                        "rate": fact.rate,
                        "tip_sum": fact.tip_sum
                    },
                )
            

    def get_order(self, conn: Connection, order_key: str) -> Optional[FctProductSalesDdsObj]:
        with conn.cursor(row_factory=class_row(FctProductSalesDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_key, 
                        order_status, 
                        restaurant_id, 
                        timestamp_id, 
                        user_id
                    FROM dds.fct_product_sales
                    WHERE order_key = %(order_key)s;
                """,
                {"order_key": order_key},
            )
            obj = cur.fetchone()
        return obj
    
    def list_fct_product_sales(self, conn: Connection) -> List[FctProductSalesDdsObj]:
        with conn.cursor(row_factory=class_row(FctProductSalesDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_key, order_status, restaurant_id, timestamp_id, user_id
                    FROM dds.fct_product_sales;
                """
            )
            obj = cur.fetchall()
        return obj


class FctProductSalesLoader:
    PAYMENT_EVENT = "bonus_transaction"
    WF_KEY = "fact_product_events_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_event_id"

    _LOG_THRESHOLD = 100

    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg
        self.raw_events = BonusEventRepository()
        self.dds_orders = OrderDdsRepository()
        self.dds_products = ProductDdsRepository()
        self.raw_deliveries = DeliveryRawRepository()
        self.dds_facts = FctProductSalesDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_order_products(self,
                             order_raw: BonusPaymentJsonObj,
                             order_id: int,
                             products: Dict[str, ProductDdsObj],
                             rate: float,
                             tip_sum: float
                             ) -> Tuple[bool, List[FctProductSalesDdsObj]]:

        res = []

        for p_json in order_raw.product_payments:
            if p_json.product_id not in products:
                return (False, [])

            t = FctProductSalesDdsObj(id=0,
                                 order_id=order_id,
                                 product_id=products[p_json.product_id].id,
                                 count=p_json.quantity,
                                 price=p_json.price,
                                 total_sum=p_json.product_cost,
                                 bonus_grant=p_json.bonus_grant,
                                 bonus_payment=p_json.bonus_payment,
                                 rate=rate,
                                 tip_sum=tip_sum
                                 )
            res.append(t)

        return (True, res)

    def load_fct_product_sales(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            log.info(f"Starting load from: {last_loaded_id}")

            load_queue = self.raw_events.load_raw_events(conn, self.PAYMENT_EVENT, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            log.info(f"Found {len(load_queue)} events to load.")

            products = self.dds_products.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p

            proc_cnt = 0
            for payment_raw in load_queue:
                payment_obj = BonusPaymentJsonObj(json.loads(payment_raw.event_value))
                order = self.dds_orders.get_order(conn, payment_obj.order_id)
                if not order:
                    log.info(f"Not found order {payment_obj.order_id}. Finishing.")
                    continue

                deliveries = self.raw_deliveries.load_raw_deliveries(conn, last_loaded_id)
                delivery_version = next((d for d in deliveries if d.object_id == payment_obj.order_id), None)
                # print(f"Доставка с ID {deliveries} не найдена.") 
                # print(f"Доставка с ID {delivery_version} не найдена.") 
                if not delivery_version:
                    # print(f"Доставка с ID {order_json['_id']} не найдена.") 
                    continue
                else:
                    delivery_value = delivery_version.object_value
                    print(f"Доставка с ID {delivery_value} не найдена.") 
                    parsed_delivery_value = json.loads(delivery_value)
                    print(f"Доставка с ID {parsed_delivery_value} не найдена.") 

                    # delivery_value["rate"]

                (success, facts_to_load) = self.parse_order_products(payment_obj, order.id, prod_dict, parsed_delivery_value["rate"], parsed_delivery_value["tip_sum"])
                if not success:
                    log.info(f"Could not parse object for order {order.id}. Finishing.")
                    continue

                self.dds_facts.insert_facts(conn, facts_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = payment_raw.id
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                proc_cnt += 1
                if proc_cnt % self._LOG_THRESHOLD == 0:
                    log.info(f"Processing events {proc_cnt} out of {len(load_queue)}.")

            log.info(f"Processed {proc_cnt} events out of {len(load_queue)}.")