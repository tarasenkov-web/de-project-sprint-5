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


class ProductJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class ProductDdsObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: Decimal
    active_from: datetime
    active_to: datetime
    restaurant_id: int 


class ProductRawRepository:
    def load_raw_products(self, conn: Connection, last_loaded_record_id: int) -> List[ProductJsonObj]:
        with conn.cursor(row_factory=class_row(ProductJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class ProductDdsRepository:
    def insert_product(self, conn: Connection, products: List[ProductDdsObj]) -> None:
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                        VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s);
                    """,
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "product_price": product.product_price,
                        "active_from": product.active_from,
                        "active_to": product.active_to,
                        "restaurant_id": product.restaurant_id,
                    },
                )
            

    def get_product(self, conn: Connection, product_id: str) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        product_id,
                        product_name, 
                        product_price,
                        active_from, 
                        active_to, 
                        restaurant_id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
        return obj
    
    def list_products(self, conn: Connection) -> List[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj


class ProductLoader:
    WF_KEY = "products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_product_id"

    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds = ProductDdsRepository()
        self.dds_restaurants = RestaurantDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_products(self, raws: RestaurantJsonObj, restaurant_version_id: int) -> List[ProductDdsObj]:
        res = []
        product_json = json.loads(raws.object_value)
        for menu_json in product_json['menu']:
            t = ProductDdsObj(id=0,
                            product_id=menu_json['_id'],
                            product_name=menu_json['name'],
                            product_price=menu_json['price'],
                            active_from=product_json['update_ts'],
                            active_to='2099-12-31 00:00:00.000',
                            restaurant_id=restaurant_version_id
                           )

            res.append(t)
        return res
    

    def load_products(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_restaurants(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)

            products = self.dds.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p

            for restaurant in load_queue:
                restaurant_version = self.dds_restaurants.get_restaurant(conn, restaurant.object_id)
                if not restaurant_version:
                    return

                products_to_load = self.parse_products(restaurant, restaurant_version.id)
                products_to_load = [p for p in products_to_load if p.product_id not in prod_dict]
                self.dds.insert_product(conn, products_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = restaurant.id
                settings_json = json.dumps(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, settings_json)