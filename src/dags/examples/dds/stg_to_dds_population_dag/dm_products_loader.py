from logging import Logger
from typing import List, Optional

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, timedelta


class DmProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int


class DmProductOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, ts_threshold: datetime, limit: int) -> List[DmProductObj]:
        with self._db.client().cursor(row_factory=class_row(DmProductObj)) as cur:
            cur.execute(
                """
                     select distinct 
                            0 as id,
                            t.json_body->>'_id' as product_id,
                            t.json_body->>'name' as product_name,
                            t.json_body->>'price' as product_price,
                            t.update_ts as active_from,
                            '2099-12-31 00:00:00.000'::timestamp as active_to,
                            dr.id as restaurant_id
                       from (select distinct jsonb_array_elements((object_value::json->>'menu')::jsonb) as json_body,
                                    update_ts,
                                    object_id,
                                    id
                               from stg.ordersystem_restaurants orr) t
                       join dds.dm_restaurants dr 
                         on t.object_Id = dr.restaurant_id 
                        and t.update_ts between dr.active_from and dr.active_to 
                      where t.update_ts > %(threshold)s --Load only new data
                      limit %(limit)s; --Processing batch size
                """, {
                    "threshold": ts_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmProductDestRepository:

    def insert_entity(self, conn: Connection, entity: DmProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, restaurant_id, active_from, active_to)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(restaurant_id)s, %(active_from)s,%(active_to)s)
                     ;
                """,
                {
                    "product_id": entity.product_id,
                    "restaurant_id": entity.restaurant_id,
                    "product_name": entity.product_name,
                    "product_price": entity.product_price,
                    "active_from": entity.active_from,
                    "active_to": entity.active_to
                },
            )

    def get_product(self, conn: Connection, product_id: str) -> Optional[DmProductObj]: # take latest loaded product record
        with conn.cursor(row_factory=class_row(DmProductObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                      FROM dds.dm_products
                     WHERE product_id = %(product_id)s
                     ORDER BY id DESC
                     LIMIT 1
                """,
                {
                    "product_id": product_id
                }
            )
            obj = cur.fetchone() 
        return obj
    
    def update_product(self, conn: Connection, product: DmProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                   UPDATE dds.dm_products
                      set active_to = %(new_active_to)s
                   where id = %(id)s
                """,
                {
                    "new_active_to": product.active_to,
                    "id": product.id
                }
            )


class DmProductLoader:
    WF_KEY = "dm_products_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 1000  # Batch size, 1000 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmProductOriginRepository(pg_stg)
        self.dds = DmProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_entities(self):
        # open transaction
        # transaction will be commited if code inside WITH will be successfully executed
        # in case of any error, rollback will be executed
        with self.pg_dds.connection() as conn:

            # Fetch workflow settings for user load
            # If worklow settings don't exist, create it
            wf_setting = self.settings_repository.get_settings(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()})

            # Read batch data to queue
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.origin.list_entities(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load to dm_products.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh considering SC2: close previous version date and adding new version
            for ent in load_queue:
                fetch_product = self.dds.get_product(conn, ent.product_id)
                if not fetch_product:
                    self.dds.insert_entity(conn, ent)
                elif not(fetch_product.product_name == ent.product_name and fetch_product.product_price == ent.product_price):
                    fetch_product.active_to = ent.active_from - timedelta(seconds=1) #set old.active_to = new.active_from - 1 sec 
                    self.dds.update_product(conn, fetch_product)
                    self.dds.insert_entity(conn, ent)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t.active_from for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
