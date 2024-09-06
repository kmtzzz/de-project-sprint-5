from logging import Logger
from typing import List

from examples.dds import DDSEtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DmProductObj(BaseModel):
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int


class DmProductOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, id_threshold: int, limit: int) -> List[DmProductObj]:
        with self._db.client().cursor(row_factory=class_row(DmProductObj)) as cur:
            cur.execute(
                """
                    select distinct t.json_body->>'_id' as product_id,
                        t.json_body->>'name' as product_name,
                        t.json_body->>'price' as product_price,
                        t.update_ts as active_from,
                        '2099-12-31 00:00:00.000'::timestamp as active_to,
                        t.id as restaurant_id
                    from (select distinct jsonb_array_elements((object_value::json->>'menu')::jsonb) as json_body,
                                update_ts,
                                object_id,
                                id
                            from stg.ordersystem_restaurants orr) t
                     WHERE t.id > %(threshold)s --Load only new data
                    ORDER BY id ASC --Sort by id is mandatory as ID is used as cursor
                    LIMIT %(limit)s; --Processing batch size
                """, {
                    "threshold": id_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmProductDestRepository:

    def insert_product(self, conn: Connection, dm_product: DmProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, restaurant_id, active_from, active_to)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(restaurant_id)s, %(active_from)s,%(active_to)s)
                    --ON CONFLICT (product_id) DO UPDATE
                    --SET
                    --     product_name = EXCLUDED.product_name,
                    --     restaurant_id = EXCLUDED.restaurant_id,
                    --     active_from = EXCLUDED.active_from,
                    --     active_to = EXCLUDED.active_to
                        ;
                """,
                {
                    "product_id": dm_product.product_id,
                    "restaurant_id": dm_product.restaurant_id,
                    "product_name": dm_product.product_name,
                    "product_price": dm_product.product_price,
                    "active_from": dm_product.active_from,
                    "active_to": dm_product.active_to
                },
            )


class DmProductLoader:
    WF_KEY = "dm_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Batch size, 1000 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmProductOriginRepository(pg_stg)
        self.dds = DmProductDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_dm_products(self):
        # open transaction
        # transaction will be commited if code inside WITH will be successfully executed
        # in case of any error, rollback will be executed
        with self.pg_dds.connection() as conn:

            # Fetch workflow settings for user load
            # If worklow settings don't exist, create it
            wf_setting = self.settings_repository.get_settings(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DDSEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Read batch data to queue
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] # fetch cursor
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load to dm_products.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh.
            for product in load_queue:
                self.dds.insert_product(conn, product)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.restaurant_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
