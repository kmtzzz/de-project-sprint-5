from logging import Logger
from typing import List

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class DmOrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int


class DmOrderOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, id_treshold: int, limit: int) -> List[DmOrderObj]:
        with self._db.client().cursor(row_factory=class_row(DmOrderObj)) as cur:
            cur.execute(
                """
                    select t.id,
                           t.order_key,
                           t.order_status,
                           dr.id as restaurant_id,
                           dt.id as timestamp_id,
                           du.id as user_id
                      from
                            (select object_id as order_key,
                                    object_value::json->>'final_status' as order_status,
                                    (object_value::json->>'restaurant')::json->>'id' as restaurant_object_id,
                                    (object_value::json->>'date')::timestamp as ts,
                                    (object_value::json->>'user')::json->>'id' as user_object_id,
                                    id
                               from stg.ordersystem_orders oo) t 
                      join dds.dm_restaurants dr 
                        on t.restaurant_object_id = dr.restaurant_id 
                        --and t.update_ts >= dr.active_from and t.update_ts <= dr.active_to 
                      join dds.dm_timestamps dt 
                        on t.ts = dt.ts
                      join dds.dm_users du 
                        on t.user_object_id = du.user_id
                     where dt.id > %(threshold)s --Load only new data
                     ORDER BY t.id ASC
                     LIMIT %(limit)s; --Processing batch size
                """, {
                    "threshold": id_treshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

class DmOrderDestRepository:

    def insert_entity(self, conn: Connection, entity: DmOrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s,%(user_id)s)
                        ;
                """,
                {
                    "order_key": entity.order_key,
                    "order_status": entity.order_status,
                    "restaurant_id": entity.restaurant_id,
                    "timestamp_id": entity.timestamp_id,
                    "user_id": entity.user_id
                },
            )


class DmOrderLoader:
    WF_KEY = "dm_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Batch size, 10000 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmOrderOriginRepository(pg_stg)
        self.dds = DmOrderDestRepository()
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
                wf_setting = DdsEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Read batch data to queue

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] # fetch cursor
            load_queue = self.origin.list_entities(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load to dds.dm_orders.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh.
            for ent in load_queue:
                self.dds.insert_entity(conn, ent)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] =  max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)


            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
