from logging import Logger
from typing import List

from examples.dds import DDSEtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class DmOrderObj(BaseModel):
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int


class DmOrderOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, ts_treshold_id: int, limit: int) -> List[DmOrderObj]:
        with self._db.client().cursor(row_factory=class_row(DmOrderObj)) as cur:
            cur.execute(
                """
                    select t.order_key,
                        t.order_status,
                        dr.id as restaurant_id,
                        dt.id as timestamp_id,
                        du.id as user_id
                    from
                            (select object_id as order_key,
                                object_value::json->>'final_status' as order_status,
                                (object_value::json->>'restaurant')::json->>'id' as restaurant_object_id,
                                (object_value::json->>'date')::timestamp as ts,
                                (object_value::json->>'user')::json->>'id' as user_object_id
                            from stg.ordersystem_orders oo) t 
                    join dds.dm_restaurants dr 
                        on t.restaurant_object_id = dr.restaurant_id 
                        --and t.update_ts >= dr.active_from and t.update_ts <= dr.active_to 
                    join dds.dm_timestamps dt 
                        on t.ts = dt.ts
                    join dds.dm_users du 
                        on t.user_object_id = du.user_id
                   where dt.id > %(threshold)s --Load only new data
                   ORDER BY t.ts ASC --Sort by update_Ts is mandatory as update_Ts is used as cursor
                   LIMIT %(limit)s; --Processing batch size
                """, {
                    "threshold": ts_treshold_id,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

class DmOrderDestRepository:

    def insert_order(self, conn: Connection, dm_order: DmOrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s,%(user_id)s)
                        ;
                """,
                {
                    "order_key": dm_order.order_key,
                    "order_status": dm_order.order_status,
                    "restaurant_id": dm_order.restaurant_id,
                    "timestamp_id": dm_order.timestamp_id,
                    "user_id": dm_order.user_id
                },
            )


class DmOrderLoader:
    WF_KEY = "dm_orders_stg_to_dds_workflow"
    LAST_LOADED_TS_ID_KEY = "last_loaded_ts_id"
    BATCH_LIMIT = 10000  # Batch size, 10000 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmOrderOriginRepository(pg_stg)
        self.dds = DmOrderDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_dm_orders(self):
        # open transactopm
        # transaction will be commited if code inside WITH will be successfully executed
        # in case of any error, rollback will be executed
        with self.pg_dds.connection() as conn:

            # Fetch workflow settings for user load
            # If worklow settings don't exist, create it
            wf_setting = self.settings_repository.get_settings(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DDSEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS_ID_KEY: -1})

            # Read batch data to queue
            #last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            #print(f"{last_loaded_ts_str = }")
            #last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            #self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_TS_ID_KEY] # fetch cursor
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load to dds.dm_orders.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh.
            for order in load_queue:
                self.dds.insert_order(conn, order)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_TS_ID_KEY] =  max([t.timestamp_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)


            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_ID_KEY]}")
