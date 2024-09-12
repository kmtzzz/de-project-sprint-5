from logging import Logger
from typing import List

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DmDeliveryObj(BaseModel):
    id: int
    order_id: int
    delivery_id: str
    courier_id: str
    rate: int
    tip_sum: float
    delivery_ts: datetime


class DmDeliveryOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, id_threshold: int, limit: int) -> List[DmDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DmDeliveryObj)) as cur:
            cur.execute(
                """
                     select t.id,
                            dor.id as order_id,
                            t.delivery_id,
                            dc.id as courier_id,
                            t.rate,
                            t.tip_sum,
                            t.delivery_ts
                       from 
                            (select id,
                                    object_value::json->>'order_id' as order_key,
                                    object_value::json->>'delivery_id' as delivery_id,
                                    object_value::json->>'courier_id' as courier_id,
                                    (object_value::json->>'rate')::int as rate,
                                    (object_value::json->>'tip_sum')::decimal as tip_sum,
                                    object_value::json->>'delivery_ts' as delivery_ts
                            from stg.deliverysystem_deliveries) t
                       join dds.dm_couriers dc 
                         on t.courier_id = dc.courier_id 
                       join dds.dm_orders dor
                         on t.order_key = dor.order_key 
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


class DmDeliveryDestRepository:

    def insert_entity(self, conn: Connection, entity: DmDeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(order_id, delivery_id, courier_id, rate, tip_sum, delivery_ts)
                    VALUES (%(order_id)s, %(delivery_id)s, %(courier_id)s, %(rate)s, %(tip_sum)s, %(delivery_ts)s)
                    ON CONFLICT (id) DO nothing
                """,
                {
                    "order_id": entity.order_id,
                    "delivery_id": entity.delivery_id,
                    "courier_id": entity.courier_id,
                    "rate": entity.rate,
                    "tip_sum": entity.tip_sum,
                    "delivery_ts": entity.delivery_ts
                },
            )


class DmDeliveryLoader:
    WF_KEY = "dm_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 5000  # Chunk size for bulk load

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmDeliveryOriginRepository(pg_stg)
        self.dds = DmDeliveryDestRepository()
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
            self.log.info(f"Found {len(load_queue)} deliveries to load to dm_deliveries.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh.
            for ent in load_queue:
                self.dds.insert_entity(conn, ent)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
