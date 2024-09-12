from logging import Logger
from typing import List

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FctDeliveryDtlObj(BaseModel):
    id: int
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    order_id: int
    order_sum: float
    rate: int
    tip_sum: float

class FctDeliveryDtlOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, id_threshold: int, limit: int) -> List[FctDeliveryDtlObj]:
        with self._db.client().cursor(row_factory=class_row(FctDeliveryDtlObj)) as cur:
            cur.execute(
                """
                     select dd.id,
                            dc.id as courier_id,
                            dc.courier_name,
                            dt."year" as settlement_year,
                            dt."month" as settlement_month,
                            dor.id as order_id,
                            t.order_sum,
                            dd.rate as rate,
                            dd.tip_sum::decimal as tip_sum 
                        from dds.dm_deliveries dd 
                        join dds.dm_couriers dc 
                            on dd.courier_id = dc.id
                        join dds.dm_orders dor
                            on dd.order_id = dor.id 
                        join dds.dm_timestamps dt 
                            on dor.timestamp_id = dt.id  
                        join (select object_id,
                            (object_value::json->>'cost')::decimal as order_sum
                        from stg.ordersystem_orders) t 
                            on dor.order_key = t.object_id
                     WHERE dd.id > %(threshold)s --Load only new data
                    ORDER BY dd.id ASC --Sort by id is mandatory as ID is used as cursor
                    LIMIT %(limit)s; --Processing batch size
                """, {
                    "threshold": id_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FctDeliveryDtlDestRepository:

    def insert_entity(self, conn: Connection, entity: FctDeliveryDtlObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_delivery_details(courier_id, courier_name, settlement_year, settlement_month, order_id, order_sum, rate, tip_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(order_id)s, %(order_sum)s, %(rate)s, %(tip_sum)s)
                        ;
                """,
                {
                    "courier_id": entity.courier_id,
                    "courier_name": entity.courier_name,
                    "settlement_year": entity.settlement_year,
                    "settlement_month": entity.settlement_month,
                    "order_id": entity.order_id,
                    "order_sum": entity.order_sum,
                    "rate": entity.rate,
                    "tip_sum": entity.tip_sum
                },
            )


class FctDeliveryDtlLoader:
    WF_KEY = "fct_delivery_details_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50000  # Batch size, 25 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = FctDeliveryDtlOriginRepository(pg_stg)
        self.dds = FctDeliveryDtlDestRepository()
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
            self.log.info(f"Found {len(load_queue)} deliveries to load to fct_delivery_details.")
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
