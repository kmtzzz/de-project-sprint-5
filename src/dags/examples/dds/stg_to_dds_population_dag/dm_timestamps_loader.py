from logging import Logger
from typing import List, Optional

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class DmTsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time


class DmTsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, ts_threshold: datetime, limit: int) -> List[DmTsObj]:
        with self._db.client().cursor(row_factory=class_row(DmTsObj)) as cur:
            cur.execute(
                """
                   select id,
                          ts,
                          extract(year from ts) as year,
                          extract(month from ts) as month,
                          extract(day from ts) as day,
                          ts::date as date,
                          ts::time as time
                     from (select id,
                                  (object_value::json->>'date')::timestamp as ts
                             from stg.ordersystem_orders) as t
                    where t.id > %(threshold)s --Load only new data
                    order by t.id ASC 
                    limit %(limit)s; --Processing batch size
                """, {
                    "threshold": ts_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

class DmTsDestRepository:

    def insert_entity(self, conn: Connection, entity: DmTsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s,%(date)s,%(time)s)
                    ;
                """,
                {
                    "ts": entity.ts,
                    "year": entity.year,
                    "month": entity.month,
                    "day": entity.day,
                    "date": entity.date,
                    "time": entity.time
                },
            )

    def get_timestamp(self, conn: Connection, ts: str) -> Optional[DmTsObj]: # take timestamps record by ts
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, date, time
                      FROM dds.dm_timestamps
                     WHERE ts = %(ts)s
                     LIMIT 1
                """,
                {
                    "ts": ts
                }
            )
            obj = cur.fetchone() 
        return obj

class DmTsLoader:
    WF_KEY = "dm_timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 20000  # Batch size, 10000 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmTsOriginRepository(pg_stg)
        self.dds = DmTsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_entities(self):
        # open transactopm
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
            self.log.info(f"Found {len(load_queue)} timestamps to load to dm_timestamps.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh.
            for ent in load_queue:
                fetch_ts = self.dds.get_timestamp(conn, ent.ts)
                if not fetch_ts:
                    self.dds.insert_entity(conn, ent)                

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

