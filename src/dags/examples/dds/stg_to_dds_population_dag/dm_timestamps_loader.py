from logging import Logger
from typing import List

from examples.dds import DDSEtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, date, time


class DmTsObj(BaseModel):
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time


class DmTsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, ts_threshold: datetime, limit: int) -> List[DmTsObj]:
        with self._db.client().cursor(row_factory=class_row(DmTsObj)) as cur:
            cur.execute(
                """
                  select ts,
                         extract(year from ts) as year,
                         extract(month from ts) as month,
                         extract(day from ts) as day,
                         ts::date as date,
                         ts::time as time
                    from (select (object_value::json->>'date')::timestamp as ts,
                                 update_ts
                            from stg.ordersystem_orders) as t
                   where t.update_ts > %(threshold)s --Load only new data
                   ORDER BY t.update_ts ASC --Sort by update_Ts is mandatory as update_Ts is used as cursor
                   LIMIT %(limit)s; --Processing batch size
                """, {
                    "threshold": ts_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

    def get_max_ts(self, ts_threshold: datetime) -> str:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                   select max(update_ts) from stg.ordersystem_orders
                    where update_ts > %(threshold)s ;
                """, {
                    "threshold": ts_threshold
                }
            )
            max_dt = cur.fetchone()
        return max_dt
 


class DmTsDestRepository:

    def insert_timestamp(self, conn: Connection, dm_ts: DmTsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s,%(date)s,%(time)s)
                    ON CONFLICT (ts) DO NOTHING
                        ;
                """,
                {
                    "ts": dm_ts.ts,
                    "year": dm_ts.year,
                    "month": dm_ts.month,
                    "day": dm_ts.day,
                    "date": dm_ts.date,
                    "time": dm_ts.time
                },
            )


class DmTsLoader:
    WF_KEY = "dm_timestamps_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 10000  # Batch size, 10000 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmTsOriginRepository(pg_stg)
        self.dds = DmTsDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_dm_timestamps(self):
        # open transactopm
        # transaction will be commited if code inside WITH will be successfully executed
        # in case of any error, rollback will be executed
        with self.pg_dds.connection() as conn:

            # Fetch workflow settings for user load
            # If worklow settings don't exist, create it
            wf_setting = self.settings_repository.get_settings(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = DDSEtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()})

            # Read batch data to queue
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            print(f"{last_loaded_ts_str = }")
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            max_currrent_ts = self.origin.get_max_ts(last_loaded_ts)
            print(f"{max_currrent_ts = }")
            print(type(max_currrent_ts))

            #last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] # fetch cursor
            load_queue = self.origin.list_timestamps(last_loaded_ts, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load to dm_timestamps.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh.
            for ts in load_queue:
                self.dds.insert_timestamp(conn, ts)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] =  max([t.ts for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)


            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
