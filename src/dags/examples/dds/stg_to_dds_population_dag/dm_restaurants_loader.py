from logging import Logger
from typing import List

from examples.dds import DDSEtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DmRestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class DmRestaurantOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, id_threshold: int, limit: int) -> List[DmRestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(DmRestaurantObj)) as cur:
            cur.execute(
                """
                 select id,
                        object_id as restaurant_id,
                        object_value::json->>'name' as restaurant_name,
                        update_ts as active_from,
                        '2099-12-31 00:00:00.000'::timestamp as active_to
                    from stg.ordersystem_restaurants or2
                     WHERE id > %(threshold)s --Load only new data
                    ORDER BY id ASC --Sort by id is mandatory as ID is used as cursor
                    LIMIT %(limit)s; --Processing batch size
                """, {
                    "threshold": id_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmRestaurantDestRepository:

    def insert_restaurant(self, conn: Connection, dm_restaurant: DmRestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(id, restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(id)s, %(restaurant_id)s, %(restaurant_name)s, %(active_from)s,%(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to
                        ;
                """,
                {
                    "id": dm_restaurant.id,
                    "restaurant_id": dm_restaurant.restaurant_id,
                    "restaurant_name": dm_restaurant.restaurant_name,
                    "active_from": dm_restaurant.active_from,
                    "active_to": dm_restaurant.active_to
                },
            )


class DmRestaurantLoader:
    WF_KEY = "dm_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Batch size, 25 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmRestaurantOriginRepository(pg_stg)
        self.dds = DmRestaurantDestRepository()
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log

    def load_dm_restaurants(self):
        # open transactopm
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
            self.log.info(f"Found {len(load_queue)} restaurants to load to dm_restaurants.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh.
            for restaurant in load_queue:
                self.dds.insert_restaurant(conn, restaurant)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
