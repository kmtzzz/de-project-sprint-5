from logging import Logger
from typing import List, Optional

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime, timedelta


class DmRestaurantObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


class DmRestaurantOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, id_threshold: int, limit: int) -> List[DmRestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(DmRestaurantObj)) as cur:
            cur.execute(
                """
                 select id as id,
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

    def insert_entity(self, conn: Connection, entity: DmRestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s,%(active_to)s)
                    ;
                """,
                {
                    "restaurant_id": entity.restaurant_id,
                    "restaurant_name": entity.restaurant_name,
                    "active_from": entity.active_from,
                    "active_to": entity.active_to
                },
            )
    
    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[DmRestaurantObj]: # take latest loaded restaurant record
        with conn.cursor(row_factory=class_row(DmRestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id, restaurant_name, active_from, active_to
                      FROM dds.dm_restaurants
                     WHERE restaurant_id = %(restaurant_id)s
                     ORDER BY id DESC
                     LIMIT 1
                """,
                {
                    "restaurant_id": restaurant_id
                }
            )
            obj = cur.fetchone() 
        return obj
    
    def update_restaurant(self, conn: Connection, restaurant: DmRestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                   UPDATE dds.dm_restaurants
                      set active_to = %(new_active_to)s
                   where id = %(id)s
                """,
                {
                    "new_name": restaurant.restaurant_name,
                    "new_active_to": restaurant.active_to,
                    "id": restaurant.id
                }
            )

class DmRestaurantLoader:
    WF_KEY = "dm_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Batch size

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmRestaurantOriginRepository(pg_stg)
        self.dds = DmRestaurantDestRepository()
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
            self.log.info(f"Found {len(load_queue)} restaurants to load to dm_restaurants.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to dwh considering SC2: close previous version date and adding new version
            for ent in load_queue:
                fetch_rest = self.dds.get_restaurant(conn, ent.restaurant_id)
                if not fetch_rest:
                    self.dds.insert_entity(conn, ent)
                elif not(fetch_rest.restaurant_name == ent.restaurant_name):
                    fetch_rest.active_to = ent.active_from - timedelta(seconds=1) #set old.active_to = new.active_from - 1 sec 
                    self.dds.update_restaurant(conn, fetch_rest)
                    self.dds.insert_entity(conn, ent)

            # Save progress
            # Using same connection session, so workflow is updated within data or rollback happens for full transaction
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to string to save in database.
            self.settings_repository.save_settings(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
