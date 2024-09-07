from logging import Logger
from typing import List

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DmUserObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class DmUserOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, id_threshold: int, limit: int) -> List[DmUserObj]:
        with self._db.client().cursor(row_factory=class_row(DmUserObj)) as cur:
            cur.execute(
                """
                    select id as id,
                           object_id as user_id,
                           object_value::json->>'name' as user_name,
                           object_value::json->>'login' as user_login
                      from stg.ordersystem_users
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


class DmUserDestRepository:

    def insert_entity(self, conn: Connection, entity: DmUserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        user_name = EXCLUDED.user_name,
                        user_login = EXCLUDED.user_login
                        ;
                """,
                {
                    "user_id": entity.user_id,
                    "user_name": entity.user_name,
                    "user_login": entity.user_login
                },
            )


class DmUserLoader:
    WF_KEY = "dm_users_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Chunk size for bulk load

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = DmUserOriginRepository(pg_stg)
        self.dds = DmUserDestRepository()
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
            self.log.info(f"Found {len(load_queue)} users to load to dm_users.")
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
