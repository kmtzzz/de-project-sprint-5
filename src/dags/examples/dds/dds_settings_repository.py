from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DdsEtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class DdsEtlSettingsRepository:
    def get_settings(self, conn: Connection, etl_key: str) -> Optional[DdsEtlSetting]:
        with conn.cursor(row_factory=class_row(DdsEtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_settings(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )
