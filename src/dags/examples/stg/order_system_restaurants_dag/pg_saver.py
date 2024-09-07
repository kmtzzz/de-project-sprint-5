from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, id: str, update_ts: datetime, val: Any, db_schema_name: str, db_table_name: str): # add table and schema name as parameters
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO {0}.{1}(object_id, object_value, update_ts)
                    VALUES (%(id)s, %(val)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """.format(db_schema_name, db_table_name),
                {
                    "id": id,
                    "val": str_val,
                    "update_ts": update_ts
                }
            )