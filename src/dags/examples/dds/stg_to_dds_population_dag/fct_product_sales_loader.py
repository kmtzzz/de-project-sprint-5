from logging import Logger
from typing import List

from examples.dds import DdsEtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FctProductSalesObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class FctProductOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_entities(self, id_threshold: int, limit: int) -> List[FctProductSalesObj]:
        with self._db.client().cursor(row_factory=class_row(FctProductSalesObj)) as cur:
            cur.execute(
                """
                     select order_items.id as id,
                            dp.id as product_id,
                            dord.id as order_id,
                            order_items.quantity as count,
                            order_items.price::numeric(19,5) as price,
                            (order_items.price::numeric * order_items.quantity::numeric)::numeric(19,5) as total_sum,
                            coalesce(bonus_transaction.bonus_payment::numeric(19,5),0) as bonus_payment,
                            coalesce(bonus_transaction.bonus_grant::numeric(19,5),0) as bonus_grant
                        from (
                              select id, 
                                    object_id as order_object_id,
                                    json_array_elements((object_value::json->>'order_items')::json)->>'id' as product_object_id,
                                    json_array_elements((object_value::json->>'order_items')::json)->>'quantity' as quantity,
                                    json_array_elements((object_value::json->>'order_items')::json)->>'price' as price,
                                    (object_value::json->>'date')::timestamp as update_ts 
                                from stg.ordersystem_orders oo
                                ) as order_items
                        left join (
                                    select json_array_elements((event_value::json->>'product_payments')::json)->>'product_id' as product_object_id,
                                        json_array_elements((event_value::json->>'product_payments')::json)->>'bonus_payment' as bonus_payment,
                                        json_array_elements((event_value::json->>'product_payments')::json)->>'bonus_grant' as bonus_grant,
                                        event_value::json->>'order_id' as order_object_id
                                    from stg.bonussystem_events be 
                                    where event_type = 'bonus_transaction'
                                    ) as bonus_transaction
                            on order_items.order_object_id  = bonus_transaction.order_object_id
                        and order_items.product_object_id = bonus_transaction.product_object_id
                        join dds.dm_products dp 
                            on order_items.product_object_id = dp.product_id 
                            --and order_items.update_ts between dp.active_from and dp.active_to 
                        join dds.dm_orders dord 
                            on order_items.order_object_id = dord.order_key 
                     WHERE order_items.id > %(threshold)s --Load only new data
                    ORDER BY order_items.id ASC --Sort by id is mandatory as ID is used as cursor
                    LIMIT %(limit)s; --Processing batch size
                """, {
                    "threshold": id_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FctProductDestRepository:

    def insert_entity(self, conn: Connection, entity: FctProductSalesObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                        ;
                """,
                {
                    "product_id": entity.product_id,
                    "order_id": entity.order_id,
                    "count": entity.count,
                    "price": entity.price,
                    "total_sum": entity.total_sum,
                    "bonus_payment": entity.bonus_payment,
                    "bonus_grant": entity.bonus_grant
                },
            )


class FctProductSalesLoader:
    WF_KEY = "fct_product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50000  # Batch size, 25 is to show incremental load is applicable

    def __init__(self, pg_stg: PgConnect, pg_dds: PgConnect, log: Logger) -> None:
        self.pg_dds = pg_dds
        self.origin = FctProductOriginRepository(pg_stg)
        self.dds = FctProductDestRepository()
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
            self.log.info(f"Found {len(load_queue)} order items to load to fct_product_sales.")
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
