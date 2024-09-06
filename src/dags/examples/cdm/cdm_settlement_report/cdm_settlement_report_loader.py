from lib import PgConnect


class CdmSettlementLoader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def load_settlement_report(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                    cur.execute(
                         """
                        delete from cdm.dm_settlement_report;

                        insert into cdm.dm_settlement_report (
                                restaurant_id, 
                                restaurant_name, 
                                settlement_date, 
                                orders_count, 
                                orders_total_sum, 
                                orders_bonus_payment_sum, 
                                orders_bonus_granted_sum, 
                                order_processing_fee, 
                                restaurant_reward_sum)
                        select  r.id as restaurant_id, 
                                r.restaurant_name, 
                                d.date as settlement_date, 
                                count(distinct s.order_id) as orders_count, 
                                sum(s.total_sum) as orders_total_sum, 
                                sum(s.bonus_payment) as orders_bonus_payment_sum, 
                                sum(s.bonus_grant) as orders_bonus_granted_sum, 
                                sum(s.total_sum * 0.25) as order_processing_fee, 
                                sum(s.total_sum * 0.75 - s.bonus_payment) as restaurant_reward_sum
                           from dds.dm_restaurants r 
                           inner join dds.dm_orders o on r.id = o.restaurant_id 
                           inner join dds.fct_product_sales s on s.order_id = o.id 
                           inner join dds.dm_timestamps d on d.id = o.timestamp_id 
                          where r.active_to > now() and o.order_status = 'CLOSED'
                          group by  r.id, r.restaurant_name, d.date
                             on conflict (restaurant_id, settlement_date) do update 
                             set
                                orders_count = EXCLUDED.orders_count,
                                orders_total_sum = EXCLUDED.orders_total_sum,
                                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                                order_processing_fee = EXCLUDED.order_processing_fee,
                                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                         """
                    )