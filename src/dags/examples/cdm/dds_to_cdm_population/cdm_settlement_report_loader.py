from lib import PgConnect

class CdmSettlementLoader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def load_settlement_report(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                    cur.execute(
                         """
                        /*truncate cdm.dm_settlement_report;*/
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
                         select dr.restaurant_id as restaurant_id, 
                                dr.restaurant_name, 
                                dt.date as settlement_date, 
                                count(distinct fps.order_id) as orders_count, 
                                sum(fps.total_sum) as orders_total_sum, 
                                sum(fps.bonus_payment) as orders_bonus_payment_sum, 
                                sum(fps.bonus_grant) as orders_bonus_granted_sum, 
                                sum(fps.total_sum * 0.25) as order_processing_fee, 
                                sum(fps.total_sum * 0.75 - fps.bonus_payment) as restaurant_reward_sum
                           from dds.dm_restaurants dr 
                           inner join dds.dm_orders dor on dr.id = dor.restaurant_id 
                           inner join dds.fct_product_sales fps on fps.order_id = dor.id 
                           inner join dds.dm_timestamps dt on dt.id = dor.timestamp_id 
                          where 1 = 1
                            and dr.active_to > now() 
                            and dor.order_status = 'CLOSED'
                          group by  dr.id, dr.restaurant_name, dt.date
                         ;
                         """
                    )