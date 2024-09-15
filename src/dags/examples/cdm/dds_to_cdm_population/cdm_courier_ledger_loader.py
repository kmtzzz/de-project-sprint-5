from lib import PgConnect
from datetime import date

class CdmCourierLedgerLoader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def delete_courier_ledger(self, execution_year: int, execution_month: int ) -> None:
        print(f'from delete_courier_ledger method {execution_year=} {execution_month=}')
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                    cur.execute(
                        """
                        delete from cdm.dm_courier_ledger
                         where settlement_year = %(year)s
                           and settlement_month = %(month)s
                         ;
                         """,
                    {
                        "year": execution_year,
                        "month": execution_month
                    }
                    )         

    def load_courier_ledger(self, execution_year: int, execution_month: int) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                    cur.execute(
                        """
                        with courier_avg_rate as (
                        select courier_id,
                            settlement_year,
                            settlement_month,
                            avg(rate) as avg_rate
                        from dds.fct_delivery_details fdd 
                        group by courier_id, settlement_year, settlement_month
                        ),
                        courier_ledger_data as (
                        select dc.courier_id,
                            fdd.courier_name,
                            fdd.settlement_year,
                            fdd.settlement_month,
                            count(distinct fdd.order_id) as orders_count,
                            sum(fdd.order_sum) as orders_total_sum,
                            ar.avg_rate as rate_avg,
                            sum(fdd.order_sum)*0.25 as order_processing_fee,
                            sum(case
                                    when fdd.rate < 4 then
                                    case
                                        when fdd.order_sum * 0.05 < 100 then 100
                                        else fdd.order_sum * 0.05
                                    end
                                    when fdd.rate < 4.5 then
                                    case
                                        when fdd.order_sum * 0.07 < 150 then 150
                                        else fdd.order_sum * 0.07
                                    end
                                    when fdd.rate < 4.9 then
                                    case
                                        when fdd.order_sum * 0.08 < 175 then 175
                                        else fdd.order_sum * 0.08
                                    end           
                                    else
                                    case
                                        when fdd.order_sum * 0.1 < 200 then 200
                                        else fdd.order_sum * 0.1
                                    end
                                    end) AS courier_order_sum,
                            sum(fdd.tip_sum) as courier_tips_sum
                        from dds.fct_delivery_details fdd
                        join courier_avg_rate ar
                            on fdd.courier_id = ar.courier_id
                        and fdd.settlement_year = ar.settlement_year
                        and fdd.settlement_month = ar.settlement_month
                        join dds.dm_couriers dc 
                            on fdd.courier_id = dc.id
                        group by dc.courier_id,
                                fdd.courier_name,
                                fdd.settlement_year,
                                fdd.settlement_month,
                                ar.avg_rate
                        )
                         insert into cdm.dm_courier_ledger (
                                courier_id, 
                                courier_name, 
                                settlement_year, 
                                settlement_month, 
                                orders_count, 
                                orders_total_sum, 
                                rate_avg, 
                                order_processing_fee, 
                                courier_order_sum,
                                courier_tips_sum,
                                courier_reward_sum)
                        select courier_id, 
                               courier_name, 
                               settlement_year, 
                               settlement_month, 
                               orders_count, 
                               orders_total_sum, 
                               rate_avg, 
                               order_processing_fee, 
                               courier_order_sum,
                               courier_tips_sum, 
                               courier_order_sum + courier_tips_sum * 0.95 
                         from courier_ledger_data
                        where settlement_year = %(year)s
                          and settlement_month = %(month)s
                         ;
                         """,
                    {
                        "year": execution_year,
                        "month": execution_month
                    }
                    )