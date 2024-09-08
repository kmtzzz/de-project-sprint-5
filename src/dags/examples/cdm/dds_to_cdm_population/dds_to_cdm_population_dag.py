import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.dds_to_cdm_population.cdm_settlement_report_loader import CdmSettlementLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *', 
    start_date=pendulum.datetime(2024, 5, 5, tz="UTC"),  
    catchup=False, 
    tags=['sprint5', 'cdm', 'origin', 'example'], 
    is_paused_upon_creation=True 
)

def sprint5_dds_to_cdm_population_dag():
    # Create connection to dwh database.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_cdm_settlement_report")
    def load_task_cdm_settlement_report():
        cdm_settlement_report = CdmSettlementLoader(dwh_pg_connect)
        cdm_settlement_report.load_settlement_report()

    cdm_settlement_report_dict = load_task_cdm_settlement_report()

    cdm_settlement_report_dict


dds_to_cdm_dag = sprint5_dds_to_cdm_population_dag()
