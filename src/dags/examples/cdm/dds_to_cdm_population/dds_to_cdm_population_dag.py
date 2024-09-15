import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from examples.cdm.dds_to_cdm_population.cdm_settlement_report_loader import CdmSettlementLoader
from examples.cdm.dds_to_cdm_population.cdm_courier_ledger_loader import CdmCourierLedgerLoader
from lib import ConnectionBuilder
from datetime import datetime

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 0 * * *',  # rebuild datamart once a day, at midnight
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

    @task(task_id="load_cdm_courier_ledger")
    def load_task_cdm_courier_ledger():
        context = get_current_context()
        execution_date = context["ds"]
        execution_year = datetime.strptime(execution_date,'%Y-%m-%d').year
        execution_month = datetime.strptime(execution_date,'%Y-%m-%d').month
        cdm_courier_ledger = CdmCourierLedgerLoader(dwh_pg_connect)
        cdm_courier_ledger.delete_courier_ledger(execution_year, execution_month) # clean only data for month from DAG logical date
        cdm_courier_ledger.load_courier_ledger(execution_year, execution_month) # reload only data for month from DAG logical date

    cdm_settlement_report_dict = load_task_cdm_settlement_report()
    cdm_courier_ledger_dict = load_task_cdm_courier_ledger()

    cdm_settlement_report_dict
    cdm_courier_ledger_dict


dds_to_cdm_dag = sprint5_dds_to_cdm_population_dag()
