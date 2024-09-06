import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.cdm_settlement_report.cdm_settlement_report_loader import CdmSettlementLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def sprint5_dds_to_cdm_population_dag():
    # Create connection to dwh database.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_cdm_settlement_report")
    def load_task_cdm_settlement_report():
        # создаем экземпляр класса, в котором реализована логика.
        cdm_settlement_report = CdmSettlementLoader(dwh_pg_connect)
        cdm_settlement_report.load_settlement_report()  # Вызываем функцию, которая перельет данные.


    cdm_settlement_report_dict = load_task_cdm_settlement_report()

    cdm_settlement_report_dict


dds_to_cdm_dag = sprint5_dds_to_cdm_population_dag()
