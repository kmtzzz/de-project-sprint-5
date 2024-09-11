import pendulum
from airflow.decorators import dag, task
from examples.stg.delivery_system_dag.couriers_loader import CourierLoader

from lib import ConnectionBuilder


@dag(
    schedule_interval='0/15 * * * *', 
    start_date=pendulum.datetime(2024, 9, 11, tz="UTC"), 
    catchup=False, 
    tags=['sprint5', 'project', 'stg', 'origin'],  
    is_paused_upon_creation=True 
)
def sprint5_project_stg_delivery_system():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_task_stg_couriers():
        courier_obj = CourierLoader(dwh_pg_connect)
        courier_obj.load_entities()

    task_courier = load_task_stg_couriers()


    task_courier


deliveries_stg_dag = sprint5_project_stg_delivery_system()  