import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from examples.stg.delivery_system_dag.couriers_loader import CourierLoader
from examples.stg.delivery_system_dag.deliveries_loader import DeliveryLoader

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
        courier_entity = CourierLoader(dwh_pg_connect)
        courier_entity.load_entities()

    @task()
    def load_task_stg_deliveries():
        context = get_current_context()
        delivery_entity = DeliveryLoader(dwh_pg_connect)
        execution_date = context["ds"]
        delivery_entity.load_entities(execution_date)

    task_couriers = load_task_stg_couriers()
    task_deliveries = load_task_stg_deliveries()

    task_couriers
    task_deliveries


deliveries_stg_dag = sprint5_project_stg_delivery_system()  