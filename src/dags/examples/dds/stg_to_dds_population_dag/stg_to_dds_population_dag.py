import logging

import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from examples.dds.stg_to_dds_population_dag.dm_users_loader import DmUserLoader
from examples.dds.stg_to_dds_population_dag.dm_restaurants_loader import DmRestaurantLoader
from examples.dds.stg_to_dds_population_dag.dm_timestamps_loader import DmTsLoader
from examples.dds.stg_to_dds_population_dag.dm_products_loader import DmProductLoader
from examples.dds.stg_to_dds_population_dag.dm_orders_loader import DmOrderLoader
from examples.dds.stg_to_dds_population_dag.fct_product_sales_loader import FctProductSalesLoader


log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2024, 9, 7, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_to_dds_population_dag():
    # Create connection to dwh database.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Define task to load users data
    @task(task_id="dm_users_load")
    def load_task_dm_users():
        # Create class instance where logic defined
        dm_users_loader = DmUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_users_loader.load_entities()  # Call function to load data finally
    
    @task(task_id="dm_restaurants_load")
    def load_task_dm_restaurants():
        dm_restaurants_loader = DmRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_restaurants_loader.load_entities()

    @task(task_id="dm_timestamps_load")
    def load_task_dm_timestamps():
        dm_timestamps_loader = DmTsLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_timestamps_loader.load_entities() 

    @task(task_id="dm_products_load")
    def load_task_dm_products():
        dm_products_loader = DmProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_products_loader.load_entities()

    @task(task_id="dm_orders_load")
    def load_task_dm_orders():
        dm_orders_loader = DmOrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_orders_loader.load_entities() 

    @task(task_id="fct_product_sales_load")
    def load_task_fct_product_sales():
        fct_product_sales_loader = FctProductSalesLoader(dwh_pg_connect, dwh_pg_connect, log)
        fct_product_sales_loader.load_entities()

    # Initialize tasks
    dm_users_dict = load_task_dm_users()
    dm_restaurants_dict = load_task_dm_restaurants()
    dm_timestamps_dict = load_task_dm_timestamps()
    dm_products_dict = load_task_dm_products()
    dm_orders_dict = load_task_dm_orders()
    fct_product_sales_dict = load_task_fct_product_sales()


    # Setup sequence of tasks
    dm_users_dict >> dm_orders_dict
    dm_restaurants_dict >> dm_products_dict
    dm_restaurants_dict >> dm_orders_dict
    dm_timestamps_dict >> dm_orders_dict
    dm_products_dict >> fct_product_sales_dict
    dm_orders_dict >> fct_product_sales_dict

stg_to_dds_dag = sprint5_stg_to_dds_population_dag()
