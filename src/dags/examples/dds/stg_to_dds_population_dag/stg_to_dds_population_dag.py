import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.stg_to_dds_population_dag.dm_users_loader import DMUserLoader
from examples.dds.stg_to_dds_population_dag.dm_restaurants_loader import DmRestaurantLoader
from examples.dds.stg_to_dds_population_dag.dm_timestamps_loader import DmTsLoader
from examples.dds.stg_to_dds_population_dag.dm_products_loader import DmProductLoader
from examples.dds.stg_to_dds_population_dag.dm_orders_loader import DmOrderLoader
from examples.dds.stg_to_dds_population_dag.fct_product_sales_loader import FctProductSalesLoader


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_stg_to_dds_population_dag():
    # Create connection to dwh database.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_task_dm_users():
        # создаем экземпляр класса, в котором реализована логика.
        dm_users_loader = DMUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_users_loader.load_dm_users()  # Вызываем функцию, которая перельет данные.
    
    @task(task_id="dm_restaurants_load")
    def load_task_dm_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        dm_restaurants_loader = DmRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_restaurants_loader.load_dm_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="dm_timestamps_load")
    def load_task_dm_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        dm_timestamps_loader = DmTsLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_timestamps_loader.load_dm_timestamps()  # Вызываем функцию, которая перельет данные.   

    @task(task_id="dm_products_load")
    def load_task_dm_products():
        # создаем экземпляр класса, в котором реализована логика.
        dm_products_loader = DmProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_products_loader.load_dm_products()  # Вызываем функцию, которая перельет данные.   

    @task(task_id="dm_orders_load")
    def load_task_dm_orders():
        # создаем экземпляр класса, в котором реализована логика.
        dm_orders_loader = DmOrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        dm_orders_loader.load_dm_orders()  # Вызываем функцию, которая перельет данные.   

    @task(task_id="fct_product_sales_load")
    def load_task_fct_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        fct_product_sales_loader = FctProductSalesLoader(dwh_pg_connect, dwh_pg_connect, log)
        fct_product_sales_loader.load_fct_product_sales()  # Вызываем функцию, которая перельет данные.   

    # Инициализируем объявленные таски.
    dm_users_dict = load_task_dm_users()
    dm_restaurants_dict = load_task_dm_restaurants()
    dm_timestamps_dict = load_task_dm_timestamps()
    dm_products_dict = load_task_dm_products()
    dm_orders_dict = load_task_dm_orders()
    fct_product_sales_dict = load_task_fct_product_sales()


    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_users_dict >> dm_orders_dict
    dm_restaurants_dict >> dm_orders_dict
    dm_timestamps_dict >> dm_orders_dict
    dm_products_dict >> fct_product_sales_dict
    dm_orders_dict >> fct_product_sales_dict

stg_to_dds_dag = sprint5_stg_to_dds_population_dag()
