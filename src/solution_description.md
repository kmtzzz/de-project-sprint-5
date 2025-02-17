### Как выполнена работа
0. Все скрипты, которые инициализируют таблицы и для спринта, и для проектной работы добавлены в sprint5_init_schema_dag  
    - Скрипты поддерживают перезапуск без дропа таблиц.
1. Сделан новый DAG sprint5_project_stg_delivery_system:  
    - stg/delivery_system_dag
2. В спринтовый DAG sprint5_stg_to_dds_population_dag  добавлена логика:  
    - dds/stg_to_dds_population_dag
        - dm_couriers_loader.py
        - dm_deliveries_loader.py
        - fct_delivery_details.py  
3. В спринтовый DAG sprint5_dds_to_cdm_population_dag добавлена логика:
    - cdm/dds_to_cdm_population
        - cdm_courier_ledger_loader.py
