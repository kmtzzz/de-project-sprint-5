1.Список полей, которые необходимы для витрины.
    id — идентификатор записи, суррогатный ключ
    courier_id — ID курьера, которому перечисляем, courier_id записи из измерения dm_couriers
    courier_name — Ф. И. О. курьера из измерения dm_couriers
    settlement_year — год отчёта из заказов dm_orders
    settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь из заказов dm_orders
    orders_count — количество заказов за период (месяц) из заказов dm_orders
    orders_total_sum — общая стоимость заказов из заказов dm_orders
    rate_avg — средний рейтинг курьера по оценкам пользователей из доставок dm_deliveries
    order_processing_fee — сумма, удержанная компанией за обработку заказов из заказов dm_orders
    courier_order_sum — сумма, которую необходимо перечислить курьеру из заказов dm_orders в зависимости от rate_avg
    courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых из доставок dm_deliveries
    courier_reward_sum — сумма, которую необходимо перечислить курьеру на основе courier_order_sum и courier_tips_sum

2.Список таблиц в слое DDS, из которых вы возьмёте поля для витрины. Отметьте, какие таблицы уже есть в хранилище, а каких пока нет. Недостающие таблицы вы создадите позднее. Укажите, как они будут называться.
    stg.deliverysystem_couriers - сырые данные из АПИ по курьерам. Новая таблица
    stg.deliverysystem_deliveries - сырые данные из АПИ по доставкам. Новая таблица
    dds.dm_couriers - измерение курьеров. Новая таблица 
    dds.dm_deliveries - измерение доставок. Новая таблица
    dds.fct_deliveriy_details - детали доставленных заказов курьерами. Новая таблица
    cdm.dm_courier_ledger - финальная витрина. Новая таблица
    dds.dm_orders - измерение ордеров, нужно для связки доставок с измерением timestamp. Существующая таблица
    dds.dm_timestamps - измерение timestamp. Существующая таблица
    stg.ordersystem_orders - информации о сумме заказа. Существующая таблица

3.На основе списка таблиц в DDS составьте список сущностей и полей, которые необходимо загрузить из API. Использовать все методы API необязательно: важно загрузить ту информацию, которая нужна для выполнения задачи.

Для выполнения задачи достаточно использовать следующие API:
    /couriers - методом GET считывается информация о курьерах
        используемые поля:
            _id
            name
    /deliveries - методом GET считывается информация о доставках в привязке к курьеру и заказу
        используемые поля:
            order_id
            delivery_id                     
            courier_id
            rate
            tip_sum
    
