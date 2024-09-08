create table if not exists cdm.dm_settlement_report (
  id serial constraint id_pk primary key,
  restaurant_id varchar not null,
  restaurant_name varchar not null,
  settlement_date date not null,
  orders_count integer not null,
  orders_total_sum numeric(14, 2) not null,
  orders_bonus_payment_sum numeric(14, 2) not null,
  orders_bonus_granted_sum numeric(14, 2) not null,
  order_processing_fee numeric(14, 2) not null,
  restaurant_reward_sum numeric(14, 2) not null
);

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_settlement_date_check  
          check(settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01');
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint dm_settlement_report_settlement_date_check already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_orders_count_check check(orders_count>=0) ;
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_orders_total_sum check(orders_total_sum>=0) ;
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;


DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_orders_bonus_payment_sum check(orders_bonus_payment_sum>=0) ;
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_orders_bonus_granted_sum check(orders_bonus_granted_sum>=0) ;
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_order_processing_fee check(order_processing_fee>=0) ;
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_restaurant_reward_sum check(restaurant_reward_sum>=0) ;
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report add constraint dm_settlement_report_unique_restaraunt UNIQUE (restaurant_id, settlement_date);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table cdm.dm_settlement_report  alter orders_count set default 0;
    alter table cdm.dm_settlement_report  alter orders_total_sum set default 0;
    alter table cdm.dm_settlement_report  alter orders_bonus_payment_sum set default 0;
    alter table cdm.dm_settlement_report  alter orders_bonus_granted_sum set default 0;
    alter table cdm.dm_settlement_report  alter order_processing_fee set default 0;
    alter table cdm.dm_settlement_report  alter restaurant_reward_sum set default 0;
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

