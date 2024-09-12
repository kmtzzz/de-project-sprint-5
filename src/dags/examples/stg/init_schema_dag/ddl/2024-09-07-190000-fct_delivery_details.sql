create table if not exists dds.fct_delivery_details(
  id serial constraint fct_delivery_details_pkey primary key,
  courier_id integer not null,
  courier_name varchar not null,
  settlement_year integer not null,
  settlement_month integer not null,
  order_id integer not null,
  order_sum numeric(14,2) default 0 not null constraint fct_delivery_details_order_sum_check check (order_sum >= 0),
  rate integer default 0 not null constraint fct_delivery_details_rate_check check ((rate >= 0) and (rate<=5)),
  tip_sum numeric(14,2) default 0 not null constraint fct_delivery_details_tip_sum_check check (tip_sum >= 0)
);

DO $$
BEGIN
  BEGIN
    alter table dds.fct_delivery_details add constraint fct_delivery_details_order_id_fkey foreign key (order_id) references dds.dm_orders(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table dds.fct_delivery_details add constraint fct_delivery_details_courier_id_fkey foreign key (courier_id) references dds.dm_couriers(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

