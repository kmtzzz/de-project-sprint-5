create table if not exists dds.dm_deliveries (
 id serial constraint dm_deliveries_pkey primary key,
 order_id integer not null,
 delivery_id varchar not null,
 courier_id integer not null,
 rate integer not null,
 tip_sum numeric(14,2) not null,
 delivery_ts timestamp NOT NULL
);
DO $$
BEGIN
  BEGIN
    alter table dds.dm_deliveries add constraint dm_deliveries_courier_id_fkey foreign key (courier_id) references dds.dm_couriers(id);
  EXCEPTION
    WHEN duplicate_table THEN
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table dds.dm_deliveries add constraint dm_deliveries_order_id_fkey foreign key (order_id) references dds.dm_orders(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;