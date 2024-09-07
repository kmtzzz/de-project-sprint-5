create table if not exists dds.dm_orders (
 id serial constraint dm_orders_pkey primary key,
 user_id integer not null,
 restaurant_id integer not null,
 timestamp_id integer not null,
 order_key varchar not null,
 order_status varchar not null
);
DO $$
BEGIN
  BEGIN
    alter table dds.dm_orders add constraint dm_orders_user_id_fkey foreign key (user_id) references dds.dm_users(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint dm_orders_user_id_fkey already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table dds.dm_orders add constraint dm_orders_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint dm_orders_restaurant_id_fkey already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table dds.dm_orders add constraint dm_orders_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_timestamps(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint dm_orders_timestamp_id_fkey already exists';
  END;
END $$;




