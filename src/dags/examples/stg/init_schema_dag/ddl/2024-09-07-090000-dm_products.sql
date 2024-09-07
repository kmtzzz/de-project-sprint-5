create table if not exists dds.dm_products (
  id serial constraint dm_products_pkey primary key,
  product_id varchar not null,
  product_name varchar not null,
  product_price numeric(14,2) default 0 not null constraint dm_products_product_price_check check (product_price >= 0),
  restaurant_id integer not null,
  active_From timestamp not null,
  active_to timestamp not null
 );
DO $$
BEGIN
  BEGIN
    alter table dds.dm_products add constraint dm_products_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint dm_products_restaurant_id_fkey already exists';
  END;
END $$;