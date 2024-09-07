create table if not exists dds.fct_product_sales(
  id serial constraint fct_product_sales_pkey primary key,
  product_id integer not null,
  order_id integer not null,
  count integer default 0 not null constraint fct_product_sales_count_check check (count >= 0),
  price numeric(14,2) default 0 not null constraint fct_product_sales_price_check check (price >= 0),
  total_sum numeric(14,2) default 0 not null constraint fct_product_sales_total_sum_check check (total_sum >= 0),
  bonus_payment numeric(14,2) default 0 not null constraint fct_product_sales_bonus_payment_check check (bonus_payment >= 0),
  bonus_grant numeric(14,2) default 0 not null constraint fct_product_sales_bonus_grant_check check (bonus_grant >= 0)
);

DO $$
BEGIN
  BEGIN
    alter table dds.fct_product_sales add constraint fct_product_sales_order_id_fkey foreign key (order_id) references dds.dm_orders(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint fct_product_sales_order_id_fkey already exists';
  END;
END $$;

DO $$
BEGIN
  BEGIN
    alter table dds.fct_product_sales add constraint fct_product_sales_product_id_fkey foreign key (product_id) references dds.dm_products(id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint fct_product_sales_product_id_fkey already exists';
  END;
END $$;

