create table if not exists stg.deliverysystem_deliveries (
  id serial not null primary key,
  object_value text not null,
  delivery_date date not null,
  update_ts timestamp NOT NULL
);
DO $$
BEGIN

  BEGIN
    alter table stg.deliverysystem_deliveries add constraint deliverysystem_deliveries_object_value_uindex UNIQUE (object_value);
  EXCEPTION
    WHEN duplicate_table THEN
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;