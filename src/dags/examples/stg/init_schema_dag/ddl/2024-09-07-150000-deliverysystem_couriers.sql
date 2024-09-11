create table if not exists stg.deliverysystem_couriers (
  id serial not null primary key,
  object_value text not null,
  update_ts timestamp NOT NULL
);
DO $$
BEGIN

  BEGIN
    alter table stg.deliverysystem_couriers add constraint deliverysystem_couriers_object_value_uindex UNIQUE (object_value);
  EXCEPTION
    WHEN duplicate_table THEN
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint already exists';
  END;
END $$;