DO $$
BEGIN
  BEGIN
    alter table stg.ordersystem_restaurants add constraint ordersystem_restaurants_object_id_uindex UNIQUE (object_id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint ordersystem_restaurants_object_id_uindex already exists';
  END;
END $$;