create table if not exists stg.ordersystem_orders (
  id serial not null primary key,-- уникальный идентификатор записи в таблице типа serial. Поле также должно быть первичным ключом.
  object_id varchar not null,-- понадобится поле типа varchar для хранения уникального идентификатора документа из MongoDB.
  object_value text not null,-- нужно поле типа text, в котором будет храниться весь документ из MongoDB.
  update_ts timestamp NOT NULL
);
DO $$
BEGIN

  BEGIN
    alter table stg.ordersystem_orders add constraint ordersystem_orders_object_id_uindex UNIQUE (object_id);
  EXCEPTION
    WHEN duplicate_table THEN  -- postgres raises duplicate_table at surprising times. Ex.: for UNIQUE constraints.
    WHEN duplicate_object THEN
      RAISE NOTICE 'Table constraint ordersystem_orders_object_id_uindex already exists';
  END;
END $$;
