create table if not exists stg.ordersystem_users(
  id serial not null primary key,-- уникальный идентификатор записи в таблице типа serial. Поле также должно быть первичным ключом.
  object_id varchar not null,-- понадобится поле типа varchar для хранения уникального идентификатора документа из MongoDB.
  object_value text not null,-- нужно поле типа text, в котором будет храниться весь документ из MongoDB.
  update_ts timestamp NOT NULL
);
alter table stg.ordersystem_users
  add constraint ordersystem_users_object_id_uindex UNIQUE (object_id);