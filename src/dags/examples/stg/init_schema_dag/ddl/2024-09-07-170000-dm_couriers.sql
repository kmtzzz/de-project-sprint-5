create table if not exists dds.dm_couriers (
  id serial constraint dm_couriers_pkey primary key,
  courier_id varchar not null,
  courier_name varchar not null
);