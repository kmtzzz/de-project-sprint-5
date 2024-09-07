create table if not exists dds.dm_restaurants (
  id serial constraint dm_restaurants_pkey primary key,
  restaurant_id varchar not null,
  restaurant_name varchar not null,
  active_From timestamp not null,
  active_to timestamp not null
);