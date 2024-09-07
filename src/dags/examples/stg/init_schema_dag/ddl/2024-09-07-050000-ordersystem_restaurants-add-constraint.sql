alter table stg.ordersystem_restaurants
  add constraint ordersystem_restaurants_object_id_uindex UNIQUE (object_id);