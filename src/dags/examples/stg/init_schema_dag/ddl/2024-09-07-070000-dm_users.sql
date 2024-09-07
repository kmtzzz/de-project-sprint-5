create table if not exists dds.dm_users (
  id serial constraint dm_users_pkey primary key,
  user_id varchar not null,
  user_name varchar not null,
  user_login varchar not null
);