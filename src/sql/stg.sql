CREATE SCHEMA IF NOT EXISTS 'stg'
;

CREATE TABLE IF NOT EXISTS stg.api_restaurants(
    id serial primary key,
    restaurant_name text
);

CREATE TABLE IF NOT EXISTS stg.api_couriers(
    id serial primary key,
    courier_name text
);

CREATE TABLE IF NOT EXISTS stg.api_deliveries(
    id serial primary key,
    content text
);
