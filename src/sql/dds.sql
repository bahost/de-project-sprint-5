CREATE SCHEMA IF NOT EXISTS 'dds'
;

CREATE TABLE IF NOT EXISTS dds.dm_api_address(
    id serial primary key,
    city varchar(255) default 'Moscow' not null,
    street varchar(255) not null,
    house_number int not null,
    apartment int not null,
);

CREATE TABLE IF NOT EXISTS dds.dm_api_restaurants(
    id serial primary key,
    restaurant_id varchar not null,
    name text not null,
);

CREATE TABLE IF NOT EXISTS dds.dm_api_couriers(
    id serial primary key,
    courier_id varchar not null,
    name text not null,
);

CREATE TABLE IF NOT EXISTS dds.dm_api_orders(
    id serial primary key,
    order_id varchar not null,
    order_ts timestamp not null,
);

CREATE TABLE IF NOT EXISTS dds.dm_delivery_details(
    id serial primary key,
    delivery_id varchar not null,
    courier_id varchar not null,
    address_id varchar not null,
    delivery_ts timestamp not null,
    rate smallint not null,
);

CREATE TABLE IF NOT EXISTS dds.fct_api_sales(
    id serial primary key,
    order_id int not null,
    delivery_id varchar not null,
    order_sum numeric(14,2) not null,
    tip_sum numeric(14,2) not null,
);