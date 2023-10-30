CREATE SCHEMA IF NOT EXISTS 'cdm'
;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
    id serial primary key,
    courier_id int,
    courier_name varchar(255),
    settlement_year smallint, --               — год отчёта.
    settlement_month smallint, --              — месяц отчёта, где 1 — январь и 12 — декабрь.
    orders_count int, --                       — количество заказов за период (месяц).
    orders_total_sum numeric(14, 2), --        — общая стоимость заказов.
    rate_avg numeric(14, 2), --                — средний рейтинг курьера по оценкам пользователей.
    order_processing_fee, numeric(14, 2), --   — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
    courier_order_sum numeric(14, 2), --       — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
    courier_tips_sum numeric(14, 2), --        — сумма, которую пользователи оставили курьеру в качестве чаевых.
    courier_reward_sum numeric(14, 2), --      — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).
);

CREATE OR REPLACE VIEW cdm.couriers_with_rates as(
    SELECT
        dd.courier_id as courier_id,
        c.name as name,
        fas.order_id as order_id,
        fas.order_sum as order_sum,
        fas.tip_sum as tip_sum,
        dd.rate as rate,
        extract('year' from o.order_ts) as year,
        extract('month' from o.order_ts) as month,
    FROM dds.fct_api_sales as fas
    LEFT JOIN dds.dm_delivery_details as dd
        ON fas.delivery_id = dd.delivery_id
    LEFT JOIN dds.dm_api_orders as o
        ON fas.order_id = o.order_id
    LEFT JOIN dds.dm_api_couriers as c
        ON dd.courier_id = c.id
);