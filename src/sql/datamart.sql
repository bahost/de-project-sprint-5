
CREATE OR REPLACE VIEW cdm.couriers_agg_with_rates as(
    SELECT
        courier_id,
        name,
        orders_count,
        orders_total_sum,
        CASE
            WHEN rate < 4 THEN greatest(0.05*orders_total_sum, 100.0)
            WHEN rate < 4.5 THEN greatest(0.07*orders_total_sum, 150.0)
            WHEN rate < 4.9 THEN greatest(0.08*orders_total_sum, 175.0)
            WHEN rate > 4.9 THEN greatest(0.1*orders_total_sum, 200.0)
        END AS courier_order_sum,
        tip_sum,
        year,
        month,
        rate
    FROM (
        SELECT
            courier_id,
            name,
            year,
            month,
            avg(rate)           as rate
            count(order_id)     as orders_count,
            sum(order_sum)      as orders_total_sum,
            sum(tip_sum)        as tip_sum
        FROM cdm.couriers_with_rates
        GROUP BY
            courier_id,
            name,
            year,
            month
    ) as t
);