

WITH
live_orders AS (
    SELECT
        id
        , order_id
        , pizza_type
        , order_status
        , received_at
    FROM
        "pizza_db"."public"."raw_pizza_orders"
)
, price_list AS (
    SELECT
        pizza_type
        , price
    FROM
        "pizza_db"."public"."pizza_prices"
)
SELECT
    lo.pizza_type
    , COUNT(lo.order_id) AS total_orders
    , SUM(pl.price) AS gross_revenue
    , CURRENT_DATE AS report_date
FROM
    live_orders AS lo
INNER JOIN
    price_list AS pl
        ON lo.pizza_type = pl.pizza_type
GROUP BY
    1