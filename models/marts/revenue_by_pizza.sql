

{{ config(materialized='table') }}

SELECT
    pizza_type
    , COUNT(order_id) AS total_orders
    , SUM(price) AS gross_revenue
    , CURRENT_DATE AS report_date
FROM
    {{ source('main', 'raw_pizza_orders') }}
GROUP BY
    pizza_type