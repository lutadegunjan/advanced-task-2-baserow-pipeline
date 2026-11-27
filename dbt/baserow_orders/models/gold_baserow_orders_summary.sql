{{ config(materialized='table') }}

SELECT
    category,
    count() AS order_count,
    sum(quantity) AS total_quantity,
    sum(total_amount) AS total_sales,
    avg(total_amount) AS average_order_value
FROM {{ ref('silver_baserow_orders') }}
GROUP BY category
ORDER BY total_sales DESC
