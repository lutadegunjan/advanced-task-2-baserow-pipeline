{{ config(materialized='view') }}

SELECT
    order_id,
    customer_name,
    product,
    category,
    quantity,
    unit_price,
    quantity * unit_price AS total_amount,
    order_status,
    order_date
FROM {{ source('baserow', 'baserow_orders') }}
