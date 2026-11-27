CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.baserow_orders
(
    order_id String,
    customer_name String,
    product String,
    category String,
    quantity Int64,
    unit_price Float64,
    order_status String,
    order_date Date
)
ENGINE = MergeTree
ORDER BY order_id;
