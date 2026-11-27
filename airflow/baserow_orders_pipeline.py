from datetime import datetime

import requests
import clickhouse_connect

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable


BASEROW_URL = (
    "https://api.baserow.io/api/database/rows/table/"
    "1226173/?user_field_names=true&size=200"
)


def load_baserow_orders_to_clickhouse():
    import os

    token = os.environ["BASEROW_TOKEN"]

    response = requests.get(
        BASEROW_URL,
        headers={"Authorization": f"Token {token}"},
        timeout=30,
    )
    response.raise_for_status()

    rows = response.json()["results"]

    data = []

    for row in rows:
        data.append(
            [
                row["Order ID"],
                row["Customer Name"],
                row["Product"],
                row["Category"]["value"],
                int(row["Quantity"]),
                float(row["Unit Price"]),
                row["Order Status"]["value"],
                datetime.strptime(row["Order Date"], "%Y-%m-%d").date(),
            ]
        )

    client = clickhouse_connect.get_client(
        host=Variable.get("CLICKHOUSE_HOST"),
        port=int(Variable.get("CLICKHOUSE_PORT")),
        username=Variable.get("CLICKHOUSE_USER"),
        password=Variable.get("CLICKHOUSE_PASSWORD"),
    )

    client.command("""
        CREATE DATABASE IF NOT EXISTS bronze
    """)

    client.command("""
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
        ORDER BY order_id
    """)

    client.command("TRUNCATE TABLE bronze.baserow_orders")

    client.insert(
        "bronze.baserow_orders",
        data,
        column_names=[
            "order_id",
            "customer_name",
            "product",
            "category",
            "quantity",
            "unit_price",
            "order_status",
            "order_date",
        ],
    )

    client.close()


with DAG(
    dag_id="baserow_orders_pipeline",
    start_date=datetime(2026, 9, 25),
    schedule="@daily",
    catchup=False,
) as dag:

    load_to_bronze = PythonOperator(
        task_id="load_baserow_orders_to_bronze",
        python_callable=load_baserow_orders_to_clickhouse,
    )