# Baserow Orders Data Pipeline

An end-to-end batch data pipeline that ingests order records from Baserow into ClickHouse and transforms them through a Bronze, Silver, and Gold medallion architecture using Apache Airflow and dbt.

## Architecture

```text
Baserow REST API
       |
       v
Apache Airflow
(Python ingestion DAG)
       |
       v
ClickHouse - Bronze
bronze.baserow_orders
       |
       v
dbt transformation
       |
       +----------------------+
       |                      |
       v                      v
ClickHouse - Silver      ClickHouse - Gold
silver_baserow_orders    gold_baserow_orders_summary
       |                      |
       v                      v
Clean order records      Category-level analytics
```

## Technology Stack

- Baserow REST API - source data
- Python and Requests - API extraction
- Apache Airflow - pipeline orchestration
- ClickHouse - analytical storage
- dbt - SQL transformations
- Docker - containerized execution

## Project Structure

```text
Advanced_Task_2_Baserow/
├── airflow/
│   └── baserow_orders_pipeline.py
├── clickhouse/
│   └── table_definitions.sql
├── dbt/
│   └── baserow_orders/
│       ├── dbt_project.yml
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── models/
│           ├── sources.yml
│           ├── silver_baserow_orders.sql
│           └── gold_baserow_orders_summary.sql
├── screenshots/
├── .gitignore
└── README.md
```

## Pipeline Workflow

### 1. Extract and Load - Bronze

The Airflow DAG `baserow_orders_pipeline` retrieves order records from the Baserow REST API.

The pipeline:
- Authenticates using the `BASEROW_TOKEN` environment variable.
- Fetches records from the configured Baserow table.
- Converts API fields into the required data types.
- Loads the records into `bronze.baserow_orders` in ClickHouse.

### 2. Transform - Silver

The dbt model `silver_baserow_orders` transforms the Bronze data into a structured order dataset and calculates:

`total_amount = quantity * unit_price`

### 3. Aggregate - Gold

The dbt model `gold_baserow_orders_summary` produces category-level metrics:

- Order count
- Total quantity
- Total sales
- Average order value

## Verified Execution Results

The pipeline was manually triggered after moving ClickHouse credentials into Airflow Variables.

| Verification | Result |
|---|---:|
| Latest Airflow DAG run | Success |
| Airflow DAG import errors | None |
| Bronze records | 20 |
| Silver records | 20 |
| Gold category records | 5 |
| Categories processed | 5 |
| Total sales across categories | 3,863 |

### Gold Aggregation Results

| Category | Orders | Quantity | Total Sales | Average Order Value |
|---|---:|---:|---:|---:|
| Books | 4 | 5 | 285 | 71.25 |
| Clothing | 4 | 13 | 755 | 188.75 |
| Electronics | 4 | 9 | 1,638 | 409.50 |
| Food | 4 | 21 | 195 | 48.75 |
| Furniture | 4 | 5 | 990 | 247.50 |

## Configuration

Credentials are not included in this repository.

Configure the following before running the pipeline:

### Airflow environment variable

- `BASEROW_TOKEN` - Baserow API authentication token.

### Airflow Variables

| Variable | Purpose |
|---|---|
| `CLICKHOUSE_HOST` | ClickHouse hostname |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | ClickHouse password |

### ClickHouse

The pipeline expects a reachable ClickHouse instance and the `batch` user with permission to create and write to the required databases and tables.

### dbt

The dbt project uses the `baserow_orders` profile. Configure the profile in the Airflow/dbt runtime with the appropriate ClickHouse connection details. Do not commit the actual `profiles.yml` file.

## Running the Pipeline

1. Start the Airflow and ClickHouse services.
2. Configure the Baserow token and ClickHouse Airflow Variables.
3. Ensure the dbt profile is configured in the Airflow runtime.
4. Trigger the `baserow_orders_pipeline` DAG in Airflow.
5. Run the dbt models:

```bash
dbt run --project-dir /opt/airflow/dbt/baserow_orders --profiles-dir /home/airflow/.dbt
```

6. Query the Bronze, Silver, and Gold tables in ClickHouse to verify the results.

## Security

- API tokens and database passwords are excluded from version control.
- The DAG reads the Baserow token from an environment variable.
- ClickHouse credentials are read from Airflow Variables.
- Local dbt profiles and generated files are excluded through `.gitignore`.

## Evidence

Execution screenshots will be added to the `screenshots/` directory.

---
