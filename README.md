# Batch ETL Pipeline — Superstore Sales Analytics

A production-ready batch ETL pipeline for sales analytics, built with Python, Apache Airflow, Pandas, and PostgreSQL.

## Architecture

```
Raw CSV → Staging Layer → Transform Layer → Data Warehouse → BI Layer
```

## Pipeline Flow (Airflow DAG)

```
check_file → load_to_staging → data_validation → transform_data → build_aggregates
```

**Schedule:** Daily (`@daily`) | **Retries:** 2 per task | **Max active runs:** 1

---

## Project Structure

```
├── dags/
│   └── sales_pipeline.py       # Airflow DAG (6 tasks, daily schedule)
├── scripts/
│   ├── ingest.py               # CSV → stg_superstore (validate + bulk insert)
│   ├── validate.py             # 7 data quality checks
│   ├── transform.py            # Clean + star schema load + aggregates
│   └── requirements.txt
├── sql/
│   ├── 01_create_staging.sql   # stg_superstore DDL
│   └── 02_create_warehouse.sql # fact + dims + agg DDL
├── data/
│   └── superstore.csv          # 10,000-row dataset (2021–2023)
├── reports/
│   └── pipeline_report.html    # HTML analytics report
└── README.md
```

---

## Quick Start

### 1. PostgreSQL Setup

```bash
createdb salesdb
psql salesdb -f sql/01_create_staging.sql
psql salesdb -f sql/02_create_warehouse.sql
```

### 2. Environment Variables

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=salesdb
export DB_USER=postgres
export DB_PASSWORD=postgres
export CSV_PATH=./data/superstore.csv
```

### 3. Install Dependencies

```bash
pip install -r scripts/requirements.txt
```

### 4. Run Pipeline Manually

```bash
cd scripts
python3 ingest.py      # Phase 1: Ingest CSV → staging
python3 validate.py    # Phase 2: Run DQ checks
python3 transform.py   # Phase 3: Transform + load warehouse
```

### 5. Airflow Setup

```bash
pip install apache-airflow
airflow db init
cp dags/sales_pipeline.py $AIRFLOW_HOME/dags/
airflow webserver --port 8080   # Terminal 1
airflow scheduler               # Terminal 2
airflow dags trigger sales_pipeline  # Trigger once
```

---

## Data Model (Star Schema)

| Table | Type | Description |
|---|---|---|
| `stg_superstore` | Staging | Raw CSV data |
| `fact_sales` | Fact | Core sales transactions |
| `dim_customer` | Dimension | Customer lookup |
| `dim_product` | Dimension | Product + category |
| `dim_region` | Dimension | Region + state |
| `dim_date` | Dimension | Date attributes |
| `agg_sales_summary` | Aggregate | KPIs by date/region/category |

---

## API Reference (SQL Queries for BI)

```sql
-- Daily revenue by region
SELECT agg_date, region, total_sales, total_profit
FROM agg_sales_summary
ORDER BY agg_date;

-- Top categories
SELECT category, SUM(total_sales) AS revenue
FROM agg_sales_summary
GROUP BY category
ORDER BY revenue DESC;

-- Customer orders
SELECT c.customer_id, COUNT(*) AS orders, SUM(f.sales) AS total
FROM fact_sales f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_id
ORDER BY total DESC
LIMIT 20;
```

---

Built by **Ved** — Mu Sigma Digital Employee 🧠
