# Batch Data Pipeline for Sales Analytics

Production-grade batch ETL pipeline — ingests Superstore sales CSV, transforms it into a PostgreSQL star schema, builds aggregated KPI tables, and delivers an HTML analytics report. Orchestrated with Apache Airflow on a daily schedule.

---

## Pipeline Architecture

```
superstore.csv
      │
  [1] check_file          Validate file exists + schema check
      │
  [2] load_to_staging     Bulk load → stg_superstore (truncate + insert)
      │
  [3] data_validation     Null %, duplicates, row count, sales >= 0
      │
  [4] transform_data      Clean + derive columns → fact/dim tables
      │
  [5] load_fact_dim       Star schema: fact_sales + 4 dimensions
      │
  [6] build_aggregates    Refresh agg_sales_summary KPI table
      │
  [7] generate_report     HTML report → outputs/reports/
      │
  PostgreSQL (sales_dw)  →  Power BI
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Orchestration | Apache Airflow 2.8.1 |
| Processing | Pandas |
| Storage | PostgreSQL 15 |
| Visualization | Power BI / HTML Report |
| Containerisation | Docker + Docker Compose |

---

## Project Structure

```
├── dags/
│   └── sales_pipeline.py        Airflow DAG (7 tasks, @daily)
├── scripts/
│   ├── ingestion.py             File check + staging load
│   ├── validate.py              Data quality checks
│   ├── transform.py             Clean + star schema loader
│   ├── build_aggregates.py      KPI table refresh
│   └── generate_report.py       HTML report generator
├── sql/
│   ├── 01_create_staging.sql    stg_superstore DDL
│   └── 02_create_warehouse.sql  fact + dim + agg DDLs
├── utils/
│   ├── db.py                    DB connection + SQL helpers
│   └── logger.py                Logging utility
├── config/config.yaml           Pipeline configuration
├── tests/test_pipeline.py       Unit tests (pytest)
├── data/                        Place superstore.csv here
├── reports/                     HTML report output
├── logs/                        Pipeline logs
├── docker-compose.yml           PostgreSQL + Airflow stack
└── requirements.txt
```

---

## Quick Start

### 1. Prerequisites
- Docker & Docker Compose
- Superstore CSV placed in `data/superstore.csv`

### 2. Start the Stack

```bash
docker-compose up -d
```

- PostgreSQL on `localhost:5432`
- Airflow UI on `http://localhost:8080` (admin/admin)

### 3. Trigger the Pipeline

```bash
# Via Airflow UI → DAGs → sales_pipeline → Trigger DAG
```

### 4. Run Locally (no Docker)

```bash
pip install -r requirements.txt
export DB_HOST=localhost DB_PORT=5432 DB_NAME=sales_dw DB_USER=airflow DB_PASSWORD=airflow

python scripts/ingestion.py
python scripts/validate.py
python scripts/transform.py
python scripts/build_aggregates.py
python scripts/generate_report.py
```

### 5. Run Tests

```bash
pytest tests/ -v
```

---

## Data Model

| Table | Type | Description |
|---|---|---|
| `stg_superstore` | Staging | Raw CSV data, truncated daily |
| `fact_sales` | Fact | Order-level transactions |
| `dim_customer` | Dimension | Customer master |
| `dim_product` | Dimension | Product + category |
| `dim_region` | Dimension | Region reference |
| `dim_date` | Dimension | Date calendar |
| `agg_sales_summary` | Aggregate | KPIs by date × region × category |

---

## Deliverables

- ✅ Airflow DAG (`dags/sales_pipeline.py`)
- ✅ SQL scripts for schema creation (`sql/`)
- ✅ HTML report (`reports/pipeline_report.html`)

---

## Non-Functional Targets

| Requirement | Target |
|---|---|
| Pipeline runtime | < 10 minutes |
| Data freshness | Daily |
| Failure recovery | Auto-retry (2 attempts, 5 min) |
| Logging | Task-level logs in Airflow |
