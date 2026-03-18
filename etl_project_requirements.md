# **Project Requirement Document (PRD)**

## **Project Title:** Batch Data Pipeline for Sales Analytics (Kaggle Dataset)

---

## **1. Objective**

Design and implement a **batch ETL pipeline** that ingests raw sales data, transforms it into analytics-ready datasets, and serves it to BI tools with **daily refresh capability**.

---

## **2. Use Case**

Enable business users to answer:

* Daily/Monthly revenue trends
* Top-performing products
* Customer purchase behavior
* Region-wise sales performance

---

## **3. Dataset**

**Source:** Kaggle – *Superstore Sales Dataset*
**Format:** CSV
**Update Frequency:** Static (simulated daily ingestion)

---

## **4. Scope Definition**

### **In Scope**

* Batch ingestion (daily simulation)
* Data cleaning and transformation
* Star schema modeling
* Aggregated KPI tables
* Pipeline orchestration

### **Out of Scope**

* Real-time streaming
* Machine learning models

---

## **5. Architecture (Concrete)**

```
Raw CSV → Staging Layer → Transform Layer → Data Warehouse → BI Layer
```

### **Tech Stack (Fixed)**

* **Language:** Python
* **Orchestration:** Apache Airflow
* **Processing:** Pandas (or PySpark if scaling needed)
* **Storage:** PostgreSQL
* **Visualization:** Power BI

---

## **6. Data Pipeline Design**

## **6.1 Ingestion Layer**

* Input: `superstore.csv`
* Load into: `stg_superstore` table
* Method: Python script (Airflow DAG task)

**Validation Checks:**

* File exists
* Schema match
* No corrupt rows

---

## **6.2 Staging Layer**

Raw structured table:

**Table: `stg_superstore`**

| Column      | Type  |
| ----------- | ----- |
| order_id    | TEXT  |
| order_date  | DATE  |
| ship_date   | DATE  |
| customer_id | TEXT  |
| region      | TEXT  |
| product_id  | TEXT  |
| sales       | FLOAT |
| quantity    | INT   |
| discount    | FLOAT |
| profit      | FLOAT |

---

## **6.3 Transformation Layer**

### **Cleaning Rules**

* Remove duplicates (order_id + product_id)
* Handle nulls:

  * `sales`, `profit` → drop if null
  * `discount` → default 0

### **Derived Columns**

* `order_month`
* `order_year`
* `profit_margin = profit / sales`

---

## **6.4 Data Model (Warehouse Layer)**

### **Fact Table**

**`fact_sales`**

* order_id
* customer_id
* product_id
* order_date
* sales
* quantity
* profit

### **Dimension Tables**

* `dim_customer`
* `dim_product`
* `dim_region`
* `dim_date`

---

### **Aggregated Table**

**`agg_sales_summary`**

| Metric       | Description     |
| ------------ | --------------- |
| total_sales  | SUM(sales)      |
| total_profit | SUM(profit)     |
| total_orders | COUNT(order_id) |
| avg_discount | AVG(discount)   |

Grouped by:

* date
* region
* category

---

## **7. Orchestration (Airflow DAG)**

### **DAG Flow**

1. `check_file`
2. `load_to_staging`
3. `data_validation`
4. `transform_data`
5. `load_fact_dim`
6. `build_aggregates`

**Schedule:** Daily (`@daily`)

---

## **8. Data Quality Rules**

* Row count consistency check
* Null % threshold (<5%)
* Duplicate check
* Sales ≥ 0 constraint

---

## **9. Non-Functional Requirements**

| Requirement      | Target                     |
| ---------------- | -------------------------- |
| Pipeline Runtime | < 10 minutes               |
| Data Freshness   | Daily                      |
| Failure Recovery | Auto-retry (2 attempts)    |
| Logging          | Task-level logs in Airflow |

---

## **10. Deliverables**

* Airflow DAG (`sales_pipeline.py`)
* SQL scripts for schema creation
* HTML report

---

## **11. Success Criteria**

* Pipeline runs successfully for 7 consecutive days
* No data quality violations
* BI dashboard reflects correct KPIs
* Query latency < 2 seconds (Postgres)

---

## **12. Risks & Mitigation**

| Risk             | Mitigation             |
| ---------------- | ---------------------- |
| Schema changes   | Schema validation step |
| Data skew        | Indexing in Postgres   |
| Pipeline failure | Retry + alerting       |

---


