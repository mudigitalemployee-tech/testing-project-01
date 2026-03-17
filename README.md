# Kaggle Data Engineering Pipelines

A scalable, modular ETL pipeline system for 10 Kaggle datasets — built for analytics, dashboards, and ML use cases.

## Architecture (Bronze → Silver → Gold)

```
Raw CSVs (Bronze) → Cleaned Data (Silver) → Aggregated/Business-Ready (Gold)
```

## Datasets Covered

| # | Dataset | Use Case | Pipeline |
|---|---------|----------|----------|
| 1 | Titanic | Survival prediction | `pipelines/titanic_pipeline.py` |
| 2 | House Prices | Price prediction | `pipelines/house_prices_pipeline.py` |
| 3 | Netflix Movies | Content analytics | `pipelines/netflix_pipeline.py` |
| 4 | Credit Card Fraud | Anomaly detection | `pipelines/fraud_pipeline.py` |
| 5 | Retail Sales | Sales forecasting | `pipelines/retail_sales_pipeline.py` |
| 6 | Iris | Species classification | `pipelines/iris_pipeline.py` |
| 7 | Diabetes | Disease prediction | `pipelines/diabetes_pipeline.py` |
| 8 | Spotify Tracks | Music analytics | `pipelines/spotify_pipeline.py` |
| 9 | E-Commerce | RFM customer analysis | `pipelines/ecommerce_pipeline.py` |
| 10 | Airline Satisfaction | Satisfaction prediction | `pipelines/airline_pipeline.py` |

## Project Structure

```
kaggle-de-project/
├── pipelines/              # Dataset-specific ETL pipelines (10 total)
├── utils/
│   ├── data_quality.py     # Reusable DQ checks
│   ├── transformations.py  # Common transformation logic
│   └── logger.py           # Logging utility
├── config/
│   └── pipeline_config.yaml  # Centralized config
├── dags/
│   └── kaggle_pipeline_dag.py  # Airflow DAG (all 10 pipelines)
├── data/
│   ├── bronze/             # Raw ingested data
│   ├── silver/             # Cleaned data
│   └── gold/               # Aggregated, business-ready
├── tests/
│   └── test_pipelines.py   # Unit tests
├── requirements.txt
└── README.md
```

## Setup

```bash
pip install -r requirements.txt
```

## Running Pipelines

```bash
# Run all 10 pipelines
python run_all.py

# Run individual pipeline
python pipelines/titanic_pipeline.py

# Via Airflow
airflow dags trigger kaggle_de_pipeline
```

## Kaggle Data Sources

| Dataset | Kaggle URL |
|---------|-----------|
| Titanic | kaggle.com/c/titanic |
| House Prices | kaggle.com/c/house-prices-advanced-regression-techniques |
| Netflix | kaggle.com/datasets/shivamb/netflix-shows |
| Credit Card Fraud | kaggle.com/datasets/mlg-ulb/creditcardfraud |
| Retail Sales | kaggle.com/c/walmart-recruiting-store-sales |
| Iris | kaggle.com/datasets/uciml/iris |
| Diabetes | kaggle.com/datasets/uciml/pima-indians-diabetes-database |
| Spotify Tracks | kaggle.com/datasets/maharshipandya/spotify-tracks-dataset |
| E-Commerce | kaggle.com/datasets/carrie1/ecommerce-data |
| Airline Satisfaction | kaggle.com/datasets/teejmahal20/airline-passenger-satisfaction |

## Success Metrics
- Pipeline success rate > 95%
- Data latency < 1 hour
- Dashboard refresh time < 5 mins
