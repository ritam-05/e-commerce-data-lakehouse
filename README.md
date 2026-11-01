# E-Commerce Data Lakehouse

A local PySpark lakehouse pipeline for the Olist Brazilian e-commerce dataset. The project implements a Medallion architecture: raw CSV source files are ingested into Bronze Parquet, cleaned into Silver Parquet, and modeled into Gold dimension and fact tables.

## Current implementation

- Raw layer contains nine Olist CSV datasets.
- Bronze ingestion converts every CSV in `data/raw/` to Snappy-compressed Parquet and adds an `ingestion_timestamp`.
- Silver transformations deduplicate data, standardize customer cities, validate positive payment/item values, and convert order-related date columns to timestamps.
- Gold modeling produces analytics-ready dimensions and facts.
- All pipeline writes use overwrite mode, so reruns are idempotent.

## Data layers

| Layer | Location | Contents |
| --- | --- | --- |
| Raw | `data/raw/` | Source Olist CSV files |
| Bronze | `data/bronze/` | Parquet copies of raw datasets with ingestion metadata |
| Silver | `data/silver/` | Cleaned, typed, and deduplicated Parquet datasets |
| Gold | `data/gold/` | Dimensional model for analytics |

The Bronze and Silver layers currently include these datasets:

`olist_customers_dataset`, `olist_geolocation_dataset`, `olist_orders_dataset`, `olist_order_items_dataset`, `olist_order_payments_dataset`, `olist_order_reviews_dataset`, `olist_products_dataset`, `olist_sellers_dataset`, and `product_category_name_translation`.

## Gold model

### Dimensions

- `dim_customers` — customer identity and location attributes.
- `dim_sellers` — seller location attributes.
- `dim_products` — product characteristics, joined with English product-category translations.

### Facts

- `fact_orders` — order events; partitioned by `purchase_year` and `purchase_month`.
- `fact_order_items` — order-line product, seller, price, and freight details.
- `fact_payments` — payment method, installments, and value.
- `fact_reviews` — review score and review timestamps.

## Project layout

```text
config/paths.py                       # Central paths for all data layers
src/ingestion/ingest_to_bronze.py     # Raw CSV -> Bronze
src/transformations/process_silver.py # Bronze -> Silver
src/transformations/process_gold.py   # Silver -> Gold
data/raw/                             # Input CSV files (not committed)
data/bronze/                          # Bronze Parquet (not committed)
data/silver/                          # Silver Parquet (not committed)
data/gold/                            # Gold Parquet (not committed)
```

## Setup

Python 3 and a Java runtime compatible with PySpark are required.

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Place the Olist CSV files in `data/raw/` before running the pipeline.

## Run the pipeline

Run each stage from the project root, in order:

```powershell
python src/ingestion/ingest_to_bronze.py
python src/transformations/process_silver.py
python src/transformations/process_gold.py
```

Each script creates a local Spark session using all available local cores and writes Snappy-compressed Parquet output.

## Dependencies

Core dependencies are PySpark, pandas, pytest, Streamlit, boto3, and python-dotenv. See `requirements.txt` for the complete install list.

## Git and data policy

Raw and processed data, virtual environments, and Spark-generated artifacts are excluded from version control. `.gitkeep` files preserve the data-layer directory structure without committing datasets.

