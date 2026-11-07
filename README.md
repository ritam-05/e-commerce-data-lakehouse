# E-Commerce Platform (Olist) Data Lakehouse

<p align="center">
  <img src="docs/images/olist-banner.png" alt="Olist shopping banner" width="100%" />
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9%2B-blue.svg" alt="Python 3.9+" />
  <img src="https://img.shields.io/badge/Apache_Spark-3.5.0-orange.svg" alt="Apache Spark 3.5.0" />
  <img src="https://img.shields.io/badge/Amazon_S3-Storage-green.svg" alt="Amazon S3" />
  <img src="https://img.shields.io/badge/Amazon_Athena-Serverless_SQL-purple.svg" alt="Amazon Athena" />
  <img src="https://img.shields.io/badge/Power_BI-Analytics-yellow.svg" alt="Power BI" />
</p>

An end-to-end data lakehouse for the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). The project uses PySpark to transform 100,000+ e-commerce orders into governed, analytics-ready Parquet tables. Curated Gold data can then be served through Amazon S3, AWS Glue Data Catalog, Amazon Athena, and Power BI.

The design follows Medallion architecture. Source CSV files are retained in Raw, converted to Bronze Parquet, cleaned in Silver, and shaped into Gold facts and dimensions. Transformations run locally; only optimized Gold datasets are published to AWS. This keeps the cloud serving layer lightweight: Athena queries files in S3 without a database server or AWS Glue ETL jobs.

## Contents

- [What this project delivers](#what-this-project-delivers)
- [Architecture](#architecture)
- [How the pipeline works](#how-the-pipeline-works)
- [Gold data model](#gold-data-model)
- [Run locally](#run-locally)
- [Publish and query in AWS](#publish-and-query-in-aws)
- [Data quality](#data-quality)
- [Power BI dashboard](#power-bi-dashboard)
- [Repository layout](#repository-layout)

## What this project delivers

| Capability | Outcome |
| --- | --- |
| Repeatable ingestion | Local Olist CSV files are converted into Snappy-compressed Parquet, with an ingestion timestamp. |
| Layered data quality | Bronze preserves source structure, Silver standardizes and validates records, and Gold provides reporting-ready tables. |
| Analytics model | A seven-table star schema supports revenue, orders, products, sellers, payments, reviews, delivery, and geography analysis. |
| Quality gate | `pytest` checks run after Gold modeling and before business analytics are generated. |
| Cloud SQL serving | Gold data can be uploaded to S3, cataloged in Glue, and queried in Athena. |
| Business intelligence | A Power BI semantic model and three dashboard pages consume the Gold model. |

## Architecture

The system intentionally separates compute from serving. PySpark performs the work locally, while AWS stores and queries the final curated data.

```mermaid
flowchart LR
    source[Olist CSV files] --> raw[(Raw<br/>source CSV)]

    subgraph local[Local PySpark pipeline]
        direction LR
        raw --> bronze[Bronze ingestion] --> bronze_data[(Bronze<br/>Parquet)]
        bronze_data --> silver[Silver cleansing] --> silver_data[(Silver<br/>Parquet)]
        silver_data --> gold[Gold modeling] --> gold_data[(Gold facts<br/>and dimensions)]
        gold_data --> quality{Data-quality<br/>tests}
        quality -->|pass| analytics[Business queries<br/>and report]
    end

    subgraph aws[AWS query layer]
        direction LR
        gold_data --> publish[Upload Gold data]
        publish --> s3[(Amazon S3)]
        s3 --> glue[AWS Glue<br/>Data Catalog]
        glue --> athena[Amazon Athena]
    end

    subgraph bi[Business intelligence]
        direction TB
        gold_data --> powerbi[Power BI semantic model]
        athena -. optional cloud access .-> powerbi
        powerbi --> dashboard[Interactive dashboard]
    end
```

### End-to-end flow

```mermaid
flowchart TD
    A[1. Download Olist CSV files] --> B[2. Put files in data/raw]
    B --> C[3. Run scripts/run_pipeline.py]
    C --> D[Bronze: CSV to Parquet + ingestion timestamp]
    D --> E[Silver: type, clean, validate, deduplicate]
    E --> F[Gold: facts, dimensions, and partitions]
    F --> G{Quality tests pass?}
    G -->|Yes| H[Generate business query report]
    G -->|No| I[Stop pipeline and investigate]
    H --> J[Optional: upload Gold to S3]
    J --> K[Catalog with Glue and query with Athena]
```

### Local orchestration

`scripts/run_pipeline.py` executes each stage in dependency order and exits immediately if a command fails. This prevents analytics from being generated from an incomplete or invalid layer.

```mermaid
flowchart LR
    A[Bronze ingestion] --> B[Silver transformations] --> C[Gold modeling]
    C --> D[pytest data-quality checks] --> E[Business analytics report]
    D -. failure stops workflow .-> F[Fix data or transformation issue]
```

## How the pipeline works

### Medallion layers

```mermaid
flowchart LR
    subgraph raw_layer[Raw — preserve the source]
        R[Original Olist CSV files]
    end
    subgraph bronze_layer[Bronze — structure the source]
        B[Parquet copies<br/>+ ingestion_timestamp]
    end
    subgraph silver_layer[Silver — make data trustworthy]
        S[Typed, trimmed, deduplicated,<br/>and validated datasets]
    end
    subgraph gold_layer[Gold — make data useful]
        G[Star-schema facts and dimensions]
    end
    R --> B --> S --> G
```

| Layer | Purpose | What happens in this project |
| --- | --- | --- |
| **Raw** | Preserve the supplied source files. | Original Olist CSV files live in `data/raw/` and are not modified by the pipeline. |
| **Bronze** | Create a reliable, efficient first copy. | Every CSV is read with Spark, including multiline reviews, then written as Snappy Parquet with `ingestion_timestamp`. |
| **Silver** | Conform and validate source records. | Timestamps are cast, selected text is standardized, duplicates are removed, and invalid prices, freight, and payments are excluded. |
| **Gold** | Provide a business-friendly analytics model. | Dimensions and facts are written as Parquet; `fact_orders` is partitioned by purchase year and month. |

### Silver cleansing rules

The Silver layer makes explicit the quality assumptions used by downstream reporting:

- `orders`: date columns are converted to timestamps and duplicate `order_id` values are removed.
- `order_items`: duplicate `(order_id, order_item_id)` rows are removed; price must be positive and freight cannot be negative.
- `payments`: duplicate rows are removed and `payment_value` must be positive.
- `customers`: duplicate `customer_id` values are removed; city values are trimmed and lowercased.
- Products, sellers, reviews, and category translations are deduplicated using their relevant keys or complete records.
- Geolocation is passed through because repeated coordinates are meaningful for density analysis.

### Gold modeling and partitioning

```mermaid
flowchart TB
    subgraph silver[Silver inputs]
        customers[customers]
        sellers[sellers]
        products[products]
        translation[category translation]
        orders[orders]
        items[order items]
        payments[payments]
        reviews[reviews]
    end

    subgraph dimensions[Gold dimensions]
        dc[dim_customers]
        ds[dim_sellers]
        dp[dim_products]
    end

    subgraph facts[Gold facts]
        fo[fact_orders<br/>partitioned by year/month]
        fi[fact_order_items]
        fp[fact_payments]
        fr[fact_reviews]
    end

    customers --> dc
    sellers --> ds
    products --> dp
    translation --> dp
    orders --> fo
    items --> fi
    payments --> fp
    reviews --> fr
```

`fact_orders` is stored under `purchase_year` and `purchase_month` partitions. When an Athena query filters on both fields, Athena can read only the relevant folders instead of scanning the full table.

## Gold data model

The Gold layer separates descriptive entities from measurable events. This star-schema style model makes common dashboard measures easier to calculate and explain.

| Type | Table | Role |
| --- | --- | --- |
| Dimension | `dim_customers` | Customer identity and location attributes. |
| Dimension | `dim_sellers` | Seller identity and location attributes. |
| Dimension | `dim_products` | Product properties and English product category. |
| Fact | `fact_orders` | Order lifecycle and purchase-date attributes. |
| Fact | `fact_order_items` | Order line items, product, seller, price, and freight. |
| Fact | `fact_payments` | Payment method, installments, and payment value. |
| Fact | `fact_reviews` | Review score and review timestamps. |

### Relationship subdiagram

```mermaid
erDiagram
    DIM_CUSTOMERS ||--o{ FACT_ORDERS : customer_id
    FACT_ORDERS ||--o{ FACT_ORDER_ITEMS : order_id
    DIM_PRODUCTS ||--o{ FACT_ORDER_ITEMS : product_id
    DIM_SELLERS ||--o{ FACT_ORDER_ITEMS : seller_id
    FACT_ORDERS ||--o{ FACT_PAYMENTS : order_id
    FACT_ORDERS ||--o{ FACT_REVIEWS : order_id

    DIM_CUSTOMERS {
        string customer_id PK
        string customer_state
        string customer_city
    }
    DIM_SELLERS {
        string seller_id PK
        string seller_state
    }
    DIM_PRODUCTS {
        string product_id PK
        string product_category
    }
    FACT_ORDERS {
        string order_id PK
        string customer_id FK
        int purchase_year
        int purchase_month
    }
    FACT_ORDER_ITEMS {
        string order_id FK
        string product_id FK
        string seller_id FK
        double price
        double freight_value
    }
    FACT_PAYMENTS {
        string order_id FK
        double payment_value
    }
    FACT_REVIEWS {
        string review_id PK
        string order_id FK
        int review_score
    }
```

This model supports monthly revenue, average order value (AOV), order growth, fulfillment and delivery performance, category and seller performance, payment behavior, reviews, and regional order volume.

## Technology stack

| Area | Technology | Responsibility |
| --- | --- | --- |
| Data processing | Python and PySpark 3.5.0 | Ingestion, transformations, and local analysis. |
| Storage format | Snappy-compressed Parquet | Efficient, columnar storage for Bronze through Gold layers. |
| Quality assurance | `pytest` | Gold-layer integrity checks. |
| Data lake and metadata | Amazon S3 and AWS Glue Data Catalog | Gold data storage and table metadata. |
| Serverless SQL | Amazon Athena | SQL directly against Gold Parquet in S3. |
| BI | Power BI and DAX | Semantic model, measures, and interactive reporting. |
| Orchestration | Python `subprocess` controller | Ordered, fail-fast execution of pipeline stages. |

## Run locally

### Prerequisites

- Python 3.9 or later.
- A Java runtime compatible with PySpark.
- The Olist dataset CSV files downloaded from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

The dataset itself is intentionally not versioned in this repository.

### Install and prepare data

```powershell
git clone https://github.com/YOUR_USERNAME/ecommerce-data-lakehouse.git
cd ecommerce-data-lakehouse
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Place the downloaded CSV files directly in `data/raw/`. The pipeline discovers every `.csv` file in that directory; the standard Olist download contains the datasets used by the downstream transformations.

### Run the complete pipeline

```powershell
python scripts/run_pipeline.py
```

On success, the pipeline creates local Bronze, Silver, and Gold outputs under `data/` and writes the analytics results to `docs/business_queries_report.txt`.

### Run an individual stage

Use individual commands while developing or troubleshooting:

```powershell
python src/ingestion/ingest_to_bronze.py
python src/transformations/process_silver.py
python src/transformations/process_gold.py
python -m pytest tests/test_data_quality.py -v --disable-warnings
python src/analytics/business_queries.py
```

## Publish and query in AWS

The cloud deployment publishes only Gold outputs. This avoids using AWS Glue ETL for transformations; Athena then provides on-demand SQL over the Parquet files in S3. Athena costs, if any, depend on data scanned by queries.

```mermaid
sequenceDiagram
    participant Local as Local Gold layer
    participant S3 as Amazon S3
    participant Glue as Glue Data Catalog
    participant Athena as Amazon Athena
    participant User as Analyst / Power BI

    Local->>S3: Upload Gold Parquet datasets
    User->>Glue: Run Athena DDL from sql/athena_ddl.sql
    Glue->>Athena: Expose table metadata
    User->>Athena: Run MSCK REPAIR TABLE for new order partitions
    User->>Athena: Query Gold tables with SQL
    Athena-->>User: Return query results
```

1. Upload Gold Parquet data to `s3://ritam-ecommerce-lakehouse-gold` in `ap-southeast-2`, using `scripts/upload_to_s3.py`.
2. Run [sql/athena_ddl.sql](sql/athena_ddl.sql) to create the `ecommerce_gold` database and external table definitions.
3. After uploading new `fact_orders` partitions, run:

   ```sql
   MSCK REPAIR TABLE ecommerce_gold.fact_orders;
   ```

4. Filter Athena queries by `purchase_year` and `purchase_month` whenever practical to take advantage of partition pruning.

### Proof of cloud execution

<p align="center">
  <img src="docs/images/athena_query.png" alt="Athena query execution" width="48%" />
  <img src="docs/images/s3_bucket.png" alt="S3 Gold bucket structure" width="48%" />
</p>

## Data quality

Data-quality tests form a gate between Gold modeling and analytics. They make reporting assumptions executable rather than implicit.

```mermaid
flowchart LR
    gold[Gold Parquet tables] --> checks{pytest checks}
    checks --> pk[Required order IDs and uniqueness]
    checks --> ref[Fact-to-dimension relationships]
    checks --> values[Non-negative revenue and freight]
    pk --> decision{All checks pass?}
    ref --> decision
    values --> decision
    decision -->|Yes| analytics[Analytics and publication]
    decision -->|No| fix[Investigate failed assertion]
```

The test suite checks:

- Non-null and unique `order_id` values in `fact_orders`.
- Referential integrity between `fact_orders` and `dim_customers`, using a left anti-join to surface orphaned keys.
- Non-negative revenue-related and freight values.
- Review scores restricted to whole-number values from 1 through 5.
- Repeatable local execution through `pytest`.

These tests do not replace source-system governance, but they prevent a known set of invalid conditions from reaching downstream analytics.

## Power BI dashboard

Power BI batch-loads the seven local Gold Parquet tables through the **Folder** connector. In Power Query, the `Extension` column is filtered to remove hidden `.crc` files and blank values before the Parquet binaries are combined.

The report uses `fact_orders` as the central hub of its one-to-many model. For map visuals, `dim_customers` includes this calculated column:

```DAX
State Location = dim_customers[customer_state] & ", Brazil"
```

### Core measures

```DAX
Total Orders = DISTINCTCOUNT(fact_orders[order_id])
Total Revenue = SUM(fact_payments[payment_value])
Average Order Value = [Total Revenue] / [Total Orders]
Average Review Score = AVERAGE(fact_reviews[review_score])
Average Freight = AVERAGE(fact_order_items[freight_value])
Product Revenue = SUM(fact_order_items[price])
Items Sold = COUNT(fact_order_items[order_item_id])
Total Reviews = COUNT(fact_reviews[review_id])
Delivery Time (Days) = AVERAGEX(fact_orders, DATEDIFF(fact_orders[order_approved_at], fact_orders[order_delivered_customer_date], DAY))
```

### Dashboard navigation

```mermaid
flowchart LR
    report[Power BI report] --> executive[Executive Summary]
    report --> product[Product & Seller Performance]
    report --> experience[Customer Experience]
    executive --> revenue[Revenue, orders, AOV,<br/>monthly trend, status]
    product --> sellers[Categories, sellers,<br/>freight comparison]
    experience --> customers[Reviews, delivery time,<br/>regional order volume]
```

#### Executive Summary

- KPI cards for Total Revenue, Total Orders, and Average Order Value.
- Monthly Revenue Trend by purchase year and month; incomplete September 2018 data is excluded.
- Order Fulfillment Status donut chart and date slicers.

<p align="center">
  <img src="docs/images/executive_summary_construction.png" alt="Power BI Executive Summary dashboard" width="100%" />
</p>

#### Product & Seller Performance

- Top 10 categories by Product Revenue.
- Seller matrix with revenue and items sold.
- Product Revenue versus Average Freight scatter plot, grouped by category.
- Product Category slicer.

<p align="center">
  <img src="docs/images/products%26sellers.png" alt="Power BI Product and Seller Performance dashboard" width="100%" />
</p>

#### Customer Experience

- KPI cards for Average Review Score and Delivery Time (Days).
- Regional order-volume map using `State Location`.
- Delivery-time analysis by customer state and review-score distribution.

<p align="center">
  <img src="docs/images/customer_experience.png" alt="Power BI Customer Experience dashboard" width="100%" />
</p>

## Repository Layout

```text
ecommerce-data-lakehouse/
├── config/                 # Shared path configuration
├── data/                   # Local Raw, Bronze, Silver, and Gold data (Gitignored)
├── dashboards/             # Power BI project, semantic model, and report definition
├── docs/                   # Generated analytics report and documentation images
├── scripts/                # Pipeline controller and S3 upload utility
├── sql/                    # Athena external-table DDL
├── src/
│   ├── analytics/          # Spark SQL business queries
│   ├── ingestion/          # CSV-to-Bronze ingestion
│   └── transformations/    # Silver cleansing and Gold modeling
└── tests/                  # Pytest data-quality assertions
```

## Key Concepts & Technologies used

| Concept | Meaning (here) |
| --- | --- |
| **Data lakehouse** | File-based, low-cost storage paired with governed Parquet tables and SQL/BI access. |
| **Medallion architecture** | A progression from preserved source data to cleaned data to business-facing models. |
| **Parquet** | Compressed, column-oriented analytics storage used for Bronze, Silver, and Gold. |
| **Star schema** | A central set of facts connected to descriptive dimensions for straightforward reporting. |
| **Partitioning** | Folder-based organization of `fact_orders` by purchase year and month to reduce Athena scan volume. |
| **AWS Glue Data Catalog** | Metadata that tells Athena a table's schema and S3 location; it does not store the data itself. |
| **Amazon Athena** | Serverless SQL over S3-resident files. |
