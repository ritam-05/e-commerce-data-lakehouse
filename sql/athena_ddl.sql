-- 1. Create the Database
CREATE DATABASE IF NOT EXISTS ecommerce_gold;

-- 2. Create Dimension Tables
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_gold.dim_customers (
  customer_id string,
  customer_unique_id string,
  customer_zip_code_prefix int,
  customer_city string,
  customer_state string
)
STORED AS PARQUET
LOCATION 's3://ritam-ecommerce-lakehouse-gold/dim_customers/';

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_gold.dim_sellers (
  seller_id string,
  seller_zip_code_prefix int,
  seller_city string,
  seller_state string
)
STORED AS PARQUET
LOCATION 's3://ritam-ecommerce-lakehouse-gold/dim_sellers/';

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_gold.dim_products (
  product_id string,
  product_category string,
  product_name_lenght int,
  product_description_lenght int,
  product_photos_qty int,
  product_weight_g int,
  product_length_cm int,
  product_height_cm int,
  product_width_cm int
)
STORED AS PARQUET
LOCATION 's3://ritam-ecommerce-lakehouse-gold/dim_products/';

-- 3. Create Fact Tables
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_gold.fact_order_items (
  order_id string,
  order_item_id int,
  product_id string,
  seller_id string,
  shipping_limit_date timestamp,
  price double,
  freight_value double
)
STORED AS PARQUET
LOCATION 's3://ritam-ecommerce-lakehouse-gold/fact_order_items/';

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_gold.fact_payments (
  order_id string,
  payment_sequential int,
  payment_type string,
  payment_installments int,
  payment_value double
)
STORED AS PARQUET
LOCATION 's3://ritam-ecommerce-lakehouse-gold/fact_payments/';

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_gold.fact_reviews (
  review_id string,
  order_id string,
  review_score int,
  review_creation_date timestamp,
  review_answer_timestamp timestamp
)
STORED AS PARQUET
LOCATION 's3://ritam-ecommerce-lakehouse-gold/fact_reviews/';

-- 4. Create Partitioned Orders Table
CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_gold.fact_orders (
  order_id string,
  customer_id string,
  order_status string,
  order_purchase_timestamp timestamp,
  order_approved_at timestamp,
  order_delivered_carrier_date timestamp,
  order_delivered_customer_date timestamp,
  order_estimated_delivery_date timestamp
)
PARTITIONED BY (purchase_year int, purchase_month int)
STORED AS PARQUET
LOCATION 's3://ritam-ecommerce-lakehouse-gold/fact_orders/';