import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, trim

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.paths import BRONZE_DIR, SILVER_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist_Silver_Transformations") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def process_orders(spark):
    logger.info("Cleaning orders dataset...")
    df = spark.read.parquet(str(BRONZE_DIR / "olist_orders_dataset"))
    
    # Cast timestamp columns
    timestamp_cols = [
        "order_purchase_timestamp", 
        "order_approved_at", 
        "order_delivered_carrier_date", 
        "order_delivered_customer_date", 
        "order_estimated_delivery_date"
    ]
    
    for c in timestamp_cols:
        df = df.withColumn(c, to_timestamp(col(c)))
        
    df_clean = df.dropDuplicates(["order_id"])
    df_clean.write.mode("overwrite").parquet(str(SILVER_DIR / "olist_orders_dataset"))
    logger.info(f"Orders processing complete. Rows: {df_clean.count()}")

def process_order_items(spark):
    logger.info("Cleaning order items dataset...")
    df = spark.read.parquet(str(BRONZE_DIR / "olist_order_items_dataset"))
    
    # Validate positive prices and freight, cast dates
    df_clean = df.dropDuplicates(["order_id", "order_item_id"]) \
        .filter((col("price") > 0) & (col("freight_value") >= 0)) \
        .withColumn("shipping_limit_date", to_timestamp(col("shipping_limit_date")))
        
    df_clean.write.mode("overwrite").parquet(str(SILVER_DIR / "olist_order_items_dataset"))
    logger.info(f"Order items processing complete. Rows: {df_clean.count()}")

def process_payments(spark):
    logger.info("Cleaning payments dataset...")
    df = spark.read.parquet(str(BRONZE_DIR / "olist_order_payments_dataset"))
    
    # Drop duplicates and ensure payment value is positive
    df_clean = df.dropDuplicates() \
        .filter(col("payment_value") > 0)
        
    df_clean.write.mode("overwrite").parquet(str(SILVER_DIR / "olist_order_payments_dataset"))
    logger.info(f"Payments processing complete. Rows: {df_clean.count()}")

def process_customers(spark):
    logger.info("Cleaning customers dataset...")
    df = spark.read.parquet(str(BRONZE_DIR / "olist_customers_dataset"))
    
    # Standardize text: trim whitespace and lowercase cities
    df_clean = df.dropDuplicates(["customer_id"]) \
        .withColumn("customer_city", lower(trim(col("customer_city"))))
        
    df_clean.write.mode("overwrite").parquet(str(SILVER_DIR / "olist_customers_dataset"))
    logger.info(f"Customers processing complete. Rows: {df_clean.count()}")

def process_generic_table(spark, table_name, pk_columns=None):
    """Generic processor for tables that only need deduplication."""
    logger.info(f"Cleaning {table_name}...")
    df = spark.read.parquet(str(BRONZE_DIR / table_name))
    
    if pk_columns:
        df_clean = df.dropDuplicates(pk_columns)
    else:
        df_clean = df.dropDuplicates()
        
    df_clean.write.mode("overwrite").parquet(str(SILVER_DIR / table_name))
    logger.info(f"{table_name} processing complete. Rows: {df_clean.count()}")

def main():
    spark = create_spark_session()
    
    # Process complex tables
    process_orders(spark)
    process_order_items(spark)
    process_payments(spark)
    process_customers(spark)
    
    # Process simpler tables with deduplication
    process_generic_table(spark, "olist_products_dataset", ["product_id"])
    process_generic_table(spark, "olist_sellers_dataset", ["seller_id"])
    process_generic_table(spark, "olist_order_reviews_dataset", ["review_id"])
    process_generic_table(spark, "product_category_name_translation")
    
    # Note: Geolocation is large and has exact duplicate coordinates intentionally for density mapping, 
    # so we'll just pass it through without dropping duplicates for now.
    logger.info("Passing through geolocation dataset...")
    df_geo = spark.read.parquet(str(BRONZE_DIR / "olist_geolocation_dataset"))
    df_geo.write.mode("overwrite").parquet(str(SILVER_DIR / "olist_geolocation_dataset"))
    
    spark.stop()
    logger.info("Silver layer processing complete.")

if __name__ == "__main__":
    main()