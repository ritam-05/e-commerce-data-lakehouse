import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.paths import SILVER_DIR, GOLD_DIR

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist_Gold_Modeling") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def build_dimensions(spark):
    logger.info("Building Dimension Tables...")

    # 1. dim_customers
    df_customers = spark.read.parquet(str(SILVER_DIR / "olist_customers_dataset"))
    dim_customers = df_customers.select(
        "customer_id", 
        "customer_unique_id", 
        "customer_zip_code_prefix", 
        "customer_city", 
        "customer_state"
    )
    dim_customers.write.mode("overwrite").parquet(str(GOLD_DIR / "dim_customers"))
    logger.info(f"dim_customers written. Rows: {dim_customers.count()}")

    # 2. dim_sellers
    df_sellers = spark.read.parquet(str(SILVER_DIR / "olist_sellers_dataset"))
    dim_sellers = df_sellers.select(
        "seller_id", 
        "seller_zip_code_prefix", 
        "seller_city", 
        "seller_state"
    )
    dim_sellers.write.mode("overwrite").parquet(str(GOLD_DIR / "dim_sellers"))
    logger.info(f"dim_sellers written. Rows: {dim_sellers.count()}")

    # 3. dim_products (Joined with English translations)
    df_products = spark.read.parquet(str(SILVER_DIR / "olist_products_dataset"))
    df_translation = spark.read.parquet(str(SILVER_DIR / "product_category_name_translation"))
    
    # Broadcast join because translation table is tiny (71 rows)
    dim_products = df_products.join(
        df_translation, 
        on="product_category_name", 
        how="left"
    ).select(
        "product_id",
        col("product_category_name_english").alias("product_category"),
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
    dim_products.write.mode("overwrite").parquet(str(GOLD_DIR / "dim_products"))
    logger.info(f"dim_products written. Rows: {dim_products.count()}")

def build_facts(spark):
    logger.info("Building Fact Tables...")

    # 1. fact_orders (Partitioned by purchase year and month for query optimization)
    df_orders = spark.read.parquet(str(SILVER_DIR / "olist_orders_dataset"))
    fact_orders = df_orders.withColumn("purchase_year", year(col("order_purchase_timestamp"))) \
                           .withColumn("purchase_month", month(col("order_purchase_timestamp")))
                           
    fact_orders.write.mode("overwrite") \
        .partitionBy("purchase_year", "purchase_month") \
        .parquet(str(GOLD_DIR / "fact_orders"))
    logger.info(f"fact_orders written. Rows: {fact_orders.count()}")

    # 2. fact_order_items
    df_items = spark.read.parquet(str(SILVER_DIR / "olist_order_items_dataset"))
    fact_order_items = df_items.select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value"
    )
    fact_order_items.write.mode("overwrite").parquet(str(GOLD_DIR / "fact_order_items"))
    logger.info(f"fact_order_items written. Rows: {fact_order_items.count()}")

    # 3. fact_payments
    df_payments = spark.read.parquet(str(SILVER_DIR / "olist_order_payments_dataset"))
    fact_payments = df_payments.select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value"
    )
    fact_payments.write.mode("overwrite").parquet(str(GOLD_DIR / "fact_payments"))
    logger.info(f"fact_payments written. Rows: {fact_payments.count()}")

    # 4. fact_reviews
    df_reviews = spark.read.parquet(str(SILVER_DIR / "olist_order_reviews_dataset"))
    fact_reviews = df_reviews.select(
        "review_id",
        "order_id",
        "review_score",
        "review_creation_date",
        "review_answer_timestamp"
    )
    fact_reviews.write.mode("overwrite").parquet(str(GOLD_DIR / "fact_reviews"))
    logger.info(f"fact_reviews written. Rows: {fact_reviews.count()}")

def main():
    spark = create_spark_session()
    build_dimensions(spark)
    build_facts(spark)
    spark.stop()
    logger.info("Gold layer processing complete.")

if __name__ == "__main__":
    main()