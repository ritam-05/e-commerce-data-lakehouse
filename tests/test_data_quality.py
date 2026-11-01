import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Add project root to path so tests can find config
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config.paths import GOLD_DIR

# ---------------------------------------------------------
# Pytest Fixture
# ---------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    """Creates a single SparkSession for all tests to speed up execution."""
    spark_session = SparkSession.builder \
        .appName("Olist_Data_Quality_Tests") \
        .master("local[*]") \
        .getOrCreate()
    
    yield spark_session
    
    spark_session.stop()

# ---------------------------------------------------------
# Data Quality Tests
# ---------------------------------------------------------
def test_no_null_primary_keys(spark):
    """Test that fact_orders does not have any null order_ids."""
    df_orders = spark.read.parquet(str(GOLD_DIR / "fact_orders"))
    null_count = df_orders.filter(col("order_id").isNull()).count()
    assert null_count == 0, f"Found {null_count} null order_ids in fact_orders"

def test_no_duplicate_primary_keys(spark):
    """Test that every order in fact_orders is completely unique."""
    df_orders = spark.read.parquet(str(GOLD_DIR / "fact_orders"))
    total_rows = df_orders.count()
    distinct_rows = df_orders.select("order_id").distinct().count()
    assert total_rows == distinct_rows, "Duplicate order_ids found! Grain is corrupted."

def test_positive_prices_and_freight(spark):
    """Test that the Silver layer cleaning correctly filtered out bad prices."""
    df_items = spark.read.parquet(str(GOLD_DIR / "fact_order_items"))
    invalid_prices = df_items.filter(col("price") <= 0).count()
    invalid_freight = df_items.filter(col("freight_value") < 0).count()
    
    assert invalid_prices == 0, f"Found {invalid_prices} zero or negative prices"
    assert invalid_freight == 0, f"Found {invalid_freight} negative freight values"

def test_valid_review_scores(spark):
    """Test that review scores are strictly integers between 1 and 5."""
    df_reviews = spark.read.parquet(str(GOLD_DIR / "fact_reviews"))
    # The ~ operator means "NOT"
    invalid_scores = df_reviews.filter(~col("review_score").isin(1, 2, 3, 4, 5)).count()
    assert invalid_scores == 0, f"Found {invalid_scores} invalid review scores. CSV parsing bug returned!"

def test_referential_integrity(spark):
    """Test that every order has a matching customer in the dimension table."""
    df_orders = spark.read.parquet(str(GOLD_DIR / "fact_orders"))
    df_customers = spark.read.parquet(str(GOLD_DIR / "dim_customers"))
    
    # Left Anti Join: Return orders that have NO matching customer_id in dim_customers
    orphans = df_orders.join(df_customers, on="customer_id", how="left_anti").count()
    assert orphans == 0, f"Found {orphans} orphaned orders without a valid customer record."