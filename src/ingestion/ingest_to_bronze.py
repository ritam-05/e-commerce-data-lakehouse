import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging

# Add project root to path so we can import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.paths import RAW_DIR, BRONZE_DIR

# Setup minimal logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Initialize and return a SparkSession."""
    return SparkSession.builder \
        .appName("Olist_Bronze_Ingestion") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

def process_file_to_bronze(spark: SparkSession, file_name: str):
    """Reads a raw CSV, adds ingestion metadata, and writes to Bronze as Parquet."""
    dataset_name = file_name.replace(".csv", "")
    raw_path = str(RAW_DIR / file_name)
    bronze_path = str(BRONZE_DIR / dataset_name)

    logger.info(f"Processing {file_name} -> {dataset_name} (Bronze)")

    try:
        # 1. Read raw CSV
        # header=True uses the first row as column names
        # inferSchema=True guesses data types (acceptable for Bronze)
        df = spark.read.csv(raw_path, header=True, inferSchema=True)

        # 2. Add ingestion metadata
        df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp())

        # 3. Write to Bronze as Parquet
        # mode="overwrite" ensures idempotent processing (rerunning script replaces old data safely)
        df_with_metadata.write.mode("overwrite").parquet(bronze_path)
        
        logger.info(f"Successfully wrote {df.count()} rows to {bronze_path}")

    except Exception as e:
        logger.error(f"Failed to process {file_name}: {str(e)}")

def main():
    spark = create_spark_session()
    
    # List all CSVs in the raw directory
    if not os.path.exists(RAW_DIR):
        logger.error(f"Raw directory not found at {RAW_DIR}. Did you download the dataset?")
        return

    raw_files = [f for f in os.listdir(RAW_DIR) if f.endswith('.csv')]
    
    if not raw_files:
        logger.warning(f"No CSV files found in {RAW_DIR}")
        return

    logger.info(f"Found {len(raw_files)} files to ingest.")

    for file_name in raw_files:
        process_file_to_bronze(spark, file_name)

    spark.stop()
    logger.info("Bronze ingestion complete.")

if __name__ == "__main__":
    main()