import subprocess
import sys
import logging
import os

# Ensure we are running from the project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def run_job(description: str, command: list):
    logger.info(f"\n{'='*60}")
    logger.info(f"🚀 STARTING: {description}")
    logger.info(f"{'='*60}")
    
    # Execute the command
    result = subprocess.run(command)
    
    # Halt the entire pipeline if a job fails
    if result.returncode != 0:
        logger.error(f"\n❌ PIPELINE FAILED AT: {description}")
        sys.exit(1)
        
    logger.info(f"\n✅ COMPLETED: {description}\n")

def main():
    logger.info("Starting E-Commerce Data Lakehouse Automated Pipeline...")
    
    # 1. Bronze Layer
    run_job(
        "Phase 1 - Bronze Ingestion", 
        [sys.executable, "src/ingestion/ingest_to_bronze.py"]
    )
    
    # 2. Silver Layer
    run_job(
        "Phase 2 - Silver Transformations", 
        [sys.executable, "src/transformations/process_silver.py"]
    )
    
    # 3. Gold Layer
    run_job(
        "Phase 3 - Gold Modeling", 
        [sys.executable, "src/transformations/process_gold.py"]
    )
    
    # 4. Data Quality Tests (pytest)
    run_job(
        "Phase 5 - Automated Data Quality Tests", 
        [sys.executable, "-m", "pytest", "tests/test_data_quality.py", "-v", "--disable-warnings"]
    )
    
    # 5. Business Analytics Report
    run_job(
        "Phase 4 - Generate Business Analytics Report", 
        [sys.executable, "src/analytics/business_queries.py"]
    )

    logger.info("\n🎉 ENTIRE PIPELINE EXECUTED SUCCESSFULLY! 🎉")

if __name__ == "__main__":
    main()