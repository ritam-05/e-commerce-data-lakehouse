import os
import sys
import logging
from pyspark.sql import SparkSession

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from config.paths import GOLD_DIR, PROJECT_ROOT

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

DOCS_DIR = PROJECT_ROOT / "docs"
REPORT_FILE = DOCS_DIR / "business_queries_report.txt"

def create_spark_session():
    return SparkSession.builder \
        .appName("Olist_Business_Analytics") \
        .master("local[*]") \
        .getOrCreate()

def register_temp_views(spark):
    tables = [
        "fact_orders", "fact_order_items", "fact_payments", "fact_reviews",
        "dim_customers", "dim_sellers", "dim_products"
    ]
    for table in tables:
        path = str(GOLD_DIR / table)
        if os.path.exists(path):
            spark.read.parquet(path).createOrReplaceTempView(table)
        else:
            logger.error(f"Missing Gold table: {table}")

QUERIES = {
    "1. Monthly Revenue": """
        SELECT o.purchase_year, o.purchase_month, ROUND(SUM(p.order_value), 2) AS total_revenue
        FROM fact_orders o
        JOIN (
            SELECT order_id, SUM(payment_value) AS order_value 
            FROM fact_payments GROUP BY order_id
        ) p ON o.order_id = p.order_id
        GROUP BY o.purchase_year, o.purchase_month
        ORDER BY o.purchase_year, o.purchase_month
    """,
    "2. Monthly Order Growth": """
        WITH monthly_counts AS (
            SELECT purchase_year, purchase_month, COUNT(order_id) AS total_orders
            FROM fact_orders
            GROUP BY purchase_year, purchase_month
        )
        SELECT purchase_year, purchase_month, total_orders,
               total_orders - LAG(total_orders) OVER (ORDER BY purchase_year, purchase_month) AS order_growth
        FROM monthly_counts
    """,
    "3. Average Order Value (AOV)": """
        SELECT ROUND(AVG(order_value), 2) AS average_order_value
        FROM (
            SELECT order_id, SUM(payment_value) AS order_value 
            FROM fact_payments GROUP BY order_id
        )
    """,
    "4. Top 5 Selling Products": """
        SELECT dp.product_category, COUNT(foi.order_item_id) AS total_units_sold
        FROM fact_order_items foi
        JOIN dim_products dp ON foi.product_id = dp.product_id
        WHERE dp.product_category IS NOT NULL
        GROUP BY dp.product_category
        ORDER BY total_units_sold DESC
        LIMIT 5
    """,
    "5. Revenue by Product Category": """
        SELECT dp.product_category, ROUND(SUM(foi.price), 2) AS category_revenue
        FROM fact_order_items foi
        JOIN dim_products dp ON foi.product_id = dp.product_id
        WHERE dp.product_category IS NOT NULL
        GROUP BY dp.product_category
        ORDER BY category_revenue DESC
        LIMIT 5
    """,
    "6. Top 5 Sellers by Revenue": """
        SELECT seller_id, ROUND(SUM(price), 2) AS total_revenue
        FROM fact_order_items
        GROUP BY seller_id
        ORDER BY total_revenue DESC
        LIMIT 5
    """,
    "7. Customer Geographic Distribution": """
        SELECT customer_state, COUNT(DISTINCT customer_unique_id) AS total_customers
        FROM dim_customers
        GROUP BY customer_state
        ORDER BY total_customers DESC
        LIMIT 5
    """,
    "8. Delivery Delay Analysis (Avg Days Late)": """
        SELECT 
            ROUND(AVG(DATEDIFF(order_delivered_customer_date, order_estimated_delivery_date)), 2) AS avg_days_diff,
            SUM(CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 1 ELSE 0 END) AS late_deliveries
        FROM fact_orders
        WHERE order_delivered_customer_date IS NOT NULL
    """,
    "9. Average Freight Cost by State": """
        SELECT c.customer_state, ROUND(AVG(i.freight_value), 2) AS avg_freight
        FROM fact_order_items i
        JOIN fact_orders o ON i.order_id = o.order_id
        JOIN dim_customers c ON o.customer_id = c.customer_id
        GROUP BY c.customer_state
        ORDER BY avg_freight DESC
        LIMIT 5
    """,
    "10. Review Score Distribution": """
        SELECT review_score, COUNT(review_id) AS total_reviews
        FROM fact_reviews
        GROUP BY review_score
        ORDER BY review_score DESC
    """,
    "11. Payment Method Popularity": """
        SELECT payment_type, COUNT(order_id) AS usage_count, ROUND(SUM(payment_value), 2) AS total_value
        FROM fact_payments
        GROUP BY payment_type
        ORDER BY usage_count DESC
    """,
    "12. Repeat Customer Rate": """
        WITH customer_order_counts AS (
            SELECT customer_unique_id, COUNT(order_id) AS order_count
            FROM dim_customers c
            JOIN fact_orders o ON c.customer_id = o.customer_id
            GROUP BY customer_unique_id
        )
        SELECT 
            COUNT(CASE WHEN order_count > 1 THEN 1 END) AS repeat_customers,
            COUNT(*) AS total_customers,
            ROUND((COUNT(CASE WHEN order_count > 1 THEN 1 END) * 100.0) / COUNT(*), 2) AS repeat_rate_pct
        FROM customer_order_counts
    """,
    "13. Peak Order Hours": """
        SELECT HOUR(order_purchase_timestamp) AS hour_of_day, COUNT(order_id) AS total_orders
        FROM fact_orders
        GROUP BY hour_of_day
        ORDER BY total_orders DESC
        LIMIT 5
    """,
    "14. Best Rated Product Categories": """
        SELECT dp.product_category, ROUND(AVG(fr.review_score), 2) AS avg_score, COUNT(fr.review_id) as total_reviews
        FROM fact_reviews fr
        JOIN fact_order_items foi ON fr.order_id = foi.order_id
        JOIN dim_products dp ON foi.product_id = dp.product_id
        WHERE dp.product_category IS NOT NULL
        GROUP BY dp.product_category
        HAVING total_reviews > 100
        ORDER BY avg_score DESC
        LIMIT 5
    """,
    "15. Delivery Time by State (Fastest vs Slowest)": """
        SELECT c.customer_state, 
               ROUND(AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)), 1) AS avg_delivery_days
        FROM fact_orders o
        JOIN dim_customers c ON o.customer_id = c.customer_id
        WHERE o.order_delivered_customer_date IS NOT NULL
        GROUP BY c.customer_state
        ORDER BY avg_delivery_days ASC
    """,
    "16. Installment Payment Behavior": """
        SELECT payment_installments, COUNT(order_id) AS total_orders, ROUND(AVG(payment_value), 2) AS avg_order_value
        FROM fact_payments
        WHERE payment_type = 'credit_card'
        GROUP BY payment_installments
        ORDER BY payment_installments ASC
    """,
    "17. Order Status Breakdown": """
        SELECT order_status, COUNT(order_id) AS total_orders, 
               ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER(), 2) AS pct_of_total
        FROM fact_orders
        GROUP BY order_status
        ORDER BY total_orders DESC
    """,
    "18. Top 5 States by Total Revenue": """
        SELECT c.customer_state, ROUND(SUM(p.payment_value), 2) AS total_revenue
        FROM fact_orders o
        JOIN fact_payments p ON o.order_id = p.order_id
        JOIN dim_customers c ON o.customer_id = c.customer_id
        GROUP BY c.customer_state
        ORDER BY total_revenue DESC
        LIMIT 5
    """,
    "19. Top 5 Product Categories by Volume Size": """
        SELECT product_category, ROUND(AVG(product_length_cm * product_height_cm * product_width_cm), 2) AS avg_volume_cm3
        FROM dim_products
        WHERE product_category IS NOT NULL
        GROUP BY product_category
        ORDER BY avg_volume_cm3 DESC
        LIMIT 5
    """,
    "20. Seller Geographic Distribution": """
        SELECT seller_state, COUNT(DISTINCT seller_id) AS total_sellers
        FROM dim_sellers
        GROUP BY seller_state
        ORDER BY total_sellers DESC
        LIMIT 5
    """
}

def main():
    spark = create_spark_session()
    register_temp_views(spark)
    
    os.makedirs(DOCS_DIR, exist_ok=True)
    
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("E-Commerce Data Lakehouse - Business Analytics Report\n")
        f.write("===================================================\n\n")
        
        for title, query in QUERIES.items():
            logger.info(f"\n--- {title} ---")
            try:
                df_result = spark.sql(query)
                df_result.show(truncate=False)
                
                f.write(f"--- {title} ---\n")
                pd_df = df_result.toPandas()
                f.write(pd_df.to_string(index=False))
                f.write("\n\n")
                
            except Exception as e:
                error_msg = f"Query Failed: {str(e)}"
                logger.error(error_msg)
                f.write(f"--- {title} ---\n{error_msg}\n\n")
                
    spark.stop()
    logger.info(f"\n[SUCCESS] All query results saved to: {REPORT_FILE}")

if __name__ == "__main__":
    main()