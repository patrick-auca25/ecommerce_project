from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, count, sum as spark_sum, avg, 
    desc, asc, round as spark_round, collect_list,
    size, array_intersect, when, lit, to_date,
    month, year, dayofweek, hour, concat, first
)
from pyspark.sql.types import *
import os

# --- Initialize Spark ---
def create_spark_session():
    """Create and configure Spark session"""
    spark = SparkSession.builder \
        .appName("Ecommerce Analytics") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("   SPARK SESSION INITIALIZED")
    print("=" * 60)
    print(f"Spark Version: {spark.version}")
    print(f"App Name: {spark.sparkContext.appName}")
    print()
    
    return spark


# ============================================================
# DATA LOADING
# ============================================================
def load_data(spark, data_dir=r"D:\Patrick\AUCA\SEM3\bigdatanalytics\ecommerce_project\raw_data"):
    """Load all JSON files into Spark DataFrames"""
    
    print("=" * 60)
    print("LOADING DATA INTO SPARK")
    print("=" * 60 + "\n")
    print(f"Data directory: {data_dir}\n")
    
    # Load users
    users_path = os.path.join(data_dir, "users.json")
    users_df = spark.read.json(users_path)
    print(f" Users loaded: {users_df.count():,} records")
    
    # Load products
    products_path = os.path.join(data_dir, "products.json")
    products_df = spark.read.json(products_path)
    print(f" Products loaded: {products_df.count():,} records")
    
    # Load categories
    categories_path = os.path.join(data_dir, "categories.json")
    categories_df = spark.read.json(categories_path)
    print(f" Categories loaded: {categories_df.count():,} records")
    
    # Load transactions
    transactions_path = os.path.join(data_dir, "transactions.json")
    transactions_df = spark.read.json(transactions_path)
    print(f" Transactions loaded: {transactions_df.count():,} records")
    
    # Load sessions (load first 3 files to balance data size and memory)
    session_dir = os.path.join(data_dir, "session")
    if os.path.exists(session_dir):
        session_files = [f for f in os.listdir(session_dir) if f.startswith('sessions_') and f.endswith('.json')]
        if session_files:
            # Load first 3 session files
            files_to_load = sorted(session_files)[:3]
            sessions_paths = [os.path.join(session_dir, f) for f in files_to_load]
            sessions_df = spark.read.json(sessions_paths)
            print(f" Sessions loaded: {sessions_df.count():,} records")
            print(f"   Loaded {len(files_to_load)} of {len(session_files)} files: {files_to_load}")
        else:
            sessions_df = None
            print(" No session files found in session folder")
    else:
        sessions_df = None
        print(" Session folder not found")
    
    print()
    return users_df, products_df, categories_df, transactions_df, sessions_df


# ============================================================
# DATA CLEANING AND NORMALIZATION
# ============================================================
def clean_and_normalize(users_df, products_df, transactions_df, sessions_df):
    """
    Clean and normalize the raw data.
    
    Tasks:
    - Handle missing values
    - Standardize date formats
    - Remove duplicates
    - Add derived columns
    """
    
    print("=" * 60)
    print("DATA CLEANING AND NORMALIZATION")
    print("=" * 60 + "\n")
    
    # --- Clean Users ---
    print("Cleaning Users...")
    users_clean = users_df \
        .dropDuplicates(["user_id"]) \
        .withColumn("registration_date", to_date(col("registration_date"))) \
        .withColumn("last_active", to_date(col("last_active")))
    
    # Extract state from geo_data
    users_clean = users_clean \
        .withColumn("state", col("geo_data.state")) \
        .withColumn("city", col("geo_data.city")) \
        .withColumn("country", col("geo_data.country"))
    
    null_count = users_clean.filter(col("user_id").isNull()).count()
    print(f"   - Removed duplicates, null user_ids: {null_count}")
    print(f"   - Added date columns and extracted geo fields")
    
    # --- Clean Products ---
    print("Cleaning Products...")
    products_clean = products_df \
        .dropDuplicates(["product_id"]) \
        .withColumn("base_price", col("base_price").cast("double")) \
        .withColumn("current_stock", col("current_stock").cast("integer")) \
        .withColumn("is_active", col("is_active").cast("boolean"))
    
    # Handle missing prices
    avg_price = products_clean.agg(avg("base_price")).collect()[0][0]
    products_clean = products_clean.fillna({"base_price": avg_price})
    print(f"   - Filled missing prices with average: ${avg_price:.2f}")
    
    # --- Clean Transactions ---
    print("Cleaning Transactions...")
    transactions_clean = transactions_df \
        .dropDuplicates(["transaction_id"]) \
        .withColumn("total", col("total").cast("double")) \
        .withColumn("subtotal", col("subtotal").cast("double")) \
        .withColumn("discount", col("discount").cast("double")) \
        .withColumn("transaction_date", to_date(col("timestamp")))
    
    # Add time-based columns
    transactions_clean = transactions_clean \
        .withColumn("transaction_month", month(col("transaction_date"))) \
        .withColumn("transaction_year", year(col("transaction_date")))
    
    print(f"   - Added transaction_date, transaction_month, transaction_year")
    
    # --- Clean Sessions ---
    if sessions_df is not None:
        print("Cleaning Sessions...")
        sessions_clean = sessions_df \
            .dropDuplicates(["session_id"]) \
            .withColumn("duration_seconds", col("duration_seconds").cast("integer")) \
            .withColumn("session_date", to_date(col("start_time")))
        
        # Extract device info
        sessions_clean = sessions_clean \
            .withColumn("device_type", col("device_profile.type")) \
            .withColumn("device_os", col("device_profile.os")) \
            .withColumn("device_browser", col("device_profile.browser"))
        
        print(f"   - Extracted device information")
    else:
        sessions_clean = None
    
    print("\n Data cleaning complete!\n")
    
    return users_clean, products_clean, transactions_clean, sessions_clean


# ============================================================
# SPARK SQL ANALYTICS
# ============================================================
def run_spark_sql_queries(spark, users_df, products_df, transactions_df, sessions_df):
    """
    Run analytical queries using Spark SQL.
    """
    
    print("=" * 60)
    print("SPARK SQL ANALYTICS")
    print("=" * 60 + "\n")
    
    # Register DataFrames as temporary views
    users_df.createOrReplaceTempView("users")
    products_df.createOrReplaceTempView("products")
    transactions_df.createOrReplaceTempView("transactions")
    if sessions_df is not None:
        sessions_df.createOrReplaceTempView("sessions")
    
    # --- Query 1: Top Customers by Total Spending ---
    print("Query 1: Top 10 Customers by Total Spending")
    print("-" * 50)
    
    top_customers = spark.sql("""
        SELECT 
            user_id,
            COUNT(*) as total_orders,
            ROUND(SUM(total), 2) as total_spent,
            ROUND(AVG(total), 2) as avg_order_value
        FROM transactions
        GROUP BY user_id
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    top_customers.show(truncate=False)
    
    # --- Query 2: Revenue by Payment Method ---
    print("Query 2: Revenue by Payment Method")
    print("-" * 50)
    
    payment_revenue = spark.sql("""
        SELECT 
            payment_method,
            COUNT(*) as transaction_count,
            ROUND(SUM(total), 2) as total_revenue,
            ROUND(AVG(total), 2) as avg_transaction
        FROM transactions
        GROUP BY payment_method
        ORDER BY total_revenue DESC
    """)
    payment_revenue.show(truncate=False)
    
    # --- Query 3: Monthly Revenue Trend ---
    print("Query 3: Monthly Revenue Trend")
    print("-" * 50)
    
    monthly_revenue = spark.sql("""
        SELECT 
            transaction_year,
            transaction_month,
            COUNT(*) as transactions,
            ROUND(SUM(total), 2) as revenue,
            ROUND(AVG(total), 2) as avg_order
        FROM transactions
        GROUP BY transaction_year, transaction_month
        ORDER BY transaction_year, transaction_month
    """)
    monthly_revenue.show(truncate=False)
    
    # --- Query 4: Product Category Performance (Join) ---
    print("Query 4: Top Categories by Revenue")
    print("-" * 50)
    
    # First, explode items from transactions
    transactions_items = spark.sql("""
        SELECT 
            transaction_id,
            user_id,
            explode(items) as item
        FROM transactions
    """)
    transactions_items.createOrReplaceTempView("transaction_items")
    
    category_performance = spark.sql("""
        SELECT 
            p.category_id,
            COUNT(DISTINCT ti.transaction_id) as order_count,
            SUM(ti.item.quantity) as items_sold,
            ROUND(SUM(ti.item.subtotal), 2) as revenue
        FROM transaction_items ti
        JOIN products p ON ti.item.product_id = p.product_id
        GROUP BY p.category_id
        ORDER BY revenue DESC
        LIMIT 10
    """)
    category_performance.show(truncate=False)
    
    # --- Query 5: User Activity Summary (if sessions available) ---
    if sessions_df is not None:
        print("Query 5: Session Conversion Rates by Device")
        print("-" * 50)
        
        device_conversion = spark.sql("""
            SELECT 
                device_type,
                COUNT(*) as total_sessions,
                SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) as conversions,
                ROUND(SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as conversion_rate,
                ROUND(AVG(duration_seconds), 0) as avg_duration_seconds
            FROM sessions
            GROUP BY device_type
            ORDER BY conversion_rate DESC
        """)
        device_conversion.show(truncate=False)
    
    return {
        'top_customers': top_customers,
        'payment_revenue': payment_revenue,
        'monthly_revenue': monthly_revenue,
        'category_performance': category_performance
    }


# ============================================================
# PRODUCT RECOMMENDATION: "Users Who Bought X Also Bought Y"
# ============================================================
def product_recommendations(spark, transactions_df):
    """
    Calculate product affinity: products frequently bought together.
    
    Algorithm:
    1. Get all products in each transaction
    2. For each pair of products bought together, count co-occurrences
    3. Rank by frequency
    """
    
    print("=" * 60)
    print("PRODUCT RECOMMENDATIONS: 'Users Who Bought X Also Bought Y'")
    print("=" * 60 + "\n")
    
    # Explode items to get product_id per transaction
    from pyspark.sql.functions import array_distinct
    
    # Get products per transaction
    products_per_txn = transactions_df \
        .withColumn("item", explode(col("items"))) \
        .select("transaction_id", col("item.product_id").alias("product_id")) \
        .groupBy("transaction_id") \
        .agg(collect_list("product_id").alias("products"))
    
    # Filter transactions with multiple products
    multi_product_txns = products_per_txn.filter(size(col("products")) > 1)
    print(f"Transactions with multiple products: {multi_product_txns.count():,}\n")
    
    # Self-join to find product pairs
    from pyspark.sql.functions import arrays_zip
    
    # Explode to get all pairs
    product_pairs = multi_product_txns \
        .withColumn("product_a", explode(col("products"))) \
        .withColumn("product_b", explode(col("products"))) \
        .filter(col("product_a") < col("product_b"))  # Avoid duplicates
    
    # Count co-occurrences
    pair_counts = product_pairs \
        .groupBy("product_a", "product_b") \
        .agg(count("*").alias("co_occurrence_count")) \
        .orderBy(desc("co_occurrence_count"))
    
    print("Top 15 Product Pairs Frequently Bought Together:")
    print("-" * 60)
    pair_counts.show(15, truncate=False)
    
    # Create recommendations view
    pair_counts.createOrReplaceTempView("product_pairs")
    
    # Get recommendations for a specific product
    print("\nExample: Recommendations for 'prod_00001'")
    print("-" * 60)
    
    recommendations = spark.sql("""
        SELECT 
            CASE 
                WHEN product_a = 'prod_00001' THEN product_b 
                ELSE product_a 
            END as recommended_product,
            co_occurrence_count
        FROM product_pairs
        WHERE product_a = 'prod_00001' OR product_b = 'prod_00001'
        ORDER BY co_occurrence_count DESC
        LIMIT 5
    """)
    recommendations.show(truncate=False)
    
    return pair_counts


# ============================================================
# COHORT ANALYSIS
# ============================================================
def cohort_analysis(spark, users_df, transactions_df):
    """
    Cohort analysis: Group users by registration month,
    track their spending over time.
    """
    
    print("=" * 60)
    print("COHORT ANALYSIS: User Spending by Registration Month")
    print("=" * 60 + "\n")
    
    # Add registration month to users
    from pyspark.sql.functions import date_format
    
    users_cohort = users_df \
        .withColumn("cohort_month", date_format(col("registration_date"), "yyyy-MM"))
    
    # Join with transactions
    users_cohort.createOrReplaceTempView("users_cohort")
    transactions_df.createOrReplaceTempView("transactions")
    
    cohort_spending = spark.sql("""
        SELECT 
            u.cohort_month,
            COUNT(DISTINCT u.user_id) as users_in_cohort,
            COUNT(DISTINCT t.user_id) as users_who_purchased,
            COUNT(t.transaction_id) as total_transactions,
            ROUND(SUM(t.total), 2) as total_revenue,
            ROUND(AVG(t.total), 2) as avg_order_value
        FROM users_cohort u
        LEFT JOIN transactions t ON u.user_id = t.user_id
        GROUP BY u.cohort_month
        ORDER BY u.cohort_month
    """)
    
    print("Spending by User Registration Cohort:")
    print("-" * 80)
    cohort_spending.show(20, truncate=False)
    
    return cohort_spending


# ============================================================
# MAIN FUNCTION
# ============================================================
def main():
    """Main function to run all Spark processing"""
    
    print("\n" + "=" * 60)
    print("   APACHE SPARK - E-COMMERCE BATCH PROCESSING")
    print("=" * 60 + "\n")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Load data
    users_df, products_df, categories_df, transactions_df, sessions_df = load_data(spark)
    
    # Clean and normalize
    users_clean, products_clean, transactions_clean, sessions_clean = \
        clean_and_normalize(users_df, products_df, transactions_df, sessions_df)
    
    # Run Spark SQL queries
    query_results = run_spark_sql_queries(
        spark, users_clean, products_clean, transactions_clean, sessions_clean
    )
    
    # Product recommendations
    recommendations = product_recommendations(spark, transactions_clean)
    
    # Cohort analysis
    cohort_results = cohort_analysis(spark, users_clean, transactions_clean)
    
    # Summary
    print("=" * 60)
    print("SPARK PROCESSING COMPLETE")
    print("=" * 60)
    print(" Data loaded and cleaned")
    print(" 5 Spark SQL queries executed")
    print(" Product recommendations calculated")
    print(" Cohort analysis completed")
    
    # Stop Spark
    spark.stop()
    print("\n Spark session stopped")


if __name__ == "__main__":
    main()