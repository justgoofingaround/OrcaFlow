#!/usr/bin/env python3
"""
Sample PySpark Workflow for OrcaFlow

Demonstrates batch data processing with:
- Data generation and schema definition
- Distributed analytics operations
- Result aggregation and analysis
- Resource tracking and metrics reporting
"""

import sys
import json
import time
from datetime import datetime, timedelta
import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, 
    StringType, TimestampType
)


def create_spark_session():
    """
    Create or get existing Spark session.
    
    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName("OrcaFlow-SampleJob") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.hadoop.fs.viewfs.impl.disable.cache", "true") \
        .config("spark.driver.extraJavaOptions", "--add-opens java.base/javax.security.auth=ALL-UNNAMED") \
        .getOrCreate()


def generate_sample_data(spark, num_records=100000):
    """
    Generate sample transaction data for testing.
    
    Args:
        spark (SparkSession): Active Spark session
        num_records (int): Number of records to generate (default 100000)
        
    Returns:
        DataFrame: Spark DataFrame with transaction schema
    """
    print(f"[JOB] Generating {num_records} sample records...")
    
    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", DoubleType(), False),
        StructField("category", StringType(), False),
        StructField("timestamp", TimestampType(), False),
    ])
    
    data = []
    categories = ["Electronics", "Groceries", "Clothing", "Services", "Entertainment"]
    base_time = datetime.now() - timedelta(days=365)
    
    for i in range(num_records):
        transaction_id = i
        customer_id = random.randint(1, 5000)
        amount = round(random.uniform(10, 1000), 2)
        category = random.choice(categories)
        timestamp = base_time + timedelta(seconds=random.randint(0, 31536000))
        
        data.append((transaction_id, customer_id, amount, category, timestamp))
    
    df = spark.createDataFrame(data, schema=schema)
    print(f"[JOB] Created DataFrame with {df.count()} records")
    return df

def analyze_transactions(df):
    """
    Perform analytics on transaction data.
    
    Args:
        df: Spark DataFrame with transaction records
        
    Returns:
        tuple: (category_stats, customer_stats) DataFrames
    """
    print("[JOB] Running analytics...")
    
    # Total transactions by category using explicit aggregation functions
    from pyspark.sql.functions import sum as spark_sum, avg as spark_avg, count as spark_count
    
    category_stats = (df.groupBy("category")
                      .agg(
                          spark_sum("amount").alias("total_spent"),
                          spark_avg("amount").alias("avg_spent"),
                          spark_count("amount").alias("transaction_count")
                      )
                      .orderBy(col("total_spent").desc()))
    
    print("[JOB] Category Statistics:")
    category_stats.show()
    
    # Customer spending analysis
    customer_stats = (df.groupBy("customer_id")
                      .agg(
                          spark_sum("amount").alias("total_spent"),
                          spark_avg("amount").alias("avg_spent"),
                          spark_count("amount").alias("transaction_count")
                      )
                      .orderBy(col("total_spent").desc()))
    
    print("[JOB] Top Customers by Spending:")
    customer_stats.limit(10).show()
    
    # High-value transactions
    high_value = df.filter(col("amount") > 500).count()
    print(f"[JOB] High-value transactions (>$500): {high_value}")
    
    return category_stats, customer_stats

def save_results(category_stats, customer_stats, output_dir="/tmp/orcaflow_output"):
    """
    Save analysis results to CSV files.

    Args:
        category_stats (DataFrame): Category aggregation results
        customer_stats (DataFrame): Customer aggregation results
        output_dir (str): Output directory path (default /tmp/orcaflow_output)
    """
    import os

    print(f"[JOB] Saving results to {output_dir}...")

    os.makedirs(output_dir, exist_ok=True)

    # Convert to Pandas and save (avoids Hadoop FileSystem issues on local mode)
    category_stats.toPandas().to_csv(f"{output_dir}/category_stats.csv", index=False)
    customer_stats.toPandas().to_csv(f"{output_dir}/customer_stats.csv", index=False)

    print("[JOB] Results saved successfully")

def main():
    """
    Main job execution orchestrator.
    
    Orchestrates the complete workflow:
    1. Initialize Spark session
    2. Generate sample transaction data
    3. Perform distributed analytics
    4. Save results
    5. Report metrics and execution status
    """
    print("=" * 60)
    print("OrcaFlow PySpark Sample Workflow")
    print("=" * 60)
    
    start_time = time.time()
    spark = None
    
    try:
        # Initialize Spark
        print("[JOB] Initializing Spark session...")
        spark = create_spark_session()
        print(f"[JOB] Spark version: {spark.version}")
        
        # Generate sample data
        df = generate_sample_data(spark, num_records=100000)
        
        # Perform analysis
        category_stats, customer_stats = analyze_transactions(df)
        
        # Save results
        save_results(category_stats, customer_stats)
        
        # Calculate metrics
        total_records = df.count()
        total_amount = df.agg(spark_sum("amount")).collect()[0][0]
        
        elapsed_time = time.time() - start_time
        records_per_sec = total_records / elapsed_time if elapsed_time > 0 else 0
        
        # Report completion
        print("\n" + "=" * 60)
        print("Job Completion Report")
        print("=" * 60)
        print(f"Total records processed: {total_records:,}")
        print(f"Total transaction amount: ${total_amount:,.2f}")
        print(f"Execution time: {elapsed_time:.2f} seconds")
        print(f"Records/sec: {records_per_sec:,.0f}")
        print("=" * 60)
        
        # Return success status as JSON
        result = {
            "status": "SUCCESS",
            "total_records": total_records,
            "total_amount": float(total_amount),
            "execution_time": elapsed_time,
            "records_per_sec": records_per_sec
        }
        
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        print(f"\n[ERROR] Job failed: {str(e)}", file=sys.stderr)
        result = {
            "status": "FAILED",
            "error": str(e)
        }
        print(json.dumps(result, indent=2), file=sys.stderr)
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()
            print("[JOB] Spark session closed")


if __name__ == "__main__":
    main()
