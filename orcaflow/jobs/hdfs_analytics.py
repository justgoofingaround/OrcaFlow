#!/usr/bin/env python3
"""
OrcaFlow HDFS Analytics Job

PySpark job designed to run on NYU Dataproc YARN cluster.
Reads data from HDFS, performs distributed analytics, writes results back to HDFS.

Usage:
    spark-submit --master yarn --deploy-mode client hdfs_analytics.py \
        --input /user/ps5390_nyu_edu/orcaflow/data/nyc_taxi \
        --output /user/ps5390_nyu_edu/orcaflow/output/job_001

If no --input is given, generates synthetic data for demo purposes.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
import random

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    StringType, TimestampType, LongType
)


def create_spark_session(app_name="OrcaFlow-Analytics"):
    """Create Spark session — auto-detects local vs YARN mode."""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def generate_synthetic_data(spark, num_records=1000000):
    """
    Generate large synthetic transaction dataset using Spark parallelism.
    Uses mapPartitions for efficient distributed generation instead of Python loop.
    """
    print(f"[JOB] Generating {num_records:,} synthetic records using distributed generation...")

    num_partitions = 100
    records_per_partition = num_records // num_partitions

    def generate_partition(partition_idx):
        """Generate records for one partition."""
        import random as rng
        rng.seed(partition_idx)
        categories = ["Electronics", "Groceries", "Clothing", "Services",
                       "Entertainment", "Travel", "Healthcare", "Education"]
        regions = ["Manhattan", "Brooklyn", "Queens", "Bronx",
                   "Staten_Island", "Jersey_City", "Hoboken"]
        base_ts = 1700000000  # Nov 2023

        rows = []
        start = partition_idx * records_per_partition
        for i in range(records_per_partition):
            rows.append((
                start + i,                          # transaction_id
                rng.randint(1, 50000),              # customer_id
                round(rng.expovariate(0.005), 2),   # amount (exponential dist)
                rng.choice(categories),             # category
                rng.choice(regions),                # region
                base_ts + rng.randint(0, 31536000), # timestamp_epoch
            ))
        return iter(rows)

    schema = StructType([
        StructField("transaction_id", LongType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("amount", DoubleType(), False),
        StructField("category", StringType(), False),
        StructField("region", StringType(), False),
        StructField("timestamp_epoch", LongType(), False),
    ])

    # Use parallelize + flatMap for distributed generation
    rdd = spark.sparkContext.parallelize(
        range(num_partitions), num_partitions
    ).flatMap(generate_partition)

    df = spark.createDataFrame(rdd, schema=schema)

    # Add derived columns
    df = df.withColumn("timestamp", F.from_unixtime("timestamp_epoch").cast("timestamp")) \
           .withColumn("date", F.to_date("timestamp")) \
           .withColumn("hour", F.hour("timestamp")) \
           .withColumn("day_of_week", F.dayofweek("timestamp"))

    print(f"[JOB] Generated DataFrame with {df.count():,} records across {df.rdd.getNumPartitions()} partitions")
    return df


def load_hdfs_data(spark, input_path):
    """Load data from HDFS (CSV or Parquet)."""
    print(f"[JOB] Loading data from HDFS: {input_path}")

    if input_path.endswith(".parquet") or input_path.endswith("/"):
        try:
            df = spark.read.parquet(input_path)
            print(f"[JOB] Loaded Parquet data: {df.count():,} records")
            return df
        except Exception:
            pass

    # Try CSV with header
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
    print(f"[JOB] Loaded CSV data: {df.count():,} records, columns: {df.columns}")
    return df


def run_analytics(df):
    """
    Run comprehensive analytics on the dataset.
    Performs multiple aggregation passes to demonstrate distributed computation.
    """
    results = {}
    print("\n" + "=" * 60)
    print("RUNNING DISTRIBUTED ANALYTICS")
    print("=" * 60)

    # 1. Category Analysis
    print("\n[ANALYSIS 1] Revenue by Category")
    category_stats = df.groupBy("category").agg(
        F.sum("amount").alias("total_revenue"),
        F.avg("amount").alias("avg_transaction"),
        F.count("*").alias("transaction_count"),
        F.stddev("amount").alias("amount_stddev"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    ).orderBy(F.desc("total_revenue"))
    category_stats.show(truncate=False)
    results["category_stats"] = category_stats

    # 2. Regional Analysis
    if "region" in df.columns:
        print("\n[ANALYSIS 2] Revenue by Region")
        region_stats = df.groupBy("region").agg(
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_transaction"),
            F.count("*").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        ).orderBy(F.desc("total_revenue"))
        region_stats.show(truncate=False)
        results["region_stats"] = region_stats

    # 3. Time-series Analysis
    if "date" in df.columns:
        print("\n[ANALYSIS 3] Daily Revenue Trend")
        daily_stats = df.groupBy("date").agg(
            F.sum("amount").alias("daily_revenue"),
            F.count("*").alias("daily_transactions"),
            F.countDistinct("customer_id").alias("daily_active_customers"),
        ).orderBy("date")
        print(f"  Days covered: {daily_stats.count()}")
        daily_stats.show(10, truncate=False)
        results["daily_stats"] = daily_stats

    # 4. Hourly Pattern Analysis
    if "hour" in df.columns:
        print("\n[ANALYSIS 4] Hourly Transaction Patterns")
        hourly_stats = df.groupBy("hour").agg(
            F.count("*").alias("transaction_count"),
            F.avg("amount").alias("avg_amount"),
        ).orderBy("hour")
        hourly_stats.show(24, truncate=False)
        results["hourly_stats"] = hourly_stats

    # 5. Top Customers Analysis
    print("\n[ANALYSIS 5] Top 20 Customers by Spending")
    top_customers = df.groupBy("customer_id").agg(
        F.sum("amount").alias("total_spent"),
        F.count("*").alias("transaction_count"),
        F.avg("amount").alias("avg_transaction"),
    ).orderBy(F.desc("total_spent")).limit(20)
    top_customers.show(truncate=False)
    results["top_customers"] = top_customers

    # 6. High-value Transaction Analysis
    print("\n[ANALYSIS 6] High-value Transactions (amount > 500)")
    high_value_count = df.filter(F.col("amount") > 500).count()
    total_count = df.count()
    print(f"  High-value transactions: {high_value_count:,} ({100*high_value_count/total_count:.1f}%)")

    # 7. Cross-tabulation: Category x Region
    if "region" in df.columns:
        print("\n[ANALYSIS 7] Category x Region Cross-tabulation (transaction counts)")
        cross_tab = df.groupBy("category").pivot("region").agg(F.count("*"))
        cross_tab.show(truncate=False)
        results["cross_tab"] = cross_tab

    return results


def save_results(results, output_path, spark):
    """Save analytics results to HDFS or local filesystem."""
    print(f"\n[JOB] Saving results to: {output_path}")

    for name, df in results.items():
        path = f"{output_path}/{name}"
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
        print(f"  Saved {name} to {path}")

    print("[JOB] All results saved")


def main():
    parser = argparse.ArgumentParser(description="OrcaFlow HDFS Analytics")
    parser.add_argument("--input", type=str, default=None,
                        help="HDFS input path (if not set, generates synthetic data)")
    parser.add_argument("--output", type=str,
                        default="/user/ps5390_nyu_edu/orcaflow/output",
                        help="HDFS output path for results")
    parser.add_argument("--records", type=int, default=1000000,
                        help="Number of synthetic records to generate (default: 1M)")
    args = parser.parse_args()

    print("=" * 60)
    print("OrcaFlow HDFS Analytics Job")
    print(f"  Input:   {args.input or 'synthetic data generation'}")
    print(f"  Output:  {args.output}")
    print(f"  Records: {args.records:,}" if not args.input else "")
    print("=" * 60)

    start_time = time.time()
    spark = None

    try:
        spark = create_spark_session()
        print(f"[JOB] Spark version: {spark.version}")
        print(f"[JOB] Master: {spark.sparkContext.master}")
        print(f"[JOB] App ID: {spark.sparkContext.applicationId}")

        # Load or generate data
        if args.input:
            df = load_hdfs_data(spark, args.input)
        else:
            df = generate_synthetic_data(spark, num_records=args.records)

        # Cache for multiple passes
        df.cache()

        # Run analytics
        results = run_analytics(df)

        # Save results
        output_path = f"{args.output}/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        save_results(results, output_path, spark)

        # Metrics
        total_records = df.count()
        elapsed = time.time() - start_time
        records_per_sec = total_records / elapsed if elapsed > 0 else 0

        print("\n" + "=" * 60)
        print("Job Completion Report")
        print("=" * 60)
        print(f"Total records processed: {total_records:,}")
        print(f"Execution time: {elapsed:.2f} seconds")
        print(f"Records/sec: {records_per_sec:,.0f}")
        print(f"Spark master: {spark.sparkContext.master}")
        print(f"Output location: {output_path}")
        print("=" * 60)

        result_json = {
            "status": "SUCCESS",
            "total_records": total_records,
            "execution_time": elapsed,
            "records_per_sec": records_per_sec,
            "spark_master": spark.sparkContext.master,
            "output_path": output_path,
        }
        print(json.dumps(result_json, indent=2))

    except Exception as e:
        print(f"\n[ERROR] Job failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        result_json = {"status": "FAILED", "error": str(e)}
        print(json.dumps(result_json, indent=2), file=sys.stderr)
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            print("[JOB] Spark session closed")


if __name__ == "__main__":
    main()
