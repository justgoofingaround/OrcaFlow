#!/usr/bin/env python3 -u
"""
OrcaFlow Analytics Engine

Unified PySpark script that reads uploaded CSV/Parquet files and runs
one of four analytics types:
  - profiling:    Column stats, null counts, distributions, correlations
  - aggregation:  Group by, top-N, pivots, time-series
  - etl:          Clean, normalize, deduplicate, convert to Parquet
  - ml:           Auto-detect label, train classifier or cluster

Usage:
    python analytics_engine.py \
        --input /path/to/uploaded/files/ \
        --analytics-type profiling \
        --output /path/to/output/
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    NumericType, StringType, TimestampType, DateType, DoubleType
)


def create_spark_session():
    """Create Spark session — auto-detects local vs YARN.

    When launched via spark-submit on YARN, the master and memory are
    already configured by spark-submit, so we only add shuffle config.
    For local execution (subprocess from OrcaFlow server), the MASTER
    env var is set to 'local[4]' by spark_executor.py.
    """
    builder = SparkSession.builder.appName("OrcaFlow-Analytics")

    # spark-submit sets spark.master via its own mechanism.
    # For local runs, spark_executor.py sets MASTER=local[4].
    # We do NOT call .master() here so we never override spark-submit.

    # Detect local mode by checking if MASTER env var is set (local runs)
    # vs spark-submit which sets spark.master in SparkConf directly.
    master_env = os.environ.get("MASTER", "")

    if master_env.startswith("local"):
        # Local execution — tuned for macOS
        builder = builder \
            .config("spark.driver.memory", "6g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.default.parallelism", "4") \
            .config("spark.hadoop.fs.viewfs.impl.disable.cache", "true") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .config("spark.driver.extraJavaOptions",
                    "--add-opens java.base/javax.security.auth=ALL-UNNAMED "
                    "--add-opens java.base/sun.security.action=ALL-UNNAMED") \
            .config("spark.executor.extraJavaOptions",
                    "--add-opens java.base/javax.security.auth=ALL-UNNAMED "
                    "--add-opens java.base/sun.security.action=ALL-UNNAMED")
    else:
        # YARN mode (via spark-submit) — just tune shuffle partitions
        builder = builder \
            .config("spark.sql.shuffle.partitions", "200")

    return builder.getOrCreate()


def is_yarn_mode(spark):
    """Check if running on YARN (vs local mode)."""
    return spark.sparkContext.master.startswith("yarn")


def load_data(spark, input_path):
    """
    Load all CSV and Parquet files from input directory into a single DataFrame.

    For local mode: reads via Pandas first to avoid Java 17 getSubject bug
    with Hadoop FileSystem, then converts to Spark DataFrame.
    For YARN mode: reads directly via Spark (HDFS works correctly).
    """
    print(f"[JOB] Loading data from: {input_path}")
    yarn = is_yarn_mode(spark)

    if yarn:
        # YARN mode — read directly from HDFS via Spark (no pandas needed)
        try:
            df = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .csv(input_path)
            count = df.count()
            print(f"[JOB] Loaded {count:,} records from HDFS")
            return df
        except Exception:
            df = spark.read.parquet(input_path)
            count = df.count()
            print(f"[JOB] Loaded {count:,} records from HDFS (Parquet)")
            return df

    # Local mode — read via Pandas to avoid Java 17 Hadoop bug
    import pandas as pd
    csv_files = []
    parquet_files = []

    if os.path.isfile(input_path):
        if input_path.endswith(".parquet"):
            parquet_files.append(input_path)
        else:
            csv_files.append(input_path)
    elif os.path.isdir(input_path):
        for fname in sorted(os.listdir(input_path)):
            fpath = os.path.join(input_path, fname)
            if not os.path.isfile(fpath):
                continue
            if fname.endswith(".parquet"):
                parquet_files.append(fpath)
            elif fname.endswith(".csv") or fname.endswith(".tsv"):
                csv_files.append(fpath)
    else:
        raise FileNotFoundError(f"No data found at: {input_path}")

    pdf_list = []

    for f in csv_files:
        print(f"[JOB]   Reading CSV: {os.path.basename(f)}")
        pdf = pd.read_csv(f)
        pdf_list.append(pdf)

    for f in parquet_files:
        print(f"[JOB]   Reading Parquet: {os.path.basename(f)}")
        pdf = pd.read_parquet(f)
        pdf_list.append(pdf)

    if not pdf_list:
        raise FileNotFoundError(f"No CSV or Parquet files found in: {input_path}")

    # Concatenate Pandas DataFrames
    if len(pdf_list) == 1:
        combined_pdf = pdf_list[0]
    else:
        base_cols = set(pdf_list[0].columns)
        matching = [pdf_list[0]]
        for pdf in pdf_list[1:]:
            if set(pdf.columns) == base_cols:
                matching.append(pdf)
            else:
                print(f"[JOB]   Schema mismatch — skipping file "
                      f"(expected {len(base_cols)} cols, got {len(pdf.columns)})")
        combined_pdf = pd.concat(matching, ignore_index=True)

    print(f"[JOB] Loaded {len(combined_pdf):,} records via Pandas")

    # Convert to Spark DataFrame
    df = spark.createDataFrame(combined_pdf)

    print(f"[JOB] Total records loaded: {df.count():,}")
    print(f"[JOB] Columns ({len(df.columns)}): {', '.join(df.columns)}")
    return df


def save_df(df, path, name, yarn_mode):
    """Save a DataFrame — toPandas for local, Spark write for YARN."""
    if yarn_mode:
        df.coalesce(1).write.mode("overwrite") \
          .option("header", "true").csv(f"{path}/{name}")
    else:
        os.makedirs(path, exist_ok=True)
        df.toPandas().to_csv(f"{path}/{name}.csv", index=False)
    print(f"[JOB]   Saved {name}")


# ─────────────────────────────────────────────────────────────
# Analytics Type 1: Data Profiling
# ─────────────────────────────────────────────────────────────

def run_profiling(df, output_path, yarn_mode):
    """Profile every column: stats, nulls, distributions, correlations."""
    print("\n" + "=" * 60)
    print("DATA PROFILING")
    print("=" * 60)

    total = df.count()
    print(f"[PROFILE] Total rows: {total:,}")
    print(f"[PROFILE] Total columns: {len(df.columns)}")

    # Column-level stats
    profile_rows = []
    numeric_cols = []

    for col_name in df.columns:
        col_type = str(df.schema[col_name].dataType)
        null_count = df.filter(F.col(col_name).isNull()).count()
        distinct_count = df.select(col_name).distinct().count()

        row = {
            "column": col_name,
            "data_type": col_type,
            "null_count": null_count,
            "null_pct": round(100 * null_count / total, 2) if total > 0 else 0,
            "distinct_count": distinct_count,
        }

        # Numeric stats
        if isinstance(df.schema[col_name].dataType, NumericType):
            numeric_cols.append(col_name)
            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.mean(col_name).alias("mean"),
                F.stddev(col_name).alias("stddev"),
                F.expr(f"percentile_approx({col_name}, 0.5)").alias("median"),
            ).first()
            row["min"] = float(stats["min"]) if stats["min"] is not None else None
            row["max"] = float(stats["max"]) if stats["max"] is not None else None
            row["mean"] = round(float(stats["mean"]), 4) if stats["mean"] is not None else None
            row["stddev"] = round(float(stats["stddev"]), 4) if stats["stddev"] is not None else None
            row["median"] = float(stats["median"]) if stats["median"] is not None else None
        else:
            # Top 5 values for non-numeric
            top_vals = df.groupBy(col_name).count() \
                         .orderBy(F.desc("count")).limit(5).collect()
            row["top_values"] = ", ".join(
                f"{r[col_name]}({r['count']})" for r in top_vals
                if r[col_name] is not None
            )

        profile_rows.append(row)

    # Print profile table
    print("\n[PROFILE] Column Summary:")
    print(f"{'Column':<25} {'Type':<15} {'Nulls':<10} {'Distinct':<10} {'Min':<12} {'Max':<12} {'Mean':<12}")
    print("-" * 96)
    for r in profile_rows:
        print(f"{r['column']:<25} {r['data_type']:<15} "
              f"{r['null_count']:<10} {r['distinct_count']:<10} "
              f"{str(r.get('min', '')):<12} {str(r.get('max', '')):<12} "
              f"{str(r.get('mean', '')):<12}")

    # Save profile as CSV
    if yarn_mode:
        from pyspark.sql import Row
        spark = df.sparkSession
        profile_df = spark.createDataFrame([Row(**r) for r in profile_rows])
        profile_df.coalesce(1).write.mode("overwrite") \
            .option("header", "true").csv(f"{output_path}/column_profile")
    else:
        import pandas as pd
        os.makedirs(output_path, exist_ok=True)
        pd.DataFrame(profile_rows).to_csv(
            f"{output_path}/column_profile.csv", index=False)
    print(f"[JOB]   Saved column_profile")

    # Correlation matrix for numeric columns
    if len(numeric_cols) >= 2:
        print(f"\n[PROFILE] Correlation matrix ({len(numeric_cols)} numeric columns):")
        corr_rows = []
        for c1 in numeric_cols:
            corr_row = {"column": c1}
            for c2 in numeric_cols:
                corr = df.stat.corr(c1, c2)
                corr_row[c2] = round(corr, 4) if corr is not None else None
            corr_rows.append(corr_row)
            vals = " | ".join(f"{corr_row.get(c, 'N/A'):>8}" for c in numeric_cols)
            print(f"  {c1:<20} {vals}")

        if yarn_mode:
            from pyspark.sql import Row
            spark = df.sparkSession
            corr_df = spark.createDataFrame([Row(**r) for r in corr_rows])
            corr_df.coalesce(1).write.mode("overwrite") \
                .option("header", "true").csv(f"{output_path}/correlations")
        else:
            import pandas as pd
            pd.DataFrame(corr_rows).to_csv(
                f"{output_path}/correlations.csv", index=False)
        print(f"[JOB]   Saved correlations")

    return total


# ─────────────────────────────────────────────────────────────
# Analytics Type 2: Aggregation Analytics
# ─────────────────────────────────────────────────────────────

def run_aggregation(df, output_path, yarn_mode):
    """Auto-detect categorical columns, group by them, aggregate numerics."""
    print("\n" + "=" * 60)
    print("AGGREGATION ANALYTICS")
    print("=" * 60)

    total = df.count()
    numeric_cols = [f.name for f in df.schema.fields
                    if isinstance(f.dataType, NumericType)]
    string_cols = [f.name for f in df.schema.fields
                   if isinstance(f.dataType, StringType)]
    date_cols = [f.name for f in df.schema.fields
                 if isinstance(f.dataType, (TimestampType, DateType))]

    print(f"[AGG] Numeric columns: {numeric_cols}")
    print(f"[AGG] String columns: {string_cols}")
    print(f"[AGG] Date columns: {date_cols}")

    # Find low-cardinality string columns (good for group-by)
    group_cols = []
    for col_name in string_cols:
        distinct = df.select(col_name).distinct().count()
        if distinct <= 50:
            group_cols.append((col_name, distinct))
            print(f"[AGG]   Group-by candidate: {col_name} ({distinct} unique values)")

    if not group_cols:
        print("[AGG] No low-cardinality columns found for grouping")
        if string_cols:
            group_cols = [(string_cols[0],
                           df.select(string_cols[0]).distinct().count())]

    saved = 0

    # Aggregate by each group-by column
    for col_name, _ in group_cols[:3]:  # limit to top 3
        print(f"\n[AGG] Grouping by: {col_name}")
        agg_exprs = []
        for nc in numeric_cols[:6]:  # limit to 6 numeric cols
            agg_exprs.extend([
                F.sum(nc).alias(f"{nc}_sum"),
                F.avg(nc).alias(f"{nc}_avg"),
                F.count(nc).alias(f"{nc}_count"),
            ])

        if agg_exprs:
            result = df.groupBy(col_name).agg(*agg_exprs) \
                       .orderBy(F.desc(f"{numeric_cols[0]}_sum") if numeric_cols else col_name)
            result.show(20, truncate=False)
            save_df(result, output_path, f"agg_by_{col_name}", yarn_mode)
            saved += 1

    # Top-N per categorical column
    if group_cols and numeric_cols:
        primary_col = group_cols[0][0]
        primary_num = numeric_cols[0]
        print(f"\n[AGG] Top 20 by {primary_num} grouped by {primary_col}:")
        top_n = df.groupBy(primary_col) \
                  .agg(F.sum(primary_num).alias("total")) \
                  .orderBy(F.desc("total")).limit(20)
        top_n.show(20, truncate=False)
        save_df(top_n, output_path, f"top20_{primary_col}", yarn_mode)
        saved += 1

    # Time-series if date columns exist
    if date_cols and numeric_cols:
        date_col = date_cols[0]
        print(f"\n[AGG] Time-series aggregation on: {date_col}")
        ts_df = df.withColumn("_date", F.to_date(F.col(date_col)))
        daily = ts_df.groupBy("_date").agg(
            F.count("*").alias("record_count"),
            *[F.sum(nc).alias(f"{nc}_total") for nc in numeric_cols[:4]]
        ).orderBy("_date")
        daily.show(20, truncate=False)
        save_df(daily, output_path, "daily_timeseries", yarn_mode)
        saved += 1

    if saved == 0:
        print("[AGG] No aggregations could be performed on this data")

    return total


# ─────────────────────────────────────────────────────────────
# Analytics Type 3: ETL Transform
# ─────────────────────────────────────────────────────────────

def run_etl(df, output_path, yarn_mode):
    """Clean, normalize, deduplicate, and output as Parquet."""
    print("\n" + "=" * 60)
    print("ETL TRANSFORM")
    print("=" * 60)

    total_before = df.count()
    print(f"[ETL] Input records: {total_before:,}")
    print(f"[ETL] Input columns: {len(df.columns)}")

    # Step 1: Drop exact duplicates
    df_dedup = df.dropDuplicates()
    after_dedup = df_dedup.count()
    removed = total_before - after_dedup
    print(f"[ETL] Dropped {removed:,} duplicate rows ({after_dedup:,} remaining)")

    # Step 2: Fill nulls
    numeric_cols = [f.name for f in df_dedup.schema.fields
                    if isinstance(f.dataType, NumericType)]
    string_cols = [f.name for f in df_dedup.schema.fields
                   if isinstance(f.dataType, StringType)]

    null_report = []
    for col_name in df_dedup.columns:
        nc = df_dedup.filter(F.col(col_name).isNull()).count()
        if nc > 0:
            null_report.append((col_name, nc))
            print(f"[ETL]   {col_name}: {nc:,} nulls")

    # Fill numeric nulls with median
    for col_name in numeric_cols:
        median_val = df_dedup.select(
            F.expr(f"percentile_approx({col_name}, 0.5)")
        ).first()[0]
        if median_val is not None:
            df_dedup = df_dedup.fillna({col_name: float(median_val)})

    # Fill string nulls with "UNKNOWN"
    for col_name in string_cols:
        df_dedup = df_dedup.fillna({col_name: "UNKNOWN"})

    nulls_after = sum(
        df_dedup.filter(F.col(c).isNull()).count() for c in df_dedup.columns
    )
    print(f"[ETL] Nulls remaining: {nulls_after}")

    # Step 3: Normalize numeric columns (min-max scaling)
    print("[ETL] Normalizing numeric columns (min-max scaling)...")
    for col_name in numeric_cols:
        stats = df_dedup.select(
            F.min(col_name).alias("min_val"),
            F.max(col_name).alias("max_val")
        ).first()
        min_val = float(stats["min_val"]) if stats["min_val"] is not None else 0
        max_val = float(stats["max_val"]) if stats["max_val"] is not None else 1
        range_val = max_val - min_val
        if range_val > 0:
            df_dedup = df_dedup.withColumn(
                f"{col_name}_normalized",
                ((F.col(col_name) - min_val) / range_val).cast(DoubleType())
            )

    # Step 4: Save as Parquet
    print(f"[ETL] Saving cleaned data ({df_dedup.count():,} rows, "
          f"{len(df_dedup.columns)} columns)...")

    if yarn_mode:
        df_dedup.write.mode("overwrite").parquet(f"{output_path}/cleaned_data")
    else:
        os.makedirs(output_path, exist_ok=True)
        df_dedup.toPandas().to_parquet(f"{output_path}/cleaned_data.parquet",
                                       index=False)

    # Save ETL report
    from pyspark.sql import Row
    spark = df.sparkSession
    report_data = [
        Row(metric="input_rows", value=str(total_before)),
        Row(metric="duplicates_removed", value=str(removed)),
        Row(metric="output_rows", value=str(after_dedup)),
        Row(metric="nulls_filled", value=str(len(null_report))),
        Row(metric="columns_normalized", value=str(len(numeric_cols))),
        Row(metric="output_columns", value=str(len(df_dedup.columns))),
    ]
    report_df = spark.createDataFrame(report_data)
    save_df(report_df, output_path, "etl_report", yarn_mode)

    print("[ETL] Transform complete")
    return after_dedup


# ─────────────────────────────────────────────────────────────
# Analytics Type 4: ML Training
# ─────────────────────────────────────────────────────────────

def run_ml(df, output_path, yarn_mode):
    """Auto-detect label column, train classifier or cluster."""
    print("\n" + "=" * 60)
    print("ML TRAINING")
    print("=" * 60)

    total = df.count()
    numeric_cols = [f.name for f in df.schema.fields
                    if isinstance(f.dataType, NumericType)]
    string_cols = [f.name for f in df.schema.fields
                   if isinstance(f.dataType, StringType)]

    print(f"[ML] Rows: {total:,}")
    print(f"[ML] Numeric features: {numeric_cols}")
    print(f"[ML] Categorical columns: {string_cols}")

    # Auto-detect label column
    label_col = None
    for candidate in ["label", "target", "class", "category", "outcome"]:
        if candidate in [c.lower() for c in df.columns]:
            label_col = [c for c in df.columns if c.lower() == candidate][0]
            break

    if label_col is None:
        # Pick last string column with < 20 distinct values
        for col_name in reversed(string_cols):
            distinct = df.select(col_name).distinct().count()
            if distinct <= 20:
                label_col = col_name
                break

    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from pyspark.ml import Pipeline

    if label_col and label_col in [c for c in df.columns]:
        # ---- Classification ----
        n_classes = df.select(label_col).distinct().count()
        print(f"[ML] Label column: {label_col} ({n_classes} classes)")

        # Prepare features
        feature_cols = [c for c in numeric_cols if c != label_col]
        indexers = []
        indexed_cols = []

        for sc in string_cols:
            if sc == label_col:
                continue
            distinct = df.select(sc).distinct().count()
            if distinct <= 50:
                out_name = f"{sc}_idx"
                indexers.append(StringIndexer(inputCol=sc, outputCol=out_name,
                                              handleInvalid="keep"))
                indexed_cols.append(out_name)

        all_feature_cols = feature_cols + indexed_cols

        if not all_feature_cols:
            print("[ML] No usable feature columns found")
            return total

        # Index the label
        label_indexer = StringIndexer(inputCol=label_col,
                                      outputCol="label_idx",
                                      handleInvalid="keep")

        assembler = VectorAssembler(inputCols=all_feature_cols,
                                    outputCol="features",
                                    handleInvalid="skip")

        from pyspark.ml.classification import RandomForestClassifier
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator

        rf = RandomForestClassifier(
            labelCol="label_idx", featuresCol="features",
            numTrees=50, maxDepth=8, seed=42
        )

        pipeline = Pipeline(stages=indexers + [label_indexer, assembler, rf])

        # Split
        train_df, test_df = df.na.drop().randomSplit([0.8, 0.2], seed=42)
        print(f"[ML] Train: {train_df.count():,} rows, Test: {test_df.count():,} rows")

        # Train
        print("[ML] Training Random Forest classifier...")
        model = pipeline.fit(train_df)

        # Evaluate
        predictions = model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label_idx", predictionCol="prediction"
        )

        accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
        f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})

        print(f"[ML] Accuracy: {accuracy:.4f}")
        print(f"[ML] F1 Score: {f1:.4f}")

        # Feature importances
        rf_model = model.stages[-1]
        importances = rf_model.featureImportances.toArray()
        feat_imp = sorted(
            zip(all_feature_cols, importances),
            key=lambda x: x[1], reverse=True
        )
        print("[ML] Feature Importances:")
        for fname, imp in feat_imp[:10]:
            print(f"  {fname:<30} {imp:.4f}")

        # Save results
        from pyspark.sql import Row
        spark = df.sparkSession

        metrics_data = [
            Row(metric="algorithm", value="RandomForestClassifier"),
            Row(metric="label_column", value=label_col),
            Row(metric="num_classes", value=str(n_classes)),
            Row(metric="num_features", value=str(len(all_feature_cols))),
            Row(metric="train_rows", value=str(train_df.count())),
            Row(metric="test_rows", value=str(test_df.count())),
            Row(metric="accuracy", value=f"{accuracy:.4f}"),
            Row(metric="f1_score", value=f"{f1:.4f}"),
            Row(metric="num_trees", value="50"),
        ]
        metrics_df = spark.createDataFrame(metrics_data)
        save_df(metrics_df, output_path, "ml_metrics", yarn_mode)

        imp_data = [Row(feature=f, importance=str(round(i, 4)))
                    for f, i in feat_imp]
        imp_df = spark.createDataFrame(imp_data)
        save_df(imp_df, output_path, "feature_importances", yarn_mode)

    else:
        # ---- Clustering (no label column) ----
        print("[ML] No label column detected — running KMeans clustering")

        if not numeric_cols:
            print("[ML] No numeric columns for clustering")
            return total

        from pyspark.ml.clustering import KMeans
        from pyspark.ml.evaluation import ClusteringEvaluator

        assembler = VectorAssembler(inputCols=numeric_cols,
                                    outputCol="features",
                                    handleInvalid="skip")
        clean_df = df.na.drop(subset=numeric_cols)

        k = min(5, max(2, clean_df.count() // 1000))
        print(f"[ML] Running KMeans with k={k}")

        kmeans = KMeans(featuresCol="features", k=k, seed=42)
        pipeline = Pipeline(stages=[assembler, kmeans])
        model = pipeline.fit(clean_df)

        predictions = model.transform(clean_df)
        evaluator = ClusteringEvaluator(featuresCol="features")
        silhouette = evaluator.evaluate(predictions)
        print(f"[ML] Silhouette Score: {silhouette:.4f}")

        # Cluster sizes
        cluster_sizes = predictions.groupBy("prediction") \
                                   .count().orderBy("prediction")
        print("[ML] Cluster sizes:")
        cluster_sizes.show()

        from pyspark.sql import Row
        spark = df.sparkSession
        metrics_data = [
            Row(metric="algorithm", value="KMeans"),
            Row(metric="k", value=str(k)),
            Row(metric="num_features", value=str(len(numeric_cols))),
            Row(metric="silhouette_score", value=f"{silhouette:.4f}"),
            Row(metric="total_rows", value=str(clean_df.count())),
        ]
        metrics_df = spark.createDataFrame(metrics_data)
        save_df(metrics_df, output_path, "ml_metrics", yarn_mode)
        save_df(cluster_sizes, output_path, "cluster_sizes", yarn_mode)

    return total


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OrcaFlow Analytics Engine")
    parser.add_argument("--input", type=str, required=True,
                        help="Path to input data (directory or file)")
    parser.add_argument("--analytics-type", type=str, required=True,
                        choices=["profiling", "aggregation", "etl", "ml"],
                        help="Type of analytics to run")
    parser.add_argument("--output", type=str, required=True,
                        help="Path for output results")
    args = parser.parse_args()

    analytics_names = {
        "profiling": "Data Profiling",
        "aggregation": "Aggregation Analytics",
        "etl": "ETL Transform",
        "ml": "ML Training",
    }

    print("=" * 60)
    print(f"OrcaFlow Analytics Engine")
    print(f"  Analytics: {analytics_names[args.analytics_type]}")
    print(f"  Input:     {args.input}")
    print(f"  Output:    {args.output}")
    print("=" * 60)

    start_time = time.time()
    spark = None

    try:
        spark = create_spark_session()
        yarn = is_yarn_mode(spark)
        print(f"[JOB] Spark version: {spark.version}")
        print(f"[JOB] Master: {spark.sparkContext.master}")
        print(f"[JOB] Mode: {'YARN' if yarn else 'Local'}")

        # Load data
        df = load_data(spark, args.input)
        df.cache()
        total_records = df.count()

        # Dispatch to analytics type
        dispatch = {
            "profiling": run_profiling,
            "aggregation": run_aggregation,
            "etl": run_etl,
            "ml": run_ml,
        }
        records = dispatch[args.analytics_type](df, args.output, yarn)

        elapsed = time.time() - start_time
        records_per_sec = total_records / elapsed if elapsed > 0 else 0

        # Completion report (format matches spark_executor.py parsing)
        print("\n" + "=" * 60)
        print("Job Completion Report")
        print("=" * 60)
        print(f"Analytics type: {analytics_names[args.analytics_type]}")
        print(f"Total records processed: {total_records:,}")
        print(f"Execution time: {elapsed:.2f} seconds")
        print(f"Records/sec: {records_per_sec:,.0f}")
        print(f"Output location: {args.output}")
        print("=" * 60)

        result = {
            "status": "SUCCESS",
            "analytics_type": args.analytics_type,
            "total_records": total_records,
            "execution_time": elapsed,
            "records_per_sec": records_per_sec,
            "output_path": args.output,
        }
        print(json.dumps(result, indent=2))

    except Exception as e:
        print(f"\n[ERROR] Job failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        result = {"status": "FAILED", "error": str(e)}
        print(json.dumps(result, indent=2), file=sys.stderr)
        sys.exit(1)

    finally:
        if spark:
            spark.stop()
            print("[JOB] Spark session closed")


if __name__ == "__main__":
    main()
