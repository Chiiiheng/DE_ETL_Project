import json
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, current_timestamp, expr, lit,
                                   monotonically_increasing_id, to_date,
                                   to_timestamp)
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)


def ingest_data_to_delta(
    load_df,
    table_name: str,
    mode='append',
    storage_path='s3a://godata2023/delta',
):
    delta_output_path = f"{storage_path}/{table_name}"
    load_df.write.format('delta').mode(mode).save(delta_output_path)
    print(f"data ingested to delta table: {delta_output_path}")


def read_data_from_delta(
    table_name: str,
    spark: SparkSession,
    filter_confition: str = None,
    storage_path='s3a://godata2023/delta'
):
    if filter_confition:
        df = spark.read.format("delta").load(
            f"{storage_path}/{table_name}").filter(filter_confition)
    else:
        df = spark.read.format("delta").load(f"{storage_path}/{table_name}")
    return df


def setup_spark_session():
    spark = SparkSession.builder \
        .appName("godata2023") \
        .master('local[*]') \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", "godata2023") \
        .config("spark.hadoop.fs.s3a.secret.key", "godata2023") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.region", "us-east-1") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
