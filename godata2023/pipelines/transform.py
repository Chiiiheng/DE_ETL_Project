import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql.window import Window
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, current_timestamp, expr, lit, first, last,
                                   monotonically_increasing_id, to_date, sum, max, min, desc,
                                   to_timestamp)
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType, DateType, LongType)


def trade_volume_aggregator(df):
    """
    aggregate trade data
    """
    df = df.withColumn('trade_date', to_date("trade_timestamp"))
    window = Window.partitionBy('trade_date', 'symbol').orderBy('trade_date')
    df = df.withColumn('daily_volume', sum('volume').over(window))
    df = df.withColumn('daily_high', max('high').over(window))
    df = df.withColumn('daily_low', min('low').over(window))

    window_open = Window.partitionBy(
        'trade_date', 'symbol').orderBy('trade_timestamp')
    window_close = Window.partitionBy(
        'trade_date', 'symbol').orderBy(desc('trade_timestamp'))
    df = df.withColumn('daily_open', first("open").over(window_open))
    df = df.withColumn('daily_close', first("close").over(window_close))
    daily_trade = df.select("trade_date", "symbol", "daily_volume", "daily_high", "daily_low", "daily_open",
                            "daily_close").distinct().orderBy("trade_date")
    daily_trade = df.select(
        df['trade_date'].cast(DateType()),
        df['symbol'].cast(StringType()),
        df['daily_volume'].cast(LongType()).alias('volume'),
        df['daily_open'].cast(DoubleType()).alias('open'),
        df['daily_high'].cast(DoubleType()).alias('high'),
        df['daily_low'].cast(DoubleType()).alias('low'),
        df['daily_close'].cast(DoubleType()).alias('close')
    ).distinct().orderBy("trade_date")

    batch_id = current_timestamp().cast("long")
    daily_trade = daily_trade.withColumn(
        "partition", col("trade_date").cast('string'))
    daily_trade = daily_trade.withColumn("batch_id", lit(batch_id))

    return daily_trade


def stock_events_normalizer(data: dict, spark: SparkSession):
    """
    take stock events and process into df
    """
    symbol = data["Meta Data"].get('2. Symbol', 'N/A')

    # Extract time series data, 2nd element of the dict
    body = data.get(list(data.keys())[1])

    enrich_events = []
    for date, dict_value in body.items():
        dict_value['symbol'] = dict_value.get('symbol', symbol)
        dict_value['trade_timestamp'] = dict_value.get('trade_timestamp', date)
        enrich_events.append(dict_value)

    df_raw = spark.createDataFrame(enrich_events)
    # type casting
    df = df_raw.select(
        df_raw["trade_timestamp"].cast(
            TimestampType()).alias("trade_timestamp"),
        df_raw["symbol"].cast(StringType()).alias("symbol"),
        df_raw["`1. open`"].cast(DoubleType()).alias("open"),
        df_raw["`2. high`"].cast(DoubleType()).alias("high"),
        df_raw["`3. low`"].cast(DoubleType()).alias("low"),
        df_raw["`4. close`"].cast(DoubleType()).alias("close"),
        df_raw["`5. volume`"].cast(IntegerType()).alias("volume")
    )

    batch_id = current_timestamp().cast("long")
    df = df.withColumn("partition", to_date("trade_timestamp").cast("string"))
    df = df.withColumn("batch_id", lit(batch_id))
    # Show the DataFrame for testing
    # df.show()
    return df
