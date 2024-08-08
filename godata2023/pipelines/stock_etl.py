from api_utils.api_factory import APIHandler
from etl_utils import (deduplication, ingest_data_to_delta,setup_spark_session,
                       read_data_from_delta, run_data_validations)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, current_timestamp, expr, lit,
                                   monotonically_increasing_id, to_date,
                                   to_timestamp)
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)
from transform import stock_events_normalizer, trade_volume_aggregator

nvd_params = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'NVDA',
    'interval': '1min',
    'outputsize': 'compact',
}

company_params = [
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'NVDA',
    'interval': '1min',
    'outputsize': 'compact',
    },
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'TSLA',
    'interval': '1min',
    'outputsize': 'compact',
    },
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'IBM',
    'interval': '1min',
    'outputsize': 'compact',
    },
    {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'AAPL',
    'interval': '1min',
    'outputsize': 'compact',
    },
]

# entry point for calling stock-etl during deployment
if __name__ == '__main__':
    spark = setup_spark_session()
    # Extract -> To Bronze (data lake staging zone)
    for param in company_params:
        trade_api = APIHandler(request_params=param)
        api_endpoint = trade_api.get_endpoint()
        data = trade_api.request_data(api_endpoint)
        df = stock_events_normalizer(data, spark)
        ingest_data_to_delta(df, 'trade')

    #Transform -> Daily Agg
    df = read_data_from_delta('trade', spark)
    daily_trade=trade_volume_aggregator(df)
    # daily_trade.show()

    #Load -> To Silver
    ingest_data_to_delta(daily_trade, 'agg_trade')

