import csv
import json
from datetime import datetime

import pandas as pd
import requests
from api_factory import APIHandler

base_url = 'https://www.alphavantage.co/query?'

apikey = ''  # access through home dir


params = {
    'function': 'TIME_SERIES_DAILY',
    'symbol': 'SHOP.TRT',
    'outputsize': 'full',
}

nvd_params = {
    'function': 'TIME_SERIES_INTRADAY',
    'symbol': 'NVDA',
    'interval': '1min',
    'outputsize': 'compact',
}


def api_request(base_url, param_dict, apikey):
    url = f'{base_url}function={param_dict.get("function")}&symbol={param_dict.get("symbol")}&interval={param_dict.get("interval")}&outputsize={param_dict.get("outputsize")}&apikey={apikey}'
    r = requests.get(url)
    data = r.json()
    return data


def normalize_events_to_csv(data):
    symbol = data["Meta Data"].get('2. Symbol', 'N/A')

    # Extract time series data, 2nd element of the dict
    body = data.get(list(data.keys())[1])

    enrich_events = []
    for date, dict_value in body.items():
        dict_value['symbol'] = dict_value.get('symbol', symbol)
        dict_value['trade_timestamp'] = dict_value.get('trade_timestamp', date)
        enrich_events.append(dict_value)
    print(f'writing {len(enrich_events)} trade records to csv...')

    file_creation_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    file_path = f'/opt/spark/work-dir/godata2023/trade_{file_creation_time}.csv' # run in docker
    # file_path = f'godata2023/trade_{file_creation_time}.csv' # run in local

    with open(file_path, "w") as csvfile:
        # creating a csv dict writer object
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "trade_timestamp",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
            ],
        )

        # writing headers (field names)
        writer.writeheader()

        # writing data rows
        for event in enrich_events:
            writer.writerow(
                {
                    "trade_timestamp": event["trade_timestamp"],
                    "symbol": event["symbol"],
                    "open": event["1. open"],
                    "high": event["2. high"],
                    "low": event["3. low"],
                    "close": event["4. close"],
                    "volume": event["5. volume"],
                }
            )


data = api_request(base_url, nvd_params, apikey)
normalize_events_to_csv(data)
