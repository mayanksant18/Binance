#REFERENCE https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md

import time
import requests
import pandas as pd
import start_http_server, Gauge from prometheus_client


class BinanceClient:
    API_URL = 'https://api.binance.com/api'

    def __init__(self):
        self.API_URL = self.API_URL
        self.prom_gauge = Gauge('absolute_delta_value',
                                'Absolute Delta Value of Price Spread', ['symbol'])

    def check_health(self):
        """Health Check"""
        uri = "/v3/ping"

        r = requests.get(self.API_URL + uri)

        if r.status_code != 200:
            raise Exception("Binance API is unreachable.")

    def get_top_symbols(self, asset, field, output=False):
        """
      1. Print the top 5 symbols with quote asset BTC and the highest volume over the last 24 hours in descending order.
      2. Print the top 5 symbols with quote asset USDT and the highest number of trades over the last 24 hours in descending order.

        """
        uri = "/v3/ticker/24hr"

        r = requests.get(self.API_URL + uri)
        df = pd.DataFrame(r.json())
        df = df[['symbol', field]]
        df = df[df.symbol.str.contains(r'(?!$){}$'.format(asset))]
        df[field] = pd.to_numeric(df[field], downcast='float', errors='coerce')
        df = df.sort_values(by=[field], ascending=False).head(5)

        if output:
            print("\n Top Symbols for %s by %s" % (asset, field))
            print(df)

        return df

    def get_notional_value(self, asset, field, output=False):
        """
       3. Using the symbols from Q1, what is the total notional value of the top 200 bids and asks currently on each order book?

        """
        uri = "/v3/depth"

        symbols = self.get_top_symbols(asset, field, output=False)
        notional_list = {}

        for s in symbols['symbol']:
            payload = {'symbol': s, 'limit': 500}
            r = requests.get(self.API_URL + uri, params=payload)
            for col in ["bids", "asks"]:
                df = pd.DataFrame(data=r.json()[col], columns=["price", "quantity"], dtype=float)
                df = df.sort_values(by=['price'], ascending=False).head(200)
                df['notional'] = df['price'] * df['quantity']
                df['notional'].sum()
                notional_list[s + '_' + col] = df['notional'].sum()

        if output:
            print("\n Total Notional value of %s by %s" % (asset, field))
            print(notional_list)

        return notional_list

    def get_price_spread(self, asset, field, output=False):
        """
       4. What is the price spread for each of the symbols from Q2?
       5. Every 10 seconds print the result of Q4 and the absolute delta from the previous value for each symbol.
        """

        uri = '/v3/ticker/bookTicker'

        symbols = self.get_top_symbols(asset, field)
        spread_list = {}

        for s in symbols['symbol']:
            payload = {'symbol': s}
            r = requests.get(self.API_URL + uri, params=payload)
            price_spread = r.json()
            spread_list[s] = float(price_spread['askPrice']) - float(price_spread['bidPrice'])

        if output:
            print("\n Price Spread for %s by %s" % (asset, field))
            print(spread_list)

        return spread_list

    def get_spread_delta(self, asset, field, output=False):

        delta = {}
        old_spread = self.get_price_spread(asset, field)
        time.sleep(10)
        new_spread = self.get_price_spread(asset, field)

        for key in old_spread:
            delta[key] = abs(old_spread[key] - new_spread[key])

        for key in delta:
            self.prom_gauge.labels(key).set(delta[key])

        if output:
            print("\n Absolute Delta for %s" % asset)
            print(delta)


if __name__ == "__main__":
    # Start up the server to expose the metrics.
    # 6. Output of Q5 accessible by querying http://localhost:8080/metrics using the Prometheus Metrics format.
    start_http_server(8080)
    client = BinanceClient()
    client.check_health()

    # To Print Details
    client.get_top_symbols('BTC', 'volume', True)
    client.get_top_symbols('USDT', 'count', True)
    client.get_notional_value('BTC', 'volume', True)
    client.get_price_spread('USDT', 'count', True)

    while True:
        client.get_spread_delta('USDT', 'count', True)
