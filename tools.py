import pandas as pd
# import numpy as np
# import pandas_datareader.data as web
import time
# import datetime
import matplotlib.pyplot as plt
import requests


class Cryptocompare:
    def __init__(self):
        self.base_url = 'https://min-api.cryptocompare.com/data/'
        self.base_url_old = 'https://www.cryptocompare.com/api/data/'

    def coin_list(self):
        url = '{}coinlist'.format(self.base_url_old)
        r = requests.get(url)

        return r.json()['Data']

    def daily(self,f_currency, t_currency, ts, n):
        url = '{}histoday'.format(self.base_url)
        # pattern = '%Y-%m-%d'
        # toTs = int(time.mktime(time.strptime(date, pattern)))
        payload = {'fsym': f_currency,
                   'tsym': t_currency,
                   'limit': n - 1,
                   'toTs': ts
                   }
        r = requests.get(url, params=payload)

        return r.json()['Data']

    def hourly(self, f_currency, t_currency, ts, n):
        url = '{}histohour'.format(self.base_url)
        payload = {'fsym': f_currency,
                   'tsym': t_currency,
                   'limit': n - 1,
                   'toTs': ts
                  }
        r = requests.get(url, params=payload)

        return r.json()['Data']


def get_asset(symbol, date, n):
    url = 'https://min-api.cryptocompare.com/data/histoday'
    pattern = '%Y-%m-%d'
    toTs = int(time.mktime(time.strptime(date, pattern)))
    payload = {'fsym': symbol,
               'tsym': 'USD',
               'limit': n - 1,
               'toTs': toTs
               }
    r = requests.get(url, params=payload)

    return r.json()['Data']


# def get_asset(asset, start, end):
#     return web.DataReader(asset, 'yahoo', start, end)['Close']


def sma(df, column="close", period=20):
    sma = df[column].rolling(window=period, min_periods=period - 1).mean()
    sma.name = 'SMA_{}'.format(period)
    return sma


def ema(df, column='close', period=20):
    ema = df[column].ewm(span=period, min_periods=period - 1).mean()
    ema.name = 'EMA_{}'.format(period)
    return ema


def rsi(df, column="close", period=14):
    # wilder's RSI
    delta = df[column].diff()
    up, down = delta.copy(), delta.copy()
    up[up < 0] = 0
    down[down > 0] = 0
    r_up = up.ewm(com=period - 1,  adjust=False).mean()
    r_down = down.ewm(com=period - 1, adjust=False).mean().abs()
    rsi = 100 - 100 / (1 + r_up / r_down)
    rsi.name = 'RSI_{}'.format(period)
    print(rsi)
    return rsi


# data = get_asset('BTC', '2018-02-19', 100)
# # output = 'Date: {} | Close: {:.2f} | High: {:.2f} | Low: {:.2f} \
# #           | Open: {:.2f}'
# # for item in data:
# #     date = datetime.datetime.fromtimestamp(item['time']).strftime('%Y-%m-%d')
# #     p = output.format(date, item['close'], item['high'], item['low'],
# #                       item['open'])
# #     print(p)
# 
# df = pd.DataFrame(data)
# # df = df.join(sma(df))
# # plt.plot(df['SMA_20'])
# # plt.plot(df['close'])
# rsi = rsi(df)
# plt.plot(rsi)
# plt.show()
