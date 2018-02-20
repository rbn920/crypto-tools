import pandas as pd
import numpy as np
import pandas_datareader.data as web
import time
import datetime
import matplotlib.pyplot as plt
import requests


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


def sma(df, column="Close", period=20):
    sma = df[column].rolling(window=period, min_periods=period - 1).mean()
    return df.join(sma.to_frame('SMA'))


def ema(df, column='Close', period=20):
    ema = df[column].ewm(span=period, min_periods=period - 1).mean()
    return df.join(ema.to_frame('EMA'))


def rsi(df, column="Close", period=14):
    # wilder's RSI

    delta = df[column].diff()
    up, down = delta.copy(), delta.copy()

    up[up < 0] = 0
    down[down > 0] = 0

    r_up = up.ewm(com=period - 1,  adjust=False).mean()
    r_down = down.ewm(com=period - 1, adjust=False).mean().abs()

    rsi = 100 - 100 / (1 + r_up / r_down)
    print(rsi)

    return df.join(rsi.to_frame('RSI'))


# def rsi(series, period):
#     delta = series.diff().dropna()
#     u = delta * 0
#     d = u.copy()
#     u[delta > 0] = delta[delta > 0]
#     d[delta < 0] = -delta[delta < 0]
#     u[u.index[period-1]] = np.mean(u[:period])  # first value is sum of avg gains
#     u = u.drop(u.index[:(period-1)])
#     d[d.index[period-1]] = np.mean(d[:period])  # first value is sum of avg losses
#     d = d.drop(d.index[:(period-1)])
#     rs = (pd.stats.moments.ewma(u, com=period-1, adjust=False) /
#           pd.stats.moments.ewma(d, com=period-1, adjust=False))
#
#     return 100 - 100 / (1 + rs)


# start = datetime.datetime(2016, 1, 1)
# end = datetime.datetime(2016, 12, 31)
# df = pd.DataFrame(get_asset('FB', start, end))
# rsi(df)
# print(df.head())
# print(rsi(df['Close'], 14))
# df = df.assign(RSI=rsi(df['Close'], 15))
# df.plot(y=['RSI'])
# plt.show()
# df['RSI'] = RSI(df['Close'], 14)
# df.tail()
# df.plot(y=['Close'])
# df.plot(y=['RSI'])

data = get_asset('BTC', '2016-01-02', 10)
output = 'Date: {} | Close: {:.2f} | High: {:.2f} | Low: {:.2f} | Open: {:.2f}'
for item in data:
    date = datetime.datetime.fromtimestamp(item['time']).strftime('%Y-%m-%d')
    p = output.format(date, item['close'], item['high'], item['low'],
                      item['open'])
    print(p)

df = pd.DataFrame(data)
print(df)
