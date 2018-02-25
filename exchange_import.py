import pandas as pd
import numpy as np
import datetime as dt
from tools import Cryptocompare
import itertools
# import sqlite3


def symbols():
    cc = Cryptocompare()
    coins = cc.coin_list()
    symbols = []
    for coin in coins:
        symbols.append(coins[coin]['Symbol'])

    return symbols


def find_pair(market, symbols):
    matches = []
    for x in symbols:
        if x in market and x not in matches:
            matches.append(x)

    pairs = list(itertools.combinations(matches, 2))
    print(matches)
    print(pairs)
    for pair in pairs:
        if (pair[0] + pair[1]) == market:
            return (pair[0], pair[1])
        elif (pair[1] + pair[0]) == market:
            return (pair[1], pair[0])

    return ('error', 'error')


def gemini():
    df = pd.read_excel(r'data/transaction_history.xlsx')
    keep = ['Date',
            'Type',
            'Symbol',
            'USD Amount',
            'Trading Fee (USD)',
            'BTC Amount',
            'ETH Amount'
            ]
    df = df[keep]
    df = df[:-1]
    df['timestamp'] = (df['Date'] - dt.datetime(1970, 1, 1)).dt.total_seconds()
    df['type'] = np.where(df['Type'] == 'Credit',
                          'deposit',
                          np.where(df['Type'] == 'Debit',
                                   'withdrawl',
                                   'trade'))

    buy = []
    sell = []
    for _, row in df.iterrows():
        if row['type'] == 'deposit':
            buy.append(row['Symbol'])
            sell.append(np.nan)

        elif row['type'] == 'withdrawl':
            buy.append(np.nan)
            sell.append(row['Symbol'])

        else:
            if row['Type'] == 'Buy':
                buy.append(row['Symbol'][:3])
                sell.append(row['Symbol'][-3:])

            else:
                buy.append(row['Symbol'][-3:])
                sell.append(row['Symbol'][:3])

    df['buy_currency'] = buy
    df['sell_currency'] = sell

    df['buy_amount'] = np.where(df['buy_currency'] == 'USD',
                                df['USD Amount'],
                                np.where(df['buy_currency'] == 'BTC',
                                         df['BTC Amount'],
                                         np.where(df['buy_currency'] == 'ETH',
                                                  df['ETH Amount'],
                                                  np.nan)))

    df['sell_amount'] = np.where(df['sell_currency'] == 'USD',
                                 df['USD Amount'],
                                 np.where(df['sell_currency'] == 'BTC',
                                          df['BTC Amount'],
                                          np.where(df['sell_currency'] == 'ETH',
                                                   df['ETH Amount'],
                                                   np.nan)))

    df = df.rename(columns={'Date': 'datetime', 'Trading Fee (USD)': 'fee_amount'})
    df['sell_amount'] = df['sell_amount'].abs()
    df['fee_amount'] = df['fee_amount'].abs()
    df['fee_currency'] = 'USD'
    df['exchange'] = 'gemini'

    out = ['datetime',
           'timestamp',
           'type',
           'buy_amount',
           'buy_currency',
           'sell_amount',
           'sell_currency',
           'fee_amount',
           'fee_currency',
           'exchange'
           ]

    out_frame = df[out]
    # out_frame = out_frame.where(df.notnull(), None)

    return out_frame


def binance():
    df = pd.read_excel(r'data/TradeHistory.xlsx')
    df['timestamp'] = (df['Date'] - dt.datetime(1970, 1, 1)).dt.total_seconds()
    df['type'] = 'trade'
    buy = []
    sell = []
    for _, row in df.iterrows():
        if row['Type'] == 'Buy':
            buy.append(row['Market'][:3])
            sell.append(row['Market'][-3:])

        else:
            buy.append(row['Market'][-3:])
            sell.append(row['Market'][:3])

    df['buy_currency'] = buy
    df['sell_currency'] = sell

    # df['buy_amount'] =
    # df['sell_amount'] =
    df = df.rename(columns={'Date': 'datetime', 'Fee': 'fee_amount', 'Fee Coin':
                            'fee_currency'})
    df['exchange'] = 'binance'

    out = ['datetime',
           'timestamp',
           'type',
           'buy_amount',
           'buy_currency',
           'sell_amount',
           'sell_currency',
           'fee_amount',
           'fee_currency',
           'exchange'
           ]

    out_frame = df[out]
    # out_frame = out_frame.where(df.notnull(), None)

    return out_frame


# db = sqlite3.connect('data/crypto.db')
# out_frame.to_sql('transactions', db, if_exists='append', index=False)
