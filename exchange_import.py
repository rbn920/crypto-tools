import pandas as pd
import numpy as np
import datetime as dt
import sqlite3


df = pd.read_excel(r'transaction_history.xlsx')
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
out_frame.head()

db = sqlite3.connect('data/crypto.db')
out_frame.to_sql('transactions', db, if_exists='append', index=False)
