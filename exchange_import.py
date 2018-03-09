import pandas as pd
import numpy as np
import datetime as dt
from tools import Cryptocompare
import itertools
# import sqlite3


class Exchange():
    def __init__(self, file_name):
        self.df = None
        self.file_name = file_name
        # self._load_file()
        self._symbols()

    def _load_xl(self):
        fn = r'data/{}'.format(self.file_name)
        self.data = pd.read_excel(fn)

    def _load_csv(self):
        fn = r'data/{}'.format(self.file_name)
        self.data = pd.read_csv(fn)

    def _symbols(self):
        cc = Cryptocompare()
        coins = cc.coin_list()
        symbols = []
        for coin in coins:
            symbols.append(coins[coin]['Symbol'])

        symbols.append('USD')
        self.symbols = symbols

    def _find_pair(self, market):
        matches = []
        for x in self.symbols:
            if x in market and x not in matches:
                matches.append(x)

        pairs = list(itertools.combinations(matches, 2))
        for pair in pairs:
            if (pair[0] + pair[1]) == market:
                base = pair[0]
                quote = pair[1]
                return (base, quote)

            elif (pair[1] + pair[0]) == market:
                base = pair[1]
                quote = pair[0]
                return (base, quote)

        return ('Error', 'Error')

    def _timestamp(self):
        return (self.data['Date'] - dt.datetime(1970, 1, 1)).dt.total_seconds()


class Gemini(Exchange):
    def __init__(self, file_name):
        super().__init__(file_name)
        self._load_xl()
        self._read_file()
        self._format_data()

    def _read_file(self):
        keep = ['Date',
                'Type',
                'Symbol',
                'USD Amount',
                'Trading Fee (USD)',
                'BTC Amount',
                'ETH Amount']
        data = self.data[keep]
        self.data = data[:-1]

    def _format_data(self):
        self.data['timestamp'] = self._timestamp()
        self.data['type'] = np.where(self.data['Type'] == 'Credit',
                                     'deposit',
                                     np.where(self.data['Type'] == 'Debit',
                                              'withdrawl',
                                              'trade'))

        buy = []
        sell = []
        for _, row in self.data.iterrows():
            base, quote = self._find_pair(row['Symbol'])
            if row['type'] == 'deposit':
                buy.append(row['Symbol'])
                sell.append(None)

            elif row['type'] == 'withdrawl':
                buy.append(None)
                sell.append(row['Symbol'])

            else:
                if row['Type'] == 'Buy':
                    buy.append(base)
                    sell.append(quote)

                else:
                    buy.append(quote)
                    sell.append(base)

        self.data['buy_currency'] = buy
        self.data['sell_currency'] = sell

        self.data['buy_amount'] = np.where(self.data['buy_currency'] == 'USD',
                                           self.data['USD Amount'],
                                           np.where(self.data['buy_currency'] == 'BTC',
                                                    self.data['BTC Amount'],
                                                    np.where(self.data['buy_currency']
                                                             == 'ETH',
                                                             self.data['ETH Amount'],
                                                             np.nan)))

        self.data['sell_amount'] = np.where(self.data['sell_currency'] == 'USD',
                                            self.data['USD Amount'],
                                            np.where(self.data['sell_currency'] == 'BTC',
                                                     self.data['BTC Amount'],
                                                     np.where(self.data['sell_currency']
                                                              == 'ETH',
                                                              self.data['ETH Amount'],
                                                              np.nan)))

        self.data = self.data.rename(columns={'Date': 'datetime',
                                              'Trading Fee (USD)': 'fee_amount'})
        self.data['sell_amount'] = self.data['sell_amount'].abs()
        self.data['fee_amount'] = self.data['fee_amount'].abs()
        self.data['fee_currency'] = 'USD'
        self.data['exchange'] = 'gemini'
        out = ['datetime',
               'timestamp',
               'type',
               'buy_amount',
               'buy_currency',
               'sell_amount',
               'sell_currency',
               'fee_amount',
               'fee_currency',
               'exchange']

        self.out_frame = self.data[out]


class Binance(Exchange):
    def __init__(self, file_name, history='trade'):
        super().__init__(file_name)
        self.history = history
        self._load_xl()
        # if history == 'trade':
        #     self._load_xl()
        # else:
        #     self._load_csv()

        self._read_file()
        self._format_data()

    def _read_file(self):
        self.data['Date'] = pd.to_datetime(self.data['Date'])
        if 'Market' in self.data:
            self.data = self.data.rename(columns={'Market': 'Symbol'})

        else:
            self.data = self.data[self.data['Status'] != 'Cancelled']
            keep = ['Date',
                    'Coin',
                    'Amount']
            self.data = self.data[keep]

    def _format_data(self):
        self.data['timestamp'] = self._timestamp()
        self.data['type'] = self.history
        if self.history == 'trade':
            buy = []
            sell = []
            for _, row in self.data.iterrows():
                base, quote = self._find_pair(row['Symbol'])
                if row['Type'] == 'BUY':
                    buy.append(base)
                    sell.append(quote)

                else:
                    buy.append(quote)
                    sell.append(base)

            self.data['buy_currency'] = buy
            self.data['sell_currency'] = sell
            self.data['buy_amount'] = np.where(self.data['Type'] == 'BUY',
                                               self.data['Amount'],
                                               self.data['Total'])

            self.data['sell_amount'] = np.where(self.data['Type'] == 'BUY',
                                                self.data['Total'],
                                                self.data['Amount'])

            self.data = self.data.rename(columns={'Fee': 'fee_amount',
                                                  'Fee Coin': 'fee_currency'})

        elif self.history == 'deposit':
            self.data = self.data.rename(columns={'Coin': 'buy_currency',
                                                  'Amount': 'buy_amount'})
            self.data['sell_currency'] = np.nan
            self.data['sell_amount'] = np.nan
            self.data['fee_currency'] = np.nan
            self.data['fee_amount'] = np.nan

        else:
            self.data = self.data.rename(columns={'Coin': 'sell_currency',
                                                  'Amount': 'sell_amount'})
            self.data['buy_currency'] = np.nan
            self.data['buy_amount'] = np.nan
            self.data['fee_currency'] = np.nan
            self.data['fee_amount'] = np.nan

        self.data = self.data.rename(columns={'Date': 'datetime'})
        self.data['exchange'] = 'binance'
        out = ['datetime',
               'timestamp',
               'type',
               'buy_amount',
               'buy_currency',
               'sell_amount',
               'sell_currency',
               'fee_amount',
               'fee_currency',
               'exchange']

        self.out_frame = self.data[out]


class Kucoin(Exchange):
    def __init__(self, file_name, history='trade'):
        super().__init__(file_name)
        self.history = history
        self._load_csv()
        self._format_data()

    def _format_data(self):
        if self.history == 'trade':
            self.data['symbol'] = self.data['symbol'].str.replace('/', '')
            self.data['type'] = self.history
            buy = []
            sell = []
            for _, row in self.data.iterrows():
                base, quote = self._find_pair(row['symbol'])
                if row['side'] == 'buy':
                    buy.append(base)
                    sell.append(quote)

                else:
                    buy.append(quote)
                    sell.append(base)

            self.data['buy_currency'] = buy
            self.data['sell_currency'] = sell
            self.data['buy_amount'] = np.where(self.data['side'] == 'buy',
                                               self.data['amount'],
                                               self.data['cost'])

            self.data['sell_amount'] = np.where(self.data['side'] == 'buy',
                                                self.data['cost'],
                                                self.data['amount'])

        elif self.history == 'transfer':
            self.data['buy_currency'] = np.where(self.data['side'] == 'DEPOSIT',
                                                 self.data['symbol'],
                                                 np.nan)
            self.data['sell_currency'] = np.where(self.data['side'] == 'WITHDRAWAL',
                                                  self.data['symbol'],
                                                  np.nan)
            self.data['buy_amount'] = np.where(self.data['side'] == 'DEPOSIT',
                                               self.data['amount'],
                                               np.nan)
            self.data['sell_amount'] = np.where(self.data['side'] == 'WITHDRAWAL',
                                                self.data['amount'],
                                                np.nan)

            self.data['type'] = np.where(self.data['side'] == 'DEPOSIT',
                                         'deposit',
                                         'withdrawal')

        self.data = self.data.rename(columns={'fee.cost': 'fee_amount',
                                              'fee.currency': 'fee_currency'})
        self.data['exchange'] = 'kucoin'
        out = ['datetime',
               'timestamp',
               'type',
               'buy_amount',
               'buy_currency',
               'sell_amount',
               'sell_currency',
               'fee_amount',
               'fee_currency',
               'exchange']

        self.out_frame = self.data[out]


class Cryptopia(Exchange):
    pass


class Hitbtc(Exchange):
    pass


class Kraken(Exchange):
    pass


class Poloniex(Exchange):
    pass


# db = sqlite3.connect('data/crypto.db')
# out_frame.to_sql('transactions', db, if_exists='append', index=False)
