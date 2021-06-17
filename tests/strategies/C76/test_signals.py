import unittest

import pandas as pd
import matplotlib.pyplot as plt

from strategies.C76.signals import C76Tester
C76Tester.read_cache('daily')


class C76TesterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        df = pd.read_csv('j.csv', parse_dates=['datetime'])
        df = df[(df.datetime >= '2015-01-01') & (df.datetime <= '2020-12-31')]
        df['preclose'] = df['close'].shift(1).fillna(df.close.iloc[0])
        df['next_c_chg'] = 0
        df['c_chg'] = 0
        params = {'fast_length': 25, 'slow_length': 115, 'macd_length': 9, 'trs': 0.045}
        tester = C76Tester(params=params, df=df)
        tester.add_signals()
        cls.tester = tester
        cls.df = tester.df

    @staticmethod
    def get_dates(signal):
        return [t.strftime('%Y-%m-%d') for t in signal.datetime.tolist()]

    def test_long_enter_signals_are_the_same(self):
        df = self.df
        long_enter = df[(df.longgo & (df.position_direction > 0) & (df.position_direction.shift(1).fillna(0) <= 0))
                        .shift(-1).fillna(False)]
        long_enter_dates = self.get_dates(long_enter)
        print(long_enter_dates)
        self.assertListEqual(long_enter_dates, ['2016-04-21', '2016-05-09', '2016-06-29', '2016-07-20', '2016-10-21',
                                                '2016-11-16', '2017-03-14', '2020-10-30', '2020-12-17'])

    def test_long_exit_signals_are_the_same(self):
        df = self.df
        long_exit = df[(df.long_exit & (df.position_direction.shift(1) > 0) & (df.position_direction == 0))
                       .shift(-1).fillna(False)]
        long_exit_dates = self.get_dates(long_exit)
        print(long_exit_dates)
        self.assertListEqual(long_exit_dates, ['2016-05-06', '2016-05-13', '2016-07-19', '2016-08-31', '2016-11-15',
                                               '2016-12-05', '2017-04-10', '2020-12-14'])

    def test_short_enter_signals_are_the_same(self):
        df = self.df
        short_enter = df[(df.shortgo & (df.position_direction < 0) & (df.position_direction.shift(1) >= 0))
                         .shift(-1).fillna(False)]
        short_enter_dates = self.get_dates(short_enter)
        print(short_enter_dates)
        self.assertListEqual(short_enter_dates, ['2015-07-29'])

    def test_short_exit_signals_are_the_same(self):
        df = self.df
        short_exit = df[(df.short_exit & (df.position_direction == 0) & (df.position_direction.shift(1) < 0))
                        .shift(-1).fillna(False)]
        short_exit_dates = self.get_dates(short_exit)
        print(short_exit_dates)
        self.assertListEqual(short_exit_dates, ['2015-12-21'])

    def test_cum_ret(self):
        res = self.tester.test()
        pd.DataFrame(res['cum_ret']).set_index('datetime').plot()
        plt.show()
