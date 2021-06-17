import unittest

import pandas as pd
import matplotlib.pyplot as plt

from strategies.C75.signals import C75Tester


class C74TesterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        df = pd.read_csv('DQA0001.csv', parse_dates=['datetime'])
        df[['adjusted_open', 'adjusted_high', 'adjusted_low', 'adjusted_close']] = df[['open', 'high', 'low', 'close']]
        df['preclose'] = df['close'].shift(1).fillna(df.close.iloc[0])
        df[['next_c_chg', 'c_chg']] = 0, 0
        df['tradingday'] = df.datetime.apply(lambda s: s.date())
        params = dict(date_length=10, n1=25, n2=10, trs=0.12, lqk_width=0.1, lqk_floor=0.5)
        tester = C75Tester(params=params, df=df)
        res = tester.test()
        cls.tester = tester
        cls.df = tester.df
        cls.res = res
        # cls.df.to_csv('details.csv', index=False)

    @staticmethod
    def get_datetime_strings(signal):
        return [t.strftime('%Y-%m-%d %H:%M') for t in signal.datetime.tolist()]

    def test_long_enter_signals_are_the_same(self):
        df = self.df
        long_enter = df[df.longgo & (df.position_direction > 0) & (df.position_direction.shift(1).fillna(0) <= 0)]
        long_enter_dates = self.get_datetime_strings(long_enter)
        print(long_enter_dates)
        self.assertListEqual(long_enter_dates, ['2020-12-21 10:38', '2020-12-24 14:01'])

    def test_long_exit_signals_are_the_same(self):
        df = self.df
        long_exit = df[(df.position_direction <= 0) & (df.position_direction.shift(1) > 0)]
        long_exit_dates = self.get_datetime_strings(long_exit)
        print(long_exit_dates)
        self.assertListEqual(long_exit_dates, ['2020-12-21 13:50'])

    def test_short_enter_signals_are_the_same(self):
        df = self.df
        short_enter = df[df.shortgo & (df.position_direction < 0) & (df.position_direction.shift(1).fillna(0) >= 0)]
        short_enter_dates = self.get_datetime_strings(short_enter)
        print(short_enter_dates)
        self.assertListEqual(short_enter_dates, ['2020-12-15 09:48', '2020-12-21 13:50'])

    def test_short_exit_signals_are_the_same(self):
        df = self.df
        short_exit = df[df.short_exit & (df.position_direction.shift(1) < 0) & (df.position_direction == 0)]
        short_exit_dates = self.get_datetime_strings(short_exit)
        print(short_exit_dates)
        self.assertListEqual(short_exit_dates, ['2020-12-21 10:36', '2020-12-22 09:02'])

    def test_cum_ret(self):
        cum_ret = pd.DataFrame(self.res['cum_ret']).set_index('datetime').plot()
        plt.show()