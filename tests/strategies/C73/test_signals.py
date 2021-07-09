import unittest

import pandas as pd
import matplotlib.pyplot as plt

from strategies.C73.signals import C73Tester
C73Tester.read_cache('daily')


class C73TesterTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        df = pd.read_csv('j.csv', parse_dates=['datetime'])
        df = df[(df.datetime >= '2015-01-01') & (df.datetime <= '2020-12-31')]
        df['preclose'] = df['close'].shift(1).fillna(df.close.iloc[0])
        df['next_c_chg'] = 0
        df['c_chg'] = 0
        params = {'length': 60, 'ema_length': 150, 'trs': 0.12, 'lqk_width': 0.1, 'lqk_floor': 0.5}
        tester = C73Tester(params=params, df=df)
        tester.add_signals()
        cls.tester = tester
        cls.df = tester.df

    @staticmethod
    def get_dates(signal):
        return [t.strftime('%Y-%m-%d') for t in signal.datetime.tolist()]

    def test_long_enter_signals_are_the_same(self):
        df = self.df
        long_enter = df[df.longgo & (df.position_direction > 0) & (df.position_direction.shift(1).fillna(0) <= 0)]
        long_enter_dates = self.get_dates(long_enter)
        self.assertListEqual(long_enter_dates, ['2016-04-06', '2016-08-02', '2016-10-12', '2017-07-11', '2018-02-27',
                                                '2018-06-15', '2018-07-30', '2019-05-22', '2020-08-10', '2020-10-14'])

    def test_long_exit_signals_are_the_same(self):
        df = self.df
        long_exit = df[df.long_exit.shift(-1) & (df.position_direction.shift(-1) == 0) & (df.position_direction > 0)]
        long_exit_dates = self.get_dates(long_exit)
        self.assertListEqual(long_exit_dates, ['2016-05-06', '2016-08-31', '2016-12-05',
                                               '2017-09-18', '2018-03-08', '2018-06-26',
                                               '2018-08-30', '2019-05-31', '2020-08-21'])

    def test_short_enter_signals_are_the_same(self):
        df = self.df
        short_enter = df[df.shortgo & (df.position_direction < 0) & (df.position_direction.shift(1).fillna(0) >= 0)]
        short_enter_dates = self.get_dates(short_enter)
        print(short_enter_dates)
        self.assertListEqual(short_enter_dates, ['2015-04-09', '2017-06-05', '2018-04-13',
                                                 '2018-12-26', '2019-08-27', '2019-11-12', '2020-04-02'])

    def test_short_exit_signals_are_the_same(self):
        df = self.df
        short_exit = df[df.short_exit.shift(-1) & (df.position_direction.shift(-1) == 0) & (df.position_direction < 0)]
        short_exit_dates = self.get_dates(short_exit)
        print(short_exit_dates)
        self.assertListEqual(short_exit_dates, ['2016-01-26', '2017-06-14', '2018-04-19',
                                                '2019-01-14', '2019-09-12', '2019-11-25', '2020-05-14'])

    def test_cum_ret(self):
        res = self.tester.test()['result']
        pd.DataFrame(index=res['datetime'], data={'cum_ret': res['cum_ret']}).plot()
        plt.show()
