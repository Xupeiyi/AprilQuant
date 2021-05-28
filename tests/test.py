import unittest

import pandas as pd

from utils import select_high_liquidity_data
from backtest import run_pd_backtest


class SelectHighLiquidityDataTest(unittest.TestCase):

    data = pd.read_csv('./test_data/high_liquidity.csv')

    def test_consistency(self):
        res = select_high_liquidity_data(self.data)
        self.assertListEqual(['2010/1/2', '2010/1/3', '2010/1/4', '2010/1/5',
                              '2010/1/6', '2010/1/7', '2010/1/8', '2010/1/9'],
                             res[0].datetime.values.tolist())
        self.assertListEqual(['2010/1/25', '2010/1/26', '2010/1/27', '2010/1/28',
                              '2010/1/29', '2010/1/30', '2010/1/31', '2010/2/1', '2010/2/2'],
                             res[1].datetime.values.tolist())


class RunPdBacktestTest(unittest.TestCase):

    COMMISSION = 0.01

    def read_csv(self, file_name):
        df = pd.read_csv(f'./test_data/pd_backtest/{file_name}', parse_dates=['datetime'])
        df[['longgo', 'long_exit', 'shortgo', 'short_exit', 'next_c_chg', 'c_chg']] =\
            df[['longgo', 'long_exit', 'shortgo', 'short_exit', 'next_c_chg', 'c_chg']].astype('bool')
        return df

    def test_chg_between_longgo_long_exit(self):
        df = self.read_csv('chg_between_longgo_long_exit.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_chg_between_shortgo_short_exit(self):
        df = self.read_csv('chg_between_shortgo_short_exit.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_next_c_chg_on_longgo(self):
        df = self.read_csv('next_c_chg_on_longgo.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_next_c_chg_on_shortgo(self):
        df = self.read_csv('next_c_chg_on_shortgo.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_next_c_chg_before_long_exit(self):
        df = self.read_csv('next_c_chg_before_long_exit.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_next_c_chg_before_short_exit(self):
        df = self.read_csv('next_c_chg_before_short_exit.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_c_chg_on_longgo(self):
        df = self.read_csv('c_chg_on_longgo.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_c_chg_on_shortgo(self):
        df = self.read_csv('c_chg_on_shortgo.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_c_chg_before_long_exit(self):
        df = self.read_csv('c_chg_before_long_exit.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)

    def test_c_chg_before_short_exit(self):
        df = self.read_csv('c_chg_before_short_exit.csv')
        df['cum_ret'] = run_pd_backtest(df, commission=self.COMMISSION).values
        deviate = abs(df['ans'] - df['cum_ret']).sum()
        self.assertAlmostEqual(deviate, 0)


if __name__ == '__main__':
    unittest.main()
