import unittest
import pandas as pd
from backtest import run_pd_backtest


class RunPdBacktestTest(unittest.TestCase):
    COMMISSION = 0.01

    def read_csv(self, file_name):
        df = pd.read_csv(f'./run_pd_backtest_data/{file_name}', parse_dates=['datetime'])
        df[['longgo', 'long_exit', 'shortgo', 'short_exit', 'next_c_chg', 'c_chg']] =\
            df[['longgo', 'long_exit', 'shortgo', 'short_exit', 'next_c_chg', 'c_chg']].astype('bool')
        return df

    def test_chg_between_longgo_long_exit(self):
        df = self.read_csv('chg_between_longgo_long_exit.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_chg_between_shortgo_short_exit(self):
        df = self.read_csv('chg_between_shortgo_short_exit.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_next_c_chg_on_longgo(self):
        df = self.read_csv('next_c_chg_on_longgo.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_next_c_chg_on_shortgo(self):
        df = self.read_csv('next_c_chg_on_shortgo.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_next_c_chg_before_long_exit(self):
        df = self.read_csv('next_c_chg_before_long_exit.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_next_c_chg_before_short_exit(self):
        df = self.read_csv('next_c_chg_before_short_exit.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_c_chg_on_longgo(self):
        df = self.read_csv('c_chg_on_longgo.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_c_chg_on_shortgo(self):
        df = self.read_csv('c_chg_on_shortgo.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_c_chg_before_long_exit(self):
        df = self.read_csv('c_chg_before_long_exit.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_c_chg_before_short_exit(self):
        df = self.read_csv('c_chg_before_short_exit.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True).reset_index()
        trade_num_diff = abs(res['trade_num'] - df['trade_num']).sum()
        ret_diff = abs(res['ret'] - df['ret']).sum()
        cum_ret_diff = abs(res['cum_ret'] - df['cum_ret']).sum()
        self.assertEqual(trade_num_diff, 0)
        self.assertAlmostEqual(ret_diff, 0)
        self.assertAlmostEqual(cum_ret_diff, 0)

    def test_transition_cost_is_not_counted_repeatedly(self):
        df = self.read_csv('transition_cost_is_not_counted_repeatedly.csv')
        res = run_pd_backtest(df, commission=self.COMMISSION, debug=True)
        df = df.set_index('datetime')
        deviate = abs(df['trade_num'] - res['trade_num']).sum()
        self.assertAlmostEqual(deviate, 0)