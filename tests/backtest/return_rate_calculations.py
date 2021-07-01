import unittest

import pandas as pd

from backtest.return_rate_calculations import cal_avg_cum_ret


class CalAvgCumRetTest(unittest.TestCase):

    def test_can_take_one_input(self):
        cum_ret = pd.DataFrame(index=['2010-01-01', '2010-01-02', '2010-01-03'], data={'cum_ret': [1, 1.02, 1.04]})
        avg_cum_ret = cal_avg_cum_ret(cum_rets=[cum_ret])
        self.assertListEqual(cum_ret.cum_ret.tolist(), avg_cum_ret.cum_ret.tolist())
        self.assertListEqual(cum_ret.index.tolist(), avg_cum_ret.index.tolist())

    def test_A_B_start_together(self):
        df = pd.read_csv('./return_rate_calculations/test_avg_cum_ret/A_B_start_together.csv',
                         parse_dates=['datetime']).set_index('datetime')
        A = df[['A']].loc['2010-01-01': '2010-01-06'].rename(columns={'A': 'cum_ret'})
        B = df[['B']].loc['2010-01-01': '2010-01-09'].rename(columns={'B': 'cum_ret'})

        avg_cum_ret = cal_avg_cum_ret([A, B])
        delta = abs(df.cum_ret - avg_cum_ret.cum_ret).sum()
        self.assertAlmostEqual(delta, 0)

    def test_A_end_earlier_than_B_start(self):
        df = pd.read_csv('./return_rate_calculations/test_avg_cum_ret/A_end_earlier_than_B_start.csv',
                         parse_dates=['datetime']).set_index('datetime')
        A = df[['A']].loc['2010-01-01': '2010-01-04'].rename(columns={'A': 'cum_ret'})
        B = df[['B']].loc['2010-01-05': '2010-01-08'].rename(columns={'B': 'cum_ret'})

        avg_cum_ret = cal_avg_cum_ret([B, A])
        delta = abs(df.cum_ret - avg_cum_ret.cum_ret).sum()
        self.assertAlmostEqual(delta, 0)

    def test_A_start_earlier_than_B(self):
        df = pd.read_csv('./return_rate_calculations/test_avg_cum_ret/A_start_earlier_than_B.csv',
                         parse_dates=['datetime']).set_index('datetime')
        A = df[['A']].loc['2010-01-01': '2010-01-05'].rename(columns={'A': 'cum_ret'})
        B = df[['B']].loc['2010-01-03': '2010-01-06'].rename(columns={'B': 'cum_ret'})

        avg_cum_ret = cal_avg_cum_ret([A, B])
        delta = abs(df.cum_ret - avg_cum_ret.cum_ret).sum()
        self.assertAlmostEqual(delta, 0)

    def test_can_take_an_empty_list(self):
        res0 = cal_avg_cum_ret([])
        res1 = cal_avg_cum_ret([])
        self.assertTrue(res0.empty)
        res3 = cal_avg_cum_ret([res0, res1])
        self.assertTrue(res3.empty)

    def test_split_cum_ret_and_concat(self):
        df = pd.read_csv('./return_rate_calculations/test_avg_cum_ret/A_B_start_together.csv',
                         parse_dates=['datetime']).set_index('datetime')
        B = df[['B']].loc['2010-01-01': '2010-01-09'].rename(columns={'B': 'cum_ret'})

        splitter = '2010-01-04'
        part1, part2 = B.loc[:splitter], B.loc[splitter:]

        init = part2.values[0]
        part2 = (part2 / init).iloc[1:]
        avg_cum_ret = cal_avg_cum_ret([part1, part2])

        delta = abs(B.cum_ret - avg_cum_ret.cum_ret).sum()
        self.assertAlmostEqual(delta, 0)






