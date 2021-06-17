import unittest
import pandas as pd
from backtest.indicators import AllNaError, HHV, LLV, BARSLAST, CCI


class RecentHighTest(unittest.TestCase):

    def test_a_normal_case(self):
        df = pd.read_csv('./indicators_data/test_cal_recent_high.csv')
        res = HHV(df.close, 3)
        self.assertListEqual(res.fillna(0).tolist(), df.ans.fillna(0).tolist())

    def test_input_cannot_be_shorter_than_length(self):
        df = pd.DataFrame(data={'close': [1]})
        self.assertRaises(AllNaError, HHV, df.close, 3)


class RecentLowTest(unittest.TestCase):

    def test_a_normal_case(self):
        df = pd.read_csv('./indicators_data/test_cal_recent_low.csv')
        res = LLV(df.close, 3)
        self.assertListEqual(res.fillna(0).tolist(), df.ans.fillna(0).tolist())

    def test_input_cannot_be_shorter_than_length(self):
        df = pd.DataFrame(data={'close': [1, 2, 3, 4, 5]})
        self.assertRaises(AllNaError, LLV, df.close, 6)


class CCITest(unittest.TestCase):

    def test_input_cannot_be_shorter_than_length(self):
        df = pd.DataFrame(data={'high': [1, 2, 3, 4, 5, 6, 7],
                                'low': [3, 5, 7, 9, 10, 2, 3],
                                'close': [5, 6, 7, 9, 1, 10, 5]})
        self.assertRaises(AllNaError, CCI, df.high, df.low, df.close, 20)


class BarsLastTest(unittest.TestCase):
    df = pd.read_csv('./indicators_data/test_bars_last.csv')

    def test_a_normal_case(self):
        res = BARSLAST(self.df.open > 1040)
        self.assertTrue((res == self.df.res_open_gt_1040).all())

    def test_when_no_bar_satisfies(self):
        res = BARSLAST(self.df.open > 2000)
        self.assertTrue((res == self.df.res_open_gt_2000).all())