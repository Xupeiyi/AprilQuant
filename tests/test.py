import unittest

import pandas as pd

from utils import select_high_liquidity_data
from strategies.CCI.signals import ChandelierSignalAdder
from strategies.CCI.make_minute_cache import resample_minute_data, correct_preclose


class SelectHighLiquidityDataTest(unittest.TestCase):

    def test_a_normal_case(self):
        data = pd.read_csv('./test_data/high_liquidity.csv')
        res = select_high_liquidity_data(data)
        self.assertListEqual(['2010/1/2', '2010/1/3', '2010/1/4', '2010/1/5',
                              '2010/1/6', '2010/1/7', '2010/1/8', '2010/1/9'],
                             res[0].datetime.values.tolist())
        self.assertListEqual(['2010/1/25', '2010/1/26', '2010/1/27', '2010/1/28',
                              '2010/1/29', '2010/1/30', '2010/1/31', '2010/2/1', '2010/2/2'],
                             res[1].datetime.values.tolist())



class ChandelierSignalAdderTest(unittest.TestCase):

    def test_add_chg_signal(self):
        df = pd.read_csv('./test_data/test_add_chg_signal.csv', parse_dates=['datetime'])
        signal_adder = ChandelierSignalAdder(df)
        signal_adder.add_chg_signal()
        self.assertListEqual(df['ans_next_c_chg'].tolist(), signal_adder.df['next_c_chg'].tolist())
        self.assertListEqual(df['ans_c_chg'].tolist(), signal_adder.df['c_chg'].tolist())

    def test_add_position_direction(self):
        df = pd.read_csv('./test_data/test_add_position_direction.csv', parse_dates=['datetime'])
        signal_adder = ChandelierSignalAdder(df)
        signal_adder.add_position_direction()
        self.assertListEqual(df['ans_position_direction'].tolist(), signal_adder.df['position_direction'].tolist())





class ResampleMinuteDataTest(unittest.TestCase):

    def test_a_normal_case(self):
        m_data = pd.read_csv('./test_data/resample_minute_data/minute_data.csv', parse_dates=['datetime'])
        resampled = resample_minute_data(m_data, '10min')
        ans = pd.read_csv('./test_data/resample_minute_data/ans.csv', parse_dates=['datetime'])
        self.assertFalse(abs(ans - resampled).sum().any())


class CorrectPrecloseTest(unittest.TestCase):

    def test_a_normal_case(self):
        m_data = pd.read_csv('./test_data/correct_preclose/minute_data.csv')
        correct_preclose(m_data)
        ans = pd.read_csv('./test_data/correct_preclose/ans.csv')
        self.assertFalse(abs(ans - m_data).sum().any())


if __name__ == '__main__':
    unittest.main()
