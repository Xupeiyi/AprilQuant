import unittest
import pandas as pd
from backtest.signals import (
    add_chg_signal,
    add_position_direction,
    add_avg_daily_last_adjusted_close
)


class AddChgSignalTest(unittest.TestCase):

    def test_a_normal_case(self):
        df = pd.read_csv('./signals_data/test_add_chg_signal.csv', parse_dates=['datetime'])
        add_chg_signal(df)
        self.assertListEqual(df['ans_next_c_chg'].tolist(), df['next_c_chg'].tolist())
        self.assertListEqual(df['ans_c_chg'].tolist(), df['c_chg'].tolist())


class AddPositionDirectionTest(unittest.TestCase):

    def test_a_normal_case(self):
        df = pd.read_csv('./signals_data/test_add_position_direction.csv', parse_dates=['datetime'])
        add_position_direction(df)
        self.assertListEqual(df['ans_position_direction'].tolist(), df['position_direction'].tolist())


class AddAvgDailyLastAdjustedClose(unittest.TestCase):

    def test_a_normal_case(self):
        df = pd.read_csv('./signals_data/test_add_avg_daily_last_adjusted_close.csv', parse_dates=['tradingday'])
        add_avg_daily_last_adjusted_close(df, 3)
        self.assertListEqual(df['ans'].fillna(-1).tolist(), df['avg_adjusted_close'].fillna(-1).tolist())


if __name__ == '__main__':
    unittest.main()
