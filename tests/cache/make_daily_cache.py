import unittest
import pandas as pd

from cache.make_daily_cache import select_high_liquidity_data


class SelectHighLiquidityDataTest(unittest.TestCase):

    def test_a_normal_case(self):
        data = pd.read_csv('./make_daily_cache_data/test_select_high_liquidity_data.csv')
        res = select_high_liquidity_data(data)
        self.assertListEqual(['2010/1/2', '2010/1/3', '2010/1/4', '2010/1/5',
                              '2010/1/6', '2010/1/7', '2010/1/8', '2010/1/9'],
                             res[0].datetime.values.tolist())
        self.assertListEqual(['2010/1/25', '2010/1/26', '2010/1/27', '2010/1/28',
                              '2010/1/29', '2010/1/30', '2010/1/31', '2010/2/1', '2010/2/2'],
                             res[1].datetime.values.tolist())
