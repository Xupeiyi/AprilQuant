import unittest
import pandas as pd
from cache.make_daily_active_near_cache import is_active, select_active_near_contract


class IsActiveTest(unittest.TestCase):

    def test_a_normal_case(self):
        df = pd.read_csv('./make_daily_active_near_cache_data/test_if_contract_is_active.csv', parse_dates=['datetime'])
        res = is_active(df, threshold=10).astype('int')
        df = df.set_index(['datetime', 'code']).join(res).reset_index()
        self.assertListEqual(df.ans.tolist(), df.active.tolist())


class SelectActiveNearContractTest(unittest.TestCase):

    def test_will_be_replaced_when_time_comes(self):
        df = pd.read_csv('./make_daily_active_near_cache_data/test_will_be_replaced_when_time_comes.csv',
                         parse_dates=['datetime'])
        selected = select_active_near_contract(df, nearest=40).fillna(-1)
        self.assertListEqual(selected.code.tolist(), [-1, -1, 2, 2, 2, 3, 3, 3, 4])

    def test_will_not_select_a_contract_twice(self):
        df = pd.read_csv('./make_daily_active_near_cache_data/test_will_not_select_a_contract_twice.csv',
                         parse_dates=['datetime'])
        selected = select_active_near_contract(df, nearest=40).fillna(-1)
        self.assertListEqual(selected.code.tolist(), [-1, -1, 1, 1, 1, 2, 2, 2, 3, 3])


if __name__ == '__main__':
    unittest.main()