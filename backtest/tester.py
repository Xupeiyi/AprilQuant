import os
from collections import defaultdict

import pandas as pd
from empyrical import sharpe_ratio

from backtest import run_pd_backtest, cum_ret_to_daily_ret
from backtest.indicators import AllNaError
from consts import CACHE_ROOT_DIR


class Tester:

    backtest_data = defaultdict(list)

    @classmethod
    def read_cache(cls, level):
        level_root_path = CACHE_ROOT_DIR + f'{level}\\'
        categories = os.listdir(level_root_path)

        for category in categories:
            category_path = level_root_path + f'{category}\\'
            csv_files = os.listdir(category_path)
            for file in csv_files:
                df = pd.read_csv(category_path + file, parse_dates=['datetime'])
                cls.backtest_data[category].append(df)

    def __init__(self, params, df=None):
        # 数据标记
        self.params = params

        if df is None:
            category = self.params['category']
            idx = self.params['idx']
            self.df = self.backtest_data[category][idx].copy()
        else:
            self.df = df.copy()

    def add_signals(self):
        raise NotImplementedError

    def test(self, commission=0.0001):
        res = dict()
        res['params'] = self.params

        try:
            self.add_signals()
        except AllNaError as e:
            res['error'] = str(e)
        else:
            # 累计收益率
            cum_ret = run_pd_backtest(self.df, commission=commission)
            res['cum_ret'] = cum_ret.reset_index().to_dict('list')

            # sharpe比率
            daily_ret = cum_ret_to_daily_ret(cum_ret)
            res['sharpe_ratio'] = sharpe_ratio(daily_ret, period='daily')

        return res
