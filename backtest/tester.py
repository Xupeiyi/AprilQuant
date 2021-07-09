import os
from collections import defaultdict

import pandas as pd

from backtest.signals import AddSignalError
from backtest.pd_backtest import run_pd_backtest
from backtest.indicators import AllNaError
from backtest.return_rate_calculations import cal_daily_sharpe
from consts import read_cache


class Tester:
    """回测测试类，负责生成交易信号并执行回测"""
    backtest_data = defaultdict(list)

    @classmethod
    def read_cache(cls, level):
        """
        从cache目录加载level数据集到backtest_data属性．
        后续可将此函数拆分出去．
        """
        cls.backtest_data = read_cache(level)

    def __init__(self, params, df=None):
        self.params = params

        if df is None:
            category = self.params['category']
            idx = self.params['idx']
            self.df = self.backtest_data[category][idx].copy()
        else:
            self.df = df.copy()

    def add_signals(self):
        """添加交易信号"""
        raise NotImplementedError

    def test(self):
        """执行回测"""
        res = dict()
        res['params'] = self.params

        try:
            self.add_signals()
        except (AllNaError, AddSignalError) as e:
            res['error'] = str(e)
        else:
            cum_ret = run_pd_backtest(self.df)
            res['cum_ret'] = cum_ret.reset_index().to_dict('list')
            res['sharpe_ratio'] = cal_daily_sharpe(cum_ret)
        return res
