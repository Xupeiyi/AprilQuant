from functools import partial

import pandas as pd

from backtest.indicators import MA, EMA, TR
from backtest.signals import add_chandelier_exit_signal, add_position_direction
from backtest.tester import Tester


def add_enter_signal(df, fast_length=25, slow_length=115, macd_length=9):
    high = df.adjusted_high
    low = df.adjusted_low
    close = df.adjusted_close

    fast_ma = MA(close, fast_length)
    slow_ma = MA(close, slow_length)
    diff = EMA(close, fast_length) - EMA(close, slow_length)
    dea = EMA(diff, macd_length)
    macd = diff - dea

    tr = TR(high, low, close)
    high_diff = high - high.shift(1)
    low_diff = low.shift(1) - low

    # 如果 hd > 0 且 hd > ld 取 hd ,否则取 0
    dmp = (((high_diff > 0) & (high_diff > low_diff)) * high_diff).rolling(slow_length).sum()
    pdi = dmp * 100 / tr

    # 如果 ld > 0 且 ld > hd 取 ld, 否则取 0
    dmm = (((low_diff > 0) & (low_diff > high_diff)) * low_diff).rolling(slow_length).sum()
    mdi = dmm * 100 / tr

    adx = MA(abs(mdi - pdi)/(mdi + pdi) * 100, fast_length)
    xadx = (adx > adx.shift(1)) & (adx > 20)

    df['longgo'] = ((macd > 0) & xadx & (fast_ma > slow_ma)).shift(1).fillna(False)
    df['shortgo'] = ((macd < 0) & xadx & (fast_ma < slow_ma)).shift(1).fillna(False)


add_exit_signal = partial(add_chandelier_exit_signal, lqk_width=0, lqk_floor=1)


class C76Tester(Tester):

    def add_signals(self):
        add_enter_signal(self.df,
                         fast_length=self.params['fast_length'],
                         slow_length=self.params['slow_length'],
                         macd_length=self.params['macd_length'])
        add_exit_signal(self.df, trs=self.params['trs'])
        add_position_direction(self.df)

