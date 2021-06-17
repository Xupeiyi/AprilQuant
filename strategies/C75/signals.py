import pandas as pd


from backtest.signals import add_chandelier_exit_signal, add_position_direction, add_avg_daily_last_adjusted_close
from backtest.indicators import HHV, LLV, SMA, BARSLAST

from backtest.tester import Tester


def add_enter_signal(df, date_length=10, n1=75, n2=30):
    """
    混沌操作法。
    """

    add_avg_daily_last_adjusted_close(df, date_length)

    high = df.adjusted_high
    low = df.adjusted_low
    close = df.adjusted_close

    hl = 0.5 * (high + low)

    s1 = SMA(hl.shift(n1 + n2), n1 + n2 + n2, 1)
    s2 = SMA(hl.shift(n2), n1 + n2, 1)
    s3 = SMA(hl.shift(n1), n2, 1)

    hh = HHV(high, 5)
    ll = LLV(low, 5)

    # h_barsrat = BARSLAST(high.shift(1) == hh) + 1
    # l_barsrat = BARSLAST(low.shift(1) == ll) + 1
    p_max = pd.concat([s1, s2, s3], axis=1).max(axis=1)
    p_min = pd.concat([s1, s2, s3], axis=1).min(axis=1)
    p_diff_pcnt = (p_max - p_min)/p_min

    dk_cond = high >= p_max
    kk_cond = low <= p_min

    df['s1'] = s1
    df['s2'] = s2
    df['s3'] = s3
    df['p_max'] = p_max
    df['p_min'] = p_min
    df['dk_cond'] = dk_cond
    df['kk_cond'] = kk_cond
    df['p_diff_pcnt'] = p_diff_pcnt

    df['longgo'] = (
        (close > df.avg_adjusted_close)
        & dk_cond
        & (close > hh.shift(1))
        & (p_diff_pcnt > 0.005)
    ).shift(1).fillna(False)

    df['shortgo'] = (
        (close < df.avg_adjusted_close)
        & kk_cond
        & (close < ll.shift(1))
        & (p_diff_pcnt > 0.005)
    ).shift(1).fillna(False)

    df['long_exit'] = (
        (close < df.avg_adjusted_close)
        & kk_cond
        & (close < ll.shift(1))
    ).shift(1).fillna(False)

    df['short_exit'] = (
        (close > df.avg_adjusted_close)
        & dk_cond
        & (close > hh.shift(1))
    ).shift(1).fillna(False)


class C75Tester(Tester):

    def add_signals(self):
        params = self.params
        add_enter_signal(self.df, date_length=params['date_length'], n1=params['n1'], n2=params['n2'])
        add_chandelier_exit_signal(self.df,
                                   trs=params['trs'],
                                   lqk_width=params['lqk_width'],
                                   lqk_floor=params['lqk_floor'])
        add_position_direction(self.df)
