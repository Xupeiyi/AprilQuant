import pandas as pd
import talib

from backtest.signals import add_chandelier_exit_signal, add_position_direction
from backtest.indicators import HHV, LLV, EMA, CCI

from backtest.tester import Tester


def add_enter_signal(df, length=60, ema_length=150):
    """
    在df中标记开仓日期。非纯函数。开仓信号使用复权价格计算。
    params:
        - df: 一段连续的行情数据。需要行情中包括前复权最高价adjusted_high,
              前复权最低价adjusted_low, 前复权收盘价adjusted_close。
        - length: 计算recent_high, recent_low, cci 所需窗宽。
        - ema_length: 计算cci指数移动平均所需窗宽。
    """

    # 开仓信号指标使用复权价格生成。
    df['recent_high'] = HHV(df.adjusted_high, length)
    df['recent_low'] = LLV(df.adjusted_low, length)
    df['avg_high_low'] = (df.adjusted_high + df.adjusted_low) * 0.5
    df['cci'] = CCI(df.adjusted_high, df.adjusted_low, df.adjusted_close, length)
    df['cci_ema'] = EMA(df.cci, timeperiod=ema_length)

    # 标记开仓日期longgo, shortgo。开仓日期为实际发生交易的日期，是计算出信号的后一天
    df['longgo'] = (
            (df.adjusted_close > df.recent_high.shift(1))
            & (df.cci_ema > 0)
            & (df.avg_high_low > df.adjusted_high.shift(1))
    )
    df['longgo'] = df['longgo'].shift(1).fillna(False)

    df['shortgo'] = (
            (df.adjusted_close < df.recent_low.shift(1))
            & (df.cci_ema < 0)
            & (df.avg_high_low < df.adjusted_low.shift(1))
    )
    df['shortgo'] = df['shortgo'].shift(1).fillna(False)


class C73Tester(Tester):

    def add_signals(self):
        params = self.params
        add_enter_signal(self.df, length=params['length'], ema_length=params['ema_length'])
        add_chandelier_exit_signal(self.df,
                                   trs=params['trs'],
                                   lqk_width=params['lqk_width'],
                                   lqk_floor=params['lqk_floor'])
        add_position_direction(self.df)

