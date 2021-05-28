import numpy as np
import pandas as pd
import talib


def cal_recent_high(high: pd.Series, length: int):
    """
    计算最近（不包括当前周期）的最高价。
    params:
        - high: 最高价序列。
        - length: 最近周期长度。
    """
    return high.rolling(length).max().shift(1)


def cal_recent_low(low: pd.Series, length: int):
    """
    计算最近（不包括当前周期）的最低价。
    params:
        - high: 最低价序列。
        - length: 最近周期长度。
    """
    return low.rolling(length).min().shift(1)


def cal_cci(high: pd.Series, low: pd.Series, close: pd.Series, length: int):
    """计算CCI的指数移动平均"""
    tp = (high + low + close) / 3.0
    tpmean = tp.rolling(length).sum() / length
    dev = tp - tpmean
    meandev = abs(dev).rolling(length).sum() / length
    cci = dev / (0.015 * meandev)
    return cci


class ChandelierSignalAdder:

    def __init__(self, df):
        self.df = df.copy()

    def has_column(self, *columns):
        return set(columns) <= set(self.df.columns)

    def add_chg_signal(self):
        """
        在df中添加更换合约标记。非纯函数。
        params:
            - df: 一段连续的行情数据。需要行情中包括合约交割月份delivery_month。
        """

        self.df['next_c_chg'] = (self.df.deliv_mon != self.df.deliv_mon.shift(-1).fillna(self.df.deliv_mon.iloc[-1]))
        self.df['c_chg'] = (self.df.deliv_mon != self.df.deliv_mon.shift(1).fillna(self.df.deliv_mon.iloc[0]))
        self.df['next_c_chg'] = self.df['next_c_chg'].astype('int')
        self.df['c_chg'] = self.df['c_chg'].astype('int')

    def cal_adjust_factor(self):
        """
        计算复权因子。
        params:
            - df: 一段连续的行情数据。需要行情中包括close和preclose。
        """
        if 0 in self.df['preclose'].values:
            raise ZeroDivisionError("preclose字段中存在0！")

        price_chg = self.df['close'] / self.df['preclose']
        adjust_factor = price_chg.cumprod()
        return adjust_factor

    def add_adjusted_price(self):
        """
        在df中添加前复权价格。非纯函数。
        params:
            - df: 一段连续的行情数据。需要行情中包括open, close, high, low, preclose。
        """
        adjust_factor = self.cal_adjust_factor()
        if 0 in adjust_factor.values:
            raise ZeroDivisionError("复权因子中存在0！")
        self.df['adjusted_close'] = adjust_factor * (self.df['close'].iloc[0] / adjust_factor.iloc[0])

        # 对 open, high, low 以同比例放缩
        if 0 in self.df['close'].values:
            raise ZeroDivisionError("close字段中存在0！")

        r = self.df['adjusted_close'] / self.df['close']
        for price in ['open', 'high', 'low']:
            self.df['adjusted_' + price] = self.df[price] * r

    def add_enter_signal(self, length=60, ema_length=150):
        """
        在df中标记开仓日期。非纯函数。开仓信号使用复权价格计算。
        params:
            - df: 一段连续的行情数据。需要行情中包括前复权最高价adjusted_high,
                  前复权最低价adjusted_low, 前复权收盘价adjusted_close。
            - length: 计算recent_high, recent_low, cci 所需窗宽。
            - ema_length: 计算cci指数移动平均所需窗宽。
        """

        if len(self.df) <= 2 * (length - 1) + ema_length - 1:
            raise ValueError('数据长度过短，不足以生成开仓信号')

        # 开仓信号指标使用复权价格生成。
        self.df['recent_high'] = cal_recent_high(self.df.adjusted_high, length)
        self.df['recent_low'] = cal_recent_low(self.df.adjusted_low, length)
        self.df['avg_high_low'] = (self.df.adjusted_high + self.df.adjusted_low) * 0.5
        self.df['cci'] = cal_cci(self.df.adjusted_high, self.df.adjusted_low, self.df.adjusted_close, length)
        self.df['cci_ema'] = talib.EMA(self.df.cci, timeperiod=ema_length)

        # 标记开仓日期longgo, shortgo。开仓日期为实际发生交易的日期，是计算出信号的后一天
        self.df['longgo'] = (
                (self.df.adjusted_close > self.df.recent_high)
                & (self.df.cci_ema > 0)
                & (self.df.avg_high_low > self.df.adjusted_high.shift(1))
        )
        self.df['longgo'] = self.df['longgo'].shift(1).fillna(False)

        self.df['shortgo'] = (
                (self.df.adjusted_close < self.df.recent_low)
                & (self.df.cci_ema < 0)
                & (self.df.avg_high_low < self.df.adjusted_low.shift(1))
        )
        self.df['shortgo'] = self.df['shortgo'].shift(1).fillna(False)

    def add_exit_signal(self, trs=0.12, lqk_width=0.1, lqk_floor=0.5):
        """
        在self.df中标记平仓日期。非纯函数。平仓信号使用复权价格计算。
        平仓信号计算方法：开多头仓位时记当期开盘价为lower_after_entry，之后在
                          持仓的每一期选取当期最低价和上一期lower_after_entry
                          中较大者为当期lower_after_entry，并计算吊灯线dliqpoint。
                          若dliqpoint低于收盘价则平多头。开空头仓位时同理。
                          止损止盈幅度乘数liqka随时间推移从1减小为0.5，每期减小0.1，
                          这使吊灯线变得更加敏感。
        params:
            - self.df: 一段连续的行情数据。需要行情中包括前复权开盘价adjusted_open,
                  最高价adjusted_high, 前复权最低价adjusted_low, 前复权收盘价
                  adjusted_close。并标注开仓时刻longgo和shortgo。
            - trs: 用于调整吊灯线与价格之间的距离。trs越大则吊灯线越不敏感。
        """

        position_direction = 0  # 当前头寸方向。1为多头，-1为空头，0为无头寸。

        self.df['higher_after_entry'] = 0  # 开仓以来最高价
        self.df['lower_after_entry'] = 0  # 开仓以来最低价
        self.df['dliqpoint'] = 0  # 多头吊灯线
        self.df['kliqpoint'] = 0  # 空头吊灯线
        self.df['liqka'] = 1  # 止盈止损幅度乘数
        self.df['long_exit'] = False  # 多头平仓日期
        self.df['short_exit'] = False  # 空头平仓日期

        for i in range(len(self.df)-1):

            # 开多头
            if self.df.longgo.iloc[i] and position_direction <= 0:
                position_direction = 1
                self.df.lower_after_entry.iloc[i] = self.df.adjusted_open.iloc[i]
                continue

            # 开空头
            if self.df.shortgo.iloc[i] and position_direction >= 0:
                position_direction = -1
                self.df.higher_after_entry.iloc[i] = self.df.adjusted_open.iloc[i]
                continue

            # 平多头
            if self.df.long_exit.iloc[i] and position_direction > 0:
                position_direction = 0
                continue

            # 平空头
            if self.df.short_exit.iloc[i] and position_direction < 0:
                position_direction = 0
                continue

            # 有持仓时计算吊灯线
            if position_direction != 0:
                self.df.liqka.iloc[i] = max(self.df.liqka.iloc[i - 1] - lqk_width, lqk_floor)
                delta = self.df.adjusted_open.iloc[i] * trs * self.df.liqka.iloc[i]

                if position_direction > 0:
                    self.df.lower_after_entry.iloc[i] = max(self.df.adjusted_low.iloc[i],
                                                            self.df.lower_after_entry.iloc[i - 1])
                    self.df.dliqpoint.iloc[i] = self.df.lower_after_entry.iloc[i] - delta

                    # 平仓日期是计算出平仓信号的后一天
                    if self.df.adjusted_close.iloc[i] < self.df.dliqpoint.iloc[i]:
                        self.df.long_exit.iloc[i + 1] = True
                else:
                    self.df.higher_after_entry.iloc[i] = min(self.df.adjusted_high.iloc[i],
                                                             self.df.higher_after_entry.iloc[i - 1])
                    self.df.kliqpoint.iloc[i] = self.df.higher_after_entry.iloc[i] + delta

                    # 平仓日期是计算出平仓信号的后一天
                    if self.df.adjusted_close.iloc[i] > self.df.kliqpoint.iloc[i]:
                        self.df.short_exit.iloc[i + 1] = True

    def add_position_direction(self):
        """
        在self.df中标记开仓方向。
        """
        self.df['position_direction'] = 0
        for i in range(len(self.df)):

            # 开多头
            if self.df.longgo.iloc[i] and self.df.position_direction.iloc[i - 1] <= 0:
                self.df.position_direction.iloc[i] = 1
                continue

            # 开空头
            if self.df.shortgo.iloc[i] and self.df.position_direction.iloc[i - 1] >= 0:
                self.df.position_direction.iloc[i] = -1
                continue

            # 平多头
            if self.df.long_exit.iloc[i] and self.df.position_direction.iloc[i - 1] > 0:
                self.df.position_direction.iloc[i] = 0
                continue

            # 平空头
            if self.df.short_exit.iloc[i] and self.df.position_direction.iloc[i - 1] < 0:
                self.df.position_direction.iloc[i] = 0
                continue

            self.df.position_direction.iloc[i] = self.df.position_direction.iloc[i - 1]
