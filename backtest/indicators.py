"""
技术指标计算模块。
"""

from functools import wraps
import talib
import pandas as pd


class AllNaError(Exception):
    """
    自定义异常：计算结果为一列NA。
    产生异常的原因多为数据长度短于移动窗宽。
    """
    pass


def not_full_of_na(f):
    """
    检查函数是否返回了一列NA。
    应使用此装饰器装饰任何计算技术指标的函数。
    使用后可避免在回测中额外编写检查数据长度是否足够产生开仓信号的代码。
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        res = f(*args, **kwargs)
        if res.isna().all():
            raise AllNaError(f'{f.__name__} returned a series of NA!')
        return res
    return decorated


@not_full_of_na
def EMA(s: pd.Series, timeperiod):
    """
    计算指数移动平均。
    params:
        - s: 价格序列。
        - timeperiod: 移动窗口宽度。
    """
    alpha = 2 / (timeperiod + 1)
    return s.ewm(alpha=alpha, adjust=False).mean()


@not_full_of_na
def SMA(s: pd.Series, timeperiod: int, weight: int):
    """
    计算算数移动平均。
    params:
        - s: 价格序列。
        - timeperiod: 移动窗口宽度。
        - weight: 最新一期价格权重。
    """
    alpha = weight / timeperiod
    return s.ewm(alpha=alpha, adjust=False).mean()


@not_full_of_na
def MA(s: pd.Series, timeperiod: int):
    """
    计算算术移动平均。
    params:
        - s: 价格序列。
        - timeperiod: 移动窗口宽度。
    """
    return s.rolling(timeperiod).mean()


@not_full_of_na
def HHV(high: pd.Series, timeperiod: int):
    """
    计算最近（包括当前周期）的最高价。
    params:
        - high: 最高价序列。
        - timeperiod: 最近周期长度。
    """
    return high.rolling(timeperiod).max()


@not_full_of_na
def LLV(low: pd.Series, timeperiod: int):
    """
    计算最近（包括当前周期）的最低价。
    params:
        - low: 最低价序列。
        - timeperiod: 最近周期长度。
    """
    return low.rolling(timeperiod).min()


@not_full_of_na
def CCI(high, low, close, timeperiod):
    """
    计算商品通道指数。
    params:
        - high: 最高价序列
        - low: 最低价序列
        - close: 收盘价序列
        - timeperiod: 移动窗口宽度
    """
    return talib.CCI(high, low, close, timeperiod)


@not_full_of_na
def BARSLAST(cond: pd.Series):
    """
    计算到上一根满足某条件的k线的距离。
    params:
        - cond: 一列bool形序列，表示当前ｋ线是否满足某个条件。
    """
    # 将k线分组，满足条件的k线为组内第一根， 不满足条件的k线与上一根满足条件的k线一组
    # cum_sum 隐式地将 bool 序列转换为了 int 序列。
    cond_group = cond.cumsum()

    # 在每一组内生成自然数序列，即为到组内第一根k线的距离
    distance = cond_group.groupby(cond_group).cumcount()

    # 序号为0的那组中没有满足条件的k线，结果为-1
    distance.loc[cond_group == 0] = -1

    return distance


@not_full_of_na
def TR(high, low, close):
    """
    计算真实波幅。
    params:
        - high: 最高价序列
        - low: 最低价序列
        - close: 收盘价序列
    """
    preclose = close.shift(1)
    return pd.concat((high - low,
                      abs(high - preclose),
                      abs(low - preclose)), axis=1).max(axis=1)


@not_full_of_na
def ATR(high, low, close, timeperiod):
    """
    计算均幅指标。
    params:
        - high: 最高价序列
        - low: 最低价序列
        - close: 收盘价序列
        - timeperiod: 移动窗口宽度
    """
    tr = TR(high, low, close)
    return MA(tr, timeperiod)


@not_full_of_na
def momentum(close, timeperiod):
    """
    计算序列的动量效应。
    第T期的动量效应定义为第(T - timeperiod)期到第(T - 1)期的累计收益率。
    """
    # 注意：这里的时间窗口应为timeperiod + 1 而非 timeperiod,
    # 因为计算(T - timperiod)期的收益率需要用到(T - (timperiod+1))期的收盘价
    # (1 + ret_{T-timeperiod}) * ... * (1 + ret_{T - 1})
    # = (close_{T-timeperiod} / close_{T - (timeperiod + 1)}) * ... (close_{T - 1} / close_{T - 2})
    # = close_{T - 1} / close_{T - (timeperiod + 1)}
    return close.rolling(timeperiod + 1).apply(lambda price: price.iloc[-1] / price.iloc[0] - 1).shift(1)
