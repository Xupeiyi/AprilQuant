from functools import wraps
import talib
import pandas as pd


class AllNaError(Exception):
    pass


def not_full_of_na(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        res = f(*args, **kwargs)
        if res.isna().all():
            raise AllNaError(f'{f.__name__} returned a series of NA!')
        return res
    return decorated


@not_full_of_na
def EMA(s: pd.Series, timeperiod):
    alpha = 2 / (timeperiod + 1)
    return s.ewm(alpha=alpha, adjust=False).mean()


@not_full_of_na
def SMA(s, timeperiod, weight=1):
    alpha = weight / timeperiod
    return s.ewm(alpha=alpha, adjust=False).mean()


@not_full_of_na
def MA(s, timeperiod):
    return s.rolling(timeperiod).mean()


@not_full_of_na
def HHV(high: pd.Series, length: int):
    """
    计算最近（包括当前周期）的最高价。
    params:
        - high: 最高价序列。
        - length: 最近周期长度。
    """
    return high.rolling(length).max()


@not_full_of_na
def LLV(low: pd.Series, length: int):
    """
    计算最近（包括当前周期）的最低价。
    params:
        - high: 最低价序列。
        - length: 最近周期长度。
    """
    return low.rolling(length).min()


@not_full_of_na
def CCI(high, low, close, timeperiod):
    return talib.CCI(high, low, close, timeperiod)


@not_full_of_na
def BARSLAST(cond: pd.Series):
    # 将k线分组，满足条件的k线为组内第一根， 不满足条件的k线与上一根满足条件的一组
    cond_group = cond.cumsum()
    # 在每一组内生成自然数序列，即为到组内第一根k线的距离
    distance = cond_group.groupby(cond_group).cumcount()
    # 序号为0的那组中没有满足条件的k线，结果为-1
    distance.loc[cond_group == 0] = -1

    return distance


@not_full_of_na
def TR(high, low, close):
    return pd.concat((high - low,
                      abs(high - close.shift(1)),
                      abs(low - close.shift(1))), axis=1).max(axis=1)


@not_full_of_na
def AATR(high, low, close, timeperiod):
    tr = TR(high, low, close)
    return MA(tr, timeperiod)