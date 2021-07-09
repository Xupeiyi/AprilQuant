"""
==============
收益率计算与分析.
==============

净值曲线cum_ret是一个index为datetime, 列名为cum_ret的DataFrame.
计划在将来重构为Series.
cum_ret是cumulative return的缩写，这是个命名上的错误，应更改为net_value．
"""

from functools import reduce
import empyrical
import numpy as np
import pandas as pd


def cal_cost_and_ret_value(cum_ret: pd.DataFrame):
    """
    计算每期收益值及成本。注意该函数假设cum_ret在开始前的值为1。
    params:
        - cum_ret: 累计收益率曲线（未计算收益时累计收益率为cum_ret初值），字段为cum_ret。
    """
    cost = cum_ret.cum_ret.shift(1).fillna(1)
    ret_value = cum_ret.cum_ret - cost
    return pd.DataFrame(index=cum_ret.index,
                        data={'cost': cost.values, 'ret_value': ret_value.values})


def _fill0_add(x: pd.DataFrame, y: pd.DataFrame):
    return x.add(y, fill_value=0)


def cal_avg_cum_ret(cum_rets):
    """
    计算平均累计收益率。注意该函数假设cum_rets在开始前的值为1。用户应自己负责将cum_ret初值化为１。
    平均累计收益率计算方法：根据每条收益率曲线计算当期收益值与成本，用加总的收益值除以加总成本得到当期平均收益率，
                        再根据每一期平均收益率得到平均累计收益率．
    """
    cum_rets = [cum_ret for cum_ret in cum_rets if len(cum_ret) > 0]

    if not cum_rets:
        return pd.DataFrame(data={'datetime': [], 'cum_ret': []}).set_index('datetime')

    sum_c_and_r = reduce(_fill0_add, map(cal_cost_and_ret_value, cum_rets))
    sum_c_and_r['avg_ret'] = (sum_c_and_r['ret_value'] / sum_c_and_r['cost']).fillna(0)
    sum_c_and_r['cum_ret'] = (1 + sum_c_and_r['avg_ret']).cumprod()
    return sum_c_and_r[['cum_ret']]


def cal_daily_ret(cum_ret: pd.Series):
    """
    将累计收益率换算为日收益率。
    :param cum_ret: 根据回测得到的累计收益率序列。
    :return: daily_ret: 日收益率。
    """
    cum_ret_daily = pd.DataFrame(cum_ret).reset_index()
    cum_ret_daily['date'] = cum_ret_daily['datetime'].apply(lambda x: x.date())
    cum_ret_daily = cum_ret_daily.drop_duplicates('date', keep='last')
    # 计算日收益率
    cum_ret_daily['daily_ret'] = cum_ret_daily['cum_ret'] / cum_ret_daily['cum_ret'].shift(1).fillna(1) - 1
    return cum_ret_daily[['daily_ret']]


def cal_daily_sharpe(cum_ret):
    """计算日收益率的夏普比率"""
    daily_ret = cal_daily_ret(cum_ret)
    return empyrical.sharpe_ratio(daily_ret, period='daily')[0]


def cal_ret(cum_ret: pd.DataFrame):
    """
    将累计收益率转化为每一期的收益率。
    注意：累计收益率的默认初值为1。
    """
    if not len(cum_ret):
        return pd.DataFrame(data={'datetime': [], 'ret': []}).set_index('datetime')

    c_and_r = cal_cost_and_ret_value(cum_ret)
    c_and_r['ret'] = c_and_r['ret_value'] / c_and_r['cost']
    return c_and_r[['ret']]


def cum_ret_from_doc(document):
    """
    从文档中获取累计收益率.
    params:
        - document: mongodb中的文档.
    """
    data = document.get('cum_ret', {'datetime': [], 'cum_ret': []})
    cum_ret = pd.DataFrame(data).set_index('datetime')
    return cum_ret


def avg_cum_ret_from_cursor(cursor):
    """
    从指针中获取平均累计收益率.
    params:
        - cursor: 指向mongodb数据库的指针.
    """
    cum_rets = filter(lambda x: len(x) > 0, [cum_ret_from_doc(doc) for doc in cursor])
    avg_cum_ret = cal_avg_cum_ret(cum_rets)
    return avg_cum_ret


def avg_annual_ret(cum_ret):
    """计算平均年化收益率。"""
    ret = cal_ret(cum_ret)
    ann_ret = empyrical.annual_return(ret)
    return np.mean(ann_ret)


def standardize(cum_ret):
    """将收益率曲线标准化，使初始值为1。"""
    return cum_ret / cum_ret.iloc[0]
