from functools import reduce
import empyrical
import numpy as np
import pandas as pd


def cal_cost_and_ret_value(cum_ret: pd.DataFrame):
    """
    计算每期收益值及成本。注意该函数假设cum_ret在开始前的值为1。用户应自己负责将cum_ret归一化和再平衡的问题。
    params:
        - cum_ret: 累计收益率曲线（未计算收益时累计收益率为cum_ret初值），字段为cum_ret。
    """
    cost = cum_ret.cum_ret.shift(1).fillna(1)
    ret_value = cum_ret.cum_ret - cost
    return pd.DataFrame(index=cum_ret.index,
                        data={'cost': cost.values, 'ret_value': ret_value.values})


def _fill0_add(x: pd.DataFrame, y: pd.DataFrame):
    return x.add(y, fill_value=0)


def cal_avg_cum_ret(cum_rets: list):
    """
    计算平均累计收益率。注意该函数假设cum_rets在开始前的值为1。
    """
    cum_rets = [cum_ret for cum_ret in cum_rets if len(cum_ret) > 0]

    if not cum_rets:
        return pd.DataFrame(data={'datetime': [], 'cum_ret': []}).set_index('datetime')

    sum_c_and_r = reduce(_fill0_add, map(cal_cost_and_ret_value, cum_rets))
    sum_c_and_r['avg_ret'] = (sum_c_and_r['ret_value'] / sum_c_and_r['cost']).fillna(0)
    sum_c_and_r['cum_ret'] = (1 + sum_c_and_r['avg_ret']).cumprod()
    return sum_c_and_r[['cum_ret']]


def cum_ret_to_daily_ret(cum_ret: pd.Series):
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
    return cum_ret_daily.daily_ret


def ret_from_cum_ret(cum_ret: pd.DataFrame):
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
    data = document.get('cum_ret', {'datetime': [], 'cum_ret': []})
    cum_ret = pd.DataFrame(data).set_index('datetime')
    return cum_ret


def avg_cum_ret_from_cursor(cursor):
    cum_rets = [cum_ret_from_doc(doc) for doc in cursor]
    cum_rets = [cum_ret for cum_ret in cum_rets if len(cum_ret) > 0]
    avg_cum_ret = cal_avg_cum_ret(cum_rets)
    return avg_cum_ret


def avg_annual_ret(cum_ret):
    ret = ret_from_cum_ret(cum_ret)
    ann_ret = empyrical.annual_return(ret)
    return np.mean(ann_ret)
