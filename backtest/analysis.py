from functools import reduce
import pandas as pd


def cal_cost_and_ret(cum_ret: pd.DataFrame):
    """
    计算每期收益及成本。
    params:
        - cum_ret: 累计收益率曲线（未计算收益时累计收益率为1），字段为cum_ret。
    """
    cost = cum_ret.cum_ret.shift(1).fillna(1)
    ret = cum_ret.cum_ret - cost
    return pd.DataFrame(index=cum_ret.index,
                        data={'cost': cost.values, 'ret': ret.values})


def fill0_add(x: pd.DataFrame, y: pd.DataFrame):
    return x.add(y, fill_value=0)


def cal_avg_cum_ret(cum_rets: list):
    """
    计算平均累计收益率。
    """
    c_and_rs = [cal_cost_and_ret(cum_ret) for cum_ret in cum_rets]
    sum_c_and_r = reduce(fill0_add, c_and_rs)
    sum_c_and_r['avg_ret'] = (sum_c_and_r['ret'] / sum_c_and_r['cost']).fillna(0)
    sum_c_and_r['avg_cum_ret'] = (1 + sum_c_and_r['avg_ret']).cumprod()
    return sum_c_and_r['avg_cum_ret']