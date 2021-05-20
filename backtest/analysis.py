import pandas as pd

from utils import expand


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


def cal_avg_cum_ret(cum_rets, time_range):
    """
    计算平均累计收益率。
    cum_rets:defaultdict(list)
    """
    c_and_rs = [cal_cost_and_ret(cum_ret)
                for cum_ret_list in cum_rets.values()
                for cum_ret in cum_ret_list]
    expanded_c_and_rs = [expand(c_and_r, time_range).fillna(0) for c_and_r in c_and_rs]
    sum_c_and_r = sum(expanded_c_and_rs)
    sum_c_and_r['avg_ret'] = (sum_c_and_r['ret'] / sum_c_and_r['cost']).fillna(0)
    sum_c_and_r['avg_cum_ret'] = (1 + sum_c_and_r['avg_ret']).cumprod()
    return sum_c_and_r['avg_cum_ret']
