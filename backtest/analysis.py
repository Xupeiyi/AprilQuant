from functools import reduce
import pandas as pd


def cal_cost_and_ret_value(cum_ret: pd.DataFrame):
    """
    计算每期收益值及成本。
    params:
        - cum_ret: 累计收益率曲线（未计算收益时累计收益率为1），字段为cum_ret。
    """
    cost = cum_ret.cum_ret.shift(1).fillna(1)
    ret_value = cum_ret.cum_ret - cost
    return pd.DataFrame(index=cum_ret.index,
                        data={'cost': cost.values, 'ret_value': ret_value.values})


def fill0_add(x: pd.DataFrame, y: pd.DataFrame):
    return x.add(y, fill_value=0)


def cal_avg_cum_ret(cum_rets: list):
    """
    计算平均累计收益率。
    """
    c_and_rs = [cal_cost_and_ret_value(cum_ret) for cum_ret in cum_rets]
    sum_c_and_r = reduce(fill0_add, c_and_rs)
    sum_c_and_r['avg_ret'] = (sum_c_and_r['ret_value'] / sum_c_and_r['cost']).fillna(0)
    sum_c_and_r['cum_ret'] = (1 + sum_c_and_r['avg_ret']).cumprod()
    return sum_c_and_r['cum_ret']


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
