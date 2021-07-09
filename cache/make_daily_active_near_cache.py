"""
缓存活跃近月合约数据。
"""
import pandas as pd
from dateutil.parser import parse


def to_datetime(deliv_mon):
    """
    将int型的deliv_mon转化为日期型。
    注意：由于不知道每个品种具体的到期日期，此处将到期月最后一天作为到期日期。
    """
    year = int(deliv_mon / 100)
    mon = deliv_mon % 100
    return parse(f'{year}-{mon}')


def all_gt_threshold(value, threshold):
    return (value > threshold).all()


def all_lt_threshold(value, threshold):
    return (value < threshold).all()


def check_volume_value_rolling(original_df, period, name, check_fn, **kwargs):
    """
    以滚动的方式检查行情序列中的成交额，并返回一列检查结果的时间序列。

    params:
        - original_df: 原始行情序列
        - period: 滚动窗口宽度
        - name: 检查结果序列名称
        - check_fn: 检查每个窗口的函数
        - kwargs: check_fn的其他参数
    """
    df = original_df.copy()
    df = df.sort_values('datetime', ascending=True)
    result = df.set_index('datetime').groupby('code') \
               .volume_value.rolling(period) \
               .apply(check_fn, kwargs=kwargs) \
               .fillna(0).astype('bool')
    result.name = name
    return result


def is_active(original_df, period=3, threshold=2e7):
    """判断合约成交额是否连续period天大于threshold"""
    return check_volume_value_rolling(original_df, period=period, name='active',
                                      check_fn=all_gt_threshold, threshold=threshold)


def is_inactive(original_df, period=3, threshold=2e7):
    """判断合约成交额是否连续period天小于threshold"""
    return check_volume_value_rolling(original_df, period=period, name='inactive',
                                      check_fn=all_lt_threshold, threshold=threshold)


def select_active_near_contract(all_contract_df, nearest=40):
    """
    选择活跃近月合约。
    params:
        - all_contract_df: 同一品种所有合约行情。
        - nearest: 入选合约距离到期日最短日期。
    """
    chosen = set()
    selected = []
    current_code = None  # 当前选择的合约
    for date, daily_df in all_contract_df.groupby('datetime'):
        # 若已选择合约, 检查是否应该被切换
        if current_code is not None:
            curr_code_info = daily_df[daily_df.code == current_code]
            # 将合约换下来
            if not len(curr_code_info):
                current_code = None
            elif curr_code_info.inactive.values[0] or curr_code_info.to_deliv_date.values[0] <= nearest:
                current_code = None

        # 若未选择合约, 检查是否有可以选择的合约
        if current_code is None:
            select_range = daily_df[(daily_df.to_deliv_date > nearest)
                                    & daily_df.active
                                    & (~daily_df.code.isin(chosen))]
            if len(select_range):
                # 选择最接近到期的合约
                current_code = select_range.iloc[select_range.to_deliv_date.argmin()].code
                chosen.add(current_code)

        selected.append((date, current_code))

    selected = pd.DataFrame(selected, columns=['datetime', 'code'])
    return selected
