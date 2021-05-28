import re
import os

from datetime import datetime
import pandas as pd
import numpy as np
import talib
import empyrical


def mkdirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


def get_trading_days(data: pd.DataFrame):
    """
    获得全部交易日期。
    params:
        - data：所有品种完整的行情序列。需包括交易日期字段datetime。
    """
    return sorted([datetime.fromtimestamp(ts/1e9 - 8*3600).date()
                   for ts in data.datetime.unique().tolist()])


def get_commodity_category(code: str):
    """
    提取期货合约代码中的英文字母，作为商品种类。
    对于更换过代码的品种，将数据中的旧代码更改为新代码。
    params:
        - code: 期货合约代码。
    """
    category = re.match('[A-Za-z]+', code).group().upper()
    change = {'ER': 'RI', 'RO': 'OI', 'WS': 'WH',
              'ME': 'MA', 'TC': 'ZC', 'WT': 'PM'}
    if category in change:
        category = change[category]
    return category


def select_high_liquidity_data(df: pd.DataFrame, threshold=50000, period=1 / 20) -> list:
    """
    从一个品种的数据中提取出几段入选的数据。非纯函数。

    入选规则：对每一天的行情添加qualified变量，成交量大于等于threshold记为1，小于threshold记为-1。
              对qualified求移动平均值，选入移动平均值大于0的每段行情。

    params:
        - df: 同一品种的一段连续的行情数据。
        - threshold: 成交量阈值。
        - period: 移动平均窗宽。默认为df长度的1/20。
    return:
        - qualified_data: 从df中选取的几段行情数据。
    """

    df['volume_gt_threshold'] = df.volume.apply(lambda x: 1 if x >= threshold else -1)
    df['volume_gt_threshold_ma'] = talib.SMA(df.volume_gt_threshold, len(df) * period)
    df['qualified'] = df.volume_gt_threshold_ma >= 0

    # 根据qualified将df分段，每一段数据中qualified都为True，或False
    df['qualified_chg'] = (df.qualified != df.qualified.shift(1).fillna(df.qualified.iloc[0]))
    df['group_num'] = df['qualified_chg'].astype('int').cumsum()

    qualified_data = [sub_df for group_num, sub_df
                      in df[df.qualified != 0].groupby('group_num')]
    return qualified_data


def expand(ts: pd.DataFrame, expand_range: pd.DataFrame):
    """
    将一段时间序列扩展到所有交易日。
    params:
        - ts: 被扩展的序列。
        - expand_ts: 要扩展到的交易日序列。
    """
    return expand_range.join(ts)



