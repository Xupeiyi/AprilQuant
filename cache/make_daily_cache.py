import re
from functools import partial
from concurrent import futures

import talib
import pandas as pd
from dateutil.parser import parse

from utils import pre_is_diff, mkdirs
from consts import DATA_DAILY_DIR
from backtest.signals import add_chg_signal, add_adjusted_price, AddSignalError


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
    df['qualified_chg'] = pre_is_diff(df.qualified)
    df['group_num'] = df['qualified_chg'].astype('int').cumsum()

    qualified_data = [sub_df for group_num, sub_df
                      in df[df.qualified != 0].groupby('group_num')]
    return qualified_data


def select_all_data(df: pd.DataFrame):
    qualified_data = [df]
    return qualified_data


def make_cache(category, filter_function, col):
    global data
    cat_data = data[data.category == category]
    qualified_data = filter_function(cat_data)
    for i, original_df in enumerate(qualified_data):
        df = original_df.copy()
        try:
            add_chg_signal(df)
            add_adjusted_price(df)
        except AddSignalError as e:
            print(f'Error with {category}: {str(e)}')
        else:
            directory_path = f'./{col}/{category}'
            mkdirs(directory_path)
            df.to_csv(f'{directory_path}/{i}.csv', encoding='utf-8', index=False)
    return category


make_daily_cache = partial(make_cache, filter_function=select_high_liquidity_data, col='daily')
make_daily_no_filter_cache = partial(make_cache, filter_function=select_all_data, col='daily_no_filter')

data = pd.read_csv(DATA_DAILY_DIR + 'commodity.csv', parse_dates=['datetime'])
data = data[data.datetime > parse('2010-01-01')]
data['category'] = data['code'].apply(get_commodity_category)
categories = data.category.unique().tolist()


if __name__ == '__main__':

    with futures.ProcessPoolExecutor(20) as executor:
        res = executor.map(make_daily_no_filter_cache, categories)
    print(f'{sorted(list(res))} completed!')

