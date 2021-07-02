import os
import numpy as np
import pandas as pd
from concurrent import futures
from functools import partial

from dateutil.parser import parse

from backtest.tester import Tester
from backtest.signals import add_chg_signal, add_adjusted_price
from utils import mkdirs
from consts import DATA_MINUTE_DIR

Tester.read_cache('daily')


def extract_category_from_file_name(file_name: str) -> str:
    """
        从csv文件名中获得品种代码。
    :param: file_name: csv文件名。
    :return: category: 品种代码
    """
    category = file_name.replace('m1_', '').replace('.csv', '').upper()
    return category


def get_datetime_str(minute_data):
    """获得字符串形式的datetime"""
    return minute_data['actionday'].strftime('%Y-%m-%d') + ' ' + minute_data['minute']


def kline_resampler(df):
    if df.name == 'mhigh':
        return df.max()
    elif df.name == 'mlow':
        return df.min()
    elif df.name == 'mopen':
        return df.iloc[0] if len(df) > 0 else np.nan
    elif df.name == 'mclose':
        return df.iloc[-1] if len(df) > 0 else np.nan
    elif df.name == 'mvolume':
        return df.sum() if len(df) > 0 else np.nan
    else:
        return df.iloc[-1] if len(df) > 0 else np.nan


def resample_minute_data(minute_data, level):
    """
    将分钟级数据合成为level级数据。合成数据的datetime为bar结束时的时间。

    :param minute_data: 分钟级数据。
    :param level: 合成数据时间粒度。
    :return: resampled 合成后数据。
    """

    resampled = minute_data.set_index('datetime')\
                           .resample(level, label='right', closed='right')\
                           .apply(kline_resampler)\
                           .dropna(how='all')\
                           .reset_index()
    resampled.rename(columns={'mopen': 'open',
                              'mclose': 'close',
                              'mhigh': 'high',
                              'mlow': 'low',
                              'mvolume': 'volume',
                              }, inplace=True)
    return resampled


def extract_sub_m_data(m_data, date_range):
    return date_range.join(m_data.set_index('actionday'), how='inner')


def correct_preclose(df):
    """
    考虑更换合约的情况，获得正确的preclose。
    若是新更换的合约，则preclose为表中字段的值。否则preclose为上一行的close。
    :param df: 已添加c_chg字段
    :return: 非纯函数。
    """
    df['above_line_close'] = df.close.shift(1).fillna(df.preclose.iloc[0])
    df['preclose'] = df.apply(lambda x: x['preclose'] if x['c_chg'] else x['above_line_close'], axis=1)
    df.drop(columns=['above_line_close'], inplace=True)


def make_cache(category, level):
    m_data = pd.read_csv(DATA_MINUTE_DIR + f'\\m1_{category.lower()}.csv', parse_dates=['actionday'])
    m_data = m_data[m_data.actionday > parse('2010-01-01')]
    m_data['datetime'] = pd.to_datetime(m_data.apply(get_datetime_str, axis=1))

    resampled_data = resample_minute_data(m_data, level)

    hi_liq_date_ranges = [d_data[['datetime']].set_index('datetime').rename(columns={'datetime': 'actionday'})
                          for d_data in Tester.backtest_data[category]]
    sub_data_list = [extract_sub_m_data(resampled_data, date_range) for date_range in hi_liq_date_ranges]
    sub_data_list = [sub_data for sub_data in sub_data_list if len(sub_data) > 0]

    for i, original_sub_data in enumerate(sub_data_list):
        sub_data = original_sub_data.copy()
        add_chg_signal(sub_data)
        correct_preclose(sub_data)
        add_adjusted_price(sub_data)

        cache_dir = f'../../cache/{level}/{category}'
        mkdirs(cache_dir)
        sub_data.to_csv(f'{cache_dir}/{i}.csv', encoding='utf-8', index=False)

    return category


make_15min_cache = partial(make_cache, level='15min')
make_30min_cache = partial(make_cache, level='30min')


if __name__ == '__main__':
    file_names = os.listdir(DATA_MINUTE_DIR)
    categories = [extract_category_from_file_name(file_name) for file_name in file_names]

    print("making 15min cache...")
    with futures.ProcessPoolExecutor(20) as executor:
        res = executor.map(make_15min_cache, categories)
    print(f'{sorted(list(res))} completed!')

    print("making 30min cache...")
    with futures.ProcessPoolExecutor(20) as executor:
        res = executor.map(make_30min_cache, categories)
    print(f'{sorted(list(res))} completed!')
    # make_15min_cache('SR')
    # make_30min_cache('SR')
