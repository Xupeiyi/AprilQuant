import os
import numpy as np
import pandas as pd
from concurrent import futures
from functools import partial
from dateutil.parser import parse

from backtest.tester import Tester
from cache.funcs import add_chg_signal, add_adjusted_price
from utils import mkdirs, must_have_col
from consts import DATA_MINUTE_DIR, CACHE_ROOT_DIR

Tester.read_cache('daily')
START = '2010-01-01'
DAILY_CACHE = Tester.backtest_data


def extract_category_from_file_name(file_name: str) -> str:
    """
    从csv文件名中获得品种代码。
    :param: file_name: csv文件名。
    :return: category: 品种代码
    """
    category = file_name.replace('m1_', '').replace('.csv', '').upper()
    return category


def get_datetime_str(df):
    """获得字符串形式的datetime"""
    return df['actionday'].strftime('%Y-%m-%d') + ' ' + df['minute']


def kline_resampler(df):
    """将多根ｋ线行情合成为一根"""
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
    :param level: 合成数据时间粒度, 如"15min", "30min"等。
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


def extract_data(df, date_range):
    """选取actionday在date_range中的行情"""
    return date_range.join(df.set_index('actionday'), how='inner')


@must_have_col('c_chg', 'preclose')
def correct_preclose(df):
    """
    获得正确的preclose（部分分钟级数据中存在preclose为0的情况）。
    若是新更换的合约，则preclose为表中字段的值。否则preclose为上一行的close。
    :param df: 已添加c_chg字段
    :return: 非纯函数。
    """
    df['above_line_close'] = df.close.shift(1).fillna(df.preclose.iloc[0])
    df['preclose'] = df.apply(lambda x: x['preclose'] if x['c_chg'] else x['above_line_close'], axis=1)
    df.drop(columns=['above_line_close'], inplace=True)


def extract_high_liq_data(data, category) -> list:
    """利用已完成过滤的日级数据过滤分钟级数据, 从中提取出流动性较高的部分."""
    high_liq_date_ranges = [daily_df[['datetime']].set_index('datetime')
                                                  .rename(columns={'datetime': 'actionday'})
                            for daily_df in DAILY_CACHE[category]]
    sub_data_list = [extract_data(data, date_range) for date_range in high_liq_date_ranges]
    sub_data_list = [sub_data for sub_data in sub_data_list if len(sub_data) > 0]
    return sub_data_list


def get_minute_data(category):
    """获得某品种期货合约的分钟级行情数据"""
    minute_data = pd.read_csv(DATA_MINUTE_DIR + f'\\m1_{category.lower()}.csv', parse_dates=['actionday'])
    minute_data = minute_data[minute_data.actionday > parse(START)]
    minute_data['datetime'] = pd.to_datetime(minute_data.apply(get_datetime_str, axis=1))
    return minute_data


def make_cache(category, level):
    """
    合成并保存可供回测使用的分钟级数据.
    params:
        - category: 期货品种
        - level: 合成数据时间级别，应输入能作为pandas.DataFrame.resample函数参数的字符串， 如"15min"。
    """
    m_data = get_minute_data(category)
    resampled_data = resample_minute_data(m_data, level)
    high_liq_data = extract_high_liq_data(resampled_data, category)

    for i, df in enumerate(high_liq_data):
        add_chg_signal(df)
        correct_preclose(df)
        add_adjusted_price(df)

        cache_dir = CACHE_ROOT_DIR + f'{level}\\{category}'
        mkdirs(cache_dir)
        df.to_csv(f'{cache_dir}\\{i}.csv', encoding='utf-8', index=False)

    return category


make_15min_cache = partial(make_cache, level='15min')
make_30min_cache = partial(make_cache, level='30min')


if __name__ == '__main__':
    minute_file_names = os.listdir(DATA_MINUTE_DIR)
    categories = [extract_category_from_file_name(file_name) for file_name in minute_file_names][0:5]

    print("making 15min cache...")
    with futures.ProcessPoolExecutor(20) as executor:
        res = executor.map(make_15min_cache, categories)
    print(f'{sorted(list(res))} completed!')

    print("making 30min cache...")
    with futures.ProcessPoolExecutor(20) as executor:
        res = executor.map(make_30min_cache, categories)
    print(f'{sorted(list(res))} completed!')

