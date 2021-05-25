import os
import numpy as np
import pandas as pd
from concurrent import futures
from functools import partial

from dateutil.parser import parse

from optimize import Tester
from signals import ChandelierSignalAdder
from utils import mkdirs

Tester.read_cache('daily')
ROOT_DIR = '..\\..\\data\\minute\\'


def extract_category_from_file_name(file_name):
    return file_name.replace('m1_', '').replace('.csv', '').upper()


def get_datetime_str(minute_data):
    return minute_data['tradingday'].strftime('%Y-%m-%d') + ' ' + minute_data['minute']


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
    resampled = minute_data.set_index('datetime')\
                           .resample(level, label='right', closed='right')\
                           .apply(kline_resampler)\
                           .dropna(how='all')\
                           .reset_index()

    resampled.rename(columns={'datetime': 'date',
                              'mopen': 'open',
                              'mclose': 'close',
                              'mhigh': 'high',
                              'mlow': 'low',
                              'mvolume': 'volume',
                              'deliv_mon': 'delivery_month'}, inplace=True)

    return resampled


def extract_sub_m_data(m_data, date_range):
    return date_range.join(m_data.set_index('tradingday'), how='inner')


def correct_preclose(df):
    """
    考虑更换合约的情况，获得正确的preclose。

    :param df: 已添加c_chg字段
    :return:
    """
    df['above_line_close'] = df.close.shift(1).fillna(df.preclose.iloc[0])
    df['preclose'] = df.apply(lambda x: x['preclose'] if x['c_chg'] else x['above_line_close'], axis=1)
    # df['preclose'] = df.apply(lambda x: x['close'] if x['preclose'] == 0 else x['preclose'], axis=1)
    df.drop(columns=['above_line_close'], inplace=True)


def make_cache(category, level):
    m_data = pd.read_csv(ROOT_DIR + f'm1_{category.lower()}.csv', parse_dates=['tradingday'])
    m_data = m_data[m_data.tradingday > parse('2010-01-01')]
    m_data['datetime'] = pd.to_datetime(m_data.apply(get_datetime_str, axis=1))

    resampled_data = resample_minute_data(m_data, level)

    hi_liq_date_ranges = [d_data[['date']].set_index('date') for d_data in Tester.backtest_data[category]]
    sub_data_list = [extract_sub_m_data(resampled_data, date_range) for date_range in hi_liq_date_ranges]
    sub_data_list = [sub_data for sub_data in sub_data_list if len(sub_data) > 0]

    for i, sub_data in enumerate(sub_data_list):
        signal_adder = ChandelierSignalAdder(sub_data)
        signal_adder.add_chg_signal()
        correct_preclose(signal_adder.df)
        signal_adder.add_adjusted_price()

        cache_dir = f'../../test/{level}/{category}'
        mkdirs(cache_dir)
        signal_adder.df.to_csv(f'{cache_dir}/{i}.csv', encoding='utf-8', index=False)

    return category


make_15min_cache = partial(make_cache, level='15min')
make_30min_cache = partial(make_cache, level='30min')


if __name__ == '__main__':
    file_names = os.listdir(ROOT_DIR)
    categories = [extract_category_from_file_name(file_name) for file_name in file_names]

    with futures.ProcessPoolExecutor(20) as executor:
        res = executor.map(make_15min_cache, categories)

    print(f'{sorted(list(res))} completed!')


