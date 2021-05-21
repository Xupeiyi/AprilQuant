import os
import numpy as np
import pandas as pd

from dateutil.parser import parse

from optimize import Tester
from signals import ChandelierSignalAdder
from utils import mkdirs

Tester.read_cache('daily')


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


if __name__ == '__main__':
    m_data_root_dir = '..\\..\\data\\minute\\'

    m_data_file_names = os.listdir(m_data_root_dir)
    categories = [extract_category_from_file_name(file_name) for file_name in m_data_file_names]

    for c, category in list(enumerate(categories))[4:]:
        print(f'making no.{c} cache for {category}...')

        m_data = pd.read_csv(m_data_root_dir + f'm1_{category.lower()}.csv', parse_dates=['tradingday'])
        m_data = m_data[m_data.tradingday > parse('2010-01-01')]
        m_data['datetime'] = pd.to_datetime(m_data.apply(get_datetime_str, axis=1))

        print(f'resampling {category}...')
        m15_data = resample_minute_data(m_data, '15min')

        hi_liq_date_ranges = [d_data[['date']].set_index('date') for d_data in Tester.backtest_data[category]]
        sub_data_list = [extract_sub_m_data(m15_data, date_range) for date_range in hi_liq_date_ranges]
        sub_data_list = [sub_data for sub_data in sub_data_list if len(sub_data) > 0]

        print('generating signals...')
        for i, sub_data in enumerate(sub_data_list):

            signal_adder = ChandelierSignalAdder(sub_data)
            signal_adder.add_chg_signal()
            signal_adder.add_adjusted_price()

            cache_dir = f'../../cache/m15/{category}'
            mkdirs(cache_dir)
            signal_adder.df.to_csv(f'{cache_dir}/{i}.csv', encoding='utf-8', index=False)
        print('\n')
