from concurrent import futures
import pandas as pd
from dateutil.parser import parse
from utils import get_commodity_category, select_high_liquidity_data, mkdirs
from signals import ChandelierSignalAdder


data = pd.read_csv('../../data/daily/commodity.csv', parse_dates=['datetime'])
data = data[data.datetime > parse('2010-01-01')]
data['category'] = data['code'].apply(get_commodity_category)
categories = data.category.unique().tolist()


def make_daily_cache(category):
    global data
    cat_data = data[data.category == category]
    qualified_data = select_high_liquidity_data(cat_data)
    for i, df in enumerate(qualified_data):
        signal_adder = ChandelierSignalAdder(df)
        signal_adder.add_chg_signal()
        signal_adder.add_adjusted_price()

        directory_path = f'../../cache/daily/{category}'
        mkdirs(directory_path)
        signal_adder.df.to_csv(f'{directory_path}/{i}.csv', encoding='utf-8', index=False)
    return category


if __name__ == '__main__':
    with futures.ProcessPoolExecutor(20) as executor:
        res = executor.map(make_daily_cache, categories)
    print(f'{sorted(list(res))} completed!')


    #
    # qualified_data = {category: select_high_liquidity_data(df) for category, df in data.groupby('category')}
    #
    # for category, df_list in qualified_data.items():
    #     for i, df in enumerate(df_list):
    #         signal_adder = ChandelierSignalAdder(df)
    #         signal_adder.add_chg_signal()
    #         signal_adder.add_adjusted_price()
    #
    #         directory_path = f'../../cache/daily/{category}'
    #         mkdirs(directory_path)
    #         signal_adder.df.to_csv(f'{directory_path}/{i}.csv', encoding='utf-8', index=False)
