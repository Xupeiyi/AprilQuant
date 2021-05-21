import pandas as pd
from dateutil.parser import parse
from utils import get_commodity_category, select_high_liquidity_data, mkdirs
from signals import ChandelierSignalAdder

if __name__ == '__main__':
    data = pd.read_csv('../../data/daily/commodity.csv', parse_dates=['date'])
    data = data[data.date > parse('2010-01-01')]
    data['category'] = data['code'].apply(get_commodity_category)
    qualified_data = {category: select_high_liquidity_data(df) for category, df in data.groupby('category')}

    for category, df_list in qualified_data.items():
        for i, df in enumerate(df_list):
            signal_adder = ChandelierSignalAdder(df)
            signal_adder.add_chg_signal()
            signal_adder.add_adjusted_price()

            directory_path = f'../../cache/daily/{category}'
            mkdirs(directory_path)
            signal_adder.df.to_csv(f'{directory_path}/{i}.csv', encoding='utf-8', index=False)
