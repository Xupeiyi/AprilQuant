import os
import warnings
from collections import defaultdict
from concurrent import futures
from itertools import product

import tqdm
import pymongo
import pandas as pd

from signals import ChandelierSignalAdder
from backtest import run_bt_backtest, run_pd_backtest

warnings.filterwarnings('ignore')


def read_cache():
    root_path = '..\\..\\cache\\'
    categories = os.listdir(root_path)

    backtest_data = defaultdict(list)

    for category in categories:
        category_path = root_path + f'{category}\\'
        csv_files = os.listdir(category_path)
        for file in csv_files:
            df = pd.read_csv(category_path + file, parse_dates=['date'])
            backtest_data[category].append(df)

    return backtest_data


class Tester:
    backtest_data = read_cache()

    def __init__(self, params):
        # 数据标记
        self.params = params

    def test(self):

        category = self.params['data_label']['category']
        idx = self.params['data_label']['idx']

        df = self.backtest_data[category][idx]
        signal_adder = ChandelierSignalAdder(df)

        res = dict()
        res['params'] = self.params
        try:
            signal_adder.add_enter_signal(**self.params['enter_signal'])
        except ValueError as e:
            res['error'] = str(e)
        else:
            signal_adder.add_exit_signal(**self.params['exit_signal'])
            signal_adder.add_position_direction()

            res['bt_cum_ret'] = run_bt_backtest(signal_adder.df).reset_index().to_dict('list')
            res['pd_cum_ret'] = run_pd_backtest(signal_adder.df).reset_index().to_dict('list')

        return res


def save_results(results):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["AprilQuant"]
    col = db['chandelier']
    col.insert_many(results)
    return


def run_test(t):
    return t.test()


def test_many(testers, max_workers=20, pg_bar=True):
    results = []
    with futures.ProcessPoolExecutor(max_workers) as executor:
        to_do = []
        for tester in testers:
            future = executor.submit(run_test, tester)
            to_do.append(future)
        done_iter = futures.as_completed(to_do)

        if pg_bar:
            done_iter = tqdm.tqdm(done_iter, total=len(to_do))

        for future in done_iter:
            r = future.result()
            results.append(r)
    return results


if __name__ == '__main__':
    length_rng = range(40, 120, 10)
    ema_length_rng = range(100, 180, 10)
    trs_rng = (0.01 * t for t in range(10, 20, 2))
    lqk_width_rng = (0.1 * w for w in range(1, 4, 1))
    lqk_floor_rng = (0.1 * f for f in range(3, 6, 1))

    data_label_list = ({'category': category, 'idx': idx}
                       for category, df_list in Tester.backtest_data.items()
                       for idx, _ in enumerate(df_list))
    enter_signal_params_list = ({'length': length, 'ema_length': ema_length}
                                for length, ema_length
                                in product(length_rng, ema_length_rng))
    exit_signal_params_list = ({'trs': trs, 'lqk_width': lqk_width, 'lqk_floor': lqk_floor}
                               for trs, lqk_width, lqk_floor
                               in product(trs_rng, lqk_width_rng, lqk_floor_rng))
    params_list = list({'data_label': data_label, 'enter_signal': enter_signal_params, 'exit_signal': exit_signal_params}
                       for data_label, enter_signal_params, exit_signal_params
                       in product(data_label_list, enter_signal_params_list, exit_signal_params_list))

    BATCH = 100
    size = int(len(params_list) / BATCH) + 1

    for i in range(BATCH):
        params_sample = params_list[size*i: size*(i+1)]
        testers_list = (Tester(params) for params in params_sample)
        results = test_many(testers_list)
        save_results(results)
        print(f'batch {i} completed!')


