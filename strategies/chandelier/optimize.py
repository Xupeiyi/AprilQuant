import os
import warnings
import traceback
from collections import defaultdict
from concurrent import futures
from itertools import product

import tqdm
import pymongo
import pandas as pd
from empyrical import sharpe_ratio

from signals import ChandelierSignalAdder
from backtest import run_pd_backtest, cum_ret_to_daily_ret


warnings.filterwarnings('ignore')


class Tester:
    backtest_data = None

    def __init__(self, params):
        # 数据标记
        self.params = params

    @classmethod
    def read_cache(cls, level):
        root_path = f'..\\..\\cache\\{level}\\'
        categories = os.listdir(root_path)

        cls.backtest_data = defaultdict(list)

        for category in categories:
            category_path = root_path + f'{category}\\'
            csv_files = os.listdir(category_path)
            for file in csv_files:
                df = pd.read_csv(category_path + file, parse_dates=['datetime'])
                cls.backtest_data[category].append(df)

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
        except:
            print(self.params['data_label'])
            print('\n')
        else:
            signal_adder.add_exit_signal(**self.params['exit_signal'])
            signal_adder.add_position_direction()

            cum_ret = run_pd_backtest(signal_adder.df)
            res['cum_ret'] = cum_ret.reset_index().to_dict('list')

            # sharpe比率
            daily_ret = cum_ret_to_daily_ret(cum_ret)
            res['sharpe_ratio'] = sharpe_ratio(daily_ret, period='daily')

        return res


def save_results(results, db_name, col_name, client_name="mongodb://localhost:27017/"):
    client = pymongo.MongoClient(client_name)
    db = client[db_name]
    col = db[col_name]
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


def gen_params_list(tester, category_rng=None, length_rng=(60,), ema_length_rng=(150,),
                    trs_rng=(0.12,), lqk_width_rng=(0.1,), lqk_floor_rng=(0.5,)):
    category_rng = category_rng or tester.backtest_data.keys()
    data_label_list = ({'category': category, 'idx': idx}
                       for category in category_rng
                       for idx, _ in enumerate(tester.backtest_data[category]))
    enter_signal_params_list = ({'length': length, 'ema_length': ema_length}
                                for length, ema_length
                                in product(length_rng, ema_length_rng))
    exit_signal_params_list = ({'trs': trs, 'lqk_width': lqk_width, 'lqk_floor': lqk_floor}
                               for trs, lqk_width, lqk_floor
                               in product(trs_rng, lqk_width_rng, lqk_floor_rng))
    params_list = list({'data_label': data_label,
                        'enter_signal': enter_signal_params,
                        'exit_signal': exit_signal_params}
                       for data_label, enter_signal_params, exit_signal_params
                       in product(data_label_list, enter_signal_params_list, exit_signal_params_list))
    return params_list



