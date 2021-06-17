import time
import warnings

from concurrent import futures
from itertools import product

import tqdm
from utils import save_results


warnings.filterwarnings('ignore')


def test_caller(tester):
    return tester.test()


def test_many_wrapper(f):
    def wrapped(params):
        return f(**params)
    return wrapped


def test_many(testers, max_workers=20, pg_bar=True):
    results = []
    with futures.ProcessPoolExecutor(max_workers) as executor:
        to_do = []
        for tester in testers:
            future = executor.submit(test_caller, tester)
            to_do.append(future)
        done_iter = futures.as_completed(to_do)

        if pg_bar:
            done_iter = tqdm.tqdm(done_iter, total=len(to_do))

        for future in done_iter:
            r = future.result()
            results.append(r)
    return results


def gen_params_list(tester, category_rng=None, **kwarg_rngs):
    category_rng = category_rng or tester.backtest_data.keys()
    data_label_list = ({'category': category, 'idx': idx}
                       for category in category_rng
                       for idx, _ in enumerate(tester.backtest_data[category]))
    # 所有可能的参数值组合
    arg_value_combinations = list(product(*kwarg_rngs.values()))

    # 对每一个组合构建一个关键字参数字典
    kwarg_list = ({k: v for k, v in zip(kwarg_rngs.keys(), combination)} for combination in arg_value_combinations)
    kwarg_list = list(kwarg_list)

    params = ({**data_label, **kwarg} for data_label in data_label_list for kwarg in kwarg_list)
    params = list(params)
    return params


def test_by_params_range(tester_cls, level, batch=1000, max_workers=20, **kwarg_rngs):
    tester_cls.read_cache(level)
    params_list = gen_params_list(tester_cls, **kwarg_rngs)

    iters = int(len(params_list) / batch) + 1

    start = time.time()
    for i in range(iters):
        params_sample = params_list[batch*i: batch*(i+1)]
        testers_list = (tester_cls(params) for params in params_sample)
        results = test_many(testers_list, max_workers=max_workers)
        save_results(results, db_name=tester_cls.__name__.replace('Tester', ''), col_name=level)
        print(f'batch {i+1} in {iters} completed!')
    end = time.time()
    print(f'Used time: {end - start}')



