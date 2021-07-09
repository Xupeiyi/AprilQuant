"""
-------------
并行回测相关函数
-------------

"""

import time
import warnings

from concurrent import futures
from itertools import product

import tqdm
from utils import save_results


warnings.filterwarnings('ignore')


def test_caller(tester):
    return tester.test()


def test_many(testers, max_workers=20, pg_bar=True):
    """并行地调用testers中每一个tester的test方法"""
    results = []
    with futures.ProcessPoolExecutor(max_workers) as executor:
        to_do = []
        for tester in testers:
            future = executor.submit(test_caller, tester)
            to_do.append(future)
        done_iter = futures.as_completed(to_do)

        # 显示进度条
        if pg_bar:
            done_iter = tqdm.tqdm(done_iter, total=len(to_do))

        for future in done_iter:
            r = future.result()
            results.append(r)
    return results


def gen_params_list(tester, **param_rngs):
    """
    根据参数范围生成参数列表。
    params:
        - tester: Tester类， backtest_data属性不为空
        - **param_rngs: 参数名及其取值范围
    """
    categories = param_rngs.get('category', tester.backtest_data.keys())
    # 行情数据标签：哪个品种的第几段行情数据。
    data_label_list = ({'category': category, 'idx': idx}
                       for category in categories
                       for idx, _ in enumerate(tester.backtest_data[category]))
    # 所有可能的参数值组合
    arg_value_combinations = list(product(*param_rngs.values()))

    # 对每一个组合构建一个关键字参数字典
    kwarg_list = ({k: v for k, v in zip(param_rngs.keys(), combination)} for combination in arg_value_combinations)
    kwarg_list = list(kwarg_list)

    params = list({**data_label, **kwarg} for data_label in data_label_list for kwarg in kwarg_list)
    return params


def test_by_params_range(tester_cls, level, batch=1000, max_workers=20, **kwarg_rngs):
    """
    在参数范围内并行地运行回测，并将结果保存到mongodb
    params:
        - tester_cls: Tester子类
        - level: 回测数据集时间级别（daily, 15min, 30min）
        - batch: 运行多少回测后保存一次数据
        - max_worker: 使用CPU数量
        - kwarg_rngs: 回测参数及其范围
    """
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



