import time

from backtest.parallel import test_many, gen_params_list
from signals import C76Tester
from utils import save_results


LEVEL = 'daily'
C76Tester.read_cache('daily')


if __name__ == '__main__':
    params_list = gen_params_list(C76Tester,
                                  fast_length=(25,),
                                  slow_length=(115,),
                                  macd_length=(9,),
                                  trs=(0.045,))
    params_list = list(params_list)
    BATCH = 1000
    iters = int(len(params_list) / BATCH) + 1

    start = time.time()
    for i in range(iters):
        params_sample = params_list[BATCH*i: BATCH*(i+1)]
        testers_list = (C76Tester(params) for params in params_sample)
        results = test_many(testers_list, max_workers=20)
        save_results(results, db_name='C76', col_name=LEVEL)
        print(f'batch {i+1} in {iters} completed!')
    end = time.time()
    print(f'Used time: {end - start}')
