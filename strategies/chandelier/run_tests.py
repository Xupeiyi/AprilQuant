import time
from optimize import Tester, gen_params_list, test_many, save_results

LEVEL = 'daily'

Tester.read_cache(LEVEL)

if __name__ == '__main__':
    # length_rng = (60,)
    # ema_length_rng = (150,)
    # trs_rng = (0.01 * t for t in range(10, 20, 2))
    # lqk_width_rng = (0.1 * w for w in range(1, 4, 1))
    # lqk_floor_rng = (0.1 * f for f in range(3, 6, 1))

    params_list = gen_params_list(Tester,
                                  length_rng=(80,),
                                  ema_length_rng=(200,))

    BATCH = 100
    iters = int(len(params_list) / BATCH) + 1

    start = time.time()
    for i in range(iters):
        params_sample = params_list[BATCH*i: BATCH*(i+1)]
        testers_list = (Tester(params) for params in params_sample)
        results = test_many(testers_list, max_workers=20)
        save_results(results, db_name='Chandelier', col_name=LEVEL)
        print(f'batch {i+1} in {iters} completed!')
    end = time.time()
    print(f'Used time: {end - start}')
