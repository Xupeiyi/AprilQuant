from optimize import Tester, gen_params_list, test_many, save_results

Tester.read_cache('daily')

if __name__ == '__main__':
    length_rng = range(20, 120, 10)
    ema_length_rng = range(80, 240, 10)
    # trs_rng = (0.01 * t for t in range(10, 20, 2))
    # lqk_width_rng = (0.1 * w for w in range(1, 4, 1))
    # lqk_floor_rng = (0.1 * f for f in range(3, 6, 1))

    params_list = gen_params_list(Tester, length_rng, ema_length_rng)

    BATCH = 1000
    iters = int(len(params_list) / BATCH) + 1

    for i in range(iters):
        params_sample = params_list[BATCH*i: BATCH*(i+1)]

        testers_list = (Tester(params) for params in params_sample)
        results = test_many(testers_list)
        save_results(results, col_name='chandelier')
        print(f'batch {i+1} in {iters} completed!')
