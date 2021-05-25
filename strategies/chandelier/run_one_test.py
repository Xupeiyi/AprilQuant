from optimize import Tester, gen_params_list, test_many, save_results
LEVEL = 'daily'

Tester.read_cache(LEVEL)


if __name__ == '__main__':
    params = {
        'data_label': {'category': 'AP', 'idx': 0},
        'enter_signal': {'length': 60, 'ema_length': 150},
        'exit_signal': {'trs': 0.12, 'lqk_width': 0.1, 'lqk_floor': 0.5}
    }
    tester = Tester(params)
    results = [tester.test()]
    # save_results(results, db_name='Chandelier', col_name=LEVEL)
