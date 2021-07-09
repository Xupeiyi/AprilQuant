from backtest.parallel import test_by_params_range
from signals import C74Tester

if __name__ == '__main__':
    test_by_params_range(C74Tester, level='15min', batch=100,
                         date_length=(5, 10, 15, 20), ma_length=(6, 12, 18, 24, 30, 36), eatr_pcnt=(2, 3, 4, 5),
                         trs=(0.14,), lqk_width=(0.1,), lqk_floor=(0.5,))
    test_by_params_range(C74Tester, level='30min', batch=100,
                         date_length=(5, 10, 15, 20), ma_length=(6, 12, 18, 24, 30, 36), eatr_pcnt=(2, 3, 4, 5),
                         trs=(0.14,), lqk_width=(0.1,), lqk_floor=(0.5,))



