from backtest.parallel import  test_by_params_range
from signals import C74Tester

if __name__ == '__main__':
    # length_rng = (60,)
    # ema_length_rng = (150,)
    # trs_rng = (0.01 * t for t in range(10, 20, 2))
    # lqk_width_rng = (0.1 * w for w in range(1, 4, 1))
    # lqk_floor_rng = (0.1 * f for f in range(3, 6, 1))
    test_by_params_range(C74Tester, level='daily', batch=100,
                         length=(60,), ema_length=(150,), trs=(0.12,), lqk_width=(0.1,), lqk_floor=(0.5,))
    test_by_params_range(C74Tester, level='15min', batch=100,
                         length=(60,), ema_length=(150,), trs=(0.12,), lqk_width=(0.1,), lqk_floor=(0.5,))
    test_by_params_range(C74Tester, level='30min', batch=100,
                         length=(60,), ema_length=(150,), trs=(0.12,), lqk_width=(0.1,), lqk_floor=(0.5,))
