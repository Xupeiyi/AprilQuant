import time

from backtest.parallel import test_many, gen_params_list, test_by_params_range
from signals import C73Tester


if __name__ == '__main__':
    length_rng = range(20, 130, 10)
    ema_length_rng = range(50, 325, 25)
    # trs_rng = (0.01 * t for t in range(10, 20, 2))
    # lqk_width_rng = (0.1 * w for w in range(1, 4, 1))
    # lqk_floor_rng = (0.1 * f for f in range(3, 6, 1))
    # trs_rng = (0.12,)
    # lqk_width_rng = (0.1,)
    # lqk_floor_rng = (0.5,)

    # test_by_params_range(C73Tester, level='daily', batch=300,
    #                     length=length_rng, ema_length=ema_length_rng, trs=(0.12,), lqk_width=(0.1,), lqk_floor=(0.5,))

    test_by_params_range(C73Tester, level='15min', batch=300,
                         length=length_rng, ema_length=ema_length_rng, trs=(0.06,), lqk_width=(0.1,), lqk_floor=(0.5,))
    test_by_params_range(C73Tester, level='30min', batch=300,
                         length=length_rng, ema_length=ema_length_rng, trs=(0.06,), lqk_width=(0.1,), lqk_floor=(0.5,))

