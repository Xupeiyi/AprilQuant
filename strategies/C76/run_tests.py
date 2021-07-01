from backtest.parallel import test_by_params_range
from signals import C76Tester


if __name__ == '__main__':
    fast_length_rng = (15, 20, 25, 30, 35)
    slow_length_rng = (85, 100, 115, 130, 145)
    macd_length_rng = (5, 7, 9, 11, 13)
    chandelier_rng_d = dict(trs=(0.12,))
    chandelier_rng_m = dict(trs=(0.06,))

    test_by_params_range(C76Tester, level='daily', batch=200, max_workers=18,
                         fast_length=fast_length_rng,
                         slow_length=slow_length_rng,
                         macd_length=macd_length_rng,
                         **chandelier_rng_d)

    test_by_params_range(C76Tester, level='15min', batch=200, max_workers=18,
                         fast_length=fast_length_rng,
                         slow_length=slow_length_rng,
                         macd_length=macd_length_rng,
                         **chandelier_rng_m)

    test_by_params_range(C76Tester, level='30min', batch=200, max_workers=18,
                         fast_length=fast_length_rng,
                         slow_length=slow_length_rng,
                         macd_length=macd_length_rng,
                         **chandelier_rng_m)
