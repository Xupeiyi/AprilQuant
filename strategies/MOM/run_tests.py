from backtest.parallel import test_by_params_range
from signals import MOMTester


if __name__ == '__main__':
    period_rng = (5, 20, 40, 60, 120, 240)

    test_by_params_range(MOMTester, level='daily', batch=200, max_workers=12,
                         period=period_rng, use_real_price=(True, False))
