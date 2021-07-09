from backtest.parallel import test_by_params_range
from signals import C73Tester


class ExampleTester(C73Tester):

    def __init__(self, params):
        super().__init__(params)


if __name__ == '__main__':
    length_rng = range(20, 130, 10)
    ema_length_rng = range(50, 325, 25)

    test_by_params_range(C73Tester, level='daily', batch=300,
                         length=length_rng, ema_length=ema_length_rng,
                         trs=(0.12,), lqk_width=(0.1,), lqk_floor=(0.5,))
    test_by_params_range(C73Tester, level='15min', batch=300,
                         length=length_rng, ema_length=ema_length_rng,
                         trs=(0.06,), lqk_width=(0.1,), lqk_floor=(0.5,))
    test_by_params_range(C73Tester, level='30min', batch=300,
                         length=length_rng, ema_length=ema_length_rng,
                         trs=(0.06,), lqk_width=(0.1,), lqk_floor=(0.5,))

