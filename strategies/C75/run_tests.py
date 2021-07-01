from backtest.parallel import test_by_params_range
from signals import C75Tester

if __name__ == '__main__':
    date_length_rng = (5, 10, 15, 20)
    n1_rng = (50, 75, 100, 125)
    n2_rng = (20, 30, 40, 50)
    chandelier_rng = dict(trs=(0.06,), lqk_width=(0.1,), lqk_floor=(0.5,))

    test_by_params_range(C75Tester, level='15min', batch=100, max_workers=18,
                         date_length=date_length_rng, n1=n1_rng, n2=n2_rng,
                         **chandelier_rng)

    test_by_params_range(C75Tester, level='30min', batch=100, max_workers=18,
                         date_length=date_length_rng, n1=n1_rng, n2=n2_rng,
                         **chandelier_rng)



