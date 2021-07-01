from backtest.parallel import test_by_params_range
from signals import DMACTester


if __name__ == '__main__':

    test_by_params_range(DMACTester, level='daily', batch=180, max_workers=18,
                         short_length=(20,), long_length=(240,), break_in=(0, 0.1, 0.2),
                         atr_length=(20,), trs=(2,),
                         use_real_price=(True, False))
