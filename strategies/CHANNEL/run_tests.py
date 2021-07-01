from backtest.parallel import test_by_params_range
from signals import CHANNELTester


if __name__ == '__main__':

    test_by_params_range(CHANNELTester, level='daily', batch=180, max_workers=18,
                         recent=(20,),
                         short_length=(20,), long_length=(240,),
                         atr_length=(20,), trs=(2,),
                         use_real_price=(True, False))
