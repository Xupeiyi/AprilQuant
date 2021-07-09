import pandas as pd
from strategies.C73.signals import C73Tester
from utils import save_results

LEVEL = 'daily'
C73Tester.read_cache(LEVEL)


if __name__ == '__main__':
    params = {
        'category': 'J', 'idx': 0,
        'length': 60, 'ema_length': 150,
        'trs': 0.12, 'lqk_width': 0.1, 'lqk_floor': 0.5
    }
    tester = C73Tester(params=params)
    result = tester.test()
    save_results([result], db_name='testbase', col_name=LEVEL)
