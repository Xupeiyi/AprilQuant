from .signals import C73Tester
from backtest import run_pd_backtest


LEVEL = 'daily'
C73Tester.read_cache(LEVEL)


if __name__ == '__main__':
    params = {
        'category': 'J', 'idx': 0,
        'length': 60, 'ema_length': 150,
        'trs': 0.012, 'lqk_width': 0.1, 'lqk_floor': 0.5
    }
    category = 'J'
    idx = 0

    df = C73Tester.backtest_data[category][idx]

    tester = C73Tester(params=params)
    tester.add_signals()
    cum_ret = run_pd_backtest(tester.df)
    tester.df.to_csv('J_0.csv')
    # save_results(results, db_name='Chandelier', col_name=LEVEL)
