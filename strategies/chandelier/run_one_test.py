from optimize import Tester, save_results
from signals import ChandelierSignalAdder
from backtest import run_pd_backtest

LEVEL = 'daily'
Tester.read_cache(LEVEL)


if __name__ == '__main__':
    params = {
        # 'data_label': {'category': 'J', 'idx': 0},
        'enter_signal': {'length': 60, 'ema_length': 15},
        'exit_signal': {'trs': 0.012, 'lqk_width': 0.1, 'lqk_floor': 0.5}
    }
    category = 'J'
    idx = 0

    df = Tester.backtest_data[category][idx]

    signal_adder = ChandelierSignalAdder(df)
    signal_adder.add_enter_signal(**params['enter_signal'])
    signal_adder.add_exit_signal(**params['exit_signal'])
    signal_adder.add_position_direction()

    cum_ret = run_pd_backtest(signal_adder.df)
    signal_adder.df['cum_ret'] = cum_ret.cum_ret.values
    signal_adder.df.to_csv(f'{category}_{idx}.csv', index=False)
    # save_results(results, db_name='Chandelier', col_name=LEVEL)
