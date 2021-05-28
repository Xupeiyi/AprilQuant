import pandas as pd


def run_pd_backtest(df, commission=0.0001):
    """基于pandas进行回测。"""
    trade_num = (df.longgo | df.shortgo | df.c_chg
                 | df.long_exit.shift(-1).fillna(False)
                 | df.short_exit.shift(-1).fillna(False)).astype('int')

    not_about_to_exit = (df.long_exit.shift(-1).fillna(False) != 1) & (df.short_exit.shift(-1).fillna(False) != 1)
    # 如果不是即将平仓则需计算更换合约的交易费用，反之则不需要
    trade_num += df.next_c_chg.astype('int') * not_about_to_exit.astype('int')
    trade_cost = trade_num * commission * abs(df.position_direction)

    price_chg_pct = df.close / df.preclose - 1
    ret = price_chg_pct * df.position_direction - trade_cost
    cum_ret = (1 + ret).cumprod()

    return pd.DataFrame(data={'datetime': df.datetime.values, 'cum_ret': cum_ret.values}).set_index('datetime')
