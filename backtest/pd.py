import pandas as pd


def run_pd_backtest(df, commission=0.0001):
    """基于pandas进行回测。"""
    trade_num = (df.longgo | df.shortgo | df.c_chg
                 | df.long_exit.shift(-1).fillna(False)
                 | df.short_exit.shift(-1).fillna(False)).astype('int')
    trade_num += df.next_c_chg.astype('int')
    trade_cost = trade_num * commission * abs(df.position_direction)

    price_chg_pct = df.close / df.preclose - 1
    ret = price_chg_pct * df.position_direction - trade_cost
    cum_ret = (1 + ret).cumprod()

    return pd.DataFrame(index=df.date.values, data={'cum_ret': cum_ret.values})
