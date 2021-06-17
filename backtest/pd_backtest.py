import pandas as pd


def run_pd_backtest(df, commission=0.0001, debug=False):
    """基于pandas进行回测。"""

    long_open = (df.position_direction == 1) & (df.position_direction.shift(1).fillna(0) <= 0)
    short_open = (df.position_direction == -1) & (df.position_direction.shift(1).fillna(0) >= 0)
    open_op = long_open | short_open | (df.c_chg & df.position_direction)

    long_close = (df.position_direction == 1) & (df.position_direction.shift(-1).ffill() <= 0)
    short_close = (df.position_direction == -1) & (df.position_direction.shift(-1).ffill() >= 0)

    close_op = long_close | short_close | (df.next_c_chg & df.position_direction)
    trade_num = open_op.astype('int') + close_op.astype('int')

    # 开仓操作与next_c_chg在同一天时，trade_num = 2

    # 平仓操作与next_c_chg在同一天时，trade_num = 1

    # 开仓操作与c_chg在同一天时， trade_num = 1

    # 平仓操作与c_chg在同一天时， trade_num = 2

    trade_cost = trade_num * commission

    price_chg_pct = df.close / df.preclose - 1

    # enter_bars = (
    #         (df.longgo & (df.position_direction.shift(1) <= 0) & (df.position_direction > 0))
    #         | (df.shortgo & (df.position_direction.shift(1) >= 0) & (df.position_direction < 0))
    # )
    # price_chg_pct.loc[enter_bars] = df.loc[enter_bars, 'close'] / df.loc[enter_bars, 'open'] - 1

    ret = price_chg_pct * df.position_direction - trade_cost
    cum_ret = (1 + ret).cumprod()

    data = {'datetime': df.datetime.values, 'cum_ret': cum_ret.values}
    if debug:
        data['trade_num'] = trade_num.values
        data['ret'] = ret.values
    return pd.DataFrame(data=data).set_index('datetime')
