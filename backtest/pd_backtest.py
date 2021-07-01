import pandas as pd


def get_datetime_list(df):
    return df.datetime.values.tolist()


def run_pd_backtest(df, commission=0.0001, debug=False):
    """基于pandas进行回测。"""

    # 开仓操作
    long_open = (df.position_direction == 1) & (df.position_direction.shift(1).fillna(0) <= 0)
    short_open = (df.position_direction == -1) & (df.position_direction.shift(1).fillna(0) >= 0)
    open_op = long_open | short_open | (df.c_chg & df.position_direction)

    # 平仓操作
    long_close = (df.position_direction == 1) & (df.position_direction.shift(-1).ffill() <= 0)
    short_close = (df.position_direction == -1) & (df.position_direction.shift(-1).ffill() >= 0)
    close_op = long_close | short_close | (df.next_c_chg & df.position_direction)

    trade_num = open_op.astype('int') + close_op.astype('int')
    trade_cost = trade_num * commission

    price_chg_pct = df.close / df.preclose - 1
    ret = price_chg_pct * df.position_direction - trade_cost
    cum_ret = (1 + ret).cumprod()

    # todo: 重构回测结果表示方法
    long_open_dates = get_datetime_list(df[long_open])
    long_close_dates = get_datetime_list(df[long_close])
    short_open_dates = get_datetime_list(df[short_open])
    short_close_dates = get_datetime_list(df[short_close])

    results = {'datetime': df.datetime.values, 'cum_ret': cum_ret.values}
    if debug:
        results['trade_num'] = trade_num.values
        results['ret'] = ret.values

    return pd.DataFrame(data=results).set_index('datetime')
