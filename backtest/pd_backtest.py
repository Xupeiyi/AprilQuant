import pandas as pd


def _get_datetime_list(df):
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
    # 严格地讲，开仓当天的收益率应为df.close / df.open, 这里仍使用
    # df.preclose一是为了简化代码，二是因为如果看成是在前一天接近收盘时计算
    # 出交易信号并立刻入场也讲得通．只是这样可能会造成一些困扰：在longgo前一天就完成了交易.
    # 今后可以再做优化。
    cum_ret = (1 + ret).cumprod()

    results = {'datetime': df.datetime.values, 'cum_ret': cum_ret.values}
    if debug:
        results['trade_num'] = trade_num.values
        results['ret'] = ret.values

    return pd.DataFrame(data=results).set_index('datetime')

