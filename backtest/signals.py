"""在行情序列df中添加额外的字段"""

import pandas as pd

from backtest.indicators import ATR
from utils import has_column, next_is_diff, pre_is_diff, must_have_col


class AddSignalError(Exception):
    pass


@must_have_col('tradingday', 'adjusted_close')
def add_avg_daily_last_adjusted_close(df, date_length):
    """
    计算之前date_length天（以交易日计算，包括当天）每天最后一根bar前复权收盘价的均值avg_adjusted_close。
    avg_adjusted_close每过date_length天计算一次。比如，若date_length为10，则第10天至
    第19天的avg_adjusted_close为第1到第10天每天收盘价均值， 第20天至第29天的avg_adjusted_close
    为第11到第20天每天收盘价均值。
    """
    # 若已有计算结果，则重新计算
    if has_column(df, 'avg_adjusted_close'):
        df.drop(columns={'avg_adjusted_close'}, inplace=True)

    trd = df.tradingday
    
    # 标记当前交易日与下一根交易日不同的bar， 作为每个交易日最后一根bar
    df['trd_last_bar'] = next_is_diff(trd)

    # 按交易日为每根bar标记日期序号date_idx，从0开始。
    trd_first_bar = pre_is_diff(trd)
    date_idx = trd_first_bar.cumsum()
    
    # 每组日期序号模date_length为0的bar的第一根为一次平均窗口的开始
    window_first_bar = ((date_idx % date_length) == 0) & trd_first_bar
    df['window_idx'] = window_first_bar.cumsum()

    # 在每个平均窗口内计算每个交易日最后一根bar的复权收盘价均值
    avg_adjusted_close = df.groupby('window_idx').apply(lambda x: x[x.trd_last_bar].adjusted_close.mean())
    avg_adjusted_close = pd.DataFrame({'avg_adjusted_close': avg_adjusted_close})

    # 对每个窗口内的bar添加前一个窗口的收盘价均值
    df['pre_window_idx'] = df.window_idx - 1
    df['avg_adjusted_close'] = df.set_index('pre_window_idx')\
                                 .join(avg_adjusted_close)\
                                 .reset_index()['avg_adjusted_close']

    # 复权收盘价均值向前移动一期，在前一个窗口结束时就可更新收盘价均值
    df['avg_adjusted_close'] = df['avg_adjusted_close'].shift(-1).ffill()



@must_have_col('deliv_mon')
def add_chg_signal(df):
    """
    在df中添加更换合约标记。
    根据合约交割月份字段deliv_mon判断合约是否切换。
    next_c_chg表示下一根bar将切换合约。c_chg表示当前bar切换合约。
    params:
        - df: 一段连续的行情数据。需要行情中包括合约交割月份deliv_mon。
    """
    df['next_c_chg'] = next_is_diff(df.deliv_mon)
    df['c_chg'] = pre_is_diff(df.deliv_mon)


@must_have_col('open', 'close', 'high', 'low', 'preclose')
def add_adjusted_price(df):
    """
    在df中添加前复权价格。
    params:
        - df: 一段连续的行情数据。需要行情中包括open, close, high, low, preclose。
    """
    for price in ['preclose', 'close']:
        if 0 in df[price].values:
            raise AddSignalError(f"{price}字段中存在0！")

    # 计算复权因子
    adjust_factor = (df['close'] / df['preclose']).cumprod()

    # 计算前复权收盘价
    df['adjusted_close'] = adjust_factor * (df['close'].iloc[0] / adjust_factor.iloc[0])

    # 对 open, high, low 以同比例放缩
    r = df['adjusted_close'] / df['close']
    for price in ['open', 'high', 'low']:
        df['adjusted_' + price] = df[price] * r


@must_have_col('longgo', 'shortgo', 'adjusted_open', 'adjusted_close', 'adjusted_high', 'adjusted_low')
def add_chandelier_exit_signal(df, trs=0.12, lqk_width=0.1, lqk_floor=0.5):
    """
    添加吊灯线止损信号。
    在df中标记平仓日期。非纯函数。平仓信号使用复权价格计算。
    平仓信号计算方法：开多头仓位时记当期最低价与前收盘价中较高者为lower_after_entry，之后在
                   持仓的每一期选取当期最低价和上一期lower_after_entry
                   中较大者为当期lower_after_entry，并计算吊灯线dliqpoint。
                   若dliqpoint低于收盘价则平多头。开空头仓位时同理。
                   止损止盈幅度乘数liqka随时间推移从1减小为lqk_floor，每期减小lqk_width，
                   这使吊灯线变得更加敏感。
    params:
        - df: 一段连续的行情数据。需要行情中包括前复权开盘价adjusted_open,
              最高价adjusted_high, 前复权最低价adjusted_low, 前复权收盘价
              adjusted_close，并标注开仓时刻longgo和shortgo。
        - trs: 用于调整吊灯线与价格之间的距离。trs越大则吊灯线越不敏感。
        - lqk_width: liqka每期减小的步长。
        - lqk_floor: liqka能达到的最小值。
    """
    if not has_column(df, 'long_exit'):
        df['long_exit'] = False  # 多头平仓日期

    if not has_column(df, 'short_exit'):
        df['short_exit'] = False  # 空头平仓日期

    df['higher_after_entry'] = 0  # 开仓以来较高价
    df['lower_after_entry'] = 0  # 开仓以来较低价
    df['dliqpoint'] = 0  # 多头吊灯线
    df['kliqpoint'] = 0  # 空头吊灯线
    df['liqka'] = 1  # 止盈止损幅度乘数
    position_direction = 0  # 当前头寸方向。1为多头，-1为空头，0为无头寸。
    for i in range(len(df)-1):

        # 开多头
        if df['longgo'].iloc[i] and position_direction <= 0:
            position_direction = 1
            df['lower_after_entry'].iloc[i] = max(df['adjusted_close'].iloc[i-1], df['adjusted_low'].iloc[i])
            continue

        # 开空头
        if df['shortgo'].iloc[i] and position_direction >= 0:
            position_direction = -1
            df['higher_after_entry'].iloc[i] = min(df['adjusted_close'].iloc[i-1], df['adjusted_high'].iloc[i])
            continue

        # 平多头
        if df['long_exit'].iloc[i] and position_direction > 0:
            position_direction = 0
            continue

        # 平空头
        if df['short_exit'].iloc[i] and position_direction < 0:
            position_direction = 0
            continue

        # 有持仓时计算吊灯线
        if position_direction != 0:
            df['liqka'].iloc[i] = max(df['liqka'].iloc[i - 1] - lqk_width, lqk_floor)
            delta = df['adjusted_open'].iloc[i] * trs * df['liqka'].iloc[i]

            if position_direction > 0:
                df['lower_after_entry'].iloc[i] = max(df['adjusted_low'].iloc[i],
                                                      df['lower_after_entry'].iloc[i - 1])
                df['dliqpoint'].iloc[i] = df['lower_after_entry'].iloc[i] - delta

                # 平仓日期是计算出平仓信号的后一天
                if df['adjusted_close'].iloc[i] < df['dliqpoint'].iloc[i]:
                    df['long_exit'].iloc[i + 1] = True
            else:
                df['higher_after_entry'].iloc[i] = min(df['adjusted_high'].iloc[i],
                                                       df['higher_after_entry'].iloc[i - 1])
                df['kliqpoint'].iloc[i] = df['higher_after_entry'].iloc[i] + delta

                # 平仓日期是计算出平仓信号的后一天
                if df['adjusted_close'].iloc[i] > df['kliqpoint'].iloc[i]:
                    df['short_exit'].iloc[i + 1] = True


@must_have_col('longgo', 'long_exit', 'shortgo', 'short_exit')
def add_position_direction(df):
    """
    在df中标记当前bar开盘时的持仓头寸方向。1为多头，-1为空头，0为无持仓。
    """
    df['position_direction'] = 0

    # 事实上, 任何开仓或平仓信号都不可能在第一根bar就发出，故这里从第二根bar开始。
    for i in range(1, len(df)):

        # 开多头
        if df['longgo'].iloc[i] and df['position_direction'].iloc[i - 1] <= 0:
            df['position_direction'].iloc[i] = 1
            continue

        # 开空头
        if df['shortgo'].iloc[i] and df['position_direction'].iloc[i - 1] >= 0:
            df['position_direction'].iloc[i] = -1
            continue

        # 平多头
        if df['long_exit'].iloc[i] and df['position_direction'].iloc[i - 1] > 0:
            df.position_direction.iloc[i] = 0
            continue

        # 平空头
        if df['short_exit'].iloc[i] and df['position_direction'].iloc[i - 1] < 0:
            df['position_direction'].iloc[i] = 0
            continue

        df['position_direction'].iloc[i] = df['position_direction'].iloc[i - 1]


@must_have_col('adjusted_high', 'adjusted_low', 'adjusted_close')
def add_atr_exit_signal(df, atr_timeperiod, trs):
    """
    使用ATR指标添加出场信号。
    止损条件：1.多头合约价格低于（持仓以来最高价 - trs倍ATR）。
            2.空头合约价格高于（持仓以来最低价 + trs倍ATR）。
            3.切换合约。
    params:
        - df: 一段连续的行情数据。需要行情中包括前复权最高价adjusted_high,
              前复权最低价adjusted_low, 前复权收盘价adjusted_close，
              并标注开仓时刻longgo和shortgo。
        - atr_timeperiod: ATR指标移动窗宽长度。
        - trs: 止损幅度乘数。
    """
    close, high, low = df.adjusted_close, df.adjusted_high, df.adjusted_low
    df['atr'] = ATR(high=high, low=low, close=close, timeperiod=atr_timeperiod)

    position_direction = 0
    df['highest_after_entry'] = 0  # 开仓以来最高价
    df['lowest_after_entry'] = 0  # 开仓以来最低价

    if not has_column(df, 'long_exit'):
        df['long_exit'] = False  # 多头平仓日期

    if not has_column(df, 'short_exit'):
        df['short_exit'] = False  # 空头平仓日期

    for i in range(len(df)-1):

        # 开多头
        if df['longgo'].iloc[i] and position_direction <= 0:
            position_direction = 1
            df['lowest_after_entry'].iloc[i] = low.iloc[i]
            continue

        # 开空头
        if df['shortgo'].iloc[i] and position_direction >= 0:
            position_direction = -1
            df['highest_after_entry'].iloc[i] = high.iloc[i]
            continue

        # 平多头
        if df['long_exit'].iloc[i] and position_direction > 0:
            position_direction = 0
            continue

        # 平空头
        if df['short_exit'].iloc[i] and position_direction < 0:
            position_direction = 0
            continue

        # 有持仓时计算出场条件
        if position_direction != 0:
            delta = trs * df['atr'].iloc[i]
            # 更新有持多头以来最高价，并判断是否出场
            if position_direction > 0:
                df['highest_after_entry'].iloc[i] = max(high.iloc[i],
                                                        df['highest_after_entry'].iloc[i - 1])
                # 平仓日期是计算出平仓信号的后一天
                if (close.iloc[i] < df['highest_after_entry'].iloc[i] - delta) or df['next_c_chg'].iloc[i]:
                    df['long_exit'].iloc[i + 1] = True

            # 更新有持多头以来最高价，并判断是否出场
            else:
                df['lowest_after_entry'].iloc[i] = min(low.iloc[i],
                                                       df['lowest_after_entry'].iloc[i - 1])
                # 平仓日期是计算出平仓信号的后一天
                if (close.iloc[i] > df['lowest_after_entry'].iloc[i] + delta) or df['next_c_chg'].iloc[i]:
                    df['short_exit'].iloc[i + 1] = True
