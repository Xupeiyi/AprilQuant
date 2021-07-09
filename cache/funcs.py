import re

from utils import must_have_col, next_is_diff, pre_is_diff


def get_commodity_category(code: str):
    """
    提取期货合约代码中的英文字母，作为商品种类。
    对于更换过代码的品种，将数据中的旧代码更改为新代码。
    params:
        - code: 期货合约代码。
    """
    category = re.match('[A-Za-z]+', code).group().upper()
    change = {'ER': 'RI', 'RO': 'OI', 'WS': 'WH',
              'ME': 'MA', 'TC': 'ZC', 'WT': 'PM'}
    if category in change:
        category = change[category]
    return category


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
    在df中添加前复权价格。前复权价格以‘adjusted_’开头
    params:
        - df: 一段连续的行情数据。需要行情中包括open, close, high, low, preclose。
    """
    for price in ['preclose', 'close']:
        if 0 in df[price].values:
            raise ZeroDivisionError(f"{price}字段中存在0！")

    # 计算复权因子
    adjust_factor = (df['close'] / df['preclose']).cumprod()

    # 计算前复权收盘价
    df['adjusted_close'] = adjust_factor * (df['close'].iloc[0] / adjust_factor.iloc[0])

    # 对 open, high, low 以同比例放缩
    r = df['adjusted_close'] / df['close']
    for price in ['open', 'high', 'low']:
        df['adjusted_' + price] = df[price] * r
