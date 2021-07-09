import os
from datetime import datetime
from functools import wraps
from collections import defaultdict

import pymongo
import pandas as pd
import matplotlib.pyplot as plt
from consts import MONGODB_ADDR


def has_column(df: pd.DataFrame, *columns):
    """检查df中是否包含名为columns的列"""
    return set(columns) <= set(df.columns)


def must_have_col(*columns):
    """
    在运行函数前检查df中是否包含columns列．
    尽管pandas自然会在发现缺失列时报错，使用此装饰器可以显式
    地表明函数依赖的字段，并避免在完成某些耗时的运算后才报错．
    """
    def decorator(f):
        @wraps(f)
        def decorated(df, *args, **kwargs):
            if not has_column(df, *columns):
                missing_cols = set(columns) - set(df.columns)
                raise KeyError(f"dataframe does not have column {', '.join(missing_cols)}")
            return f(df, *args, **kwargs)
        return decorated
    return decorator


def mkdirs(path: str):
    """创建路径为path的目录"""
    if not os.path.exists(path):
        os.makedirs(path)


def get_trading_days(data: pd.DataFrame):
    """
    获得全部交易日期。
    params:
        - data：所有品种完整的行情序列。需包括交易日期字段datetime。
    """
    return sorted([datetime.fromtimestamp(ts/1e9 - 8*3600).date()
                   for ts in data.datetime.unique().tolist()])


def expand(ts: pd.DataFrame, expand_range: pd.DataFrame):
    """
    将一段时间序列扩展到所有交易日。
    params:
        - ts: 被扩展的序列。
        - expand_ts: 要扩展到的交易日序列。
    """
    return expand_range.join(ts)


def next_is_diff(s: pd.Series) -> pd.Series:
    """判断序列下一行的值是否与当前行不同，是则当前行为True，否则为False"""
    return s != s.shift(-1).ffill()


def pre_is_diff(s: pd.Series) -> pd.Series:
    """判断序列上一行的值是否与当前行不同，是则当前行为True，否则为False"""
    return s != s.shift(1).bfill()


def query(db_name, col, addr=MONGODB_ADDR, **kwargs):
    """
    根据回测参数返回对应mongodb文档的指针。
    params:
        - db_name: 数据库名称
        - col: 数据集名称
        - addr: mongodb地址
        - kwargs: 回测参数名及取值，参数必须在params字段中
    """
    params_dict = {f'params.{k}': v for k, v in kwargs.items()}
    client = pymongo.MongoClient(addr)
    db = client[db_name]
    return db[col].find(params_dict)


def save_results(results, db_name, col_name, addr=MONGODB_ADDR):
    """
    将回测结果保存到mongodb.
    params:
        - results:
        - db_name: 数据库名称
        - col: 数据集名称
        - addr: mongodb地址
    """
    client = pymongo.MongoClient(addr)
    db = client[db_name]
    col = db[col_name]
    col.insert_many(results)


def plot_curves(curves: list, title: str, legends: list):
    """
    在同一张图上作多条曲线。
    params:
        - curves: 曲线值列表
        - title: 图标题
        - legends: 曲线图例
    """
    fig = plt.figure(figsize=(16, 9))
    plt.title(title)
    for curve in curves:
        plt.plot(curve)
    plt.legend(legends)


def select_by_quantile(sequence, q1, q2):
    """截取序列q1分位数到q2分位数上的元素"""
    length = len(sequence)
    start_idx = int(length * q1)
    end_idx = int(length * q2)
    return sequence[start_idx: end_idx]


