import os
from datetime import datetime
from functools import wraps

import pymongo
import pandas as pd
import matplotlib.pyplot as plt

from consts import MONGODB_CLIENT


def has_column(df: pd.DataFrame, *columns):
    """检查df中是否包含名为columns的列"""
    return set(columns) <= set(df.columns)


def must_have_col(*columns):
    """在运行函数前检查df中是否包含columns列"""
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


def query(db_name, col, client_name=MONGODB_CLIENT, **kwargs):
    params_dict = {f'params.{k}': v for k, v in kwargs.items()}
    client = pymongo.MongoClient(client_name)
    db = client[db_name]
    return db[col].find(params_dict)


def save_results(results, db_name, col_name, client_name=MONGODB_CLIENT):
    client = pymongo.MongoClient(client_name)
    db = client[db_name]
    col = db[col_name]
    col.insert_many(results)


def plot_curves(curves, title, legends):
    fig = plt.figure(figsize=(16, 9))
    plt.title(title)
    for curve in curves:
        plt.plot(curve)
    plt.legend(legends)
