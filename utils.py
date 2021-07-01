import os
from datetime import datetime

import pymongo
import pandas as pd
import matplotlib.pyplot as plt


def has_column(df, *columns):
    return set(columns) <= set(df.columns)


def mkdirs(path):
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


def query(db_name, col, client_name="mongodb://localhost:27017/", **kwargs):
    params_dict = {f'params.{k}': v for k, v in kwargs.items()}
    client = pymongo.MongoClient(client_name)
    db = client[db_name]
    return db[col].find(params_dict)


def save_results(results, db_name, col_name, client_name="mongodb://localhost:27017/"):
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
