import os
from datetime import datetime

import pymongo
import pandas as pd


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


# def query0(db, col, category=None, length=60, ema_length=150, trs=0.12, lqk_width=0.1, lqk_floor=0.5):
#     params_dict = dict()
#     if category is not None:
#         params_dict['params.data_label.category'] = category
#     params_dict['params.enter_signal'] = {
#         'length': length,
#         'ema_length': ema_length
#     }
#     params_dict['params.exit_signal'] = {
#         'trs': trs,
#         'lqk_width': lqk_width,
#         'lqk_floor': lqk_floor
#     }
#     return db[col].find(params_dict)


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
