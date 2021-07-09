import os
import pandas as pd
from collections import UserDict, defaultdict

_current_path = os.path.dirname(os.path.realpath(__file__))
CACHE_ROOT_DIR = _current_path + '\\cache\\'
DATA_DAILY_DIR = _current_path + '\\data\\daily\\'
DATA_MINUTE_DIR = _current_path + '\\data\\minute\\'
C_DAILY = os.listdir(CACHE_ROOT_DIR + 'daily')
C_15MIN = os.listdir(CACHE_ROOT_DIR + '15min')
C_30MIN = os.listdir(CACHE_ROOT_DIR + '30min')
MONGODB_ADDR = "mongodb://localhost:27017/"


def read_cache(level):
    """
    从cache目录加载level数据集．
    """
    backtest_data = defaultdict(list)

    level_root_path = CACHE_ROOT_DIR + f'{level}\\'
    categories = os.listdir(level_root_path)

    for category in categories:
        category_path = level_root_path + f'{category}\\'
        csv_files = os.listdir(category_path)
        for file in csv_files:
            df = pd.read_csv(category_path + file, parse_dates=['datetime'])
            backtest_data[category].append(df)
    return backtest_data


class CacheDict(UserDict):

    def __getitem__(self, key):
        if key not in self.data:
            self.__setitem__(key, read_cache(key))
        return self.data[key]


CACHE = CacheDict()

