import os

_current_path = os.path.dirname(os.path.realpath(__file__))
CACHE_ROOT_DIR = _current_path + '\\cache\\'
DATA_DAILY_DIR = _current_path + '\\data\\daily\\'
DATA_MINUTE_DIR = _current_path + '\\data\\minute\\'
C_DAILY = os.listdir(CACHE_ROOT_DIR + 'daily')
C_15MIN = os.listdir(CACHE_ROOT_DIR + '15min')
C_30MIN = os.listdir(CACHE_ROOT_DIR + '30min')
MONGODB_CLIENT = "mongodb://localhost:27017/"
