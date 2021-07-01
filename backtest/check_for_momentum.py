import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from utils import query
from backtest.return_rate_calculations import cum_ret_from_doc


def cal_hist_and_pred_ret(cum_ret, hist_len, pred_len):
    """
    计算过去hist_len窗宽的收益率（不含当期）
    与未来pred_len窗宽的收益率（含当期）。
    """
    df = pd.DataFrame(index=cum_ret.index)
    df['hist_ret'] = cum_ret.shift(1) / cum_ret.shift(hist_len) - 1
    df['pred_ret'] = cum_ret.shift(-(pred_len - 1)) / cum_ret - 1
    df = df.dropna()
    return df.hist_ret, df.pred_ret


def cal_correlation(hist_ret, pred_ret):
    """计算历史收益率与未来收益率相关性。"""
    if len(hist_ret) < 2:
        return None, None
    corr, p_value = spearmanr(hist_ret, pred_ret)
    return corr, p_value


def cal_pred_win_rate(hist_ret, pred_ret):
    """计算预测胜率。"""
    if not len(hist_ret):
        return None
    equal_sign = (np.sign(hist_ret) == np.sign(pred_ret))
    return len(equal_sign[equal_sign]) / len(equal_sign)


def successful_win_rate_prediction(df):
    """预测胜率高于50%的比例。"""
    return len(df[df.pred_win_rate > 0.5]) / len(df)


def sig_pos_related(df):
    """显著正相关：相关系数大于0且P值小于0.1的比例。"""
    return len(df[(df['corr'] > 0) & (df.p_value < 0.1)]) / len(df)


def sig_neg_related(df):
    """显著负相关：相关系数小于0且P值小于0.1的比例。"""
    return len(df[(df['corr'] < 0) & (df.p_value < 0.1)]) / len(df)


def get_cum_ret_dict(db_name, col, **kwargs):
    """获取每次回测的累计收益率。各品种在不同的回测时期上算作不同的品种。"""
    cum_ret_dict = {}
    cursor = query(db_name=db_name, col=col, **kwargs)
    for doc in cursor:
        c = doc['params']['category']
        i = doc['params']['idx']
        cr = cum_ret_from_doc(doc)
        if len(cr) > 0:
            cum_ret_dict[f'{c}-{i}'] = cr
    return cum_ret_dict


def cal_category_tendency(cum_ret_dict, hist_len, pred_len):
    """计算各品种收益的趋势性。"""
    tendency_df = pd.DataFrame(index=cum_ret_dict.keys())

    for c, cum_ret in cum_ret_dict.items():
        hist_ret, pred_ret = cal_hist_and_pred_ret(cum_ret.cum_ret, hist_len, pred_len)
        corr, p_value = cal_correlation(hist_ret, pred_ret)
        pred_win_rate = cal_pred_win_rate(hist_ret, pred_ret)

        tendency_df.loc[c, 'corr'] = corr
        tendency_df.loc[c, 'p_value'] = p_value
        tendency_df.loc[c, 'pred_win_rate'] = pred_win_rate

    tendency_df = tendency_df.dropna()
    return tendency_df


def cal_strategy_overall_tendency(cum_ret_dict, hist_len, pred_len):
    """计算策略总体的趋势性。"""

    overall_tendency = dict(hist_len=hist_len, pred_len=pred_len)
    tendency_df = cal_category_tendency(cum_ret_dict, hist_len, pred_len)
    overall_tendency['category_num'] = len(tendency_df)
    overall_tendency['sig_pos_related'] = sig_pos_related(tendency_df)
    overall_tendency['sig_neg_related'] = sig_neg_related(tendency_df)
    overall_tendency['successful_prediction'] = successful_win_rate_prediction(tendency_df)
    return overall_tendency
