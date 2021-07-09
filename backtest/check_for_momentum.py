"""
动量效应分析

参考文献：https://mp.weixin.qq.com/s/Mhu2SB6AMzga_Y1pvwM6PQ
"""
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from utils import query
from backtest.return_rate_calculations import cum_ret_from_doc
from backtest.indicators import ATR, AllNaError
from consts import CACHE


def cal_hist_and_pred_ret(cum_ret, hist_len: int, pred_len: int):
    """
    计算过去hist_len窗宽的收益率（不含当期）
    与未来pred_len窗宽的收益率（含当期）。
    """
    df = pd.DataFrame(index=cum_ret.index)
    df['hist_ret'] = cum_ret.shift(1) / cum_ret.shift(hist_len) - 1
    df['pred_ret'] = cum_ret.shift(-(pred_len - 1)) / cum_ret - 1
    df = df.dropna()
    return df.hist_ret, df.pred_ret


def cal_correlation(hist_ret: pd.Series, pred_ret: pd.Series):
    """计算历史收益率与未来收益率相关性。"""
    if len(hist_ret) < 2:
        return None, None
    corr, p_value = spearmanr(hist_ret, pred_ret)
    return corr, p_value


def cal_pred_win_rate(hist_ret: pd.Series, pred_ret: pd.Series):
    """计算预测胜率。"""
    if not len(hist_ret):
        return None
    sign_is_equal = (np.sign(hist_ret) == np.sign(pred_ret))
    return len(sign_is_equal[sign_is_equal]) / len(sign_is_equal)


def successful_win_rate_prediction(tendency_df):
    """预测胜率高于50%的比例。"""
    return len(tendency_df[tendency_df.pred_win_rate > 0.5]) / len(tendency_df)


def sig_pos_related(tendency_df):
    """显著正相关：相关系数大于0且P值小于0.1的品种的比例。"""
    return len(tendency_df[(tendency_df['corr'] > 0) & (tendency_df.p_value < 0.1)]) / len(tendency_df)


def sig_neg_related(tendency_df):
    """显著负相关：相关系数小于0且P值小于0.1的品种的比例。"""
    return len(tendency_df[(tendency_df['corr'] < 0) & (tendency_df.p_value < 0.1)]) / len(tendency_df)


def get_cum_ret_dict(db_name, col, **kwargs) -> dict:
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


def cal_category_tendency(cum_ret_dict, hist_len, pred_len) -> pd.DataFrame:
    """计算各品种收益的趋势性, 包括历史期收益率与预测期收益率相关系数及其p值, 历史期与预测期收益率同符号比例"""
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
    overall_tendency['category_num'] = len(tendency_df)  # 总品种数量
    overall_tendency['sig_pos_related'] = sig_pos_related(tendency_df)  # 策略在多少品种上有动量效应
    overall_tendency['sig_neg_related'] = sig_neg_related(tendency_df)  # 策略在多少品种上有反转效应
    overall_tendency['successful_prediction'] = successful_win_rate_prediction(tendency_df)  # 有多少品种预测期收益率与历史期方向大体一致
    return overall_tendency


def atr_of_all_categories(level, atr_length):
    """计算所有品种的ATR。"""
    atr_dict = dict()
    for c, df_list in CACHE[level].items():
        for i, df in enumerate(df_list):
            df = df.set_index('datetime')
            try:
                atr = ATR(high=df.adjusted_high, low=df.adjusted_low, close=df.adjusted_close, timeperiod=atr_length)
            except AllNaError:
                continue
            else:
                atr_dict[f'{c}-{i}'] = atr
    return pd.DataFrame(atr_dict)


def ATR_position_sizing(cum_ret_dict, level, atr_length, rebalance_period):
    """
    使用固定比例法为各品种设定权重，并计算加权平均后的累计收益率曲线。
    params:
        - cum_ret_dict: 各品种累计收益率
        - level: 行情数据时间周期
        - atr_length: ATR移动平均长度
        - rebalance_period: 再平衡周期，每隔rebalance_period重新计算权重
    """
    # todo: 修复bug: 目前该函数在分钟级数据上会返回一列NA，原因不明
    # 计算每期持仓权重
    atr_df = atr_of_all_categories(level, atr_length)
    atr_df = atr_df[[c for c in atr_df.columns if c in cum_ret_dict.keys()]]
    weight = (1 / atr_df).apply(lambda x: x / x.sum(), axis=1).fillna(0)

    # 非再平衡时点的持仓权重与上一次再平衡时点保持一致
    rebalance_bar = weight.iloc[::rebalance_period]
    weight.loc[~weight.index.isin(rebalance_bar.index), :] = None
    weight.fillna(method='ffill', inplace=True)

    # 计算加权平均的收益率和累计收益率
    cum_ret_df = pd.DataFrame({k: v.cum_ret for k, v in cum_ret_dict.items()})
    ret_value = (cum_ret_df.diff().fillna(0) * weight).sum(axis=1)
    cost_df = (cum_ret_df.shift(1).fillna(1) * weight).sum(axis=1)
    weighted_ret = ret_value / cost_df
    weighted_cum_ret = (1 + weighted_ret).cumprod()
    return pd.DataFrame(index=weighted_cum_ret.index, data={'cum_ret': weighted_cum_ret.values})