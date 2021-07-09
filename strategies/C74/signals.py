from backtest.tester import Tester
from backtest.signals import add_chandelier_exit_signal, add_avg_daily_last_adjusted_close, add_position_direction
from backtest.indicators import EMA, ATR, TR


def add_enter_signal(df, date_length, ma_length=12, eatr_pcnt=3):

    add_avg_daily_last_adjusted_close(df, date_length)

    close = df.adjusted_close
    high = df.adjusted_high
    low = df.adjusted_low
    
    df['dea'] = EMA(EMA(close, ma_length) - EMA(close, ma_length * 2), ma_length * 4)
    df['up_trend'] = df.dea > 0
    df['dn_trend'] = df.dea < 0
    df['dea_up_cross_0'] = ~df.up_trend.shift(1).fillna(False) & df.up_trend  # dea上穿0轴
    df['dea_dn_cross_0'] = ~df.dn_trend.shift(1).fillna(False) & df.dn_trend  # dea下穿0轴
    df['tr'] = TR(high, low, close)
    df['atr'] = ATR(high, low, close, ma_length * 2)

    df.loc[df.dea_up_cross_0, 'upper_band'] = df.loc[df.dea_up_cross_0].eval('adjusted_high + @eatr_pcnt * atr')
    df.loc[df.dea_dn_cross_0, 'lower_band'] = df.loc[df.dea_dn_cross_0].eval('adjusted_low - @eatr_pcnt * atr')
    df[['upper_band', 'lower_band']] = df[['upper_band', 'lower_band']].fillna(method='ffill')

    df['longgo'] = (
            df.up_trend
            & (df.adjusted_close > df.avg_adjusted_close)
            & (df.adjusted_high >= df.upper_band)
    ).shift(1).fillna(False)

    df['shortgo'] = (
            df.dn_trend
            & (df.adjusted_close < df.avg_adjusted_close)
            & (df.adjusted_low <= df.lower_band)
    ).shift(1).fillna(False)


def add_exit_signal(df, xatr_pcnt=3):
    df.loc[df.dea_up_cross_0, 'exit_band_d'] = df.loc[df.dea_up_cross_0].eval('adjusted_low - @xatr_pcnt * atr')
    df.loc[df.dea_dn_cross_0, 'exit_band_k'] = df.loc[df.dea_dn_cross_0].eval('adjusted_high + @xatr_pcnt * atr')
    df[['exit_band_d', 'exit_band_k']] = df[['exit_band_d', 'exit_band_k']].fillna(method='ffill')
    df['long_exit'] = (df.adjusted_low <= df.exit_band_d).shift(1).fillna(False)
    df['short_exit'] = (df.adjusted_high >= df.exit_band_k).shift(1).fillna(False)


class C74Tester(Tester):

    def add_signals(self):
        add_enter_signal(self.df,
                         date_length=self.params['date_length'],
                         ma_length=self.params['ma_length'],
                         eatr_pcnt=self.params['eatr_pcnt'])
        add_exit_signal(self.df, xatr_pcnt=self.params['eatr_pcnt'])  # 为了减少依赖的变量，xatr_pcnt取值与eatr_pcnt相同
        add_chandelier_exit_signal(self.df,
                                   trs=self.params['trs'],
                                   lqk_width=self.params['lqk_width'],
                                   lqk_floor=self.params['lqk_floor'])
        add_position_direction(self.df)
