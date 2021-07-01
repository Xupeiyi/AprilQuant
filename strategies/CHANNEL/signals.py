from backtest.indicators import HHV, LLV, MA
from backtest.signals import add_atr_exit_signal, add_position_direction
from backtest.tester import Tester


def add_enter_signal(df, recent, short_length, long_length):
    close = df.adjusted_close
    stma = MA(close, short_length)
    ltma = MA(close, long_length)
    df['longgo'] = ((close > HHV(close, recent).shift(1)) & (stma > ltma)).shift(1)
    df['shortgo'] = ((close < LLV(close, recent).shift(1)) & (stma < ltma)).shift(1)


def add_exit_signal(df, atr_length, trs):
    add_atr_exit_signal(df, atr_length, trs)
    df['long_exit'] = df['long_exit'] | df['next_c_chg']
    df['short_exit'] = df['short_exit'] | df['next_c_chg']


class CHANNELTester(Tester):

    def __init__(self, params, df=None):
        super().__init__(params, df)

        if "use_real_price" not in self.params:
            raise ValueError("Please specify whether real price or adjusted price should be used!")

        if self.params['use_real_price']:
            self.df[['adjusted_open', 'adjusted_close', 'adjusted_high', 'adjusted_low']] = \
                self.df[['open', 'close', 'high', 'low']]

    def add_signals(self):
        add_enter_signal(self.df,
                         recent=self.params['recent'],
                         short_length=self.params['short_length'],
                         long_length=self.params['long_length'])
        add_exit_signal(self.df,
                        atr_length=self.params['atr_length'],
                        trs=self.params['trs'])
        add_position_direction(self.df)
