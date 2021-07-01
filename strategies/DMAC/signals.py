from backtest.indicators import MA
from backtest.signals import add_atr_exit_signal, add_position_direction
from backtest.tester import Tester


def add_enter_signal(df, short_length, long_length, break_in):
    close = df.adjusted_close

    short_ma = MA(close, short_length)
    long_ma = MA(close, long_length)

    df['longgo'] = (short_ma > (1 + break_in)*long_ma).shift(1)
    df['shortgo'] = (short_ma < (1 - break_in)*long_ma).shift(1)


def add_exit_signal(df, atr_length, trs):
    add_atr_exit_signal(df, atr_length, trs)
    df['long_exit'] = df['long_exit'] | df['next_c_chg']
    df['short_exit'] = df['short_exit'] | df['next_c_chg']


class DMACTester(Tester):

    def __init__(self, params, df=None):
        super().__init__(params, df)

        if "use_real_price" not in self.params:
            raise ValueError("Please specify whether real price or adjusted price should be used!")

        if self.params['use_real_price']:
            self.df[['adjusted_open', 'adjusted_close', 'adjusted_high', 'adjusted_low']] = \
                self.df[['open', 'close', 'high', 'low']]

    def add_signals(self):
        add_enter_signal(self.df,
                         short_length=self.params['short_length'],
                         long_length=self.params['long_length'],
                         break_in=self.params['break_in'])
        add_exit_signal(self.df,
                        atr_length=self.params['atr_length'],
                        trs=self.params['trs'])
        add_position_direction(self.df)
