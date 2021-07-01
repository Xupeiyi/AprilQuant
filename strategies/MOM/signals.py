from backtest.indicators import momentum
from backtest.signals import add_position_direction
from backtest.tester import Tester


def add_enter_signal(df, period):
    """
    基于动量效应的趋势策略。
    - params:
        period: 动量效应形成窗宽
    """

    mom = momentum(df.adjusted_close, period)
    df['longgo'] = (mom > 0).shift(1)
    df['shortgo'] = (mom < 0).shift(1)


def add_exit_signal(df):
    df['long_exit'] = False
    df['short_exit'] = False


class MOMTester(Tester):

    def __init__(self, params, df=None):
        super().__init__(params, df)

        if "use_real_price" not in self.params:
            raise ValueError("Please specify whether real price or adjusted price should be used!")

        if self.params['use_real_price']:
            self.df[['adjusted_open', 'adjusted_close', 'adjusted_high', 'adjusted_low']] = \
                self.df[['open', 'close', 'high', 'low']]

    def add_signals(self):
        add_enter_signal(self.df, period=self.params['period'])
        add_exit_signal(self.df)
        add_position_direction(self.df)
