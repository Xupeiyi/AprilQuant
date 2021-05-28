from collections import OrderedDict

import backtrader as bt
import pandas as pd


class CommodityPandasData(bt.feeds.PandasData):
    lines = ('longgo', 'shortgo',
             'long_exit', 'short_exit',
             'next_c_chg', 'c_chg')

    params = dict(
        longgo='longgo',
        shortgo='shortgo',
        long_exit='long_exit',
        short_exit='short_exit',
        c_chg='c_chg',
        next_c_chg='next_c_chg'
    )


class Strategy(bt.Strategy):
    """
    策略根据数据中的longgo, shortgo, long_exit, short_exit, next_c_chg, c_chg
    执行开仓，平仓，换仓的操作。为了能在同一根bar的开盘和收盘时都进行交易，将
    一根OHLC的bar拆分为OHLX和CCCC。在bar OHLX上完成开仓、平仓的操作。在bar CCCC
    上完成旧合约平仓的操作，并在下一根OHLX bar上换回价值相近的新合约。在bar OHLX
    上只能完成开仓、平仓、换回新合约中的一种操作，且开/平仓优先于换回新合约。
    即如果在前一天收盘时平仓A元的多头合约（此时将self.chg_value置为-A），并根
    据开仓信号在第二天开盘时开价值B元的空头合约，则在第二天开空头仓位后无需再
    开A元的多头仓位（此时将self.chg_value置0）。

    properties:
        - position_direction: 当前开仓方向，只能在发出开仓、平仓指令后更改。
                              1为多头, -1为空头，0为无头寸。
                              用额外的变量记录当前仓位方向是为了防止以下情况发生：
                              在拥有多头仓位时，因为更换合约使当前仓位暂时为0，导
                              致再次触发开多头仓位的指令（新开的仓位价值可能与原
                              持仓价值不同）。因此，需要知悉尽管当前持仓为0，但当
                              前仍处于多头中。

        - chg_value: 更换合约前持仓的价值，更换合约后应尽量仍持有同样多的价值。
    """
    params = dict(printlog=False, hand=1)

    def __init__(self):
        self.position_direction = 0
        self.chg_value = 0

    def log(self, txt, dt=None, doprint=False):
        """记录每个操作发生的日期"""
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def size_of_value(self, value, rounding=int):
        """
        计算value资金能交易多少单位。
        params：
            - value: 资金数量。
            - rounding：舍入方法。开仓操作使用向下舍入(int)，换仓操作使用四舍五入（rounding）。
        """
        lots = rounding(value / (self.datas[0].open[0] * self.params.hand))  # 开仓手数
        size = max(lots, 1) * self.params.hand  # 实际交易单位
        return size

    def notify_order(self, order):
        """订单状态发生变化时的回调函数。"""
        if order.status in [order.Submitted, order.Accepted]:
            return

        elif order.status in [order.Completed]:
            self.log(f'订单 {order.ref} 执行价格为 {order.executed.price} 数量为 {order.executed.size}' +
                     f'手续费为{order.executed.comm}')

            if order.info.chg_open:
                self.chg_value = order.executed.price * order.executed.size
                self.log(f'换仓订单 {order.ref} 价值:{order.executed.price * order.executed.size}, ' \
                         + f'将 chg_value 设为 {self.chg_value}')
            elif order.info.chg_close:
                self.chg_value = 0
                self.log(f'换仓订单 {order.ref} 价值:{order.executed.price * order.executed.size}, ' \
                         + f'将 chg_value 设为 {self.chg_value}')

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            status = {order.Canceled: '被取消',
                      order.Margin: '保证金不足',
                      order.Rejected: '被拒绝'}
            self.log(f'Order {order.ref} {status[order.status]}')

    def next_open(self):
        """在开盘时调用。发出的交易指令在当前bar完成。"""
        # 1.开仓=================
        # 平空开多
        if self.datas[0].longgo and self.position_direction <= 0:

            if self.chg_value != 0:
                self.chg_value = 0

            size = self.size_of_value(self.broker.getvalue() * 0.98)
            order = self.buy(size=size - self.position.size, coc=False)
            self.position_direction = 1

            self.log(f'创建开仓订单 {order.ref} 将仓位买至 {size}, 买入 {size - self.position.size}')

        # 平多开空
        elif self.datas[0].shortgo and self.position_direction >= 0:

            if self.chg_value != 0:
                self.chg_value = 0

            size = self.size_of_value(self.broker.getvalue() * 0.98)
            order = self.sell(size=size + self.position.size, coc=False)
            self.position_direction = -1

            self.log(f'创建开仓订单 {order.ref} 将仓位卖至 {-size}, 卖出 {size + self.position.size}')

        # 2.平仓=================
        # 平多
        if self.datas[0].long_exit and self.position_direction > 0:

            if self.chg_value != 0:
                self.chg_value = 0
                self.log(f'平多头')
            else:
                order = self.sell(size=self.position.size, coc=False)
                if order is not None:
                    self.log(f'创建平仓订单 {order.ref} 平多头')

            self.position_direction = 0

        # 平空
        elif self.datas[0].short_exit and self.position_direction < 0:

            if self.chg_value != 0:
                self.chg_value = 0
                self.log(f'平空头')
            else:
                order = self.buy(size=-self.position.size, coc=False)
                if order is not None:
                    self.log(f'创建平仓订单 {order.ref} 平空头')

            self.position_direction = 0

        # 3.换仓=================
        # 开盘时另一合约开仓
        if self.datas[0].c_chg and self.position.size == 0 and self.chg_value != 0:

            if self.chg_value < 0:
                size = self.size_of_value(-self.chg_value, round)
                order = self.buy(size=size, coc=False)
                order.addinfo(chg_close=True)

                self.log(f'更换合约，另一合约开仓，创建订单 {order.ref} 买入 {size}')

            elif self.chg_value > 0:
                size = self.size_of_value(self.chg_value, round)
                order = self.sell(size=size, coc=False)
                order.addinfo(chg_close=True)

                self.log(f'更换合约，另一合约开仓，创建订单 {order.ref} 卖出 {size}')

    def next(self):
        """
        在结束时调用，发出的交易指令在下一个bar完成。
        对于平仓旧合约，是在bar OHLX结束时发出指令，在bar CCCC结束时完成指令。
        完成指令后chg_value非0，因此不会在bar CCCC结束时再次发出平仓旧合约指令。
        """
        # 3.换仓=================
        # 收盘时此合约平仓
        if self.datas[0].next_c_chg and self.position.size != 0 and self.chg_value == 0:

            if self.position.size > 0:
                order = self.sell(size=self.position.size, coc=True, exectype=bt.Order.Close)
                order.addinfo(chg_open=True)

                self.log(f'更换合约，当前合约平仓，创建订单 {order.ref} 卖出 {self.position.size}')

            elif self.position.size < 0:
                order = self.buy(size=-self.position.size, coc=True, exectype=bt.Order.Close)
                order.addinfo(chg_open=True)

                self.log(f'更换合约，当前合约平仓，创建订单 {order.ref} 买入 {-self.position.size}')

    def stop(self):
        self.log('账户价值 %.2f' % self.broker.getvalue())


class CumReturn(bt.Analyzer):
    """记录每一期的累计收益率"""

    def __init__(self):
        self.cum_rets = OrderedDict()
        self.prev_value = self.strategy.broker.getvalue()
        self.prev_cum_ret = 1
        self.time = None

    def next(self):
        # 在bar开始时记录时间
        if len(self) % 2 == 1:
            self.time = self.datas[0].datetime.datetime(0)
        else:
            cur_value = self.strategy.broker.getvalue()
            cur_cum_ret = self.prev_cum_ret * (cur_value / self.prev_value)
            self.cum_rets[self.time] = cur_cum_ret
            self.prev_value = cur_value
            self.prev_cum_ret = cur_cum_ret

    def get_analysis(self):
        return pd.DataFrame(index=self.cum_rets.keys(), data={'cum_ret': self.cum_rets.values()})


def run_bt_backtest(df, printlog=False, cash=2000000, commission=0.0001):
    """生成回测对象. 目前只对日级数据有效。"""

    cerebro = bt.Cerebro(runonce=False, cheat_on_open=True)

    # 添加策略
    cerebro.addstrategy(Strategy, printlog=printlog)

    # 添加数据
    datafeed = CommodityPandasData(dataname=df, datetime='datetime',
                                   open='open', close='close',
                                   high='high', low='low',
                                   volume='volume', openinterest=-1, )
    datafeed.addfilter(bt.filters.DaySplitter_Close)  # 关键步骤，将一根bar拆分为两根
    cerebro.replaydata(datafeed)

    # 设置交易参数
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission)
    cerebro.broker.set_coc(True)  # 允许以收盘价交易

    # 添加分析工具
    cerebro.addanalyzer(CumReturn, _name='CumReturn')

    results = cerebro.run()
    cum_ret = results[0].analyzers.CumReturn.get_analysis()
    return cum_ret


if __name__ == '__main__':
    df = pd.read_csv('../cache/A/0.csv', parse_dates=['datetime'])
    lg = [0] * len(df)
    le = [0] * len(df)
    lg[2] = 1
    le[20] = 1
    df['longgo'] = lg
    df['shortgo'] = 0
    df['long_exit'] = le
    df['short_exit'] = 0
    run_bt_backtest(df)