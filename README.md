# AprilQuant: 基于Pandas和Mongodb的期货策略回测框架


## 使用步骤
数据流动：  
```html
原始行情数据(data目录)  
==(步骤1)==>&emsp;缓存行情数据(cache目录)   
==(步骤2)==>&emsp;添加交易信号(signals)  
==(步骤3)==>&emsp;运行回测(Tester)  
==(步骤4)==>&emsp;Mongodb数据库  
==(步骤5)==>&emsp;研究者
```
### 1.1 步骤1: 创建数据缓存
**相关代码： data目录， cache目录**

在步骤1完成数据清洗工作，包括标明合约品种，合成更长时间周期数据，筛选出流动性较高的行情，添加合约换月字段，
前复权价格字段等, 避免回测时重复计算。  
相关工作在cache目录下完成，并在cache目录下创建缓存。   
原始数据行情应包括字段：
- open, high, low, close: 真实价格
- volume: 成交量
- deliv_mon: 交割月份
- code: 期货合约代码
- datetime: 产生行情的时间（收盘价也得到确认的时间)
- tradingday: 交易日（对分钟级数据）

清洗后的数据应新增字段：
- category: 合约品种
- next_c_chg, c_chg：bool型，表示下一根bar合约是否换月，当前bar合约是否换月.该字段用于在回测时计算交易费用
- adjusted_open, adjusted_high, adjusted_low, adjusted_close: 前复权价格，该字段用于计算交易信号

清洗后的应保存在cache/{level}/{idx}.csv中，其中level代表数据时间周期，如"daily"和“15min"，
'idx'表示这是该品种的第几段行情（比如，以流动性为筛选标准，可能筛选出多段行情）。
缓存完成后cache目录的结构应类似于：
```html
cache  
|-- 30min  
|-- daily  
|-- 15min  
&emsp;&emsp;&emsp;&emsp;|-- A  
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;|-- 0.csv  
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;|-- 1.csv  
&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;|-- ...  
&emsp;&emsp;&emsp;&emsp;|-- AL   
&emsp;&emsp;&emsp;&emsp;|-- ...   
``` 

### 1.2 步骤2: 添加开平仓信号、头寸方向
**相关代码：strategies/{strategy_name}/signals.py，backtest/signals.py，backtest/indicators.py**  

根据策略的逻辑在一段清洗后的行情中加入longgo（多头开仓）, long_exit（多头平仓）, 
shortgo（空头开仓）, short_exit（空头平仓）信号字段。交易信号标记在满足交易条件的bar的后一根。

e.g. strategies/C73/signals.py
```python
def add_enter_signal(df, length=60, ema_length=150):
    """
    在df中标记开仓日期。非纯函数。df为缓存的行情数据。
    ...
    """
    # 开仓信号指标使用复权价格生成。
    df['recent_high'] = HHV(df.adjusted_high, length)
    ...
    df['avg_high_low'] = (df.adjusted_high + df.adjusted_low) * 0.5
    df['cci'] = CCI(df.adjusted_high, df.adjusted_low, df.adjusted_close, length)
    df['cci_ema'] = EMA(df.cci, timeperiod=ema_length)

    # 标记开仓信号longgo，信号标记于满足开仓条件的后一根bar
    df['longgo'] = (
            (df.adjusted_close > df.recent_high.shift(1))
            & (df.cci_ema > 0)
            & (df.avg_high_low > df.adjusted_high.shift(1))
    )
    df['longgo'] = df['longgo'].shift(1).fillna(False)
    ...
```
此处使用了backtest/indicators.py中定义的技术指标计算方法。indicators.py中的函数用装饰器not_full_of_na检查
技术指标计算是否返回一列NA，从而检查行情长度是否足够计算技术指标并产生交易信号。

backtest/signals.py提供了一些通用的止损信号添加函数，如吊灯线止损方法，以及一些向行情序列中添加额外字段的非纯函数。
utils.py提供了装饰器must_have_col,用于在添加信号前检查行情序列中是否存在需要的字段。  



### 1.3 步骤3: 运行回测；步骤四：保存结果
**相关代码：backtest/tester.py, backtest/parallel.py**  

创建Tester子类(命名规则：策略名＋Tester)，在add_signals方法下调用交易信号生成函数， 并用add_position_direction标记头寸方向
(在今后的开发中可考虑将add_position_direction写入Tester基类，避免重复调用)

```python
class C73Tester(Tester):

    def add_signals(self):
        params = self.params
        add_enter_signal(self.df, length=params['length'], ema_length=params['ema_length'])
        add_chandelier_exit_signal(self.df,
                                   trs=params['trs'],
                                   lqk_width=params['lqk_width'],
                                   lqk_floor=params['lqk_floor'])
        add_position_direction(self.df)
```

1. 回测参数是单个值
通过赋值Tester类的params属性设定回测需要的参数．
```python
from strategies.C73.signals import C73Tester
from utils import save_results

LEVEL = 'daily'
C73Tester.read_cache(LEVEL) #使用类方法读入日线缓存数据，做为类属性


if __name__ == '__main__':
    params = {
        'category': 'J', 'idx': 0,   # 使用数据：J品种第0段行情
        'length': 60, 'ema_length': 150,  # 开仓信号计算参数
        'trs': 0.12, 'lqk_width': 0.1, 'lqk_floor': 0.5  # 止损信号计算参数
    }
    tester = C73Tester(params=params)
    result = tester.test()
    save_results([result], db_name='testbase', col_name=LEVEL)  # 将回测结果写入mongodb
```
在mongodb的testbase数据库daily数据集中回测结果被表示为：
```json
{
  "_id": "60e555e43aa39a78620773e5",
  "params": {
    "category": "J",
    "idx": 0,
    "length": 60,
    "ema_length": 150,
    "trs": 0.12,
    "lqk_width": 0.1,
    "lqk_floor": 0.5
  },
  "cum_ret": {
    "datetime": [...],
    "cum_ret": [...]
  },
  "sharpe_ratio": 1.1920331876114225
}
```
2. 回测参数具有取值范围  
   可以使用backtest/parallel.py提供的test_by_params_range并行执行回测。该函数接受一个Tester类，数据集名称
   以及各个参数的取值范围．回测完成后，结果将被存入mongodb
```python
...
if __name__ == '__main__':
    test_by_params_range(C73Tester, level='daily',  # 回测结果保存到C73数据库daily数据集中 
                         # 回测300次保存一次数据， 使用16个cpu
                         batch=300, max_workers=16,
                         # 不传入category参数时则对daily数据集下所有品种进行回测
                         category=('I', 'J', 'RB'),  
                         length=(20, 30, 40, 50), 
                         ema_length=(50, 100, 150, 200), 
                         trs=(0.12,), lqk_width=(0.1,), lqk_floor=(0.5,))
```

### 1.4步骤五：读取回测结果并分析
**相关代码：utils.py, backtest/return_rate_calculations.py, backtest/check_for_momentum.py, .\*ipynb**

utils.py提供了函数query用于根据回测参数返回对应mongodb文档的指针(pymongo.cursor.Cursor)．比如
```python
query(db_name='C73', col='daily', length=20, trs=0.12)
```
返回指向C73数据库daily数据集中所有回测参数length为20， trs为0.12的回测结果文档的指针。
backtest/return_rate_calculations.py提供了为了从mongodb文档中取出净值曲线的方法
cum_ret_from_doc, 以及其他对静止曲线进行计算的方法,详见函数文档。

各策略目录下的jupyter notebook文件研究了策略的收益情况，动态选择品种和参数对收益的改善情况，已经策略趋势性的分析．



