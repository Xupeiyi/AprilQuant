import pandas as pd


def if_contract_is_active(original_df, period=3, threshold=2e7):
    """判断合约成交额是否连续period天大于threshold"""
    df = original_df.copy()
    df = df.sort_values('datetime', ascending=True)
    is_active_series = df.set_index('datetime').groupby('code')\
                                               .volume_value.rolling(period)\
                                               .apply(lambda x: (x > threshold).all())\
                                               .fillna(0).astype('bool')
    is_active_series.name = 'is_active'
    return is_active_series


def select_contract(all_contract_df, nearest=40):
    chosen = set()
    selected = []
    current_code = None
    for date, daily_df in all_contract_df.groupby('datetime'):
        # 若已选择合约, 检查是否应该被切换
        if current_code is not None:
            curr_code_info = daily_df[daily_df.code == current_code]
            if curr_code_info.inactive.values[0] or curr_code_info.to_deliv_date.values[0] <= nearest:
                current_code = None

        # 若未选择合约, 检查是否有可以选择的合约
        if current_code is None:
            can_be_selected = daily_df[(daily_df.to_deliv_date > nearest)
                                       & daily_df.active
                                       & (~daily_df.code.isin(chosen))]
            if len(can_be_selected):
                # 选择最接近到期的合约
                current_code = can_be_selected.iloc[can_be_selected.to_deliv_date.argmin()].code
                chosen.add(current_code)

        selected.append((date, current_code))

    selected = pd.DataFrame(selected, columns=['datetime', 'code'])
    return selected
