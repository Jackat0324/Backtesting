import pandas as pd
import numpy as np

def calculate_metrics(returns_list):
    """
    與 strategy_gui.py 中 update_summary 完全相同的計算邏輯
    """
    valid_data = pd.Series(returns_list)
    
    if valid_data.empty:
        return None

    avg_ret = valid_data.mean()
    win_data = valid_data[valid_data > 0]
    loss_data = valid_data[valid_data <= 0]
    
    win_rate = len(win_data) / len(valid_data) * 100
    
    # 1. 獲利因子 (Profit Factor)
    total_profit = win_data.sum()
    total_loss = abs(loss_data.sum())
    profit_factor = total_profit / total_loss if total_loss > 0 else (float('inf') if total_profit > 0 else 0)
    
    # 2. 期望值 (Expectancy)
    avg_profit = win_data.mean() if not win_data.empty else 0
    avg_loss = abs(loss_data.mean()) if not loss_data.empty else 0
    loss_rate = len(loss_data) / len(valid_data)
    win_rate_dec = len(win_data) / len(valid_data)
    expectancy = (win_rate_dec * avg_profit) - (loss_rate * avg_loss)
    
    # 3. 最大回撤 (MDD) - 單利累加模式
    # 假設初始資金 100
    equity = 100 + valid_data.cumsum()
    peak = equity.cummax()
    drawdown = (equity - peak) / peak
    mdd = drawdown.min() * 100 if not drawdown.empty else 0
    
    # 4. 最大連續虧損 (Max Consecutive Losses)
    is_loss = valid_data <= 0
    consecutive_losses = is_loss.astype(int).groupby((is_loss != is_loss.shift()).cumsum()).cumsum().max()
    
    return {
        'count': len(valid_data),
        'avg_ret': f"{avg_ret:+.2f}%",
        'win_rate': f"{win_rate:.1f}%",
        'profit_factor': f"{profit_factor:.2f}",
        'expectancy': f"{expectancy:+.2f}%",
        'mdd': f"{mdd:+.2f}%",
        'max_con_losses': int(consecutive_losses)
    }

if __name__ == "__main__":
    # --- 測試案例 ---
    # 10 筆交易報酬
    test_returns = [10, -5, 20, 0, -10, -10, 30, -5, 10, -5]
    
    print("=== 測試輸入數據 ===")
    print(test_returns)
    print("\n=== 手動解析驗證 ===")
    print("1. 總訊號: 10")
    print("2. 贏家 (Ret > 0): [10, 20, 30, 10] -> 4 筆 (勝率 40%)")
    print("3. 輸家 (Ret <= 0): [-5, 0, -10, -10, -5, -5] -> 6 筆")
    print("4. 總獲利: 10+20+30+10 = 70")
    print("5. 總虧損: |-5|+0+|-10|+|-10|+|-5|+|-5| = 35")
    print("6. 獲利因子: 70 / 35 = 2.00")
    print("7. 期望值: (0.4 * 17.5) - (0.6 * 5.83) = 7 - 3.5 = 3.5%")
    print("8. 連盈虧序列: [輸, 贏, 輸, 輸, 輸, 輸, 贏, 輸, 贏, 輸] -> 最長連輸是 3 (由 0, -10, -10 貢獻)")
    print("9. MDD (單利):")
    print("   Equity: [110, 105, 125, 125, 115, 105, 135, 130, 140, 135]")
    print("   Peak:   [110, 110, 125, 125, 125, 125, 135, 135, 140, 140]")
    print("   MaxDD:  (105-125)/125 = -16.0%")
    
    results = calculate_metrics(test_returns)
    
    print("\n=== 程式計算結果 ===")
    for k, v in results.items():
        print(f"{k}: {v}")
    
    # 進行斷言檢查 (簡單版)
    assert results['count'] == 10
    assert results['win_rate'] == "40.0%"
    assert results['profit_factor'] == "2.00"
    assert results['max_con_losses'] == 3
    assert results['mdd'] == "-16.00%"
    
    print("\n[V] 驗證完全正確！程式邏輯與手動解析一致。")
