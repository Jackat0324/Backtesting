import pandas as pd
import strategies
import strategy_backtester
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)

def verify_all_rank_strategies():
    print(f"Weekly Rank Strategies: {len(strategies.WEEKLY_STRATEGIES_RANKS)}")
    print(f"Daily Rank Strategies: {len(strategies.DAILY_STRATEGIES_RANKS)}")
    
    bt = strategy_backtester.StrategyBacktester()
    
    # Check Weekly
    missing_w = [s for s in strategies.WEEKLY_STRATEGIES_RANKS if strategies.get_strategy(s) is None]
    if missing_w:
        print(f"FAILED: Missing Weekly strategy definitions for: {missing_w}")
        return False
        
    # Check Daily
    missing_d = [s for s in strategies.DAILY_STRATEGIES_RANKS if strategies.get_strategy(s) is None]
    if missing_d:
        print(f"FAILED: Missing Daily strategy definitions for: {missing_d}")
        return False
        
    print("Testing Daily Rank Scan...")
    try:
        # Run a very minimal scan
        results = bt.run_scan(strategies.DAILY_STRATEGIES_RANKS, start_date='2026-02-15')
        print(f"Daily Scan PASSED. Signals found: {len(results)}")
        
        print("Testing Weekly Rank Scan...")
        results_w = bt.run_weekly_scan(strategies.WEEKLY_STRATEGIES_RANKS, start_date='2026-02-15')
        print(f"Weekly Scan PASSED. Signals found: {len(results_w)}")
        
        return True
    except Exception as e:
        print(f"Scan integration test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if verify_all_rank_strategies():
        print("\nAll rank strategies (Daily & Weekly) verified successfully.")
    else:
        exit(1)
