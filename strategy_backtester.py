import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
import strategies

# 預設資料庫路徑 (假設與 reader.py 同目錄下的 data/twse_data.db)
# 預設資料庫路徑 (假設與 reader.py 同目錄下的 data/twse_data.db)
import sys
def get_base_dir():
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).resolve().parent
    else:
        return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()
DEFAULT_DB_PATH = BASE_DIR / "data" / "twse_data.db"

class StrategyBacktester:
    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH

    def load_data(self, start_date=None, end_date=None):
        """從資料庫讀取股票資料 (增加日期過濾以提升效能)"""
        logging.info(f"Loading data from {self.db_path} (Range: {start_date} ~ {end_date})...")
        if not self.db_path.exists():
            raise FileNotFoundError(f"資料庫不存在: {self.db_path}")
        
        # 連接 DB
        with sqlite3.connect(self.db_path) as conn:
            # 讀取必要欄位，並依照 代號, 日期 排序
            if start_date or end_date:
                query = """
                SELECT 日期, 代號, 名稱, 開盤, 最高, 最低, 收盤
                FROM stock_prices
                WHERE 1=1
                """
                params = []
                if start_date:
                    query += " AND 日期 >= ?"
                    params.append(start_date)
                if end_date:
                    query += " AND 日期 <= ?"
                    params.append(end_date)
                
                query += " ORDER BY 代號, 日期"
                df = pd.read_sql(query, conn, params=params)
            else:
                query = """
                SELECT 日期, 代號, 名稱, 開盤, 最高, 最低, 收盤
                FROM stock_prices
                ORDER BY 代號, 日期
                """
                df = pd.read_sql(query, conn)
            
        # 轉換日期格式
        df['日期'] = pd.to_datetime(df['日期'])
        logging.info(f"Data Loaded: {len(df)} rows.")
        return df

    def run_scan(self, strategy_types, latest_only=False, start_date=None, end_date=None, progress_callback=None):
        """
        執行策略掃描
        :param strategy_types: 策略清單 (List of strings)
        :param latest_only: 是否只看最近一個交易日的訊號
        :param start_date: 回測起始日 ('YYYY-MM-DD')
        :param end_date: 回測結束日 ('YYYY-MM-DD')
        :param progress_callback: callback(current, total)
        """
        if isinstance(strategy_types, str):
            strategy_types = [strategy_types]

        logger = logging.getLogger("Backtester")
        logger.info(f"Running Scan: {strategy_types}, LatestOnly={latest_only}, Range={start_date}-{end_date}")

        # --- 效能優化: 計算所需的最早日期 (Buffer) ---
        buffer_days = 250 # 日線緩衝 250 天以利計算均線
        fetch_start = None
        if latest_only:
            # 僅看最新，抓最近一年資料即可
            fetch_start = (datetime.now() - timedelta(days=buffer_days + 30)).strftime('%Y-%m-%d')
        elif start_date:
            fetch_start = (pd.to_datetime(start_date) - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
            
        df_all = self.load_data(start_date=fetch_start, end_date=end_date)
        all_results = []

        # 針對每一檔股票分組處理
        grouped = df_all.groupby('代號')
        total_stocks = grouped.ngroups
        
        for i, (code, df_stock) in enumerate(grouped):
            if progress_callback:
                progress_callback(i + 1, total_stocks)
                
            df_stock = df_stock.copy().sort_values('日期').reset_index(drop=True)
            
            # 計算移動平均線
            df_stock['MA2'] = df_stock['收盤'].rolling(window=2).mean()
            df_stock['MA5'] = df_stock['收盤'].rolling(window=5).mean()
            df_stock['MA10'] = df_stock['收盤'].rolling(window=10).mean()
            df_stock['MA20'] = df_stock['收盤'].rolling(window=20).mean()
            df_stock['MA60'] = df_stock['收盤'].rolling(window=60).mean()
            
            for strategy_type in strategy_types:
                strategy_obj = strategies.get_strategy(strategy_type)
                if not strategy_obj:
                    logger.warning(f"Strategy {strategy_type} not found.")
                    continue
                
                df_stock['Signal'] = strategy_obj.calculate_signals(df_stock)
                
                signals = df_stock[df_stock['Signal']].copy()
                if signals.empty:
                    continue

                if latest_only:
                    last_day = df_stock.iloc[-1]['日期']
                    recent_signal = signals[signals['日期'] == last_day]
                    if not recent_signal.empty:
                        row = recent_signal.iloc[0]
                        all_results.append({
                            '策略': strategy_type,
                            '代號': code,
                            '名稱': row['名稱'],
                            '訊號日期': row['日期'].strftime('%Y-%m-%d'),
                            '收盤價': row['收盤'],
                            '買入日期': '-',
                            '買入價': '-',
                            '報酬5日': '-', '報酬10日': '-', '報酬20日': '-', '報酬60日': '-'
                        })
                    continue

                if start_date:
                    signals = signals[signals['日期'] >= pd.to_datetime(start_date)]
                if end_date:
                    signals = signals[signals['日期'] <= pd.to_datetime(end_date)]

                for idx, row in signals.iterrows():
                    buy_idx = idx + 1
                    if buy_idx >= len(df_stock):
                        continue
                        
                    buy_row = df_stock.iloc[buy_idx]
                    buy_date = buy_row['日期']
                    buy_price = buy_row['開盤']
                    
                    res = {
                        '策略': strategy_type,
                        '代號': code,
                        '名稱': row['名稱'],
                        '訊號日期': row['日期'].strftime('%Y-%m-%d'),
                        '收盤價': row['收盤'],
                        '買入日期': buy_date.strftime('%Y-%m-%d'),
                        '買入價': buy_price,
                        '報酬5日': self._calc_return(df_stock, buy_idx, 5, buy_price),
                        '報酬10日': self._calc_return(df_stock, buy_idx, 10, buy_price),
                        '報酬20日': self._calc_return(df_stock, buy_idx, 20, buy_price),
                        '報酬60日': self._calc_return(df_stock, buy_idx, 60, buy_price),
                    }
                    all_results.append(res)
                
        df_res = pd.DataFrame(all_results)
        logging.info(f"Scan Completed. Signals Found: {len(df_res)}")
        return df_res

    def _calc_return(self, df, buy_idx, days, buy_price):
        """計算持有 N 天後的報酬率"""
        target_idx = buy_idx + days
        if target_idx < len(df):
            # 持有 N 天後的收盤價
            sell_price = df.iloc[target_idx]['收盤']
            ret = (sell_price - buy_price) / buy_price * 100
            return round(ret, 2)
        else:
            return "N/A" # 資料不足

    def run_weekly_scan(self, strategy_types, start_date=None, end_date=None, progress_callback=None):
        """
        執行週線策略掃描
        """
        if isinstance(strategy_types, str):
            strategy_types = [strategy_types]

        # --- 效能優化: 計算所需的最早日期 (Buffer) ---
        buffer_days = 600 # 週線 MA60 需要較長緩衝
        fetch_start = None
        if start_date:
            fetch_start = (pd.to_datetime(start_date) - timedelta(days=buffer_days)).strftime('%Y-%m-%d')
        else:
            # 若無起始日 (即最新)，預設抓兩年內資料
            fetch_start = (datetime.now() - timedelta(days=buffer_days + 100)).strftime('%Y-%m-%d')

        df_all = self.load_data(start_date=fetch_start, end_date=end_date)
        all_results = []

        grouped = df_all.groupby('代號')
        total_stocks = grouped.ngroups
        
        for i, (code, df_stock) in enumerate(grouped):
            if progress_callback:
                progress_callback(i + 1, total_stocks)

            df_stock = df_stock.copy().sort_values('日期')
            df_stock.set_index('日期', inplace=True)
            
            ohlc_dict = {
                '開盤': 'first',
                '最高': 'max',
                '最低': 'min',
                '收盤': 'last',
                '名稱': 'first'
            }
            df_weekly = df_stock.resample('W-FRI').agg(ohlc_dict).dropna()
            df_weekly = df_weekly.reset_index()
            
            df_weekly['MA5'] = df_weekly['收盤'].rolling(window=5).mean()
            df_weekly['MA10'] = df_weekly['收盤'].rolling(window=10).mean()
            df_weekly['MA20'] = df_weekly['收盤'].rolling(window=20).mean()
            df_weekly['MA60'] = df_weekly['收盤'].rolling(window=60).mean()

            for strategy_type in strategy_types:
                strategy_obj = strategies.get_strategy(strategy_type)
                if not strategy_obj:
                    logging.warning(f"Strategy {strategy_type} not found.")
                    continue
                
                df_weekly['Signal'] = strategy_obj.calculate_signals(df_weekly)

                signals = df_weekly[df_weekly['Signal']].copy()
                if signals.empty:
                    continue
                    
                if start_date:
                    signals = signals[signals['日期'] >= pd.to_datetime(start_date)]
                if end_date:
                    signals = signals[signals['日期'] <= pd.to_datetime(end_date)]

                for idx, row in signals.iterrows():
                    buy_idx = idx
                    buy_row = df_weekly.iloc[buy_idx]
                    buy_date = buy_row['日期']
                    buy_price = buy_row['收盤']
                    
                    res = {
                        '策略': strategy_type, # [NEW]
                        '代號': code,
                        '名稱': row['名稱'],
                        '訊號日期': row['日期'].strftime('%Y-%m-%d'),
                        '收盤價': row['收盤'],
                        '買入日期(週)': buy_date.strftime('%Y-%m-%d'),
                        '買入價': buy_price,
                        '報酬5週': self._calc_return(df_weekly, buy_idx, 5, buy_price),
                        '報酬10週': self._calc_return(df_weekly, buy_idx, 10, buy_price),
                        '報酬20週': self._calc_return(df_weekly, buy_idx, 20, buy_price),
                        '報酬60週': self._calc_return(df_weekly, buy_idx, 60, buy_price),
                    }
                    all_results.append(res)
                
        return pd.DataFrame(all_results)

if __name__ == "__main__":
    # 簡單測試
    bt = StrategyBacktester()
    try:
        print("Running test scan...")
        # 測試最近是否有訊號
        df_res = bt.run_scan('MA5_MA10_Flat', latest_only=True)
        print("Latest Signals:", len(df_res))
        if not df_res.empty:
            print(df_res.head())
    except Exception as e:
        print(f"Error: {e}")
