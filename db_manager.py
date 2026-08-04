import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import date, datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        
    def get_connection(self, autocommit=False):
        """獲取資料庫連接，並開啟 WAL 模式以提升效能"""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if autocommit:
            # For operations like VACUUM that cannot run inside a transaction block
            conn = sqlite3.connect(self.db_path, isolation_level=None)
        else:
            conn = sqlite3.connect(self.db_path)
        
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception as e:
            logging.warning(f"設定 PRAGMA 時發生異常: {e}")
        return conn

    def init_database(self):
        """初始化資料表與索引"""
        logging.info("初始化資料庫：%s", self.db_path)
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_prices (
                    日期 TEXT,
                    代號 TEXT,
                    名稱 TEXT,
                    開盤 REAL,
                    最高 REAL,
                    最低 REAL,
                    收盤 REAL,
                    成交金額 INTEGER CHECK(成交金額 >= 0),
                    資料來源 TEXT,
                    下載時間 TEXT,
                    PRIMARY KEY (日期, 代號)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_stock_prices_code_date
                ON stock_prices(代號, 日期)
                """
            )
            conn.commit()

    def bulk_upsert(self, rows):
        """批次寫入/更新股票價格資料"""
        rows_list = list(rows)
        if not rows_list:
            return 0
        with self.get_connection() as conn:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO stock_prices
                (日期, 代號, 名稱, 開盤, 最高, 最低, 收盤, 成交金額, 資料來源, 下載時間)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(日期, 代號) DO UPDATE SET
                    名稱=excluded.名稱,
                    開盤=excluded.開盤,
                    最高=excluded.最高,
                    最低=excluded.最低,
                    收盤=excluded.收盤,
                    成交金額=excluded.成交金額,
                    資料來源=excluded.資料來源,
                    下載時間=excluded.下載時間
                """,
                rows_list,
            )
            conn.commit()
            return cur.rowcount

    def load_all_prices(self, start_date=None, end_date=None):
        """回測引擎使用：整批歷史資料讀取"""
        logging.info(f"從資料庫讀取全市場資料 (Range: {start_date} ~ {end_date})...")
        with self.get_connection() as conn:
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
        return df

    def fetch_stock_history(self, code, start_date=None, end_date=None, limit=None):
        """個股 K 線繪圖數據載入"""
        with self.get_connection() as conn:
            if start_date or end_date:
                query = """
                SELECT 日期, 開盤, 最高, 最低, 收盤
                FROM stock_prices
                WHERE 代號 = ? AND 日期 >= ? AND 日期 <= ?
                ORDER BY 日期 ASC
                """
                df = pd.read_sql(query, conn, params=(code, start_date, end_date))
            else:
                query = """
                SELECT 日期, 開盤, 最高, 最低, 收盤
                FROM stock_prices
                WHERE 代號 = ?
                ORDER BY 日期 DESC
                LIMIT ?
                """
                df = pd.read_sql(query, conn, params=(code, limit))
        return df

    def get_latest_data_info(self):
        """取得資料庫最新資料日期與下載時間"""
        with self.get_connection() as conn:
            df_info = pd.read_sql(
                "SELECT 日期, 下載時間 FROM stock_prices WHERE 日期 = (SELECT MAX(日期) FROM stock_prices) LIMIT 1",
                conn
            )
        if df_info.empty:
            return None, None
        return df_info['日期'][0], df_info['下載時間'][0]

    def get_database_stats(self):
        """取得資料庫總行數與總股數"""
        with self.get_connection() as conn:
            df_stats = pd.read_sql(
                "SELECT COUNT(DISTINCT 代號) as stocks, COUNT(*) as total FROM stock_prices",
                conn
            )
        if df_stats.empty:
            return 0, 0
        return int(df_stats['stocks'][0]), int(df_stats['total'][0])

    def get_detailed_stats(self):
        """取得詳細資料庫統計資訊"""
        with self.get_connection() as conn:
            df_info = pd.read_sql(
                "SELECT MIN(日期) as start, MAX(日期) as end, COUNT(*) as count FROM stock_prices",
                conn
            )
            df_days = pd.read_sql(
                "SELECT COUNT(DISTINCT 日期) as d_count FROM stock_prices",
                conn
            )
        if df_info.empty:
            return "N/A", "N/A", 0, 0
        return (
            df_info['start'][0] or "N/A", 
            df_info['end'][0] or "N/A", 
            int(df_info['count'][0] or 0), 
            int(df_days['d_count'][0] or 0)
        )

    def prune_old_data(self, keep_years):
        """清理歷史資料並壓縮資料庫"""
        logging.info("開始清理資料庫，僅保留最近 %.1f 年的資料...", keep_years)
        cutoff_date = (date.today() - timedelta(days=int(365 * keep_years))).strftime("%Y-%m-%d")
        logging.info("截止日期：%s (在此日期之前的資料將被刪除)", cutoff_date)
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM stock_prices WHERE 日期 < ?", (cutoff_date,))
                deleted_rows = cur.rowcount
                logging.info("已刪除 %d 筆舊資料。", deleted_rows)
                conn.commit()
                
            # autocommit mode runs VACUUM
            with self.get_connection(autocommit=True) as conn:
                logging.info("執行資料庫壓縮 (VACUUM)...")
                conn.execute("VACUUM")
                
            logging.info("資料庫清理與壓縮完成。")
        except Exception as exc:
            logging.error("清理資料庫時發生錯誤：%s", exc)
            raise exc
