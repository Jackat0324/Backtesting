try:
    import tkinter as tk
    from tkinter import messagebox
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    TK_AVAILABLE = True
except ImportError:
    TK_AVAILABLE = False
    class MockMessagebox:
        def showwarning(self, *args, **kwargs): print("TK Warning:", args)
    messagebox = MockMessagebox()

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.dates import DateFormatter, WeekdayLocator, MONDAY
from pathlib import Path
import matplotlib.ticker as ticker
import logging

# 依賴 reader.py/strategy_backtester.py 的路徑設定
# 依賴 reader.py/strategy_backtester.py 的路徑設定
import sys
def get_base_dir():
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).resolve().parent
    else:
        return Path(__file__).resolve().parent

BASE_DIR = get_base_dir()
DEFAULT_DB_PATH = BASE_DIR / "data" / "twse_data.db"

class ChartCursor:
    """
    處理 K 線圖的互動功能: 查價線 (Crosshair)、資訊框、滾輪縮放
    """
    def __init__(self, ax, figure, canvas, df, frequency='D'):
        self.ax = ax
        self.figure = figure
        self.canvas = canvas
        self.df = df
        self.frequency = frequency
        
        # 初始 X 軸範圍 (用於縮放)
        self.xlim = self.ax.get_xlim()
        
        # 查價線 (Crosshair)
        self.v_line = self.ax.axvline(x=0, color='gray', linestyle='--', linewidth=0.8, alpha=0)
        self.h_line = self.ax.axhline(y=0, color='gray', linestyle='--', linewidth=0.8, alpha=0)
        
        # 資訊框 (Text Annotation) - 固定在左上角
        self.text = self.ax.text(0.02, 0.95, '', transform=self.ax.transAxes, 
                                 fontsize=9, verticalalignment='top',
                                 bbox=dict(boxstyle='round', facecolor='black', alpha=0.7, edgecolor='white'))
                                 
        # 綁定事件
        self.cid_motion = self.canvas.mpl_connect('motion_notify_event', self.on_mouse_move)
        self.cid_leave = self.canvas.mpl_connect('axes_leave_event', self.on_leave)

    def on_mouse_move(self, event):
        if not event.inaxes or event.inaxes != self.ax:
            return
            
        x, y = event.xdata, event.ydata
        idx = int(round(x))
        
        # 邊界檢查
        if 0 <= idx < len(self.df):
            # 更新十字線位置
            self.v_line.set_xdata([x])
            self.h_line.set_ydata([y])
            self.v_line.set_alpha(1)
            self.h_line.set_alpha(1)
            
            # 取得該日資料
            row = self.df.iloc[idx]
            dt_str = row['日期'].strftime('%Y-%m-%d')
            open_p = row['開盤']
            high = row['最高']
            low = row['最低']
            close = row['收盤']
            ma5 = row.get('MA5', 0)
            ma10 = row.get('MA10', 0)
            ma20 = row.get('MA20', 0)
            ma60 = row.get('MA60', 0)
            
            info_text = (f"日期: {dt_str}\n"
                         f"開: {open_p:.2f}  高: {high:.2f}\n"
                         f"低: {low:.2f}  收: {close:.2f}\n"
                         f"MA5: {ma5:.2f}  MA10: {ma10:.2f}\n"
                         f"MA20: {ma20:.2f}  MA60: {ma60:.2f}")
                
            self.text.set_text(info_text)
            self.text.set_color('white')
            
            self.canvas.draw_idle()

    def on_leave(self, event):
        # 滑鼠離開圖表時隱藏
        self.v_line.set_alpha(0)
        self.h_line.set_alpha(0)
        self.text.set_text("")
        self.canvas.draw_idle()


class StockPlotter:
    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else DEFAULT_DB_PATH

    def get_stock_data(self, code, center_date=None, total_days=150, frequency='D'):
        """
        讀取股票資料.
        :param center_date: 若有指定 (str or datetime), 則抓取該日的前後資料
        :param total_days: 若無指定 center_date (即看最新), 則抓取最近 N 天
        :param frequency: 'D' (Daily) or 'W' (Weekly)
        """
        if not self.db_path.exists():
            return None
        
        # 調整抓取範圍: 週線需要更多日資料來合成
        fetch_days = total_days * 5 if frequency == 'W' else total_days
            
        with sqlite3.connect(self.db_path) as conn:
            if center_date:
                try:
                    c_dt = pd.to_datetime(center_date)
                    # 週線抓更長的前後範圍 (MA60 需要至少 420 天，抓 1200 天確保足夠)
                    pre_days = 1200 if frequency == 'W' else 120
                    post_days = 200 if frequency == 'W' else 60
                    
                    start_dt = c_dt - pd.Timedelta(days=pre_days)
                    end_dt = c_dt + pd.Timedelta(days=post_days)
                    
                    query = """
                    SELECT 日期, 開盤, 最高, 最低, 收盤
                    FROM stock_prices
                    WHERE 代號 = ? AND 日期 >= ? AND 日期 <= ?
                    ORDER BY 日期 ASC
                    """
                    df = pd.read_sql(query, conn, params=(code, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")))
                except Exception as e:
                    print(f"Date parsing error: {e}")
                    return None
            else:
                query = """
                SELECT 日期, 開盤, 最高, 最低, 收盤
                FROM stock_prices
                WHERE 代號 = ?
                ORDER BY 日期 DESC
                LIMIT ?
                """
                # LIMIT 限制筆數，週線合成需要多取
                df = pd.read_sql(query, conn, params=(code, fetch_days))
            
        if df.empty:
            return None
            
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期').reset_index(drop=True)
        
        # 如果需要週線，進行 Resample
        if frequency == 'W':
            df.set_index('日期', inplace=True)
            ohlc_dict = {
                '開盤': 'first',
                '最高': 'max',
                '最低': 'min',
                '收盤': 'last'
            }
            # Resample Weekly (Friday)
            df = df.resample('W-FRI').agg(ohlc_dict).dropna()
            df = df.reset_index()
            # 重新計算均線 (週MA)
            df['MA5'] = df['收盤'].rolling(window=5).mean()
            df['MA10'] = df['收盤'].rolling(window=10).mean()
            df['MA20'] = df['收盤'].rolling(window=20).mean()
            # 週線策略通常看 MA60? (看使用者需求，本例策略是 5/10/20/60)
            df['MA60'] = df['收盤'].rolling(window=60).mean()
        else:
            # 日線 MA
            df['MA5'] = df['收盤'].rolling(window=5).mean()
            df['MA10'] = df['收盤'].rolling(window=10).mean()
            df['MA20'] = df['收盤'].rolling(window=20).mean()
            df['MA60'] = df['收盤'].rolling(window=60).mean() # 加減算一下
        
        return df

    def show_chart(self, parent, code, name, signal_date=None, frequency='D'):
        """跳出新視窗顯示 K 線圖"""
        if not TK_AVAILABLE:
            logging.error("Tkinter is not available in this environment. Cannot show desktop chart.")
            return

        logging.info(f"Showing Chart: {code} {name}, Date={signal_date}, Freq={frequency}")
        df = self.get_stock_data(code, center_date=signal_date, frequency=frequency)
        if df is None or df.empty:
            messagebox.showwarning("無資料", f"找不到 {code} {name} 的資料")
            return

        # 建立新視窗
        title_suffix = "週線圖" if frequency == 'W' else "日線圖"
        top = tk.Toplevel(parent)
        top.title(f"{code} {name} - {title_suffix}")
        top.geometry("1000x600")

        # 設定 Matplotlib 圖表
        # 使用 dark_background 讓顏色更鮮明 (可選)
        plt.style.use('bmh') 
        
        # [Fix] 設定中文字型 (Windows 預設微軟正黑體)
        plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei'] 
        plt.rcParams['axes.unicode_minus'] = False
        
        # 使用 Figure 物件，不使用 plt.subplots() 以避免 Threading/GC 問題
        fig = Figure(figsize=(10, 6), dpi=100)
        ax = fig.add_subplot(111)
        
        fig.suptitle(f"{code} {name} {title_suffix}", fontsize=16)

        # 繪製 K 線 (手動繪製以避免 mplfinance 依賴)
        width = 0.6
        width2 = 0.1
        
        up = df[df['收盤'] >= df['開盤']]
        down = df[df['收盤'] < df['開盤']]
        
        # 漲 (紅)
        ax.bar(up.index, up['收盤'] - up['開盤'], width, bottom=up['開盤'], color='red', edgecolor='red', alpha=0.8)
        ax.bar(up.index, up['最高'] - up['收盤'], width2, bottom=up['收盤'], color='red', edgecolor='red')
        ax.bar(up.index, up['開盤'] - up['最低'], width2, bottom=up['最低'], color='red', edgecolor='red')
        
        # 跌 (綠)
        ax.bar(down.index, down['開盤'] - down['收盤'], width, bottom=down['收盤'], color='green', edgecolor='green', alpha=0.8)
        ax.bar(down.index, down['最高'] - down['開盤'], width2, bottom=down['開盤'], color='green', edgecolor='green')
        ax.bar(down.index, down['收盤'] - down['最低'], width2, bottom=down['最低'], color='green', edgecolor='green')

        # 繪製均線
        ax.plot(df.index, df['MA5'], label='MA5', color='blue', linewidth=1.2)
        ax.plot(df.index, df['MA10'], label='MA10', color='orange', linewidth=1.2)
        ax.plot(df.index, df['MA20'], label='MA20', color='purple', linewidth=1.2)
        ax.plot(df.index, df['MA60'], label='MA60', color='brown', linewidth=1.2)

        # 設定 X 軸標籤 (日期)
        # 動態調整標籤間隔，目標是顯示約 30 個日期
        step = max(1, len(df) // 30)
        ax.set_xticks(df.index[::step])
        ax.set_xticklabels(df['日期'].dt.strftime('%Y-%m-%d')[::step], rotation=30, fontsize=8)
        
        # [NEW] 繪製訊號日期垂直線
        if signal_date:
            try:
                sig_dt = pd.to_datetime(signal_date)
                # 找出對應的 index
                idx_matches = df.index[df['日期'] == sig_dt].tolist()
                if idx_matches:
                    idx = idx_matches[0]
                    ax.axvline(x=idx, color='lime', linestyle='--', linewidth=2, alpha=0.4, label='訊號')
            except Exception as e:
                print(f"Drawing signal line error: {e}")

        ax.legend()
        ax.grid(True, linestyle='--', alpha=0.5)
        ax.set_ylabel("價格")
        ax.set_xlabel("日期")

        # [NEW] 自動調整 Y 軸範圍 (避免從 0 開始)
        # 收集所有相關數據的最小值與最大值
        columns_to_check = ['最低', '最高', 'MA5', 'MA10', 'MA20', 'MA60']
            
        # 找出整張表在這些欄位的 min/max (過濾 NaN)
        y_min = df[columns_to_check].min().min()
        y_max = df[columns_to_check].max().max()
        
        if pd.notna(y_min) and pd.notna(y_max):
            # 加上 5% 緩衝
            margin = (y_max - y_min) * 0.05
            if margin == 0: margin = y_max * 0.05 # 避免 flat line
            ax.set_ylim(y_min - margin, y_max + margin)

        fig.tight_layout()

        # 嵌入 Tkinter
        canvas = FigureCanvasTkAgg(fig, master=top)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # [NEW] 啟用游標互動功能
        # 必須保留 reference 否則會被 GC
        cursor = ChartCursor(ax, fig, canvas, df, frequency)
        # 隨意綁定在 canvas 上以防止 GC
        canvas.cursor = cursor
        
        # 關閉視窗時釋放資源
        def on_close():
            # plt.close(fig) # 不需要了，因為是獨立 Figure 物件
            top.destroy()
        top.protocol("WM_DELETE_WINDOW", on_close)

# 測試用
if __name__ == "__main__":
    pass
