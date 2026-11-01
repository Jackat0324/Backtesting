import streamlit as st
# Trigger reload
import pandas as pd
from datetime import date, timedelta, datetime
import strategy_backtester
from db_manager import DatabaseManager
import strategies
import plotter
import logging
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

# Set page config
# --- 頁面設定 ---
st.set_page_config(page_title="TWSE 策略回測雲端版", layout="wide")

st.markdown("""
<style>
    .main {
        background-color: #f5f7f9;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    /* 防止多選標籤文字被截斷 */
    .stMultiSelect div[data-baseweb="tag"] {
        white-space: normal !important;
        height: auto !important;
        max-width: 100% !important;
    }
    .stMultiSelect div[data-baseweb="tag"] > span {
        overflow: visible !important;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_backtester():
    return strategy_backtester.StrategyBacktester()

def render_chart_streamlit(code, name, signal_date, frequency, db_path, strategy_name=""):
    """在 Streamlit 中渲染 K 線圖 (加強版：包含錯誤處理與 Linux 字型支援)"""
    try:
        p = plotter.StockPlotter(db_path)
        df = p.get_stock_data(code, center_date=signal_date, frequency=frequency)
        
        if df is None or df.empty:
            st.error(f"找不到 {code} {name} 的資料庫數據。請確認資料庫是否已上傳至 GitHub。")
            return

        # --- 字型設定 (相容 Linux/Windows) ---
        # Streamlit Cloud (Linux) 通常沒有微軟正黑體，嘗試使用常見的 Linux 中文字型
        fonts = ['Microsoft JhengHei', 'Arial Unicode MS', 'Noto Sans CJK JP', 'DejaVu Sans', 'sans-serif']
        plt.rcParams['font.sans-serif'] = fonts
        plt.rcParams['axes.unicode_minus'] = False
        
        fig = Figure(figsize=(10, 5))
        ax = fig.add_subplot(111)
        
        title_suffix = "週線圖" if frequency == 'W' else "日線圖"
        ax.set_title(f"{code} {name} {title_suffix}\n策略: {strategy_name} (中心日期: {signal_date})", fontsize=10)

        # 繪製 K 線
        width = 0.6
        width2 = 0.1
        up = df[df['收盤'] >= df['開盤']]
        down = df[df['收盤'] < df['開盤']]
        
        ax.bar(up.index, up['收盤'] - up['開盤'], width, bottom=up['開盤'], color='red', alpha=0.8)
        ax.bar(up.index, up['最高'] - up['收盤'], width2, bottom=up['收盤'], color='red')
        ax.bar(up.index, up['開盤'] - up['最低'], width2, bottom=up['最低'], color='red')
        
        ax.bar(down.index, down['開盤'] - down['收盤'], width, bottom=down['收盤'], color='green', alpha=0.8)
        ax.bar(down.index, down['最高'] - down['開盤'], width2, bottom=down['開盤'], color='green')
        ax.bar(down.index, down['收盤'] - down['最低'], width2, bottom=down['最低'], color='green')

        # 均線
        ax.plot(df.index, df['MA5'], label='MA5', color='blue', linewidth=1)
        ax.plot(df.index, df['MA10'], label='MA10', color='orange', linewidth=1)
        ax.plot(df.index, df['MA20'], label='MA20', color='purple', linewidth=1)
        ax.plot(df.index, df['MA60'], label='MA60', color='brown', linewidth=1)

        # 訊號線
        if signal_date:
            sig_dt = pd.to_datetime(signal_date)
            idx_matches = df.index[df['日期'] == sig_dt].tolist()
            if idx_matches:
                ax.axvline(x=idx_matches[0], color='lime', linestyle='--', linewidth=2, alpha=0.5, label='訊號日')

        # Y 軸自動縮放
        cols = ['最低', '最高', 'MA5', 'MA10', 'MA20', 'MA60']
        y_min, y_max = df[cols].min().min(), df[cols].max().max()
        if pd.notna(y_min):
            margin = (y_max - y_min) * 0.1
            ax.set_ylim(y_min - margin, y_max + margin)

        ax.legend(loc='upper left', fontsize=8)
        ax.grid(True, linestyle='--', alpha=0.3)
        
        # X 軸日期縮寫
        step = max(1, len(df) // 10)
        ax.set_xticks(df.index[::step])
        ax.set_xticklabels(df['日期'].dt.strftime('%m/%d')[::step], rotation=0, fontsize=8)

        st.pyplot(fig)
        plt.close(fig) # 重要：釋放記憶體
    except Exception as e:
        st.error(f"渲染圖表時發生錯誤: {e}")
def render_sequence_table(code, center_date, frequency, db_path):
    try:
        p = plotter.StockPlotter(db_path)
        df = p.get_stock_data(code, center_date=center_date, frequency=frequency)
        if df is None or df.empty:
            return
        
        c_dt = pd.to_datetime(center_date)
        df_target = df[df['日期'] <= c_dt]
        if df_target.empty:
            df_target = df
            
        df_last20 = df_target.tail(20).copy()
        
        reverse_shortcuts = {tuple(v): k for k, v in strategies.SEQUENCE_SHORTCUTS.items()}
        
        table_rows = []
        for idx, row in df_last20.iterrows():
            d_str = row['日期'].strftime('%Y-%m-%d')
            ma5 = row.get('MA5')
            ma10 = row.get('MA10')
            ma20 = row.get('MA20')
            ma60 = row.get('MA60')
            
            if pd.isna(ma5) or pd.isna(ma10) or pd.isna(ma20) or pd.isna(ma60):
                code_val = "N/A"
                order_val = "資料不足"
            else:
                pairs = [(5, ma5), (10, ma10), (20, ma20), (60, ma60)]
                pairs_sorted = sorted(pairs, key=lambda x: x[1], reverse=True)
                perm = tuple(p[0] for p in pairs_sorted)
                code_val = reverse_shortcuts.get(perm, "其他")
                order_val = " > ".join(f"{p[0]}MA" for p in pairs_sorted)
                
            table_rows.append({
                "日期": d_str,
                "均線位階代號": code_val,
                "均線排列順序": order_val
            })
            
        df_display = pd.DataFrame(table_rows)
        df_display = df_display.iloc[::-1].reset_index(drop=True)
        
        st.write("📋 **近 20 日均線位階歷史紀錄**" if frequency == 'D' else "📋 **近 20 週均線位階歷史紀錄**")
        st.dataframe(df_display, use_container_width=True, hide_index=True, height=250)
        
    except Exception as e:
        st.error(f"計算均線位階表時發生錯誤: {e}")

def get_latest_date_and_time(db_path):
    try:
        db = DatabaseManager(db_path)
        latest_date, download_time_str = db.get_latest_data_info()
        if not latest_date:
            return "未知", "未知"
            
        try:
            dt = datetime.strptime(download_time_str, "%Y-%m-%d %H:%M:%S")
            # 判斷是否為 UTC 時間：由於台股收盤資料在台北時間 13:30 之前不可能產生，
            # 因此若資料庫紀錄的小時數小於 13，代表該時間必定是 GitHub Action (UTC 時區) 寫入的，需加 8 小時轉換為台北時間。
            is_utc = dt.hour < 13
            
            if is_utc:
                dt = dt + timedelta(hours=8)
            
            period = "上午" if dt.hour < 12 else "下午"
            h = dt.hour if dt.hour <= 12 else dt.hour - 12
            if h == 0:
                h = 12
            time_str = f"台北時間 {period} {h}:{dt.minute:02d}"
        except Exception:
            time_str = download_time_str
            
        return latest_date, time_str
    except Exception as e:
        return "未知", f"讀取錯誤: {e}"

import urllib.request
from pathlib import Path

def check_and_download_db(db_path):
    db_path = Path(db_path)
    url = "https://github.com/Jackat0324/Backtesting/releases/download/database-storage/twse_data.db"
    
    now = datetime.now()
    if 'last_db_check' not in st.session_state:
        st.session_state.last_db_check = None
        
    need_download = not db_path.exists()
    
    # 僅在檔案存在且距離上次檢查超過 10 分鐘時才向 GitHub 發送 HEAD 請求，避免影響頁面交互流暢度
    if db_path.exists() and (st.session_state.last_db_check is None or (now - st.session_state.last_db_check).total_seconds() > 600):
        st.session_state.last_db_check = now
        try:
            req = urllib.request.Request(url, method='HEAD')
            with urllib.request.urlopen(req, timeout=5) as resp:
                remote_size = int(resp.info().get('Content-Length', 0))
            local_size = db_path.stat().st_size
            if local_size != remote_size:
                need_download = True
                logging.info(f"偵測到雲端資料庫大小變更 (本地: {local_size}，雲端: {remote_size})，準備更新...")
        except Exception as e:
            logging.warning(f"檢查資料庫更新時發生異常: {e}")
            
    if need_download:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with st.spinner("正在下載/更新最新股票資料庫 (約 35MB)..."):
            try:
                urllib.request.urlretrieve(url, db_path)
                st.success("資料庫下載/更新完成！")
                st.session_state.last_db_check = now
                st.rerun()
            except Exception as e:
                st.error(f"資料庫下載失敗: {e}")

def main():
    st.title("🚀 TWSE 策略回測雲端儀表板")
    bt = get_backtester()
    check_and_download_db(bt.db_path)
    latest_date, update_time = get_latest_date_and_time(bt.db_path)
    
    st.info(f"這是一個基於 Streamlit 的網頁介面，展示您的策略回測結果與即時線圖。 (最後資料日期: **{latest_date}**，更新時間: **{update_time}**)")

    # --- Sidebar ---
    st.sidebar.header("⚙️ 掃描設定")
    
    # 模式選擇
    mode = st.sidebar.radio("數據頻率", ["日K (Daily)", "週K (Weekly)", "日K (位階)", "週K (位階)", "週K(當日K)"])
    is_weekly = "Weekly" in mode or "週K" in mode
    is_ranks = "位階" in mode
    is_kbar = "當日K" in mode
    mode_key = 'W' if is_weekly else 'D'


    # 策略選擇
    if is_kbar:
        available_strategies = strategies.WEEKLY_STRATEGIES_KBAR
    elif is_ranks:
        available_strategies = strategies.WEEKLY_STRATEGIES_RANKS if is_weekly else strategies.DAILY_STRATEGIES_RANKS
    elif is_weekly:
        available_strategies = strategies.WEEKLY_STRATEGIES
    else:
        available_strategies = strategies.DAILY_STRATEGIES


    
    container = st.sidebar.container()
    if is_kbar:
        container.write(f"已讀取: {len(strategies.WEEKLY_STRATEGIES_KBAR)} 個週K(當日K)策略")
    elif is_ranks:
        container.write(f"已讀取: {len(strategies.WEEKLY_STRATEGIES_RANKS)} 個位階策略")
    else:
        container.write(f"已讀取: {len(strategies.WEEKLY_STRATEGIES)} 個週策略")

    select_all = container.checkbox("全選所有策略")
    
    if select_all:
        selected_strategies = container.multiselect("選擇策略", available_strategies, default=available_strategies)
    else:
        selected_strategies = container.multiselect("選擇策略", available_strategies)

    st.sidebar.divider()

    # 執行模式 (與 GUI 邏輯一致：勾選回測 = True, 不勾選 = 僅看最新)
    is_backtest = st.sidebar.checkbox("啟用歷史回測", value=False)
    latest_only = not is_backtest

    # 日期範圍
    if is_backtest:
        d_start = st.sidebar.date_input("起始日期", date.today() - timedelta(days=90))
        d_end = st.sidebar.date_input("結束日期", date.today())
    else:
        st.sidebar.info("💡 目前為「最新訊號模式」，系統僅掃描最近一個交易日。")
        d_start, d_end = None, None

    # 執行按鈕
    run_button = st.sidebar.button("🔍 開始執行掃描", type="primary", use_container_width=True)

    # --- Data Health Check (Sidebar) ---
    st.sidebar.divider()
    with st.sidebar.expander("📊 數據庫狀態", expanded=False):
        try:
            db = DatabaseManager(bt.db_path)
            d_start_val, d_end_val, total_count, days_count = db.get_detailed_stats()
            st.write(f"**資料筆數**: {total_count:,}")
            st.write(f"**起始日期**: {d_start_val}")
            st.write(f"**最後日期**: {d_end_val}")
            
            # 簡單檢查週線數據是否足夠 (MA60 需要約 300 交易日)
            if days_count < 300 and is_weekly:
                st.warning("⚠️ 數據不足 300 天，週線 MA60 策略可能無法產生訊號。")
            elif days_count >= 300:
                st.success("✅ 數據充足")
        except Exception as e:
            st.error(f"無法讀取資料庫狀態: {e}")

    # --- Initialize Session State ---
    if 'df_res' not in st.session_state:
        st.session_state.df_res = None
    if 'mode_key' not in st.session_state:
        st.session_state.mode_key = 'D'
    if 'current_page' not in st.session_state:
        st.session_state.current_page = 1

    # --- Main Content ---
    if run_button:
        if not selected_strategies:
            st.warning("請至少選擇一個策略")
            return

        with st.spinner("正在掃描數據庫中..."):
            try:
                start_str = d_start.strftime('%Y-%m-%d') if d_start else None
                end_str = d_end.strftime('%Y-%m-%d') if d_end else None
                
                if is_weekly:
                    df = bt.run_weekly_scan(selected_strategies, start_date=start_str, end_date=end_str)
                    if latest_only and not df.empty:
                        max_date = df['訊號日期'].max()
                        df = df[df['訊號日期'] == max_date]
                else:
                    df = bt.run_scan(selected_strategies, latest_only=latest_only, start_date=start_str, end_date=end_str)
                
                st.session_state.df_res = df
                st.session_state.mode_key = mode_key
                st.session_state.current_page = 1 # Reset to page 1 on new scan
                
            except Exception as e:
                st.error(f"執行過程中發生錯誤: {e}")
                st.session_state.df_res = None

    # --- Render Results (Always If Exists) ---
    if st.session_state.df_res is not None:
        df_res = st.session_state.df_res
        res_mode_key = st.session_state.mode_key
        
        if df_res.empty:
            st.success("掃描完成：未發現符合條件的訊號。")
        else:
            st.subheader(f"📊 掃描結果 (發現 {len(df_res)} 個訊號)")
            
            # 績效摘要
            if is_backtest:
                st.write("---")
                col1, _ = st.columns([1, 2])
                col1.metric("總訊號數", len(df_res))
                st.write("---")

            # 資料表格
            st.dataframe(df_res, use_container_width=True)
            
            # 匯出按鈕
            csv = df_res.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="📥 下載報表 (CSV)",
                data=csv,
                file_name=f"scan_results_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime='text/csv',
            )

            # --- 個股線圖區塊 ---
            st.write("---")
            st.subheader("📈 個股線圖查閱 (分頁顯示)")
            
            batch_size = 1
            total_signals = len(df_res)
            total_pages = (total_signals + batch_size - 1) // batch_size
            
            # 建立個股選擇清單，以供下拉搜尋與直接跳轉
            stock_options = [
                f"{i+1}. {row['代號']} {row['名稱']} (策略: {row['策略']})"
                for i, row in df_res.iterrows()
            ]
            
            # 使用分欄：左邊下拉選單搜尋，右邊上一頁/下一頁按鈕
            col_sel, col_btns = st.columns([3, 2])
            
            with col_sel:
                selected_stock = st.selectbox(
                    "🎯 選擇個股或輸入代號搜尋",
                    options=stock_options,
                    index=st.session_state.current_page - 1,
                    key="stock_select_box"
                )
                selected_idx = stock_options.index(selected_stock)
                st.session_state.current_page = selected_idx + 1
                
            with col_btns:
                st.write("")  # 垂直微調對齊
                col_p1, col_p2 = st.columns(2)
                if col_p1.button("⬅️ 上一頁", disabled=(st.session_state.current_page <= 1), use_container_width=True):
                    st.session_state.current_page -= 1
                    st.rerun()
                if col_p2.button("下一頁 ➡️", disabled=(st.session_state.current_page >= total_pages), use_container_width=True):
                    st.session_state.current_page += 1
                    st.rerun()
            
            page = st.session_state.current_page
            start_idx = (page - 1) * batch_size
            end_idx = min(start_idx + batch_size, total_signals)
            
            st.info(f"正在顯示第 {page} / {total_pages} 支股票 (共 {total_signals} 支)")
            
            # 遍歷當前分頁的結果並直接顯示圖表
            page_results = df_res.iloc[start_idx:end_idx]
            
            for idx, row in page_results.iterrows():
                code = str(row['代號'])
                name = str(row['名稱'])
                s_date = str(row['訊號日期'])
                strat = str(row['策略'])
                
                st.markdown(f"#### 📊 {code} {name} (策略: {strat})")
                render_sequence_table(code, s_date, res_mode_key, bt.db_path)
                render_chart_streamlit(code, name, s_date, res_mode_key, bt.db_path, strategy_name=strat)
                st.write("---")

            # 底部重複分頁按鈕 (方便看完直接下一頁)
            col_b1, col_b2, col_b3 = st.columns([1, 1, 4])
            if col_b1.button("⬅️ 上一頁", key="prev_bottom", disabled=(st.session_state.current_page <= 1)):
                st.session_state.current_page -= 1
                st.rerun()
            if col_b2.button("下一頁 ➡️", key="next_bottom", disabled=(st.session_state.current_page >= total_pages)):
                st.session_state.current_page += 1
                st.rerun()
    else:
        if not run_button:
            st.write("👈 請在左側設定參數並點擊「開始執行掃描」。")

if __name__ == "__main__":
    main()
