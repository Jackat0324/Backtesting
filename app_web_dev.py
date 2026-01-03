import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import sqlite3
import strategy_backtester
import strategies
import plotter
import logging
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

# Set page config
# --- 頁面設定 ---
st.set_page_config(page_title="TWSE 策略回測 (Experimental)", layout="wide")

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
    """在 Streamlit 中渲染 K 線圖"""
    p = plotter.StockPlotter(db_path)
    df = p.get_stock_data(code, center_date=signal_date, frequency=frequency)
    
    if df is None or df.empty:
        st.error(f"找不到 {code} {name} 的資料庫數據")
        return

    # 設定中文字型
    plt.rcParams['font.sans-serif'] = ['Microsoft JhengHei', 'Arial Unicode MS', 'sans-serif']
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
    if frequency == 'W':
        ax.plot(df.index, df['MA60'], label='MA60', color='brown', linewidth=1)

    # 訊號線
    if signal_date:
        sig_dt = pd.to_datetime(signal_date)
        idx_matches = df.index[df['日期'] == sig_dt].tolist()
        if idx_matches:
            ax.axvline(x=idx_matches[0], color='lime', linestyle='--', linewidth=2, alpha=0.5, label='訊號日')

    # Y 軸自動縮放
    cols = ['最低', '最高', 'MA5', 'MA10', 'MA20']
    if frequency == 'W': cols.append('MA60')
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

def main():
    st.title("🚀 TWSE 策略回測雲端儀表板 (Prototype)")
    st.info("這是一個基於 Streamlit 的網頁介面原型，展示如何將您的回測系統雲端化。")

    bt = get_backtester()

    # --- Sidebar ---
    st.sidebar.header("⚙️ 掃描設定")
    
    # 模式選擇
    mode = st.sidebar.radio("數據頻率", ["日K (Daily)", "週K (Weekly)"])
    is_weekly = "Weekly" in mode
    mode_key = 'W' if is_weekly else 'D'

    # 策略選擇
    available_strategies = strategies.WEEKLY_STRATEGIES if is_weekly else strategies.DAILY_STRATEGIES
    
    container = st.sidebar.container()
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
            with sqlite3.connect(bt.db_path) as conn:
                df_info = pd.read_sql("SELECT MIN(日期) as start, MAX(日期) as end, COUNT(*) as count FROM stock_prices", conn)
                st.write(f"**資料筆數**: {df_info['count'][0]:,}")
                st.write(f"**起始日期**: {df_info['start'][0]}")
                st.write(f"**最後日期**: {df_info['end'][0]}")
                
                # 簡單檢查週線數據是否足夠 (MA60 需要約 300 交易日)
                days_count = pd.read_sql("SELECT COUNT(DISTINCT 日期) as d_count FROM stock_prices", conn)['d_count'][0]
                if days_count < 300 and is_weekly:
                    st.warning("⚠️ 數據不足 300 天，週線 MA60 策略可能無法產生訊號。")
                elif days_count >= 300:
                    st.success("✅ 數據充足")
        except:
            st.error("無法讀取資料庫狀態")

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
            
            # 分頁按鈕
            col_p1, col_p2, col_p3 = st.columns([1, 1, 4])
            if col_p1.button("⬅️ 上一頁", disabled=(st.session_state.current_page <= 1)):
                st.session_state.current_page -= 1
                st.rerun()
            if col_p2.button("下一頁 ➡️", disabled=(st.session_state.current_page >= total_pages)):
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
