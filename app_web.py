import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
import strategy_backtester
import strategies
import logging

# Set page config
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
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_backtester():
    return strategy_backtester.StrategyBacktester()

def main():
    st.title("🚀 TWSE 策略回測雲端儀表板 (Prototype)")
    st.info("這是一個基於 Streamlit 的網頁介面原型，展示如何將您的回測系統雲端化。")

    bt = get_backtester()

    # --- Sidebar ---
    st.sidebar.header("⚙️ 掃描設定")
    
    # 模式選擇
    mode = st.sidebar.radio("數據頻率", ["日K (Daily)", "週K (Weekly)"])
    is_weekly = "Weekly" in mode

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
                    df_res = bt.run_weekly_scan(selected_strategies, start_date=start_str, end_date=end_str)
                    # 週線的 latest_only 需要在此處過濾
                    if latest_only and not df_res.empty:
                        max_date = df_res['訊號日期'].max()
                        df_res = df_res[df_res['訊號日期'] == max_date]
                else:
                    df_res = bt.run_scan(selected_strategies, latest_only=latest_only, start_date=start_str, end_date=end_str)
                
                if df_res.empty:
                    st.success("掃描完成：未發現符合條件的訊號。")
                else:
                    st.subheader(f"📊 掃描結果 (發現 {len(df_res)} 個訊號)")
                    
                    # 績效摘要 (簡單展示)
                    if is_backtest:
                        st.write("---")
                        col1, col2, col3 = st.columns(3)
                        col1.metric("總訊號數", len(df_res))
                        # 這裡可以加入更多計算指標
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

            except Exception as e:
                st.error(f"執行過程中發生錯誤: {e}")
    else:
        st.write("👈 請在左側設定參數並點擊「開始執行掃描」。")

if __name__ == "__main__":
    main()
