import os
import requests
import json
from datetime import datetime
from db_manager import DatabaseManager

def get_db_summary(db_path):
    """取得資料庫簡要統計訊息"""
    if not os.path.exists(db_path):
        return "資料庫檔案不存在"
    try:
        db = DatabaseManager(db_path)
        start, end, count, _ = db.get_detailed_stats()
        return f"- **總資料筆數**: {count:,}\n- **資料範圍**: {start} ~ {end}"
    except Exception as e:
        return f"無法讀取資料庫統計: {e}"

def send_discord_notification(webhook_url, summary_text):
    """發送 Discord 通知"""
    payload = {
        "username": "TWSE 選股小助手 (Cloud)",
        "embeds": [{
            "title": "✅ 每日資料自動更新完成",
            "description": f"系統已成功向證交所同步資料！\n\n{summary_text}\n\n[🌐 前往網頁儀表板看訊號](https://backtesting-s9wl7dptf5appbz5mpj4z8m.streamlit.app/)",
            "color": 3066993,
            "footer": {
                "text": "GitHub Actions 自動執行"
            },
            "timestamp": datetime.utcnow().isoformat()
        }]
    }
    
    response = requests.post(webhook_url, json=payload)
    response.raise_for_status()

if __name__ == "__main__":
    # 從環境變數讀取 Webhook URL (基於 DevOps 安全考量)
    WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK")
    DB_PATH = "data/twse_data.db"
    
    if not WEBHOOK_URL:
        print("Error: Missing DISCORD_WEBHOOK environment variable.")
        exit(1)
        
    summary = get_db_summary(DB_PATH)
    try:
        send_discord_notification(WEBHOOK_URL, summary)
        print("Success: Notification sent.")
    except Exception as e:
        print(f"Error: {e}")
        exit(1)
