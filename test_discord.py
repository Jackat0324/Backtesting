import requests
import json

def send_discord_message(webhook_url, content, title="🚀 策略掃描通知"):
    """
    發送預設格式的 Discord 訊息 (使用 Embeds 讓介面更漂亮)
    """
    payload = {
        "username": "TWSE 選股小助手",
        "embeds": [{
            "title": title,
            "description": content,
            "color": 3066993, # 漂亮的藍色
            "footer": {
                "text": "Antigravity DevOps System"
            },
            "timestamp": None
        }]
    }
    
    try:
        response = requests.post(
            webhook_url, 
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print("✅ 訊息已成功發送到 Discord！")
    except Exception as e:
        print(f"❌ 發送失敗: {e}")

if __name__ == "__main__":
    # --- 請在此處貼上您的 Discord Webhook 網址 ---
    WEBHOOK_URL = "YOUR_DISCORD_WEBHOOK_URL_HERE"
    # ------------------------------------------

    if "YOUR_DISCORD" in WEBHOOK_URL:
        print("💡 請先在 WEBHOOK_URL 變數中填入您的 Discord Webhook 網址。")
    else:
        test_content = "這是一則來自您 Python 系統的測試訊息。\n\n**今日掃描結果：**\n- 發現 36 個符合週線策略的訊號\n- 數據庫已成功更新至 2026-01-02"
        send_discord_message(WEBHOOK_URL, test_content)
