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
        print("Success: Message sent to Discord!")
    except Exception as e:
        print(f"Error: Failed to send - {e}")

if __name__ == "__main__":
    # --- 請在此處貼上您的 Discord Webhook 網址 ---
    WEBHOOK_URL = "https://discord.com/api/webhooks/1456669691232911650/5AaIr0yjte9roomb3kGCwYh9g1XYOvRZAS042_qo6HIvOT6IZ7ro-0Z2JBNI-Wxskf3o"
    # ------------------------------------------

    if "YOUR_DISCORD" in WEBHOOK_URL:
        print("Tip: Please fill in your Discord Webhook URL in the WEBHOOK_URL variable.")
    else:
        test_content = "這是一則來自您 Python 系統的測試訊息。\n\n**今日掃描結果：**\n- 發現 36 個符合週線策略的訊號\n- 數據庫已成功更新至 2026-01-02"
        send_discord_message(WEBHOOK_URL, test_content)
