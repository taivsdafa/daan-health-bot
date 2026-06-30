import os
import queue
import threading
import requests
from flask import Flask, request, jsonify, send_file
from datetime import datetime
import pandas as pd

app = Flask(__name__)

# 🔑 LINE 憑證與群組 ID
LINE_TOKEN = "BJP7xszvQckgV4Aefdpdne1RQZz8a5lx525X29LcmDy9PS2PiNJTrsXX3P8F3inV2ezBjzHMbQH3mVEHwZliHmQ45gsrMucw2FteDC7u7FMWhGliNJTnp3QzFnGdRZWAzJKdqBUcUY+NJV0oTC5SZQdB04t89/1O/w1cDnyilFU="
TARGET_ID = "C5911e6bbf171871b3c0ea852e4a73324"

# 🧠 全域狀態機與記憶體資料庫
user_states = {}       
user_locations = {}    
last_locations = {}    # 💡 【新增】用來備份上一次的通報地點，防止取消時找不到地點
event_queue = queue.Queue()  
historical_logs = []  

def send_line_push(message_text, user_id, status_desc, custom_loc=None):
    """負責發送 LINE Push 訊息，並將紀錄同步寫入記憶體日誌"""
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": TARGET_ID, "messages": [{"type": "text", "text": message_text}]}
    try:
        requests.post(url, json=payload, headers=headers, timeout=5)
        
        # 實時寫入歷史日誌（如果有傳入 custom_loc 優先使用，否則去撈 user_locations）
        loc = custom_loc if custom_loc else user_locations.get(user_id, "未填入")
        historical_logs.append({
            "通報時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "使用者識別碼": f"User_{user_id[:6]}",
            "通報事件位置": loc,
            "傷勢嚴重分流": status_desc
        })
    except Exception as e:
        print(f"📡 [LINE 發送失敗] 異常原因: {e}")

def process_event_worker():
    """背景 Worker：從 Queue 提取訊息，防止 LINE Webhook 超時 500 錯誤"""
    while True:
        source_info, user_message = event_queue.get()
        try:
            user_id = source_info.get("userId")
            if not user_id:
                event_queue.task_done()
                continue
            
            # 階層一：一般呼叫
            if user_message == "000":
                send_line_push("護理師 有人找您歐", user_id, "一般呼叫護理師")
                event_queue.task_done()
                continue
                
            # 階層二：啟動通報流程
            if user_message == "200":
                user_states[user_id] = "WAITING_FOR_LOCATION"
                event_queue.task_done()
                continue
                
            # 階層三：記錄地點，等待分流代碼
            if user_states.get(user_id) == "WAITING_FOR_LOCATION":
                user_locations[user_id] = user_message  
                user_states[user_id] = "WAITING_FOR_201" 
                event_queue.task_done()
                continue

            # 💡 【新增/重構】獨立處理：取消借輪椅的邏輯 (代碼設為 990)
            if user_message == "990":
                # 從備份記憶區撈出上一次的地點
                old_loc = last_locations.get(user_id, "未知地點")
                loc_header = f"地點:\n{old_loc}"
                send_line_push(f"{loc_header}\n取消借用輪椅", user_id, "取消借用輪椅", custom_loc=old_loc)
                event_queue.task_done()
                continue

            loc_header = f"地點:\n{user_locations[user_id]}" if user_id in user_locations else "地點:未填入"

            # 階層四：判斷嚴重度分流代碼
            if user_message == "201" or user_message == "210" or user_message == "221":
                pass
            elif user_message == "211":
                # 💡 在刪除地點前，先複製一份到備份區（last_locations）
                if user_id in user_locations:
                    last_locations[user_id] = user_locations[user_id]
                    
                send_line_push(f"{loc_header}\n傷患有意識但人不能過來\n請備妥一個輪椅\n有人會過來拿", user_id, "有意識-需要輪椅")
                user_states.pop(user_id, None); user_locations.pop(user_id, None)
            elif user_message == "212":
                send_line_push(f"{loc_header}\n傷患有意識但人不能過來\n會有人協助攙扶進健康中心", user_id, "有意識-協助攙扶")
                user_states.pop(user_id, None); user_locations.pop(user_id, None)
            elif user_message == "220":
                send_line_push(f"{loc_header}\n有人無意識\n請立即前往", user_id, "第一階段：有人無意識通報")
            elif user_message == "222":
                send_line_push(f"{loc_header}\n有人在抽搐\n請立即前往", user_id, "無意識-有抽搐")
                user_states.pop(user_id, None); user_locations.pop(user_id, None)
            elif user_message == "223":
                send_line_push(f"{loc_header}\n有人無意識\n請立即前往", user_id, "無意識-無抄搐")
                user_states.pop(user_id, None); user_locations.pop(user_id, None)
            elif user_message == "999":
                send_line_push(f"{loc_header}\nOHCA\n請立即前往", user_id, "🚨 紅色警戒：疑似 OHCA")
                user_states.pop(user_id, None); user_locations.pop(user_id, None)
        except Exception as ex:
            print(f"❌ Worker 處理異常: {ex}")
        event_queue.task_done()

# 啟動非同步 Worker 執行緒
threading.Thread(target=process_event_worker, daemon=True).start()

@app.route("/callback", methods=['POST'])
def webhook():
    body = request.get_json(silent=True)
    if body and "events" in body and len(body["events"]) > 0:
        event = body["events"][0]
        if event.get("type") == "message" and event["message"].get("type") == "text":
            event_queue.put((event["source"], event["message"]["text"].strip()))
    return "OK", 200

@app.route("/download_log", methods=['GET'])
def download_log():
    if not historical_logs:
        display_logs = [{"通報時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "使用者識別碼": "User_Sample", "通報事件位置": "系統測試", "傷勢嚴重分流": "雲端主機運作正常"}]
    else:
        display_logs = historical_logs
        
    df = pd.DataFrame(display_logs)
    df = df[["通報時間", "使用者識別碼", "通報事件位置", "傷勢嚴重分流"]]
    
    filename = "daan_health_log.xlsx"
    df.to_excel(filename, index=False)
    return send_file(filename, as_attachment=True, download_name=f"大安救護日誌_{datetime.now().strftime('%Y%m%d')}.xlsx")

@app.route("/", methods=['GET'])
def index():
    return f"""
    <html>
        <head><title>大安高工健康小幫手</title></head>
        <body style='font-family: Arial, sans-serif; text-align: center; padding-top: 50px;'>
            <h2>🩺 大安高工健康小幫手雲端伺服器</h2>
            <p>系統狀態：24小時線上常駐中</p>
            <p>目前記憶庫已累積：<b>{len(historical_logs)}</b> 筆通報紀錄</p>
            <br>
            <a href='/download_log'>
                <button style='padding: 15px 30px; font-size: 16px; background-color: #28a745; color: white; border: none; border-radius: 5px; cursor: pointer;'>
                    點我下載今日 Excel 救護日誌
                </button>
            </a>
        </body>
    </html>
    """, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
