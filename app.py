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
user_states = {}       # 紀錄使用者目前處於哪一個關卡 (FSM 狀態)
user_locations = {}    # 紀錄使用者當前輸入的即時地點
last_locations = {}    # 💡 永久備份區：防止 211/212 或二階段深化時地點被刪除或洗掉
event_queue = queue.Queue()  
historical_logs = []  

def send_line_push(message_text, user_id, status_desc, custom_loc=None):
    """負責發送 LINE Push 訊息，並將紀錄同步寫入記憶體日誌"""
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": TARGET_ID, "messages": [{"type": "text", "text": message_text}]}
    try:
        requests.post(url, json=payload, headers=headers, timeout=5)
        
        # 實時寫入歷史日誌（優先使用傳入的 custom_loc，否則去撈 user_locations）
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
    """背景 Worker：從 Queue 提取訊息，落實白皮書狀態機控制邏輯"""
    while True:
        source_info, user_message = event_queue.get()
        try:
            user_id = source_info.get("userId")
            if not user_id:
                event_queue.task_done()
                continue
            
            # ========================================================
            # ️⃣ 獨立觸發代碼：000（一般呼叫）
            # ========================================================
            if user_message == "000":
                send_line_push("護理師 有人找您歐", user_id, "一般呼叫護理師", custom_loc="未填入")
                event_queue.task_done()
                continue
                
            # ========================================================
            # ️⃣ 通報鏈啟動代碼：200（初始化通報）
            # ========================================================
            if user_message == "200":
                user_states[user_id] = "WAITING_FOR_LOCATION"
                event_queue.task_done()
                continue
                
            # ========================================================
            # ️⃣ 按鈕攔截機制：地點輸入完成
            # ========================================================
            if user_message == "地點輸入完成":
                # 推進狀態機：進入分流攔截階層
                user_states[user_id] = "WAITING_FOR_201"
                event_queue.task_done()
                continue

            # ========================================================
            # ️⃣ 任意文字輸入（地點攔截機制）
            # ========================================================
            # 當狀態是等待地點，且進來的字「不是」按鈕文字時，這才是真正要記錄的地點！
            if user_states.get(user_id) == "WAITING_FOR_LOCATION" and user_message != "地點輸入完成":
                user_locations[user_id] = user_message  # 寫入主要區（允許重複打字覆蓋）
                last_locations[user_id] = user_message  # 寫入永久備份區
                event_queue.task_done()
                continue

            # ========================================================
            # ️⃣ 分流攔截機制：狀態為 WAITING_FOR_201 或二階段深化狀態
            # ========================================================
            current_state = user_states.get(user_id)
            if current_state in ["WAITING_FOR_201", "WAITING_FOR_DEEPEN", "WAITING_FOR_LOCATION"]:
                
                # 💡 安全提取地點（主要區如果被清空，立刻轉向永久備份區撈取）
                raw_loc = user_locations.get(user_id)
                if not raw_loc:
                    raw_loc = last_locations.get(user_id, "未填入")
                loc_header = f"地點:\n{raw_loc}"

                # 1. 保留/未定義代碼：201, 210, 221
                if user_message in ["201", "210", "221"]:
                    event_queue.task_done()
                    continue
                    
                # 2. 有意識分流代碼：211（需要輪椅）
                elif user_message == "211":
                    send_line_push(f"{loc_header}\n傷患有意識但人不能過來\n請備妥一個輪椅\n有人會過來拿", user_id, "有意識-需要輪椅", custom_loc=raw_loc)
                    # 任務完成，執行 .pop() 清除暫存
                    user_states.pop(user_id, None)
                    user_locations.pop(user_id, None)
                    
                # 3. 有意識分流代碼：212（協助攙扶 / 取消輪椅）
                elif user_message == "212":
                    send_line_push(f"{loc_header}\n傷患有意識但人不能過來\n會有人協助攙扶進健康中心\n(取消借用輪椅)", user_id, "有意識-協助攙扶(取消輪椅)", custom_loc=raw_loc)
                    # 任務完成，執行 .pop() 清除暫存
                    user_states.pop(user_id, None)
                    user_locations.pop(user_id, None)
                    
                # 4. 無意識第一階段代碼：220（無意識通報）
                elif user_message == "220":
                    send_line_push(f"{loc_header}\n有人無意識\n請立即前往", user_id, "第一階段：有人無意識通報", custom_loc=raw_loc)
                    # ⚠️ 依照白皮書特殊設計：不清除狀態與地點！推進到深化階段，等待 222/223
                    user_states[user_id] = "WAITING_FOR_DEEPEN"
                    
                # 5. 無意識深化代碼：222（有抽搐）
                elif user_message == "222":
                    send_line_push(f"{loc_header}\n有人在抽搐\n請立即前往", user_id, "無意識-有抽搐", custom_loc=raw_loc)
                    user_states.pop(user_id, None)
                    user_locations.pop(user_id, None)
                    
                # 6. 無意識深化代碼：223（無抽搐）
                elif user_message == "223":
                    send_line_push(f"{loc_header}\n有人無意識\n請立即前往", user_id, "無意識-無抄搐", custom_loc=raw_loc)
                    user_states.pop(user_id, None)
                    user_locations.pop(user_id, None)
                    
                # 7. 紅色警戒代碼：999（疑似 OHCA）
                elif user_message == "999":
                    send_line_push(f"{loc_header}\nOHCA\n請立即前往", user_id, "🚨 紅色警戒：疑似 OHCA", custom_loc=raw_loc)
                    user_states.pop(user_id, None)
                    user_locations.pop(user_id, None)
                    
                event_queue.task_done()
                continue
                
        except Exception as ex:
            print(f"❌ Worker 處理異常: {ex}")
        event_queue.task_done()

# 啟動背景監聽執行緒
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
            <p>系統狀態：🟢 24小時看門狗常駐中 (秒讀秒回完全體)</p>
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
