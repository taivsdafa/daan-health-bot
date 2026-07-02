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
last_locations = {}    # 備份區
wheelchair_requested = {} # 💡 紀錄該使用者有沒有點過 211 (True/False)，用來分流 212 的訊息
active_timers = {}     # 💡 存放每個使用者的「5分鐘定時炸彈(Timer)」物件

event_queue = queue.Queue()  
historical_logs = []  

def clear_user_data_timeout(user_id):
    """💡 定時器觸發函數：5 分鐘時間到，自動洗掉地點與狀態"""
    print(f"⏰ [⏱️ 定時器觸發] 使用者 {user_id[:6]} 超時 5 分鐘，後台地點與狀態已自動洗掉、重置。")
    user_states.pop(user_id, None)
    user_locations.pop(user_id, None)
    last_locations.pop(user_id, None)
    wheelchair_requested.pop(user_id, None)
    active_timers.pop(user_id, None)

def start_or_refresh_timer(user_id, minutes=5):
    """💡 啟動或重置一個 5 分鐘的定時器"""
    # 如果原本就有計時器在跑，先取消它（重新計時）
    if user_id in active_timers:
        active_timers[user_id].cancel()
    
    # 建立一個新定時器，時間到就執行清除資料
    t = threading.Timer(minutes * 60, clear_user_data_timeout, args=[user_id])
    active_timers[user_id] = t
    t.start()

def send_line_push(message_text, user_id, status_desc, custom_loc=None):
    """負責發送 LINE Push 訊息，並將紀錄同步寫入記憶體日誌"""
    url = "https://api.line.me/v2/bot/message/push"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"to": TARGET_ID, "messages": [{"type": "text", "text": message_text}]}
    try:
        requests.post(url, json=payload, headers=headers, timeout=5)
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
    """背景 Worker：從 Queue 提取訊息，落實白皮書狀態機與超時清除控制邏輯"""
    while True:
        source_info, user_message = event_queue.get()
        try:
            user_id = source_info.get("userId")
            if not user_id:
                event_queue.task_done()
                continue
            
            # ========================================================
            # 代碼：000（一般呼叫）
            # ========================================================
            if user_message == "000":
                send_line_push("護理師 有人找您歐", user_id, "一般呼叫護理師", custom_loc="未填入")
                event_queue.task_done()
                continue
                
            # ========================================================
            # 代碼：200（初始化通報）
            # ========================================================
            if user_message == "200":
                user_states[user_id] = "WAITING_FOR_LOCATION"
                # 💡 開始倒數 5 分鐘，如果使用者卡在輸入地點，5分鐘後自動洗掉
                start_or_refresh_timer(user_id, minutes=5)
                event_queue.task_done()
                continue
                
            # ========================================================
            # 按鈕：地點輸入完成
            # ========================================================
            if user_message == "地點輸入完成":
                user_states[user_id] = "WAITING_FOR_201"
                # 💡 點擊按鈕，重新整理並續約 5 分鐘計時器
                start_or_refresh_timer(user_id, minutes=5)
                event_queue.task_done()
                continue

            # ========================================================
            # 任意文字輸入（地點攔截機制）
            # ========================================================
            if user_states.get(user_id) == "WAITING_FOR_LOCATION" and user_message != "地點輸入完成":
                user_locations[user_id] = user_message  
                last_locations[user_id] = user_message  
                # 💡 每打一次字，重新整理 5 分鐘計時器
                start_or_refresh_timer(user_id, minutes=5)
                event_queue.task_done()
                continue

            # ========================================================
            # 分流攔截機制
            # ========================================================
            current_state = user_states.get(user_id)
            if current_state in ["WAITING_FOR_201", "WAITING_FOR_DEEPEN", "WAITING_FOR_LOCATION"]:
                
                raw_loc = user_locations.get(user_id)
                if not raw_loc:
                    raw_loc = last_locations.get(user_id, "未填入")
                loc_header = f"地點:\n{raw_loc}"

                # 1. 保留代碼
                if user_message in ["201", "210", "221"]:
                    event_queue.task_done()
                    continue
                    
                # 2. 有意識：211（需要輪椅）
                elif user_message == "211":
                    send_line_push(f"{loc_header}\n傷患有意識但人不能過來\n請備妥一個輪椅\n有人會過來拿", user_id, "有意識-需要輪椅", custom_loc=raw_loc)
                    
                    # 💡 標記此人點過輪椅
                    wheelchair_requested[user_id] = True
                    # 💡 核心關鍵：需要輪椅之後，不要立刻清空！啟動一個最後的 5 分鐘黃金計時器，到期才會洗掉地點！
                    start_or_refresh_timer(user_id, minutes=5)
                    
                # 3. 有意識：212（協助攙扶 / 取消輪椅）
                elif user_message == "212":
                    # 💡 【訊息分流檢查】
                    if wheelchair_requested.get(user_id) == True:
                        # 狀況 A：需要輪椅後取消
                        msg = f"{loc_header}\n傷患有意識但人不能過來\n(變更通報：取消借用輪椅，改為由人員協助攙扶進健康中心)"
                        desc = "有意識-取消輪椅改協助攙扶"
                    else:
                        # 狀況 B：本來就不用輪椅，直接點 212
                        msg = f"{loc_header}\n傷患有意識但人不能過來\n會有人協助攙扶進健康中心"
                        desc = "有意識-協助攙扶"
                        
                    send_line_push(msg, user_id, desc, custom_loc=raw_loc)
                    
                    # 💡 完成通報(取消借輪椅)，既然已經按下 212 結案了，立刻關閉計時器並全面洗掉資料！
                    if user_id in active_timers:
                        active_timers[user_id].cancel()
                    clear_user_data_timeout(user_id)
                    
                # 4. 無意識第一階段：220（無意識通報）
                elif user_message == "220":
                    send_line_push(f"{loc_header}\n有人無意識\n請立即前往", user_id, "第一階段：有人無意識通報", custom_loc=raw_loc)
                    user_states[user_id] = "WAITING_FOR_DEEPEN"
                    start_or_refresh_timer(user_id, minutes=5) # 續約 5 分鐘
                    
                # 5. 無意識深化：222（有抽搐）
                elif user_message == "222":
                    send_line_push(f"{loc_header}\n有人在抽搐\n請立即前往", user_id, "無意識-有抽搐", custom_loc=raw_loc)
                    if user_id in active_timers: active_timers[user_id].cancel()
                    clear_user_data_timeout(user_id) # 完成通報，立刻洗掉
                    
                # 6. 無意識深化：223（無抽搐）
                elif user_message == "223":
                    send_line_push(f"{loc_header}\n有人無意識\n請立即前往", user_id, "無意識-無抄搐", custom_loc=raw_loc)
                    if user_id in active_timers: active_timers[user_id].cancel()
                    clear_user_data_timeout(user_id) # 完成通報，立刻洗掉
                    
                # 7. 紅色警戒：999（疑似 OHCA）
                elif user_message == "999":
                    send_line_push(f"{loc_header}\nOHCA\n請立即前往", user_id, "🚨 紅色警戒：疑似 OHCA", custom_loc=raw_loc)
                    if user_id in active_timers: active_timers[user_id].cancel()
                    clear_user_data_timeout(user_id) # 完成通報，立刻洗掉
                    
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
            <p>系統狀態：🟢 24小時看門狗常駐中</p>
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
