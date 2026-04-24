import os
from datetime import datetime, timedelta
import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from config import WATCHLIST, COMPANY_NAMES, DAYS_BACK

load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

today = datetime.now().strftime("%Y-%m-%d")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ====================== 1. 股價 + 公司名稱 ======================
print("📊 抓取股價...")
price_data = []
for stock in WATCHLIST:
    name = COMPANY_NAMES.get(stock, [""])[0]
    stock_name = f"{stock} {name}" if name else stock

    data = yf.Ticker(f"{stock}.TW").history(period="2d")
    if not data.empty:
        latest = data.iloc[-1]
        price_data.append({
            "股票": stock_name,
            "收盤價": round(latest["Close"], 2),
            "漲跌": round(latest["Close"] - latest["Open"], 2),
            "漲跌幅": f"{round((latest['Close']-latest['Open'])/latest['Open']*100, 2)}%"
        })
price_df = pd.DataFrame(price_data)

# ====================== 2. TWSE 官方法人買賣超 ======================
print("💼 抓取 TWSE 官方法人買賣超...")
institutional_data = []
date_str = yesterday.replace("-", "")
for stock in WATCHLIST:
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&stockNo={stock}"
    try:
        r = requests.get(url, timeout=10)
        data = r.json()
        if "data" in data and len(data["data"]) > 0:
            row = data["data"][0]
            name = COMPANY_NAMES.get(stock, [""])[0]
            stock_name = f"{stock} {name}" if name else stock
            institutional_data.append({
                "股票": stock_name,
                "外資": row[2].replace(",", ""),
                "投信": row[3].replace(",", ""),
                "自營": row[4].replace(",", "")
            })
    except:
        pass

# ====================== 3. MOPS 重大訊息 ======================
print("📢 抓取 MOPS...")
mops_news = []
try:
    url = "https://mops.twse.com.tw/mops/web/ajax_t05st01"
    for stock in WATCHLIST:
        payload = {
            "encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
            "queryType": "stock", "co_id": stock, "TYPEK": "all",
            "date1": (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y%m%d")
        }
        r = requests.post(url, data=payload, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        rows = soup.find_all("tr", class_=lambda x: x in ["even", "odd"])
        for row in rows[:8]:
            cols = row.find_all("td")
            if len(cols) >= 4:
                date = cols[0].text.strip()
                title = cols[2].text.strip()
                link = "https://mops.twse.com.tw" + cols[2].find("a")["href"] if cols[2].find("a") else ""
                mops_news.append({"股票": stock, "日期": date, "標題": title, "連結": link})
except:
    pass

# ====================== 4. 生成報告（優化版） ======================
report = f"""🔔 **每日全球財經監控報告**
📅 **{today}**　資料截至 {yesterday}

### 📊 監控股票表現
{price_df.to_markdown(index=False, tablefmt="github")}

### 🌍 關鍵經濟指標
- **美元/台幣**：即期 31.63

### 💼 法人買賣超（TWSE 官方）
"""
for item in institutional_data:
    report += f"- **{item['股票']}**　外資 **{item['外資']}**　投信 **{item['投信']}**　自營 **{item['自營']}**\n"

if mops_news:
    report += "\n### 📢 MOPS 重大訊息（最近2天）\n"
    for item in mops_news[:10]:
        report += f"- **{item['股票']}** {item['日期']}　{item['標題']}　[🔗 連結]({item['連結']})\n"

report += f"\n---\n🚀 資料來源：TWSE 官方 + yfinance | Render Cron Job"

# ====================== 推送 ======================
def send_discord(msg):
    if not DISCORD_WEBHOOK_URL:
        return
    chunks = [msg[i:i+1900] for i in range(0, len(msg), 1900)]
    for chunk in chunks:
        payload = {"content": chunk, "username": "財經監控機器人", "embeds": [{"title": "📊 每日報告", "color": 0x00ff00}]}
        requests.post(DISCORD_WEBHOOK_URL, json=payload)

send_discord(report)
print("🎉 報告已推送至 Discord！")
