import os
from datetime import datetime, timedelta
import pandas as pd
from FinMind.data import DataLoader
import requests
from bs4 import BeautifulSoup
import yfinance as yf
from twstock import Stock
from dotenv import load_dotenv
from config import WATCHLIST, COMPANY_NAMES, DAYS_BACK

load_dotenv()

dl = DataLoader()
dl.login_by_token(os.getenv("FINMIND_TOKEN"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MARKETAUX_TOKEN = os.getenv("MARKETAUX_TOKEN")

today = datetime.now().strftime("%Y-%m-%d")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# ====================== 1. 股價 + 公司名稱 ======================
print("📊 抓取股價...")
price_data = []
for stock in WATCHLIST:
    name = COMPANY_NAMES.get(stock, [""])[0]
    stock_name = f"{stock} {name}" if name else stock

    df = dl.get_data("TaiwanStockPrice", data_id=stock, start_date=yesterday, end_date=today)
    if not df.empty:
        latest = df.iloc[-1]
        price_data.append({
            "股票": stock_name,
            "收盤價": round(latest["close"], 2),
            "漲跌": round(latest["close"] - latest["open"], 2),
            "漲跌幅": f"{round((latest['close']-latest['open'])/latest['open']*100, 2)}%"
        })
    else:
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

# ====================== 3. twstock 月營收 ======================
print("📑 抓取月營收...")
financial_data = []
for stock_code in WATCHLIST:
    try:
        stock_obj = Stock(stock_code)
        rev = stock_obj.monthly_revenue
        if rev and len(rev) > 0:
            latest = rev[-1]
            name = COMPANY_NAMES.get(stock_code, [""])[0]
            stock_name = f"{stock_code} {name}" if name else stock_code
            financial_data.append({
                "股票": stock_name,
                "月營收": f"{latest['revenue']} 千元",
                "YoY": f"{latest.get('revenue_yoy', 'N/A')}%"
            })
    except:
        pass

# ====================== 4. MOPS 重大訊息 ======================
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

# ====================== 5. Marketaux 全球新聞 ======================
print("🌐 抓取 Marketaux...")
global_news = []
if MARKETAUX_TOKEN:
    try:
        params = {
            "api_token": MARKETAUX_TOKEN,
            "filter_entities": "true",
            "must_have_entities": "true",
            "limit": 15,
            "published_after": yesterday,
            "language": "zh,en"
        }
        resp = requests.get("https://api.marketaux.com/v1/news/all", params=params, timeout=15)
        if resp.status_code == 200:
            for item in resp.json().get("data", []):
                for entity in item.get("entities", []):
                    score = entity.get("sentiment_score", 0)
                    emoji = "🟢" if score > 0.3 else "🔴" if score < -0.3 else "⚪"
                    global_news.append({
                        "類型": entity.get("symbol", "市場"),
                        "標題": item.get("title", ""),
                        "情緒": round(score, 2),
                        "時間": item.get("published_at", "")[:16],
                        "來源": item.get("source", ""),
                        "連結": item.get("url", ""),
                        "emoji": emoji
                    })
    except:
        pass
global_news = sorted(global_news, key=lambda x: x["時間"], reverse=True)[:12]

# ====================== 6. 優化報告格式 ======================
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

if financial_data:
    report += "\n### 📑 最新月營收（twstock）\n"
    for item in financial_data:
        report += f"- **{item['股票']}**　月營收 **{item['月營收']}**　YoY **{item['YoY']}**\n"

if mops_news:
    report += "\n### 📢 MOPS 重大訊息（最近2天）\n"
    for item in mops_news[:10]:
        report += f"- **{item['股票']}** {item['日期']}　{item['標題']}　[🔗 連結]({item['連結']})\n"

if global_news:
    report += "\n### 🌐 Marketaux 全球新聞 + 情緒分析\n"
    for news in global_news:
        report += f"- {news['emoji']} **{news['類型']}** {news['時間']}　{news['標題']}　分數 **{news['情緒']}**　({news['來源']})　[🔗 連結]({news['連結']})\n"

report += f"\n---\n🚀 資料來源：TWSE 官方 + twstock + FinMind + Marketaux | Render Cron Job"

# ====================== 推送 Discord ======================
def send_discord(msg):
    if not DISCORD_WEBHOOK_URL:
        return
    chunks = [msg[i:i+1900] for i in range(0, len(msg), 1900)]
    for chunk in chunks:
        payload = {"content": chunk, "username": "財經監控機器人", "embeds": [{"title": "📊 每日報告", "color": 0x00ff00}]}
        requests.post(DISCORD_WEBHOOK_URL, json=payload)

send_discord(report)
print("🎉 報告已推送至 Discord！")
