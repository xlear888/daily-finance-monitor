import os
from datetime import datetime, timedelta
import pandas as pd
from FinMind.data import DataLoader
import requests
from bs4 import BeautifulSoup
import yfinance as yf
from dotenv import load_dotenv
from config import WATCHLIST, DAYS_BACK

load_dotenv()

dl = DataLoader()
dl.login_by_token(os.getenv("FINMIND_TOKEN"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
MARKETAUX_TOKEN = os.getenv("MARKETAUX_TOKEN")

today = datetime.now().strftime("%Y-%m-%d")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# 1. 股價
print("📊 抓取股價...")
price_data = []
for stock in WATCHLIST:
    df = dl.get_data("TaiwanStockPrice", data_id=stock, start_date=yesterday, end_date=today)
    if not df.empty:
        latest = df.iloc[-1]
        price_data.append({
            "股票": stock,
            "收盤價": round(latest["close"], 2),
            "漲跌": round(latest["close"] - latest["open"], 2),
            "漲跌幅": f"{round((latest['close']-latest['open'])/latest['open']*100, 2)}%"
        })
    else:
        data = yf.Ticker(f"{stock}.TW").history(period="2d")
        if not data.empty:
            latest = data.iloc[-1]
            price_data.append({
                "股票": stock,
                "收盤價": round(latest["Close"], 2),
                "漲跌": round(latest["Close"] - latest["Open"], 2),
                "漲跌幅": f"{round((latest['Close']-latest['Open'])/latest['Open']*100, 2)}%"
            })
price_df = pd.DataFrame(price_data)

# 2. 美元匯率
print("🌍 抓取經濟指標...")
econ_data = {}
try:
    df = dl.get_data("TaiwanExchangeRate", data_id="USD", start_date=yesterday)
    if not df.empty:
        latest = df.iloc[-1]
        econ_data["美元/台幣"] = f"即期 {latest.get('spot_sell', 'N/A')}"
except:
    econ_data["美元/台幣"] = "N/A"

# 3. 法人買賣超
print("💼 抓取法人買賣超...")
institutional_data = []
for stock in WATCHLIST:
    df = dl.get_data("TaiwanStockInstitutionalInvestorsBuySell", data_id=stock, start_date=yesterday, end_date=yesterday)
    if not df.empty:
        latest = df.iloc[-1]
        institutional_data.append({
            "股票": stock,
            "外資": f"{latest.get('Foreign_Investor', 0):,}",
            "投信": f"{latest.get('Investment_Trust', 0):,}",
            "自營": f"{latest.get('Dealer_Hedging', 0):,}"
        })

# 4. 月營收
print("📑 抓取月營收...")
financial_data = []
for stock in WATCHLIST:
    rev_df = dl.get_data("TaiwanStockMonthRevenue", data_id=stock, start_date=yesterday)
    if not rev_df.empty:
        latest = rev_df.iloc[-1]
        financial_data.append({
            "股票": stock,
            "月營收": f"{latest.get('revenue', 'N/A')} 千元",
            "YoY": f"{latest.get('revenue_yoy', 'N/A')}%"
        })

# 5. MOPS 重大訊息
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
        for row in rows[:6]:
            cols = row.find_all("td")
            if len(cols) >= 4:
                date = cols[0].text.strip()
                title = cols[2].text.strip()
                link = "https://mops.twse.com.tw" + cols[2].find("a")["href"] if cols[2].find("a") else ""
                mops_news.append({"股票": stock, "日期": date, "標題": title, "連結": link})
except:
    pass

# 6. Marketaux 全球新聞
print("🌐 抓取 Marketaux...")
global_news = []
if MARKETAUX_TOKEN:
    try:
        params = {
            "api_token": MARKETAUX_TOKEN,
            "filter_entities": "true",
            "must_have_entities": "true",
            "limit": 12,
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
global_news = sorted(global_news, key=lambda x: x["時間"], reverse=True)[:10]

# 7. 生成報告
report = f"""🔔 **每日全球財經監控報告**
📅 日期：{today} （資料截至 {yesterday}）

### 📊 監控股票表現
{price_df.to_markdown(index=False)}

### 🌍 關鍵經濟指標
"""
for k, v in econ_data.items():
    report += f"- **{k}**：{v}\n"

if institutional_data:
    report += "\n### 💼 法人買賣超（昨天）\n"
    for item in institutional_data:
        report += f"- **{item['股票']}** 外資 {item['外資']} | 投信 {item['投信']} | 自營 {item['自營']}\n"

if financial_data:
    report += "\n### 📑 最新月營收\n"
    for item in financial_data:
        report += f"- **{item['股票']}** 月營收 {item['月營收']} (YoY {item['YoY']})\n"

report += "\n### 📢 MOPS 重大訊息（最近2天）\n"
if mops_news:
    for item in mops_news[:8]:
        report += f"- **{item['股票']}** {item['日期']}：{item['標題']} [連結]({item['連結']})\n"
else:
    report += "- 今日無重大訊息\n"

report += "\n### 🌐 Marketaux 全球新聞 + 情緒分析\n"
if global_news:
    for news in global_news:
        report += f"- {news['emoji']} **{news['類型']}** {news['時間']}：{news['標題']} 分數 **{news['情緒']}**（{news['來源']}）[連結]({news['連結']})\n"
else:
    report += "- 今日無全球新聞\n"

report += "\n🚀 資料來源：FinMind + Marketaux + MOPS | Render Cron Job"

# 推送 Discord
def send_discord(msg):
    if not DISCORD_WEBHOOK_URL:
        return
    chunks = [msg[i:i+1900] for i in range(0, len(msg), 1900)]
    for chunk in chunks:
        payload = {"content": chunk, "username": "財經監控機器人", "embeds": [{"title": "📊 每日報告", "color": 0x00ff00}]}
        requests.post(DISCORD_WEBHOOK_URL, json=payload)

send_discord(report)
print("🎉 報告已推送至 Discord！")
