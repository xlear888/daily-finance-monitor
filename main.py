import os
from datetime import datetime, timedelta
import pandas as pd
from FinMind.data import DataLoader
import requests
from bs4 import BeautifulSoup
import yfinance as yf
import finnhub
from dotenv import load_dotenv
from config import WATCHLIST, COMPANY_NAMES, DAYS_BACK, US_WATCH_SYMBOLS

load_dotenv()

dl = DataLoader()
dl.login_by_token(os.getenv("FINMIND_TOKEN"))
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
finnhub_client = finnhub.Client(api_key=os.getenv("FINNHUB_TOKEN")) if os.getenv("FINNHUB_TOKEN") else None
MARKETAUX_TOKEN = os.getenv("MARKETAUX_TOKEN")

today = datetime.now().strftime("%Y-%m-%d")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# 1. 股價
print("📊 抓取股價...")
price_data = []
for stock in WATCHLIST:
    df = dl.get_dataset("TaiwanStockPrice", data_id=stock, start_date=yesterday, end_date=today)
    if not df.empty:
        latest = df.iloc[-1]
        price_data.append({
            "股票": stock, "收盤價": round(latest["close"], 2),
            "漲跌": round(latest["close"] - latest["open"], 2),
            "漲跌幅": f"{round((latest['close']-latest['open'])/latest['open']*100, 2)}%"
        })
    else:
        data = yf.Ticker(f"{stock}.TW").history(period="2d")
        if not data.empty:
            latest = data.iloc[-1]
            price_data.append({
                "股票": stock, "收盤價": round(latest["Close"], 2),
                "漲跌": round(latest["Close"] - latest["Open"], 2),
                "漲跌幅": f"{round((latest['Close']-latest['Open'])/latest['Open']*100, 2)}%"
            })
price_df = pd.DataFrame(price_data)

# 2. 經濟指標
print("🌍 抓取經濟指標...")
econ_data = {}
for cur in ["USD", "JPY", "CNY", "EUR"]:
    df = dl.get_dataset("TaiwanExchangeRate", data_id=cur, start_date=yesterday)
    if not df.empty:
        latest = df.iloc[-1]
        econ_data[f"匯率_{cur}"] = f"即期 {latest.get('spot_sell', 'N/A')}"

df_bi = dl.get_dataset("TaiwanBusinessIndicator", start_date=yesterday)
if not df_bi.empty:
    latest = df_bi.iloc[-1]
    econ_data["景氣指標"] = f"領先:{latest.get('leading','N/A')} | 燈號:{latest.get('monitoring','N/A')}"

for ds, name in [("CrudeOilPrices", "原油"), ("GoldPrice", "黃金")]:
    df = dl.get_dataset(ds, start_date=yesterday)
    if not df.empty:
        econ_data[name] = f"{df.iloc[-1].get('price', 'N/A')} USD"

# 3. 法人買賣超
print("💼 抓取法人買賣超...")
institutional_data = []
for stock in WATCHLIST:
    df = dl.get_dataset("TaiwanStockInstitutionalInvestorsBuySell", data_id=stock, start_date=yesterday, end_date=today)
    if not df.empty:
        latest = df.iloc[-1]
        institutional_data.append({
            "股票": stock,
            "外資": f"{latest.get('Foreign_Investor', 0):,}",
            "投信": f"{latest.get('Investment_Trust', 0):,}",
            "自營": f"{latest.get('Dealer_Hedging', 0):,}"
        })

# 4. 財報
print("📑 抓取財報...")
financial_data = []
for stock in WATCHLIST:
    rev_df = dl.get_dataset("TaiwanStockMonthRevenue", data_id=stock, start_date=yesterday)
    if not rev_df.empty:
        latest = rev_df.iloc[-1]
        financial_data.append({
            "股票": stock,
            "月營收": f"{latest.get('revenue', 'N/A')} 千元",
            "YoY": f"{latest.get('revenue_yoy', 'N/A')}%"
        })

# 5. MOPS 重大訊息
print("📢 抓取 MOPS...")
def get_mops_news(stock_list, days=2):
    url = "https://mops.twse.com.tw/mops/web/ajax_t05st01"
    news_list = []
    for stock in stock_list:
        payload = {"encodeURIComponent": "1", "step": "1", "firstin": "1", "off": "1",
                   "queryType": "stock", "co_id": stock, "TYPEK": "all",
                   "date1": (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")}
        try:
            r = requests.post(url, data=payload, timeout=10)
            soup = BeautifulSoup(r.text, "lxml")
            rows = soup.find_all("tr", class_=lambda x: x in ["even", "odd"])
            for row in rows[:5]:
                cols = row.find_all("td")
                if len(cols) >= 4:
                    date = cols[0].text.strip()
                    title = cols[2].text.strip()
                    link = "https://mops.twse.com.tw" + cols[2].find("a")["href"] if cols[2].find("a") else ""
                    news_list.append({"股票": stock, "日期": date, "標題": title, "連結": link})
        except:
            pass
    return news_list

mops_news = get_mops_news(WATCHLIST, DAYS_BACK)

# 6. 美國國會議員交易
print("🇺🇸 抓取國會議員交易...")
congress_trades = []
if finnhub_client:
    for symbol in US_WATCH_SYMBOLS:
        try:
            trades = finnhub_client.congressional_trading(symbol=symbol, _from=yesterday, to=today)
            for t in trades[:3]:
                congress_trades.append({
                    "議員": t.get("representative", "N/A"),
                    "股票": symbol,
                    "類型": t.get("transaction", "N/A"),
                    "金額": t.get("amount", "N/A"),
                    "日期": t.get("transaction_date", "N/A")
                })
        except:
            pass

# 7. Marketaux 全球新聞 + 情緒
print("🌐 抓取 Marketaux...")
global_news = []
if MARKETAUX_TOKEN:
    try:
        params = {
            "api_token": MARKETAUX_TOKEN, "filter_entities": "true",
            "must_have_entities": "true", "limit": 15,
            "published_after": yesterday, "language": "zh,en",
            "symbols": ",".join([f"{s}.TW" for s in WATCHLIST] + ["TSM"])
        }
        resp = requests.get("https://api.marketaux.com/v1/news/all", params=params, timeout=15)
        if resp.status_code == 200:
            for item in resp.json().get("data", []):
                for entity in item.get("entities", []):
                    score = entity.get("sentiment_score", 0)
                    emoji = "🟢" if score > 0.3 else "🟡" if score > -0.3 else "🔴"
                    global_news.append({
                        "類型": entity.get("symbol", "市場"),
                        "標題": item.get("title", ""),
                        "情緒": round(score, 3),
                        "時間": item.get("published_at", "")[:16],
                        "來源": item.get("source", ""),
                        "連結": item.get("url", ""),
                        "emoji": emoji
                    })
    except:
        pass
global_news = sorted(global_news, key=lambda x: x["時間"], reverse=True)[:12]

# 8. 生成報告
report = f"""🔔 **每日全球財經監控報告**
📅 日期：{today}

### 📊 監控股票表現
{price_df.to_markdown(index=False)}

### 🌍 關鍵經濟指標
"""
for k, v in econ_data.items():
    report += f"- **{k}**：{v}\n"

report += "\n### 💼 法人買賣超\n"
for item in institutional_data:
    report += f"- **{item['股票']}** 外資 {item['外資']} | 投信 {item['投信']} | 自營 {item['自營']}\n"

report += "\n### 📑 最新財報\n"
for item in financial_data:
    report += f"- **{item['股票']}** 月營收 {item.get('月營收', '')} (YoY {item.get('YoY', '')})\n"

report += "\n### 📢 MOPS 重大訊息\n"
for item in mops_news[:8]:
    report += f"- **{item['股票']}** {item['日期']}：{item['標題']} [連結]({item['連結']})\n"

if congress_trades:
    report += "\n### 🇺🇸 美國國會議員股票申報\n"
    for t in congress_trades:
        report += f"- **{t['議員']}** {t['股票']} {t['類型']} {t['金額']} ({t['日期']})\n"

report += "\n### 🌐 Marketaux 全球新聞 + 情緒分析\n"
for news in global_news:
    report += f"- {news['emoji']} **{news['類型']}** {news['時間']}：{news['標題']} 分數 **{news['情緒']}**（{news['來源']}）[連結]({news['連結']})\n"

report += "\n🚀 資料來源：FinMind + Marketaux + MOPS + Finnhub | Render Cron Job"

# 9. 推送 Discord
def send_discord(msg):
    if not DISCORD_WEBHOOK_URL:
        print("⚠️ 未設定 DISCORD_WEBHOOK_URL")
        return
    chunks = [msg[i:i+1900] for i in range(0, len(msg), 1900)]
    for chunk in chunks:
        payload = {"content": chunk, "username": "財經監控機器人", "embeds": [{"title": "📊 每日報告", "color": 0x00ff00}]}
        requests.post(DISCORD_WEBHOOK_URL, json=payload)

send_discord(report)
print("🎉 報告已推送至 Discord！")
