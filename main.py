# ============================================================
# AI ALGORITHM LAB - main.py
# Binance USDⓈ-M Futures + NVIDIA AI + MemoryDB/PostgreSQL + Telegram
# ============================================================

import os
import json
import time
import math
import asyncio
import logging
import traceback
from collections import deque
from datetime import datetime, timezone

import requests
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import websockets

# ============================================================
# CONFIG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

APP_NAME = "AI ALGORITHM LAB"

TELEGRAM_TOKEN = "8688907472:AAHOsxXowXD4HD2GiV5CgPYLHLKx5HJLbi8"

# قراءة رابط قاعدة البيانات
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://USER:PASSWORD@HOST:PORT/DATABASE")

# تفعيل وضع الذاكرة تلقائياً إذا كان الرابط هو الرابط الافتراضي
MEMORY_MODE = not DATABASE_URL or "HOST:PORT" in DATABASE_URL

NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"

# ============================================================
# AI AGENTS (4 models)
# ============================================================

AGENTS = [
    {
        "id": 1,
        "name": "DEEPSEEK_1",
        "model": "deepseek-ai/deepseek-v4-flash-0731",
        "api_key": "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",
        "temperature": 1.0,
        "reasoning_effort": "high",
        "max_tokens": 16384,
        "max_attempts": 3,
        "request_timeout": 300,
    },
    {
        "id": 2,
        "name": "DEEPSEEK_2",
        "model": "deepseek-ai/deepseek-v4-flash-0731",
        "api_key": "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",
        "temperature": 1.0,
        "reasoning_effort": "high",
        "max_tokens": 16384,
        "max_attempts": 3,
        "request_timeout": 300,
    },
    {
        "id": 3,
        "name": "KIMI",
        "model": "moonshotai/kimi-k3",
        "api_key": "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",
        "temperature": 1.0,
        "reasoning_effort": "max",
        "max_tokens": 16384,
        "max_attempts": 3,
        "request_timeout": 300,
    },
    {
        "id": 4,
        "name": "LLAMA",
        "model": "meta/llama-3.2-90b-vision-instruct",
        "api_key": "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",
        "temperature": 1.0,
        "reasoning_effort": None,
        "max_tokens": 16384,
        "max_attempts": 2,
        "request_timeout": 180,
    },
]

# ============================================================
# MARKET CONFIG - 30 Symbols
# ============================================================

SYMBOLS = [
    "btcusdt", "ethusdt", "solusdt", "xrpusdt", "adausdt",
    "dogeusdt", "dotusdt", "avaxusdt", "maticusdt", "linkusdt",
    "uniusdt", "atomusdt", "ltcusdt", "bchusdt", "nearusdt",
    "filusdt", "aptusdt", "arbusdt", "opusdt", "vetusdt",
    "icpusdt", "etcusdt", "xlmusdt", "thetausdt", "runeusdt",
    "aaveusdt", "mkrusdt", "crvusdt", "sushiusdt", "1inchusdt"
]

BINANCE_WS = "wss://fstream.binance.com/stream?streams="

CACHE_SIZE = 5000

market_cache = {
    "trades": {},
    "depth": {},
    "candles_1m": {},
    "prices": {},
}

for symbol in SYMBOLS:
    market_cache["trades"][symbol] = deque(maxlen=CACHE_SIZE)
    market_cache["depth"][symbol] = deque(maxlen=500)
    market_cache["candles_1m"][symbol] = deque(maxlen=3000)
    market_cache["prices"][symbol] = 0.0

# ============================================================
# STATE & IN-MEMORY DATABASE
# ============================================================

state = {
    "started_at": datetime.now(timezone.utc),
    "cycle": 0,
    "agents_ok": 0,
    "agents_failed": 0,
}

memory_db = {
    "algorithms": [],
    "trades": []
}

# ============================================================
# DATABASE FUNCTIONS (SMART FALLBACK)
# ============================================================

def db_connect():
    if MEMORY_MODE:
        return None
    return psycopg2.connect(DATABASE_URL)

def init_database():
    if MEMORY_MODE:
        logging.info("🟡 Running in MEMORY MODE (No Database configured - Everything works fine locally)")
        return True
    try:
        conn = db_connect()
        cur = conn.cursor()
        sql = """
        CREATE TABLE IF NOT EXISTS algorithms (id BIGSERIAL PRIMARY KEY, lab TEXT NOT NULL, symbol TEXT, agent_id INTEGER, agent_name TEXT, model_name TEXT, hypothesis TEXT, code TEXT NOT NULL, score DOUBLE PRECISION DEFAULT 0, win_rate DOUBLE PRECISION DEFAULT 0, profit_factor DOUBLE PRECISION DEFAULT 0, max_drawdown DOUBLE PRECISION DEFAULT 0, status TEXT DEFAULT 'RESEARCH', created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS agent_logs (id BIGSERIAL PRIMARY KEY, agent_id INTEGER, agent_name TEXT, status TEXT, message TEXT, duration_ms BIGINT, created_at TIMESTAMPTZ DEFAULT NOW());
        CREATE TABLE IF NOT EXISTS paper_trades (id BIGSERIAL PRIMARY KEY, algorithm_id BIGINT REFERENCES algorithms(id), symbol TEXT, side TEXT, entry_price DOUBLE PRECISION, take_profit DOUBLE PRECISION, stop_loss DOUBLE PRECISION, exit_price DOUBLE PRECISION, status TEXT DEFAULT 'OPEN', pnl_percent DOUBLE PRECISION DEFAULT 0, opened_at TIMESTAMPTZ DEFAULT NOW(), closed_at TIMESTAMPTZ);
        """
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("✅ PostgreSQL tables created successfully")
        return True
    except Exception as e:
        logging.error("❌ init_database FAILED: %s", e)
        return False

def log_agent(agent, status, message, duration_ms=0):
    if MEMORY_MODE: return
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute("INSERT INTO agent_logs (agent_id, agent_name, status, message, duration_ms) VALUES (%s, %s, %s, %s, %s)",
                    (agent["id"], agent["name"], status, str(message)[:5000], duration_ms))
        conn.commit()
        cur.close()
        conn.close()
    except: pass

def save_algorithm(agent, symbol, hypothesis, code):
    if MEMORY_MODE:
        algo_id = len(memory_db["algorithms"]) + 1
        memory_db["algorithms"].append({
            "id": algo_id, "agent_name": agent["name"], "model_name": agent["model"],
            "symbol": symbol, "hypothesis": hypothesis, "code": code,
            "score": 0, "win_rate": 0, "profit_factor": 0, "max_drawdown": 0, "status": "RESEARCH",
            "created_at": str(datetime.now(timezone.utc))
        })
        return algo_id
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute("INSERT INTO algorithms (lab, symbol, agent_id, agent_name, model_name, hypothesis, code) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
                    ("scalping", symbol, agent["id"], agent["name"], agent["model"], hypothesis, code))
        algorithm_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return algorithm_id
    except: return None

def update_algorithm_score(algorithm_id, result):
    if not algorithm_id: return
    if MEMORY_MODE:
        for algo in memory_db["algorithms"]:
            if algo["id"] == algorithm_id:
                algo.update({
                    "score": result["score"], "win_rate": result["win_rate"],
                    "profit_factor": result["profit_factor"], "max_drawdown": result["max_drawdown"],
                    "status": "TESTED"
                })
        return
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute("UPDATE algorithms SET score=%s, win_rate=%s, profit_factor=%s, max_drawdown=%s, status='TESTED' WHERE id=%s",
                    (result["score"], result["win_rate"], result["profit_factor"], result["max_drawdown"], algorithm_id))
        conn.commit()
        cur.close()
        conn.close()
    except: pass

def save_trades_batch(algorithm_id, trades):
    if not trades or not algorithm_id: return
    if MEMORY_MODE:
        for t in trades:
            t["id"] = len(memory_db["trades"]) + 1
            t["algorithm_id"] = algorithm_id
            memory_db["trades"].append(t)
        return
    try:
        conn = db_connect()
        cur = conn.cursor()
        for trade in trades:
            cur.execute("INSERT INTO paper_trades (algorithm_id, symbol, side, entry_price, take_profit, stop_loss, exit_price, status, pnl_percent, opened_at, closed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (algorithm_id, trade["symbol"], trade["side"], trade["entry_price"], trade["take_profit"], trade["stop_loss"], trade["exit_price"], trade["status"], trade["pnl_percent"], trade["opened_at"], trade["closed_at"]))
        conn.commit()
        cur.close()
        conn.close()
    except: pass

def get_best_algorithm():
    if MEMORY_MODE:
        tested = [a for a in memory_db["algorithms"] if a.get("status") == "TESTED"]
        if not tested: return None
        return sorted(tested, key=lambda x: x["score"], reverse=True)[0]
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM algorithms WHERE status='TESTED' ORDER BY score DESC LIMIT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except: return None

def get_recent_trades(limit=10):
    if MEMORY_MODE: return list(reversed(memory_db["trades"]))[:limit]
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM paper_trades ORDER BY opened_at DESC LIMIT %s", (limit,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except: return []

def get_algorithms_list(limit=5):
    if MEMORY_MODE: return list(reversed(memory_db["algorithms"]))[:limit]
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, agent_name, symbol, score, created_at FROM algorithms ORDER BY created_at DESC LIMIT %s", (limit,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except: return []

def get_algo_by_id(algo_id):
    if MEMORY_MODE:
        for a in memory_db["algorithms"]:
            if str(a["id"]) == str(algo_id): return a
        return None
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM algorithms WHERE id=%s", (algo_id,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except: return None

# ============================================================
# DIRECT BINANCE REST (Bypass CCXT Issues)
# ============================================================

def fetch_klines_sync(symbol):
    """جلب البيانات مباشرة من منصة بينانس باستخدام requests"""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval=1m&limit=200"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json()

async def preload_market_data(symbol):
    try:
        logging.info(f"📥 Fetching historical data: {symbol.upper()}")
        data = await asyncio.to_thread(fetch_klines_sync, symbol)
        candles = []
        for row in data:
            candles.append({
                "open_time": row[0],
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
            })
        market_cache["candles_1m"][symbol].clear()
        market_cache["candles_1m"][symbol].extend(candles)
        market_cache["prices"][symbol] = candles[-1]["close"]
        logging.info(f"✅ {symbol}: loaded {len(candles)} candles")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to fetch {symbol}: {e}")
        return False

# ============================================================
# BINANCE WEBSOCKET
# ============================================================

def build_ws_url():
    streams = [f"{s}@trade" for s in SYMBOLS] + [f"{s}@depth20@100ms" for s in SYMBOLS] + [f"{s}@kline_1m" for s in SYMBOLS]
    return BINANCE_WS + "/".join(streams)

async def websocket_worker():
    url = build_ws_url()
    while True:
        try:
            logging.info("CONNECTING BINANCE WEBSOCKET")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                logging.info("BINANCE WEBSOCKET CONNECTED")
                async for raw in ws:
                    message = json.loads(raw)
                    stream = message.get("stream", "")
                    data = message.get("data", {})
                    symbol = data.get("s", "").lower()
                    if not symbol: continue

                    if "@trade" in stream:
                        price = float(data["p"])
                        market_cache["prices"][symbol] = price
                        market_cache["trades"][symbol].append({
                            "price": price, "qty": float(data["q"]), "time": data["T"], "maker": data["m"],
                        })
                    elif "@depth" in stream:
                        market_cache["depth"][symbol].append({
                            "bids": data.get("b", []), "asks": data.get("a", []), "time": data.get("E"),
                        })
                    elif "@kline" in stream:
                        k = data["k"]
                        if k["x"]:
                            market_cache["candles_1m"][symbol].append({
                                "open_time": k["t"], "open": float(k["o"]), "high": float(k["h"]),
                                "low": float(k["l"]), "close": float(k["c"]), "volume": float(k["v"]),
                            })
                            market_cache["prices"][symbol] = float(k["c"])
        except Exception as e:
            logging.error("WEBSOCKET ERROR: %s", e)
            await asyncio.sleep(5)

# ============================================================
# FEATURE ENGINE
# ============================================================

def calculate_features(symbol):
    candles = list(market_cache["candles_1m"][symbol])
    # تقليل الحد الأدنى للشموع إلى 20 بدلاً من 50 ليعمل فوراً
    if len(candles) < 20:
        logging.warning(f"⚠️ {symbol}: only {len(candles)} candles (< 20), not enough data")
        return None

    trades = list(market_cache["trades"][symbol])
    depth = list(market_cache["depth"][symbol])

    df = pd.DataFrame(candles)
    closes = df["close"].values
    returns = np.diff(np.log(closes))
    
    price_velocity = float(np.mean(returns[-10:])) if len(returns) >= 10 else 0
    price_acceleration = float(np.mean(returns[-5:]) - np.mean(returns[-10:-5])) if len(returns) >= 10 else 0
    volatility = float(np.std(returns[-30:])) if len(returns) >= 2 else 0
    volumes = df["volume"].values
    volume_ratio = float(np.mean(volumes[-5:]) / (np.mean(volumes[-30:]) + 1e-12)) if len(volumes) >= 5 else 0

    buy_volume, sell_volume = 0.0, 0.0
    for t in trades[-500:]:
        if t["maker"]: sell_volume += t["qty"]
        else: buy_volume += t["qty"]
    trade_imbalance = (buy_volume - sell_volume) / (buy_volume + sell_volume + 1e-12)

    book_imbalance, spread = 0.0, 0.0
    if depth:
        last_book = depth[-1]
        bid_volume = sum(float(x[1]) for x in last_book["bids"])
        ask_volume = sum(float(x[1]) for x in last_book["asks"])
        book_imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume + 1e-12)
        if last_book["bids"] and last_book["asks"]:
            best_bid = float(last_book["bids"][0][0])
            best_ask = float(last_book["asks"][0][0])
            spread = (best_ask - best_bid) / ((best_bid + best_ask) / 2 + 1e-12)

    return {
        "symbol": symbol.upper(),
        "current_price": market_cache["prices"][symbol],
        "candles": len(candles),
        "trades": len(trades),
        "price_velocity": price_velocity,
        "price_acceleration": price_acceleration,
        "volatility": volatility,
        "volume_ratio": volume_ratio,
        "buy_sell_imbalance": trade_imbalance,
        "orderbook_imbalance": book_imbalance,
        "spread": spread,
        "last_50_closes": closes[-50:].tolist() if len(closes) >= 50 else closes.tolist(),
    }

# ============================================================
# AI PROMPT & NVIDIA CALL
# ============================================================

def build_prompt(features):
    return f"""
You are an independent quantitative research scientist.
Your goal is NOT to create a basic trading indicator. Do not simply combine RSI, MACD, EMA.
Study the supplied market features as a mathematical system and independently create a novel mathematical scalping indicator.
You are NOT allowed to copy a conventional BUY/SELL rule.

The final Python code MUST define exactly:
def generate_signal(df):

Input dataframe columns: open, high, low, close, volume
Return a pandas Series containing only: 1 = LONG, -1 = SHORT, 0 = HOLD
Do not use external APIs or imports. You may use numpy as np, pandas as pd, math.

Return EXACTLY in this format:
HYPOTHESIS:
<short explanation>

CODE:
```python
def generate_signal(df):
    ...
