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

# -------------------- IMPORTANT --------------------
# Set DATABASE_URL in Railway environment variables:
#   postgresql://user:pass@host:port/dbname
# If not set, bot runs in MEMORY_MODE (data lost on restart).
# Also, ensure only one replica (scale=1) to avoid Telegram 409 conflict.
# ---------------------------------------------------
TELEGRAM_TOKEN = "8688907472:AAHOsxXowXD4HD2GiV5CgPYLHLKx5HJLbi8"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://USER:PASSWORD@HOST:PORT/DATABASE")
MEMORY_MODE = (not DATABASE_URL) or ("HOST:PORT" in DATABASE_URL)

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
        CREATE TABLE IF NOT EXISTS algorithms (
            id BIGSERIAL PRIMARY KEY,
            lab TEXT NOT NULL,
            symbol TEXT,
            agent_id INTEGER,
            agent_name TEXT,
            model_name TEXT,
            hypothesis TEXT,
            code TEXT NOT NULL,
            score DOUBLE PRECISION DEFAULT 0,
            win_rate DOUBLE PRECISION DEFAULT 0,
            profit_factor DOUBLE PRECISION DEFAULT 0,
            max_drawdown DOUBLE PRECISION DEFAULT 0,
            status TEXT DEFAULT 'RESEARCH',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS agent_logs (
            id BIGSERIAL PRIMARY KEY,
            agent_id INTEGER,
            agent_name TEXT,
            status TEXT,
            message TEXT,
            duration_ms BIGINT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS paper_trades (
            id BIGSERIAL PRIMARY KEY,
            algorithm_id BIGINT REFERENCES algorithms(id),
            symbol TEXT,
            side TEXT,
            entry_price DOUBLE PRECISION,
            take_profit DOUBLE PRECISION,
            stop_loss DOUBLE PRECISION,
            exit_price DOUBLE PRECISION,
            status TEXT DEFAULT 'OPEN',
            pnl_percent DOUBLE PRECISION DEFAULT 0,
            opened_at TIMESTAMPTZ DEFAULT NOW(),
            closed_at TIMESTAMPTZ
        );
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
    if MEMORY_MODE:
        return
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO agent_logs (agent_id, agent_name, status, message, duration_ms) VALUES (%s, %s, %s, %s, %s)",
            (agent["id"], agent["name"], status, str(message)[:5000], duration_ms)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def save_algorithm(agent, symbol, hypothesis, code):
    if MEMORY_MODE:
        algo_id = len(memory_db["algorithms"]) + 1
        memory_db["algorithms"].append({
            "id": algo_id,
            "agent_name": agent["name"],
            "model_name": agent["model"],
            "symbol": symbol,
            "hypothesis": hypothesis,
            "code": code,
            "score": 0,
            "win_rate": 0,
            "profit_factor": 0,
            "max_drawdown": 0,
            "status": "RESEARCH",
            "created_at": str(datetime.now(timezone.utc))
        })
        return algo_id
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO algorithms (lab, symbol, agent_id, agent_name, model_name, hypothesis, code) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
            ("scalping", symbol, agent["id"], agent["name"], agent["model"], hypothesis, code)
        )
        algorithm_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return algorithm_id
    except Exception:
        return None

def update_algorithm_score(algorithm_id, result):
    if not algorithm_id:
        return
    if MEMORY_MODE:
        for algo in memory_db["algorithms"]:
            if algo["id"] == algorithm_id:
                algo.update({
                    "score": result["score"],
                    "win_rate": result["win_rate"],
                    "profit_factor": result["profit_factor"],
                    "max_drawdown": result["max_drawdown"],
                    "status": "TESTED"
                })
        return
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            "UPDATE algorithms SET score=%s, win_rate=%s, profit_factor=%s, max_drawdown=%s, status='TESTED' WHERE id=%s",
            (result["score"], result["win_rate"], result["profit_factor"], result["max_drawdown"], algorithm_id)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def save_trades_batch(algorithm_id, trades):
    if not trades or not algorithm_id:
        return
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
            cur.execute(
                "INSERT INTO paper_trades (algorithm_id, symbol, side, entry_price, take_profit, stop_loss, exit_price, status, pnl_percent, opened_at, closed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (algorithm_id, trade["symbol"], trade["side"], trade["entry_price"],
                 trade["take_profit"], trade["stop_loss"], trade["exit_price"],
                 trade["status"], trade["pnl_percent"],
                 trade["opened_at"], trade["closed_at"])
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

def get_best_algorithm():
    if MEMORY_MODE:
        tested = [a for a in memory_db["algorithms"] if a.get("status") == "TESTED"]
        if not tested:
            return None
        return sorted(tested, key=lambda x: x["score"], reverse=True)[0]
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM algorithms WHERE status='TESTED' ORDER BY score DESC LIMIT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except Exception:
        return None

def get_recent_trades(limit=10):
    if MEMORY_MODE:
        return list(reversed(memory_db["trades"]))[:limit]
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM paper_trades ORDER BY opened_at DESC LIMIT %s", (limit,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except Exception:
        return []

def get_algorithms_list(limit=5):
    if MEMORY_MODE:
        return list(reversed(memory_db["algorithms"]))[:limit]
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, agent_name, symbol, score, created_at FROM algorithms ORDER BY created_at DESC LIMIT %s", (limit,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except Exception:
        return []

def get_algo_by_id(algo_id):
    if MEMORY_MODE:
        for a in memory_db["algorithms"]:
            if str(a["id"]) == str(algo_id):
                return a
        return None
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM algorithms WHERE id=%s", (algo_id,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except Exception:
        return None

# ============================================================
# DIRECT BINANCE REST (Improved logging & retries)
# ============================================================

def fetch_klines_sync(symbol, retries=3):
    """جلب البيانات مع logging مفصل وإعادة محاولة"""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval=1m&limit=200"
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"🔄 Fetching {symbol} (attempt {attempt})")
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                logging.error(f"❌ Binance HTTP {resp.status_code} for {symbol}: {resp.text[:200]}")
                if resp.status_code == 451:
                    logging.error("🚫 Binance 451: Unavailable for legal reasons (likely US region). Please deploy in EU region or use a proxy.")
                raise requests.exceptions.HTTPError(f"HTTP {resp.status_code}")
            data = resp.json()
            logging.info(f"✅ {symbol}: received {len(data)} candles")
            return data
        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt}/{retries} failed for {symbol}: {e}")
            if attempt < retries:
                wait = 2 ** attempt  # exponential backoff: 2, 4, 8 seconds
                logging.info(f"⏳ Retrying {symbol} in {wait}s...")
                time.sleep(wait)
    raise Exception(f"Failed to fetch {symbol} after {retries} attempts")

async def preload_market_data(symbol):
    try:
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
        logging.info(f"✅ {symbol}: loaded {len(candles)} candles into cache")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to preload {symbol}: {e}")
        return False

# ============================================================
# BINANCE WEBSOCKET
# ============================================================

def build_ws_url():
    streams = []
    for s in SYMBOLS:
        streams.append(f"{s}@trade")
        streams.append(f"{s}@depth20@100ms")
        streams.append(f"{s}@kline_1m")
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
                    if not symbol:
                        continue

                    if "@trade" in stream:
                        price = float(data["p"])
                        market_cache["prices"][symbol] = price
                        market_cache["trades"][symbol].append({
                            "price": price,
                            "qty": float(data["q"]),
                            "time": data["T"],
                            "maker": data["m"],
                        })
                    elif "@depth" in stream:
                        market_cache["depth"][symbol].append({
                            "bids": data.get("b", []),
                            "asks": data.get("a", []),
                            "time": data.get("E"),
                        })
                    elif "@kline" in stream:
                        k = data["k"]
                        if k["x"]:
                            market_cache["candles_1m"][symbol].append({
                                "open_time": k["t"],
                                "open": float(k["o"]),
                                "high": float(k["h"]),
                                "low": float(k["l"]),
                                "close": float(k["c"]),
                                "volume": float(k["v"]),
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
    if len(candles) < 20:
        logging.warning(f"⚠️ {symbol}: only {len(candles)} candles (< 20), skipping")
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
        if t["maker"]:
            sell_volume += t["qty"]
        else:
            buy_volume += t["qty"]
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

You are competing against other AI researchers.

Your goal is NOT to create a basic trading indicator.

Do not simply combine RSI, MACD, EMA, or fixed thresholds.

Study the supplied market features as a mathematical system.

Search for hidden relationships involving:

- temporal dependencies
- nonlinear transformations
- derivatives
- acceleration and deceleration
- changing distributions
- order flow
- trade imbalance
- liquidity asymmetry
- volatility regimes
- interaction terms
- normalization
- entropy-like behavior
- structural transitions

You must independently create a novel mathematical
scalping indicator.

You are NOT allowed to copy a conventional
BUY/SELL rule.

The final Python code MUST define exactly:

def generate_signal(df):

Input dataframe columns:

open
high
low
close
volume

The function must return a pandas Series containing only:

1 = LONG
-1 = SHORT
0 = HOLD

Do not use external APIs.

Do not use files.

Do not use network.

Do not use subprocess.

Do not use eval or exec.

Do not import anything.

You may use:

numpy as np
pandas as pd
math

Return your answer EXACTLY in this format:

HYPOTHESIS:
<short explanation>

CODE:
```python
def generate_signal(df):
    ...

MARKET DATA SNAPSHOT:

{json.dumps(features, indent=2)[:12000]} """

def call_agent_sync(agent, features):
    prompt = build_prompt(features)
    headers = {
        "Authorization": f"Bearer {agent['api_key']}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "model": agent["model"],
        "messages": [{"role": "user", "content": prompt}],
        "temperature": agent["temperature"],
        "max_tokens": agent.get("max_tokens", 8000),
        "stream": False,
    }
    if agent.get("reasoning_effort"):
        payload["reasoning_effort"] = agent["reasoning_effort"]

    response = requests.post(NVIDIA_URL, headers=headers, json=payload, timeout=agent.get("request_timeout", 110))
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"]

async def run_agent_with_retries(agent, features):
    agent_name = agent["name"]
    max_attempts = agent.get("max_attempts", 3)
    timeout = agent.get("request_timeout", 300)
    start_total = time.time()

    for attempt in range(1, max_attempts + 1):
        logging.info(f"🤖 {agent_name} | Attempt {attempt}/{max_attempts}")
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(call_agent_sync, agent, features),
                timeout=timeout
            )
            if result and len(str(result).strip()) > 50:
                duration = int((time.time() - start_total) * 1000)
                log_agent(agent, "SUCCESS", f"Attempt {attempt} succeeded", duration)
                state["agents_ok"] += 1
                logging.info(f"✅ {agent_name} succeeded on attempt {attempt}")
                return {
                    "agent": agent,
                    "status": "SUCCESS",
                    "attempt": attempt,
                    "content": result,
                }
            else:
                logging.warning(f"⚠️ {agent_name} returned empty or invalid response on attempt {attempt}")
        except asyncio.TimeoutError:
            logging.warning(f"⏱ {agent_name} timeout on attempt {attempt} ({timeout}s)")
        except Exception as e:
            logging.warning(f"⚠️ {agent_name} error on attempt {attempt}: {e}")

        if attempt < max_attempts:
            wait = 15 * attempt
            logging.info(f"⏳ {agent_name} waiting {wait}s before next attempt")
            await asyncio.sleep(wait)

    duration = int((time.time() - start_total) * 1000)
    log_agent(agent, "FAILED", f"All {max_attempts} attempts failed", duration)
    state["agents_failed"] += 1
    logging.error(f"❌ {agent_name} failed after {max_attempts} attempts")
    return {
        "agent": agent,
        "status": "FAILED",
        "attempt": max_attempts,
        "content": None,
    }

def parse_ai_output(text):
    if not text:
        return None, None
    hypothesis = ""
    if "HYPOTHESIS:" in text:
        part = text.split("HYPOTHESIS:", 1)[1]
        if "CODE:" in part:
            hypothesis = part.split("CODE:", 1)[0].strip()
    code = None
    if "```python" in text:
        code = text.split("```python", 1)[1]
        if "```" in code:
            code = code.split("```", 1)[0]
        code = code.strip()
    elif "def generate_signal" in text:
        index = text.find("def generate_signal")
        code = text[index:].strip()
    return hypothesis, code

def validate_code(code):
    if not code:
        return False, "No code"
    forbidden = [
        "import ", "__import__", "open(", "exec(", "eval(",
        "subprocess", "requests", "socket", "os.", "sys.",
        "pathlib", "shutil"
    ]
    lowered = code.lower()
    for word in forbidden:
        if word in lowered:
            return False, f"Forbidden: {word}"
    if "def generate_signal" not in code:
        return False, "generate_signal missing"
    try:
        compile(code, "<ai_algorithm>", "exec")
        return True, "OK"
    except Exception as e:
        return False, str(e)

# ============================================================
# RUN GENERATED INDICATOR & BACKTEST
# ============================================================

def run_algorithm(code, df):
    safe_globals = {
        "np": np,
        "pd": pd,
        "math": math,
        "__builtins__": {
            "abs": abs, "min": min, "max": max,
            "len": len, "range": range,
            "float": float, "int": int,
        },
    }
    local_vars = {}
    exec(code, safe_globals, local_vars)
    fn = local_vars.get("generate_signal")
    if not fn:
        raise ValueError("generate_signal missing")
    signals = fn(df.copy())
    signals = pd.Series(signals, index=df.index)
    return signals.clip(-1, 1).fillna(0)

def backtest(symbol, code, algorithm_id=None):
    candles = list(market_cache["candles_1m"][symbol])
    if len(candles) < 20:
        return {
            "symbol": symbol.upper(),
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "profit_factor": 0,
            "max_drawdown": 0,
            "score": 0,
            "trades": [],
        }

    df = pd.DataFrame(candles)
    try:
        signals = run_algorithm(code, df)
    except Exception as e:
        logging.error("ALGORITHM EXECUTION FAILED: %s", e)
        return {
            "symbol": symbol.upper(),
            "total_trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": 0,
            "profit_factor": 0,
            "max_drawdown": 100,
            "score": -100,
            "trades": [],
        }

    position = None
    equity = 1000.0
    peak = equity
    max_drawdown = 0.0
    wins = 0
    losses = 0
    profits = []
    loss_sum = 0.0
    TP = 0.004
    SL = 0.0025
    trades_record = []

    for i in range(10, len(df) - 1):
        price = float(df["close"].iloc[i])
        high = float(df["high"].iloc[i])
        low = float(df["low"].iloc[i])
        signal = int(signals.iloc[i])

        if position is None:
            if signal == 1:
                position = {
                    "side": "LONG",
                    "entry": price,
                    "tp": price * (1 + TP),
                    "sl": price * (1 - SL),
                    "opened_at": df["open_time"].iloc[i],
                }
            elif signal == -1:
                position = {
                    "side": "SHORT",
                    "entry": price,
                    "tp": price * (1 - TP),
                    "sl": price * (1 + SL),
                    "opened_at": df["open_time"].iloc[i],
                }
        else:
            exit_price = None
            if position["side"] == "LONG":
                if low <= position["sl"]:
                    exit_price = position["sl"]
                elif high >= position["tp"]:
                    exit_price = position["tp"]
            else:
                if high >= position["sl"]:
                    exit_price = position["sl"]
                elif low <= position["tp"]:
                    exit_price = position["tp"]

            if exit_price:
                entry = position["entry"]
                if position["side"] == "LONG":
                    pnl_pct = (exit_price - entry) / entry
                else:
                    pnl_pct = (entry - exit_price) / entry

                equity *= (1 + pnl_pct)
                profits.append(pnl_pct)
                if pnl_pct > 0:
                    wins += 1
                else:
                    losses += 1
                    loss_sum += abs(pnl_pct)

                peak = max(peak, equity)
                dd = (peak - equity) / peak
                max_drawdown = max(max_drawdown, dd)

                trade_record = {
                    "symbol": symbol.upper(),
                    "side": position["side"],
                    "entry_price": entry,
                    "take_profit": position["tp"],
                    "stop_loss": position["sl"],
                    "exit_price": exit_price,
                    "status": "CLOSED",
                    "pnl_percent": round(pnl_pct * 100, 4),
                    "opened_at": position["opened_at"],
                    "closed_at": df["open_time"].iloc[i],
                }
                trades_record.append(trade_record)
                position = None

    total = wins + losses
    win_rate = wins / total * 100 if total else 0
    profit_sum = sum(x for x in profits if x > 0)
    profit_factor = profit_sum / loss_sum if loss_sum > 0 else profit_sum
    score = win_rate * 0.40 + min(profit_factor, 5) * 10 - max_drawdown * 100 * 0.30

    result = {
        "symbol": symbol.upper(),
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 4),
        "max_drawdown": round(max_drawdown * 100, 4),
        "score": round(score, 4),
        "trades": trades_record,
    }

    if algorithm_id and trades_record:
        save_trades_batch(algorithm_id, trades_record)

    return result

# ============================================================
# RESEARCH CYCLE & LOOP
# ============================================================

async def research_cycle():
    state["cycle"] += 1
    cycle_num = state["cycle"]
    logging.info(f"🔬 CYCLE {cycle_num} START")

    for symbol in SYMBOLS:
        features = calculate_features(symbol)
        if not features:
            continue

        logging.info(f"🤖 Starting AI agents for {symbol}...")
        tasks = [
            run_agent_with_retries(agent, features)
            for agent in AGENTS
            if not agent["api_key"].startswith("PUT_")
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logging.error(f"❌ Agent task raised exception: {result}")
                continue
            if result["status"] != "SUCCESS":
                continue

            agent = result["agent"]
            hypothesis, code = parse_ai_output(result["content"])
            valid, reason = validate_code(code)
            if not valid:
                log_agent(agent, "INVALID_CODE", reason)
                continue

            algorithm_id = save_algorithm(agent, symbol.upper(), hypothesis, code)
            if algorithm_id is None:
                continue

            bt_result = backtest(symbol, code, algorithm_id=algorithm_id)
            update_algorithm_score(algorithm_id, bt_result)

            logging.info(
                "🎯 AI=%s | SYMBOL=%s | SCORE=%s | TRADES=%s",
                agent["name"], symbol.upper(),
                bt_result["score"], bt_result["total_trades"]
            )

    logging.info(f"🔬 CYCLE {cycle_num} COMPLETE")

async def research_loop():
    while True:
        try:
            await research_cycle()
        except Exception:
            logging.error(traceback.format_exc())
        logging.info("⏳ Sleeping 30 minutes...")
        await asyncio.sleep(30 * 60)

# ============================================================
# TELEGRAM API (with conflict handling)
# ============================================================

TELEGRAM_OFFSET = 0

def telegram_request(method, payload=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    response = requests.post(url, json=payload or {}, timeout=30)
    return response.json()

def telegram_send(chat_id, text):
    return telegram_request("sendMessage", {"chat_id": chat_id, "text": text[:4000]})

async def telegram_loop():
    global TELEGRAM_OFFSET
    while True:
        try:
            data = await asyncio.to_thread(
                telegram_request,
                "getUpdates",
                {"offset": TELEGRAM_OFFSET, "timeout": 30}
            )

            # Handle conflict (409) – multiple instances using same token
            if data and not data.get("ok"):
                if data.get("error_code") == 409:
                    logging.error("🚨 TELEGRAM CONFLICT: Another instance is using this bot token. Ensure only one replica is running (scale=1).")
                    await asyncio.sleep(30)
                    continue
                else:
                    logging.error(f"❌ Telegram API error: {data}")
                    await asyncio.sleep(5)
                    continue

            for update in data.get("result", []):
                TELEGRAM_OFFSET = update["update_id"] + 1
                message = update.get("message", {})
                text = message.get("text", "")
                chat = message.get("chat", {})
                chat_id = chat.get("id")
                if not chat_id:
                    continue

                if text == "/status":
                    uptime = datetime.now(timezone.utc) - state["started_at"]
                    db_mode = "🟢 Memory DB" if MEMORY_MODE else "🟢 PostgreSQL"
                    reply = f"""
🤖 {APP_NAME}

{db_mode}

🔄 Research cycles: {state["cycle"]}
🤖 Agents success: {state["agents_ok"]}
❌ Agents failed: {state["agents_failed"]}
⏱ Uptime: {uptime}

📊 Symbols: {len(SYMBOLS)} pairs
"""
                    await asyncio.to_thread(telegram_send, chat_id, reply)

                elif text in ["/best", "/scalping"]:
                    best = await asyncio.to_thread(get_best_algorithm)
                    if not best:
                        reply = "⏳ No tested algorithms yet."
                    else:
                        reply = f"""
🏆 BEST ALGORITHM

ID: {best["id"]}
🤖 AI: {best["agent_name"]}
🧠 Model: {best["model_name"]}
📊 Symbol: {best["symbol"]}
⭐ Score: {best["score"]}
🎯 Win Rate: {best["win_rate"]:.2f}%
📈 Profit Factor: {best["profit_factor"]:.3f}
📉 Max Drawdown: {best["max_drawdown"]:.2f}%

📝 Hypothesis:
{best["hypothesis"][:1200]}
"""
                    await asyncio.to_thread(telegram_send, chat_id, reply)

                elif text.startswith("/code"):
                    parts = text.split()
                    if len(parts) < 2:
                        reply = "Use: /code ID"
                    else:
                        algo_id = parts[1]
                        algo = await asyncio.to_thread(get_algo_by_id, algo_id)
                        if not algo:
                            reply = "❌ Algorithm not found"
                        else:
                            reply = f"💻 ALGORITHM #{algo['id']}\n\n{algo['code']}"
                    await asyncio.to_thread(telegram_send, chat_id, reply)

                elif text == "/trades":
                    trades = await asyncio.to_thread(get_recent_trades, 10)
                    if not trades:
                        reply = "📊 No trades yet."
                    else:
                        lines = ["📊 LAST TRADES"]
                        for t in trades:
                            lines.append(
                                f"ID:{t['id']} {t['symbol']} {t['side']} | "
                                f"Entry:{t['entry_price']:.2f} | "
                                f"Exit:{t['exit_price']:.2f} | "
                                f"PnL:{t['pnl_percent']}%"
                            )
                        reply = "\n".join(lines)
                    await asyncio.to_thread(telegram_send, chat_id, reply)

                elif text == "/list":
                    algos = await asyncio.to_thread(get_algorithms_list, 5)
                    if not algos:
                        reply = "📋 No algorithms yet."
                    else:
                        lines = ["📋 RECENT ALGORITHMS"]
                        for a in algos:
                            lines.append(
                                f"ID:{a['id']} | {a['agent_name']} | "
                                f"{a['symbol']} | Score:{a['score']}"
                            )
                        reply = "\n".join(lines)
                    await asyncio.to_thread(telegram_send, chat_id, reply)

                elif text in ["/start", "/help"]:
                    reply = """
🤖 AI ALGORITHM LAB

Commands:
/status   - System status
/best     - Best algorithm
/scalping - Same as /best
/code ID  - Show algorithm code
/trades   - Recent paper trades
/list     - Recent algorithms
/help     - This message
"""
                    await asyncio.to_thread(telegram_send, chat_id, reply)

        except Exception as e:
            logging.error("TELEGRAM ERROR: %s", e)
            await asyncio.sleep(5)

# ============================================================
# MAIN
# ============================================================

async def main():
    logging.info("%s STARTING", APP_NAME)

    # Init database (or memory mode)
    await asyncio.to_thread(init_database)

    # Preload historical data directly from Binance REST
    logging.info("📥 Preloading historical market data directly from Binance REST API...")
    preload_tasks = [preload_market_data(symbol) for symbol in SYMBOLS]
    results = await asyncio.gather(*preload_tasks, return_exceptions=True)

    success_count = 0
    for i, r in enumerate(results):
        if isinstance(r, Exception):
            logging.error(f"❌ Failed to load {SYMBOLS[i]}: {r}")
        elif r is True:
            success_count += 1
        else:
            logging.warning(f"⚠️ Failed to load {SYMBOLS[i]}")

    logging.info(f"📊 MARKET CACHE READY: {success_count}/{len(SYMBOLS)}")

    if success_count == 0:
        logging.error("❌ No market data loaded. Bot will continue but AI research may not work.")
    else:
        logging.info("✅ Market data loaded successfully. AI research will start with the next cycle.")

    # Run core tasks
    await asyncio.gather(
        websocket_worker(),
        research_loop(),
        telegram_loop(),
    )

if __name__ == "__main__":
    asyncio.run(main())
