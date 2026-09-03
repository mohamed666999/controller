# ============================================================
# AI ALGORITHM LAB - main.py
# Binance USDⓈ-M Futures + NVIDIA AI + MemoryDB/PostgreSQL + Telegram
# ============================================================
# Requirements:
#   pip install requests numpy pandas websockets psycopg2-binary
#   (psycopg2 / websockets are optional - the bot degrades gracefully)
#
# Fixes applied in this version:
#  1. Removed stray "Pasted Content....txt" first line (it was a
#     SyntaxError - the file could not even be parsed before).
#  2. Sandboxed exec now provides full safe builtins (sum, sorted,
#     zip, enumerate, list, dict, ...). Previously almost every
#     AI-generated algorithm crashed with NameError -> score -100.
#  3. paper_trades DB inserts: ms-epoch ints are now converted to
#     timezone-aware datetimes (inserts silently failed on
#     TIMESTAMPTZ columns before).
#  4. research_cycle: backtest + DB writes now run through
#     asyncio.to_thread so the event loop (WebSocket + Telegram)
#     is never blocked during research.
#  5. Telegram: HTTP timeout > long-poll timeout (no more borderline
#     read timeouts), safe JSON handling, startup backlog drain,
#     None-safe /trades and /best formatting.
#  6. run_algorithm: correct signal alignment (no silent all-HOLD
#     reindex), numeric coercion, 30s hard timeout vs infinite loops.
#  7. validate_code: word-boundary regex scan (no more false
#     positives like "cos." matching "os."), comments/strings
#     stripped before scanning.
#  8. parse_ai_output: tolerant of markdown ("**HYPOTHESIS**",
#     "``` python", unclosed fences, multiple code fences).
#  9. NVIDIA API call: robust parsing of error/empty responses.
# 10. Optional dependency guards + preload rate limiting + secrets
#     can be overridden via environment variables (defaults kept).
# 11. Roles architecture: LLAMA researches all 30 symbols while the deep
#     reasoning models (DEEPSEEK_1/DEEPSEEK_2/KIMI) research only 3 random
#     symbols per cycle (39 tasks instead of 120 -> no NVIDIA bottleneck).
# 12. Detailed per-agent metrics in state["stats"]: api_ok / api_timeout /
#     invalid_code / tested / rejected - surfaced in /status and /agents.
# 13. error_stats registry (timeout / connection / empty_response /
#     invalid_response / api_error + Arabic last-error & time) powering
#     the full Arabic diagnostics report via /error and /errors.
# 14. Klines limit=1500; TESTED only if trades>=30 & win_rate>=50% &
#     profit_factor>=1.2 & max_drawdown<=20, otherwise REJECTED; new
#     commands /error /errors /agents /algorithms (alias of /list).
# 15. Pagination for /list with inline "More" button.
# 16. /best now returns highest score algorithm (even if REJECTED)
#     and shows its status.
# ============================================================

import os
import re
import json
import time
import math
import asyncio
import logging
import threading
import traceback
import builtins as _py_builtins
from collections import deque
from datetime import datetime, timezone

import requests
import numpy as np
import pandas as pd

# --- optional dependencies (the bot must still boot without them) ---
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False

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
# ---------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8688907472:AAHOsxXowXD4HD2GiV5CgPYLHLKx5HJLbi8")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://USER:PASSWORD@HOST:PORT/DATABASE")

MEMORY_MODE = (
    (not DATABASE_URL)
    or ("HOST:PORT" in DATABASE_URL)
    or (not PSYCOPG2_AVAILABLE)
)

NVIDIA_URL = "https://integrate.api.nvidia.com/v1/chat/completions"

# ============================================================
# AI AGENTS (4 models)
# ============================================================

AGENTS = [
    {
        "id": 1,
        "name": "DEEPSEEK_1",
        "role": "deep",
        "emoji": "🧠",
        "model": "deepseek-ai/deepseek-v4-flash-0731",
        "api_key": os.getenv("NVIDIA_API_KEY_1", "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"),
        "temperature": 1.0,
        "reasoning_effort": "high",
        "max_tokens": 16384,
        "max_attempts": 3,
        "request_timeout": 300,
    },
    {
        "id": 2,
        "name": "DEEPSEEK_2",
        "role": "deep",
        "emoji": "🧠",
        "model": "deepseek-ai/deepseek-v4-flash-0731",
        "api_key": os.getenv("NVIDIA_API_KEY_2", "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"),
        "temperature": 1.0,
        "reasoning_effort": "high",
        "max_tokens": 16384,
        "max_attempts": 3,
        "request_timeout": 300,
    },
    {
        "id": 3,
        "name": "KIMI",
        "role": "deep",
        "emoji": "🌙",
        "model": "moonshotai/kimi-k3",
        "api_key": os.getenv("NVIDIA_API_KEY_3", "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"),
        "temperature": 1.0,
        "reasoning_effort": "max",
        "max_tokens": 16384,
        "max_attempts": 3,
        "request_timeout": 300,
    },
    {
        "id": 4,
        "name": "LLAMA",
        "role": "fast",
        "emoji": "🦙",
        "model": "meta/llama-3.2-90b-vision-instruct",
        "api_key": os.getenv("NVIDIA_API_KEY_4", "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"),
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
    "stats": {
        "DEEPSEEK_1": {"api_ok": 0, "api_timeout": 0, "invalid_code": 0, "tested": 0, "rejected": 0},
        "DEEPSEEK_2": {"api_ok": 0, "api_timeout": 0, "invalid_code": 0, "tested": 0, "rejected": 0},
        "KIMI":       {"api_ok": 0, "api_timeout": 0, "invalid_code": 0, "tested": 0, "rejected": 0},
        "LLAMA":      {"api_ok": 0, "api_timeout": 0, "invalid_code": 0, "tested": 0, "rejected": 0},
    }
}

# سجل تفصيلي لأخطاء كل نموذج (يغذي أمر /error في التليجرام)
error_stats = {
    "DEEPSEEK_1": {
        "timeout": 0,
        "connection": 0,
        "empty_response": 0,
        "invalid_response": 0,
        "api_error": 0,
        "last_error": "",
        "last_time": "",
    },
    "DEEPSEEK_2": {
        "timeout": 0,
        "connection": 0,
        "empty_response": 0,
        "invalid_response": 0,
        "api_error": 0,
        "last_error": "",
        "last_time": "",
    },
    "KIMI": {
        "timeout": 0,
        "connection": 0,
        "empty_response": 0,
        "invalid_response": 0,
        "api_error": 0,
        "last_error": "",
        "last_time": "",
    },
    "LLAMA": {
        "timeout": 0,
        "connection": 0,
        "empty_response": 0,
        "invalid_response": 0,
        "api_error": 0,
        "last_error": "",
        "last_time": "",
    },
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


def _db_run(sql, params=None, fetch="none"):
    """Execute one SQL statement safely.

    fetch: "none" (write, returns True) | "one" (fetchone) | "all" (fetchall).
    Returns None on any failure so callers can degrade gracefully.
    """
    if MEMORY_MODE or not PSYCOPG2_AVAILABLE:
        return None
    conn = None
    try:
        conn = db_connect()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = None
            if fetch == "one":
                rows = cur.fetchone()
            elif fetch == "all":
                rows = cur.fetchall()
            conn.commit()
            return rows if rows is not None else True
    except Exception as e:
        logging.error("❌ DB operation failed: %s | SQL: %s", e, str(sql)[:200])
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def init_database():
    if MEMORY_MODE:
        reason = "psycopg2 not installed" if not PSYCOPG2_AVAILABLE else "no DATABASE_URL configured"
        logging.info("🟡 Running in MEMORY MODE (%s - Everything works fine locally)", reason)
        return True
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
    result = _db_run(sql)
    if result is None:
        logging.error("❌ init_database FAILED")
        return False
    logging.info("✅ PostgreSQL tables created successfully")
    return True


def log_agent(agent, status, message, duration_ms=0):
    if MEMORY_MODE:
        return
    _db_run(
        "INSERT INTO agent_logs (agent_id, agent_name, status, message, duration_ms) VALUES (%s, %s, %s, %s, %s)",
        (agent["id"], agent["name"], status, str(message)[:5000], duration_ms),
    )


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
    row = _db_run(
        "INSERT INTO algorithms (lab, symbol, agent_id, agent_name, model_name, hypothesis, code) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        ("scalping", symbol, agent["id"], agent["name"], agent["model"], hypothesis, code),
        fetch="one",
    )
    if row is None:
        logging.error("❌ save_algorithm FAILED for agent %s", agent["name"])
        return None
    return row["id"]


def update_algorithm_score(algorithm_id, result):
    if not algorithm_id:
        return

    total_trades = result.get("total_trades", 0)
    win_rate = result.get("win_rate", 0)
    profit_factor = result.get("profit_factor", 0)
    max_drawdown = result.get("max_drawdown", 0)

    # الشروط الدنيا لاعتماد الخوارزمية لتكون صالحة للتداول
    if total_trades >= 30 and win_rate >= 50 and profit_factor >= 1.2 and max_drawdown <= 20:
        status = "TESTED"
    else:
        status = "REJECTED"

    if MEMORY_MODE:
        for algo in memory_db["algorithms"]:
            if algo["id"] == algorithm_id:
                algo.update({
                    "score": result["score"],
                    "win_rate": result["win_rate"],
                    "profit_factor": result["profit_factor"],
                    "max_drawdown": result["max_drawdown"],
                    "status": status
                })
        return
    _db_run(
        "UPDATE algorithms SET score=%s, win_rate=%s, profit_factor=%s, max_drawdown=%s, status=%s WHERE id=%s",
        (result["score"], result["win_rate"], result["profit_factor"], result["max_drawdown"], status, algorithm_id),
    )


def save_trades_batch(algorithm_id, trades):
    if not trades or not algorithm_id:
        return
    if MEMORY_MODE:
        for t in trades:
            t["id"] = len(memory_db["trades"]) + 1
            t["algorithm_id"] = algorithm_id
            memory_db["trades"].append(t)
        return
    conn = None
    try:
        conn = db_connect()
        with conn.cursor() as cur:
            for trade in trades:
                cur.execute(
                    "INSERT INTO paper_trades (algorithm_id, symbol, side, entry_price, take_profit, stop_loss, exit_price, status, pnl_percent, opened_at, closed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (
                        algorithm_id,
                        trade["symbol"],
                        trade["side"],
                        trade["entry_price"],
                        trade["take_profit"],
                        trade["stop_loss"],
                        trade["exit_price"],
                        trade["status"],
                        trade["pnl_percent"],
                        trade["opened_at"],
                        trade["closed_at"],
                    ),
                )
        conn.commit()
    except Exception as e:
        logging.error("❌ save_trades_batch FAILED: %s", e)
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# ======================== تم التعديل هنا ========================
def get_best_algorithm():
    """إرجاع الخوارزمية ذات أعلى سكور (حتى لو لم تكن TESTED)."""
    if MEMORY_MODE:
        if not memory_db["algorithms"]:
            return None
        # نأخذ أعلى سكور بشكل عام
        return max(memory_db["algorithms"], key=lambda x: x.get("score", -float('inf')))
    # نبحث أولاً عن TESTED
    row = _db_run(
        "SELECT * FROM algorithms WHERE status='TESTED' ORDER BY score DESC LIMIT 1",
        fetch="one",
    )
    if row:
        return row
    # إذا لم يوجد TESTED، نأخذ أعلى سكور من الكل
    row = _db_run(
        "SELECT * FROM algorithms ORDER BY score DESC LIMIT 1",
        fetch="one",
    )
    return row


def get_recent_trades(limit=10):
    if MEMORY_MODE:
        return list(reversed(memory_db["trades"]))[:limit]
    rows = _db_run(
        "SELECT * FROM paper_trades ORDER BY opened_at DESC LIMIT %s",
        (limit,),
        fetch="all",
    )
    return rows or []


def get_algorithms_list(limit=4, offset=0):
    """جلب الخوارزميات مع إمكانية التحديد (للـ pagination)."""
    if MEMORY_MODE:
        # ترتيب تنازلي حسب تاريخ الإنشاء (الأحدث أولاً) باستخدام created_at إن وجد
        sorted_algos = sorted(memory_db["algorithms"], key=lambda x: x.get("created_at", x.get("id", 0)), reverse=True)
        return sorted_algos[offset:offset+limit]
    rows = _db_run(
        "SELECT id, agent_name, symbol, score, created_at FROM algorithms ORDER BY created_at DESC LIMIT %s OFFSET %s",
        (limit, offset),
        fetch="all",
    )
    return rows or []


def get_algo_by_id(algo_id):
    if MEMORY_MODE:
        for a in memory_db["algorithms"]:
            if str(a["id"]) == str(algo_id):
                return a
        return None
    row = _db_run("SELECT * FROM algorithms WHERE id=%s", (algo_id,), fetch="one")
    return row

# ============================================================
# DIRECT BINANCE REST (with improved logging & retries)
# ============================================================

def fetch_klines_sync(symbol, retries=3):
    """جلب البيانات مباشرة من منصة بينانس مع إعادة محاولة وتسجيل مفصل."""
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval=1m&limit=1500"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code != 200:
                logging.error(f"❌ Binance HTTP {resp.status_code} for {symbol}: {resp.text[:200]}")
                if resp.status_code == 451:
                    logging.error("🚫 Binance 451: Unavailable for legal reasons (likely US region). Consider using a VPN/proxy or deploy in EU region.")
                raise requests.exceptions.HTTPError(f"HTTP {resp.status_code}")
            data = resp.json()
            if isinstance(data, dict):
                # Binance returned a JSON error object instead of kline rows
                raise ValueError(f"Binance error payload for {symbol}: {str(data)[:200]}")
            if not data:
                raise ValueError(f"Binance returned empty klines for {symbol}")
            return data
        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt}/{retries} failed for {symbol}: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)  # exponential backoff
    raise Exception(f"Failed to fetch {symbol} after {retries} attempts")


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
        if not candles:
            logging.error(f"❌ Empty klines payload for {symbol}")
            return False
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
    streams = []
    for s in SYMBOLS:
        streams.append(f"{s}@trade")
        streams.append(f"{s}@depth20@100ms")
        streams.append(f"{s}@kline_1m")
    return BINANCE_WS + "/".join(streams)


async def websocket_worker():
    if not WEBSOCKETS_AVAILABLE:
        logging.error("❌ 'websockets' package not installed - live market stream disabled (install: pip install websockets)")
        return
    url = build_ws_url()
    while True:
        try:
            logging.info("CONNECTING BINANCE WEBSOCKET")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=2**23) as ws:
                logging.info("BINANCE WEBSOCKET CONNECTED")
                async for raw in ws:
                    try:
                        message = json.loads(raw)
                    except (ValueError, TypeError):
                        continue
                    stream = message.get("stream", "")
                    data = message.get("data", {})
                    if not isinstance(data, dict):
                        continue
                    symbol = str(data.get("s", "")).lower()
                    if not symbol or symbol not in market_cache["prices"]:
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
        logging.warning(f"⚠️ {symbol}: only {len(candles)} candles (< 20), not enough data")
        return None

    trades = list(market_cache["trades"][symbol])
    depth = list(market_cache["depth"][symbol])

    df = pd.DataFrame(candles)
    closes = df["close"].values
    if np.any(closes <= 0):
        return None
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
```

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

async def run_agent_with_retries(agent, features):
agent_name = agent["name"]
max_attempts = agent.get("max_attempts", 3)
timeout = agent.get("request_timeout", 300)
start_total = time.time()
timeout_on_last_attempt = False

============================================================

PARSE & VALIDATE AI OUTPUT

============================================================

def parse_ai_output(text):
"""Extract (hypothesis, code). Tolerant to markdown formatting."""
if not text:
return None, None
cleaned = str(text).replace("**", "")

def _extract_code_block(text):
"""Return the generate_signal python code from a model answer."""
# 1) Prefer fenced blocks (python / py / plain )
    for m in re.finditer(r"(?:python|py)?[ \t]*\r?\n", text, re.IGNORECASE):
rest = text[m.end():]
if "" in rest:
            block = rest.split("", 1)[0]
else:
block = rest  # response was truncated before the closing fence
block = block.strip()
if "def generate_signal" in block:
return block
# 2) Fallback: raw function in the text
idx = text.find("def generate_signal")
if idx != -1:
code = text[idx:]
if "" in code:
            code = code.split("", 1)[0]
return code.strip()
return None

_FORBIDDEN_CODE_PATTERNS = [
r"\bimport\b",
r"\bfrom\s+[\w.]+\s+import\b",
r"import",
r"\bopen\s*\(",
    r"\bexec\s*\(",
    r"\beval\s*\(",
    r"\bcompile\s*\(",
    r"\bglobals\s*\(",
    r"\blocals\s*\(",
    r"\bvars\s*\(",
    r"\bsubprocess\b",
    r"\brequests\b",
    r"\burllib\b",
    r"\bsocket\b",
    r"\bos\s*\.",
    r"\bsys\s*\.",
    r"\bpathlib\b",
    r"\bshutil\b",
]

def validate_code(code):
if not code:
return False, "No code"
if "def generate_signal" not in code:
return False, "generate_signal missing"
# Strip comments and string literals before scanning, so words like
# "requests" or "os." inside comments/docstrings cannot cause false
# rejections (word-boundary regex also prevents "cos." -> "os.").
scanned = re.sub(r"#.", "", code)
scanned = re.sub(r"'(?:\.|[^'\])'|"(?:\.|[^"\])*"", "", scanned)
for pattern in _FORBIDDEN_CODE_PATTERNS:
if re.search(pattern, scanned):
return False, f"Forbidden pattern: {pattern}"
try:
compile(code, "<ai_algorithm>", "exec")
return True, "OK"
except SyntaxError as e:
return False, f"SyntaxError: {e}"
except Exception as e:
return False, str(e)

============================================================

SANDBOX FOR GENERATED INDICATORS

============================================================

Full builtins minus the dangerous ones. The previous tiny whitelist

(abs/min/max/len/range/float/int) made almost every AI algorithm

crash with NameError (sum, sorted, zip, enumerate, list...).

_BUILTIN_BLACKLIST = {
"import", "open", "exec", "eval", "compile", "input", "breakpoint",
"exit", "quit", "help", "license", "credits", "globals", "locals", "vars",
"setattr", "delattr",
}

SAFE_BUILTINS = {
name: obj
for name, obj in vars(_py_builtins).items()
if name not in _BUILTIN_BLACKLIST
}

def _run_with_timeout(fn, args, timeout_s):
"""Run fn(*args) in a daemon thread with a hard timeout."""
box = {}

def _normalize_signals(signals, df):
"""Coerce the indicator output into a clean numeric Series aligned to df."""
if isinstance(signals, pd.DataFrame):
if signals.shape[1] != 1:
raise ValueError("generate_signal must return a single column")
signals = signals.iloc[:, 0]
if isinstance(signals, pd.Series):
raw = signals.to_numpy()
elif isinstance(signals, (list, tuple, np.ndarray)):
raw = np.asarray(signals)
else:
raise TypeError("generate_signal must return a Series / list / array")
if len(raw) != len(df):
raise ValueError(f"generate_signal returned {len(raw)} values, expected {len(df)}")
numeric = pd.to_numeric(pd.Series(raw), errors="coerce")
numeric = numeric.set_axis(df.index)
return numeric.clip(-1, 1).fillna(0).astype(float)

def run_algorithm(code, df):
safe_globals = {
"np": np,
"pd": pd,
"math": math,
"builtins": SAFE_BUILTINS,
"name": "ai_algorithm",
}
local_vars = {}
exec(code, safe_globals, local_vars)  # sandboxed + pre-validated code
fn = local_vars.get("generate_signal")
if not callable(fn):
raise ValueError("generate_signal missing or not callable")
signals = _run_with_timeout(fn, (df.copy(),), 30)
return _normalize_signals(signals, df)

============================================================

BACKTEST

============================================================

def _ms_to_dt(ms):
"""Convert Binance open_time (ms epoch) to a timezone-aware datetime.

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

============================================================

RESEARCH CYCLE & LOOP

============================================================

async def research_cycle():
state["cycle"] += 1
cycle_num = state["cycle"]
logging.info(f"🔬 CYCLE {cycle_num} START")

async def research_loop():
while True:
try:
await research_cycle()
except Exception:
logging.error(traceback.format_exc())
logging.info("⏳ Sleeping 30 minutes...")
await asyncio.sleep(30 * 60)

============================================================

TELEGRAM API (with conflict handling)

============================================================

TELEGRAM_OFFSET = 0

def telegram_request(method, payload=None, http_timeout=45):
url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
try:
response = requests.post(url, json=payload or {}, timeout=http_timeout)
return response.json()
except Exception as e:
logging.error("❌ telegram_request %s failed: %s", method, e)
return {"ok": False, "error_code": -1, "description": str(e)}

======================== تم التعديل هنا ========================

def telegram_send(chat_id, text, reply_markup=None):
"""إرسال رسالة مع إمكانية إضافة أزرار Inline."""
payload = {"chat_id": chat_id, "text": str(text)[:4000]}
if reply_markup:
payload["reply_markup"] = reply_markup
return telegram_request("sendMessage", payload)

def telegram_edit_message(chat_id, message_id, text, reply_markup=None):
"""تعديل رسالة موجودة (للتحديث عند الضغط على زر)."""
payload = {"chat_id": chat_id, "message_id": message_id, "text": str(text)[:4000]}
if reply_markup:
payload["reply_markup"] = reply_markup
return telegram_request("editMessageText", payload)

def telegram_skip_backlog():
"""On startup, discard queued old updates so restarts don't replay
every old command (which previously re-executed on each restart)."""
global TELEGRAM_OFFSET
data = telegram_request("getUpdates", {"offset": -1, "limit": 1})
if data and data.get("ok"):
result = data.get("result", [])
if result:
TELEGRAM_OFFSET = result[-1]["update_id"] + 1
logging.info("↪️ Telegram backlog skipped (old commands will not be replayed)")

def _fmt_price(value):
if value is None:
return "OPEN"
try:
return f"{float(value):.6g}"
except (TypeError, ValueError):
return "?"

============================================================

TELEGRAM REPORT BUILDERS (Arabic error & performance reports)

============================================================

_ERROR_CATEGORY_LABELS = [
("timeout", "⏱ انتهت المهلة"),
("connection", "🔌 انقطاع اتصال"),
("empty_response", "📭 رد فارغ"),
("invalid_response", "📭 رد غير صالح"),
("api_error", "⚠️ خطأ API"),
]

_ERROR_DIAGNOSIS = {
"timeout": "بطء استجابة NVIDIA والنماذج ذات التفكير الطويل.",
"connection": "انقطاع الاتصال بشبكة NVIDIA API.",
"empty_response": "النماذج تعيد ردوداً فارغة أو غير مكتملة.",
"invalid_response": "ردود غير صالحة (JSON مكسور أو بنية غير متوقعة) من NVIDIA API.",
"api_error": "أخطاء من NVIDIA API (تحقق من المفاتيح وحدود الاستخدام).",
}

def _fmt_rate(rate):
"""50.0 -> '50', 6.25 -> '6.25' (matches the report examples)."""
return f"{rate:.2f}".rstrip("0").rstrip(".") or "0"

def build_error_report():
"""تقرير الأخطاء التفصيلي بالعربية (يُعرض عبر /error و /errors)."""
total_failed = sum(d["api_timeout"] for d in state["stats"].values())
cat_totals = {
cat: sum(error_stats.get(a["name"], {}).get(cat, 0) for a in AGENTS)
for cat, _ in _ERROR_CATEGORY_LABELS
}

def build_agents_report():
"""تقرير أداء النماذج (يُعرض عبر /agents)."""
lines = ["🤖 أداء النماذج", ""]
for agent in AGENTS:
name = agent["name"]
d = state["stats"].get(name, {})
ok = d.get("api_ok", 0)
fail = d.get("api_timeout", 0)
calls = ok + fail
rate = (ok / calls * 100) if calls else 0.0
lines.append(f"{agent.get('emoji', '🤖')} {name}")
lines.append(f"✅ نجح: {ok}")
lines.append(f"❌ فشل: {fail}")
lines.append(f"📊 نسبة الاستجابة: {_fmt_rate(rate)}%")
lines.append("")
return "\n".join(lines).strip()

============================================================

TELEGRAM LOOP (مع دعم الأزرار)

============================================================

async def telegram_loop():
global TELEGRAM_OFFSET
await asyncio.to_thread(telegram_skip_backlog)
while True:
try:
# NOTE: HTTP timeout (45s) must be strictly greater than the
# long-poll timeout (25s), otherwise borderline read timeouts.
data = await asyncio.to_thread(
telegram_request,
"getUpdates",
{"offset": TELEGRAM_OFFSET, "timeout": 25},
45,
)

🤖 {APP_NAME}
{db_mode}
⏱ Uptime: {uptime}
🔄 Cycles: {state["cycle"]}

📊 Agent Performance Metrics:
{stats_str}
"""
await asyncio.to_thread(telegram_send, chat_id, reply)

🏆 أفضل خوارزمية (حسب أعلى سكور)

🆔 ID: {best.get("id")}
🤖 AI: {best.get("agent_name") or "-"}
🧠 Model: {best.get("model_name") or "-"}
📊 Symbol: {best.get("symbol") or "-"}
⭐ Score: {best.get("score") or 0}
🎯 Win Rate: {win_rate:.2f}%
📈 Profit Factor: {profit_factor:.3f}
📉 Max Drawdown: {max_drawdown:.2f}%
🏷 الحالة: {status_str}

📝 Hypothesis:
{hypothesis}
"""
await asyncio.to_thread(telegram_send, chat_id, reply)

🤖 AI ALGORITHM LAB

Commands:
/status      - System status
/best        - Best algorithm (highest score)
/scalping    - Same as /best
/code ID     - Show algorithm code
/trades      - Recent paper trades
/list        - Recent algorithms (paginated)
/algorithms  - Same as /list
/error       - تقرير الأخطاء التفصيلي بالعربية
/errors      - Same as /error
/agents      - Per-model performance
/help        - This message
"""
await asyncio.to_thread(telegram_send, chat_id, reply)

============================================================

MAIN

============================================================

async def preload_all():
"""Preload all symbols with a concurrency limit (5) to avoid
hitting Binance REST rate limits with 30 simultaneous calls."""
semaphore = asyncio.Semaphore(5)

async def main():
logging.info("%s STARTING", APP_NAME)

if name == "main":
try:
asyncio.run(main())
except KeyboardInterrupt:
logging.info("🛑 Shutdown requested (Ctrl+C). Bye!")
