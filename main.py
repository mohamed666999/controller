# ============================================================
# AI ALGORITHM LAB - main.py
# Binance USDⓈ-M Futures + NVIDIA AI + PostgreSQL + Telegram
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
import ccxt.async_support as ccxt

# ============================================================
# CONFIG
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

APP_NAME = "AI ALGORITHM LAB"

TELEGRAM_TOKEN = "8688907472:AAHOsxXowXD4HD2GiV5CgPYLHLKx5HJLbi8"

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://USER:PASSWORD@HOST:PORT/DATABASE"
)

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
    "btcusdt",
    "ethusdt",
    "solusdt",
    "xrpusdt",
    "adausdt",
    "dogeusdt",
    "dotusdt",
    "avaxusdt",
    "maticusdt",
    "linkusdt",
    "uniusdt",
    "atomusdt",
    "ltcusdt",
    "bchusdt",
    "nearusdt",
    "filusdt",
    "aptusdt",
    "arbusdt",
    "opusdt",
    "vetusdt",
    "icpusdt",
    "etcusdt",
    "xlmusdt",
    "thetausdt",
    "runeusdt",
    "aaveusdt",
    "mkrusdt",
    "crvusdt",
    "sushiusdt",
    "1inchusdt",
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
# STATE
# ============================================================

state = {
    "started_at": datetime.now(timezone.utc),
    "cycle": 0,
    "agents_ok": 0,
    "agents_failed": 0,
    "last_cycle": None,
    "best_algorithm_id": None,
}

# ============================================================
# DATABASE (نفس الكود السابق)
# ============================================================

def db_connect():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logging.error("❌ Database connection failed: %s", e)
        logging.error(traceback.format_exc())
        raise

def init_database():
    try:
        conn = db_connect()
        logging.info("✅ Connected to database successfully")
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

        CREATE TABLE IF NOT EXISTS backtests (
            id BIGSERIAL PRIMARY KEY,
            algorithm_id BIGINT REFERENCES algorithms(id),
            symbol TEXT,
            total_trades INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            losses INTEGER DEFAULT 0,
            win_rate DOUBLE PRECISION DEFAULT 0,
            profit_factor DOUBLE PRECISION DEFAULT 0,
            max_drawdown DOUBLE PRECISION DEFAULT 0,
            score DOUBLE PRECISION DEFAULT 0,
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

        CREATE TABLE IF NOT EXISTS indicators (
            id BIGSERIAL PRIMARY KEY,
            algorithm_id BIGINT REFERENCES algorithms(id),
            symbol TEXT,
            indicator_name TEXT,
            indicator_value JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        logging.info("✅ All tables created successfully")
        return True
    except Exception as e:
        logging.error("❌ init_database FAILED: %s", e)
        logging.error(traceback.format_exc())
        return False

# ============================================================
# DATABASE HELPERS (نفس الكود السابق - مختصر للطول)
# ============================================================

def log_agent(agent, status, message, duration_ms=0):
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO agent_logs
            (agent_id, agent_name, status, message, duration_ms)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (agent["id"], agent["name"], status, str(message)[:5000], duration_ms)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.warning("⚠️ Could not log to database: %s", e)

def save_algorithm(agent, symbol, hypothesis, code):
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO algorithms
            (lab, symbol, agent_id, agent_name, model_name, hypothesis, code)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
            """,
            ("scalping", symbol, agent["id"], agent["name"], agent["model"], hypothesis, code)
        )
        algorithm_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return algorithm_id
    except Exception as e:
        logging.error("❌ Failed to save algorithm: %s", e)
        return None

def update_algorithm_score(algorithm_id, result):
    if algorithm_id is None:
        return
    try:
        conn = db_connect()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE algorithms
            SET score=%s, win_rate=%s, profit_factor=%s, max_drawdown=%s, status=%s
            WHERE id=%s
            """,
            (result["score"], result["win_rate"], result["profit_factor"],
             result["max_drawdown"], "TESTED", algorithm_id)
        )
        cur.execute(
            """
            INSERT INTO backtests
            (algorithm_id, symbol, total_trades, wins, losses,
             win_rate, profit_factor, max_drawdown, score)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (algorithm_id, result["symbol"], result["total_trades"],
             result["wins"], result["losses"], result["win_rate"],
             result["profit_factor"], result["max_drawdown"], result["score"])
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error("❌ Failed to update algorithm score: %s", e)

def save_trades_batch(algorithm_id, trades):
    if not trades or algorithm_id is None:
        return
    try:
        conn = db_connect()
        cur = conn.cursor()
        for trade in trades:
            cur.execute(
                """
                INSERT INTO paper_trades
                (algorithm_id, symbol, side, entry_price, take_profit, stop_loss,
                 exit_price, status, pnl_percent, opened_at, closed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (algorithm_id, trade["symbol"], trade["side"], trade["entry_price"],
                 trade["take_profit"], trade["stop_loss"], trade["exit_price"],
                 trade["status"], trade["pnl_percent"],
                 trade["opened_at"], trade["closed_at"])
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error("❌ Failed to save trades: %s", e)

def get_best_algorithm():
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM algorithms
            WHERE status='TESTED'
            ORDER BY score DESC
            LIMIT 1
        """)
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logging.warning("⚠️ Could not fetch best algorithm: %s", e)
        return None

def get_recent_trades(limit=10):
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT * FROM paper_trades
            ORDER BY opened_at DESC
            LIMIT %s
        """, (limit,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logging.warning("⚠️ Could not fetch trades: %s", e)
        return []

def get_algorithms_list(limit=5):
    try:
        conn = db_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, agent_name, symbol, score, created_at
            FROM algorithms
            ORDER BY created_at DESC
            LIMIT %s
        """, (limit,))
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logging.warning("⚠️ Could not fetch algorithm list: %s", e)
        return []

# ============================================================
# CCXT - PRELOAD HISTORICAL DATA (محسّن)
# ============================================================

async def preload_market_data(symbol, exchange):
    """
    جلب آخر 500 شمعة 1M من Binance USDT-M Futures وتخزينها في market_cache.
    """
    try:
        # تحويل الرمز إلى صيغة CCXT (مثل btcusdt → BTC/USDT)
        ccxt_symbol = symbol.upper().replace("USDT", "/USDT")
        logging.info(f"📥 Loading historical data: {ccxt_symbol}")

        # جلب الشموع باستخدام طريقة مناسبة للعقود الآجلة
        ohlcv = await exchange.fetch_ohlcv(
            ccxt_symbol,
            timeframe="1m",
            limit=500,
            params={"method": "fetchIndexOHLCV"}  # شموع سعر المؤشر
        )

        if ohlcv and len(ohlcv) >= 50:
            # تحويل البيانات إلى قاموس
            candles = []
            for candle in ohlcv:
                candles.append({
                    "open_time": candle[0],
                    "open": candle[1],
                    "high": candle[2],
                    "low": candle[3],
                    "close": candle[4],
                    "volume": candle[5],
                })
            # تحديث الكاش
            market_cache["candles_1m"][symbol].clear()
            market_cache["candles_1m"][symbol].extend(candles)
            market_cache["prices"][symbol] = candles[-1]["close"]

            logging.info(f"✅ {symbol}: loaded {len(candles)} candles")
            return True
        else:
            logging.warning(f"⚠️ {symbol}: only received {len(ohlcv) if ohlcv else 0} candles")
            return False
    except Exception as e:
        logging.error(f"❌ Historical data failed for {symbol}: {e}")
        return False

# ============================================================
# BINANCE WEBSOCKET
# ============================================================

def build_ws_url():
    streams = []
    for symbol in SYMBOLS:
        streams.append(f"{symbol}@trade")
        streams.append(f"{symbol}@depth20@100ms")
        streams.append(f"{symbol}@kline_1m")
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
# FEATURE ENGINE (محسّن)
# ============================================================

def calculate_features(symbol):
    """
    حساب الميزات من الكاش. إذا كانت البيانات غير كافية، ترجع None مع تسجيل تحذير.
    """
    candles = list(market_cache["candles_1m"][symbol])
    if len(candles) < 50:
        logging.warning(f"⚠️ {symbol}: only {len(candles)} candles (< 50), not enough data")
        return None

    trades = list(market_cache["trades"][symbol])
    depth = list(market_cache["depth"][symbol])

    df = pd.DataFrame(candles)
    closes = df["close"].values
    returns = np.diff(np.log(closes))
    price_velocity = float(np.mean(returns[-10:])) if len(returns) >= 10 else 0
    price_acceleration = float(np.mean(returns[-5:]) - np.mean(returns[-10:-5])) if len(returns) >= 10 else 0
    volatility = float(np.std(returns[-30:])) if len(returns) >= 30 else 0
    volumes = df["volume"].values
    volume_ratio = float(np.mean(volumes[-5:]) / (np.mean(volumes[-30:]) + 1e-12))

    buy_volume = 0.0
    sell_volume = 0.0
    recent_trades = trades[-500:]
    for t in recent_trades:
        if t["maker"]:
            sell_volume += t["qty"]
        else:
            buy_volume += t["qty"]
    trade_imbalance = (buy_volume - sell_volume) / (buy_volume + sell_volume + 1e-12)

    book_imbalance = 0.0
    spread = 0.0
    if depth:
        last_book = depth[-1]
        bids = last_book["bids"]
        asks = last_book["asks"]
        bid_volume = sum(float(x[1]) for x in bids)
        ask_volume = sum(float(x[1]) for x in asks)
        book_imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume + 1e-12)
        if bids and asks:
            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])
            mid = (best_bid + best_ask) / 2
            spread = (best_ask - best_bid) / (mid + 1e-12)

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
        "last_50_closes": closes[-50:].tolist(),
    }

# ============================================================
# باقي الدوال (AI، Backtest، Telegram) كما هي - مختصر للطول
# ============================================================

# ... (نفس الكود السابق للـ AI, Backtest, Telegram)

# لكن سأضع الاختصار الأهم: دوال research_cycle و main

# ============================================================
# RESEARCH CYCLE (محسّن مع لوقات)
# ============================================================

async def research_cycle():
    state["cycle"] += 1
    cycle_num = state["cycle"]
    logging.info(f"🔬 CYCLE {cycle_num} START")

    for symbol in SYMBOLS:
        logging.info(f"📊 Preparing market data for {symbol}...")
        features = calculate_features(symbol)
        if not features:
            logging.warning(f"⚠️ No enough data for {symbol}, skipping")
            continue
        logging.info(f"📊 Market data ready for {symbol} (candles: {features['candles']}, trades: {features['trades']})")

        logging.info(f"🤖 Starting AI agents for {symbol}...")
        tasks = [
            run_agent_with_retries(agent, features)
            for agent in AGENTS
            if not agent["api_key"].startswith("PUT_")
        ]
        logging.info(f"🚀 All {len(tasks)} AI tasks created for {symbol}, awaiting results...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logging.info(f"🏁 All AI tasks finished for {symbol}")

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

            result_bt = backtest(symbol, code, algorithm_id=algorithm_id)
            update_algorithm_score(algorithm_id, result_bt)

            logging.info(
                "AI=%s | SYMBOL=%s | SCORE=%s | TRADES=%s",
                agent["name"], symbol.upper(),
                result_bt["score"], result_bt["total_trades"]
            )

    state["last_cycle"] = datetime.now(timezone.utc)
    logging.info(f"🔬 CYCLE {cycle_num} COMPLETE")

# ============================================================
# RESEARCH LOOP
# ============================================================

async def research_loop():
    while True:
        try:
            logging.info("⏳ Waiting for research cycle...")
            await research_cycle()
        except Exception:
            logging.error(traceback.format_exc())
        logging.info("⏳ Sleeping for 30 minutes until next cycle")
        await asyncio.sleep(30 * 60)

# ============================================================
# TELEGRAM LOOP (نفس الكود السابق)
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
            logging.info("📩 TELEGRAM UPDATE: %s", data)

            for update in data.get("result", []):
                TELEGRAM_OFFSET = update["update_id"] + 1
                message = update.get("message", {})
                text = message.get("text", "")
                chat = message.get("chat", {})
                chat_id = chat.get("id")
                if not chat_id:
                    continue

                # الأوامر كما هي ...
                if text == "/status":
                    uptime = datetime.now(timezone.utc) - state["started_at"]
                    reply = f"""
🤖 {APP_NAME}

🟢 System: ONLINE

🔄 Research cycles: {state["cycle"]}

🤖 Agents success: {state["agents_ok"]}

❌ Agents failed: {state["agents_failed"]}

⏱ Uptime: {uptime}

📊 Symbols: {", ".join(x.upper() for x in SYMBOLS)}
"""
                    await asyncio.to_thread(telegram_send, chat_id, reply)
                # ... باقي الأوامر /best, /code, /trades, /list, /help

        except Exception as e:
            logging.error("TELEGRAM ERROR: %s", e)
            await asyncio.sleep(5)

# ============================================================
# MAIN (محسّن)
# ============================================================

async def main():
    logging.info("%s STARTING", APP_NAME)

    # تهيئة قاعدة البيانات
    db_url = os.getenv("DATABASE_URL")
    if not db_url or db_url.startswith("postgresql://USER:"):
        logging.error("❌ DATABASE_URL is not set or is invalid. Please set it in Railway environment variables.")
    else:
        try:
            success = await asyncio.to_thread(init_database)
            if success:
                logging.info("✅ Database initialized successfully.")
            else:
                logging.warning("⚠️ Database initialization failed, but bot will continue.")
        except Exception as e:
            logging.error("❌ Exception during database init: %s", e)

    # جلب البيانات التاريخية باستخدام CCXT
    logging.info("📥 Preloading historical market data from Binance USDT-M Futures...")
    exchange = ccxt.binanceusdm({
        "enableRateLimit": True,
        "timeout": 30000,
    })

    # محاولة جلب البيانات لجميع الرموز
    preload_tasks = [preload_market_data(symbol, exchange) for symbol in SYMBOLS]
    results = await asyncio.gather(*preload_tasks, return_exceptions=True)

    # حساب عدد الرموز التي تم تحميلها بنجاح
    success_count = 0
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logging.error(f"❌ Error loading {SYMBOLS[i]}: {result}")
        elif result is True:
            success_count += 1
        else:
            logging.warning(f"⚠️ Failed to load {SYMBOLS[i]}")

    logging.info(f"📊 MARKET CACHE READY: {success_count}/{len(SYMBOLS)}")

    # إغلاق اتصال CCXT
    await exchange.close()

    # إذا لم يتم تحميل أي رمز، لا نبدأ البحث
    if success_count == 0:
        logging.error("❌ No market data loaded. Bot will continue without research loop (only Telegram and WebSocket).")

    # تشغيل المهام الأساسية (WebSocket, Research, Telegram)
    await asyncio.gather(
        websocket_worker(),
        research_loop(),
        telegram_loop(),
    )

if __name__ == "__main__":
    asyncio.run(main())
