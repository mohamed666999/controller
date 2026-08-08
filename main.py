#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║     APEX MONITOR BOT — Telegram AI Monitor v4.0            ║
║  Architecture: Monitor & Analysis Only (No Execution)      ║
║  No local trades DB, reads from APEX API only             ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import sys
import time
import json
import logging
import threading
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass, field
from collections import deque, defaultdict
from queue import Queue, Empty
from functools import wraps

import numpy as np
import requests
import ccxt
from openai import OpenAI

# -------------------- استيرادات تيليجرام --------------------
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

# =============================================================================
# 🌐 عرض IP فور بدء التشغيل (لـ Railway Logs) — IP DEPLOYMENT DETECTION
# =============================================================================

def get_public_ip():
    """جلب الـ IP العام لـ Railway Deployment"""
    try:
        urls = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
            "https://checkip.amazonaws.com"
        ]
        for url in urls:
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    ip = resp.text.strip()
                    if ip and len(ip) > 7:
                        return ip
            except:
                continue
        return "UNKNOWN"
    except Exception as e:
        return f"ERROR: {e}"

print("=" * 70)
print("🚀 RAILWAY DEPLOYMENT IP DETECTION")
print("=" * 70)
DEPLOYMENT_IP = get_public_ip()
print(f"📌 PUBLIC IP: {DEPLOYMENT_IP}")
print("=" * 70)
print("⚠️  ADD THIS IP TO BINANCE API WHITELIST!")
print("🔗 https://www.binance.com/en/support/faq/how-to-configure-ip-access-restrictions-on-binance-api-360041267251")
print("=" * 70)

try:
    with open("deployment_ip.txt", "w") as f:
        f.write(f"DEPLOYMENT_IP={DEPLOYMENT_IP}\n")
        f.write(f"TIMESTAMP={datetime.now().isoformat()}\n")
        f.write("BINANCE_API_KEY=6wsmpKnCpMpC3u8H6GuEbIarvCPtK2fyNmbl7GfEVq0dK2BDsC2fTsBrqxmFK5pB\n")
        f.write("NEWS_API_KEY=ee6adc6bb00849d5bb0b1a29e62d5ed4\n")
        f.write(f"TELEGRAM_BOT=8122906116:AAHAWsXfaiymnvdeNO0BURyRVccJU8_gIco\n")
    print("✅ IP saved to deployment_ip.txt")
except Exception as e:
    print(f"⚠️ Could not save IP file: {e}")

# =============================================================================
# 🔧 CONFIG
# =============================================================================

TELEGRAM_TOKEN = "8122906116:AAHAWsXfaiymnvdeNO0BURyRVccJU8_gIco"
ADMIN_CHAT_ID = 6033203084

# 🔹 تفعيل API وتعيين رابط البوت الأول
USE_API_INSTEAD_OF_DB = True
APEX_API_URL = "https://binancetrading-production.up.railway.app"   # <-- رابط البوت الأول

# 🔹 تعطيل Binance Fallback نهائياً
USE_BINANCE_FALLBACK = False

# لا نستخدم SQLite للصفقات، فقط للتحليلات الخاصة بالبوت الثاني (اختياري)
MONITOR_DB_PATH = "monitor.db"

# 🔹 لم نعد بحاجة لمفاتيح Binance في البوت الثاني
BINANCE_API_KEY = ""
BINANCE_SECRET = ""
NVIDIA_API_KEY = "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"
NVIDIA_API_KEY_OSS = "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"

MONITOR_INTERVAL = 60
ANALYSIS_DELAY_HOURS = 24
MAX_OPEN_TRADES_TO_MONITOR = 20
AI_MODEL = "mistralai/mistral-medium-3.5-128b"
AI_MODEL_OSS = "openai/gpt-oss-120b"
USE_AI_VETO = False
DUAL_AI_ENABLED = True

NEWS_API_KEY = "ee6adc6bb00849d5bb0b1a29e62d5ed4"
NEWS_ENABLED = True
NEWS_CACHE_TTL = 1800

RECOMMENDATION_THRESHOLD = 0.65

AI_CACHE_TTL = 300
AI_MIN_PROFIT_CHANGE = 0.5
AI_MIN_PRICE_CHANGE = 0.3

CIRCUIT_BREAKER_FAILURE_THRESHOLD = 5
CIRCUIT_BREAKER_TIMEOUT = 60

# =============================================================================
# 🛡️ CIRCUIT BREAKER
# =============================================================================

class CircuitBreaker:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = 0
        self.last_failure = None
        self.state = "CLOSED"

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if self.state == "OPEN":
                if time.time() - self.last_failure > self.timeout:
                    self.state = "HALF_OPEN"
                    logger.info(f"Circuit breaker {func.__name__} -> HALF_OPEN")
                else:
                    logger.warning(f"Circuit breaker {func.__name__} OPEN, skipping call")
                    return None
            try:
                result = func(*args, **kwargs)
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failures = 0
                    logger.info(f"Circuit breaker {func.__name__} -> CLOSED")
                return result
            except Exception as e:
                self.failures += 1
                self.last_failure = time.time()
                if self.failures >= self.failure_threshold:
                    self.state = "OPEN"
                    logger.error(f"Circuit breaker {func.__name__} -> OPEN due to {e}")
                raise
        return wrapper

circuit_breaker = CircuitBreaker(
    failure_threshold=CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    timeout=CIRCUIT_BREAKER_TIMEOUT
)

# =============================================================================
# 🗄️ DATABASE (فقط للتحليلات الخاصة بالبوت الثاني، لا صفقات)
# =============================================================================

class MonitorDB:
    def __init__(self, monitor_db_path):
        self.monitor_conn = sqlite3.connect(monitor_db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_tables()

    def _init_tables(self):
        with self.lock:
            # جداول التحليل فقط، لا جداول للصفقات
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS open_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    symbol TEXT,
                    side TEXT,
                    entry_price REAL,
                    current_price REAL,
                    profit_pct REAL,
                    time_open_minutes INTEGER,
                    target_progress REAL,
                    trend_strength REAL,
                    momentum_score REAL,
                    funding_rate REAL,
                    oi_change_1h REAL,
                    oi_trend REAL,
                    apex_score REAL,
                    iss_score REAL,
                    ai_decision TEXT,
                    ai_confidence REAL,
                    ai_explanation TEXT,
                    ai2_decision TEXT,
                    ai2_confidence REAL,
                    ai2_explanation TEXT,
                    recommendation TEXT,
                    probability_tp REAL,
                    probability_sl REAL,
                    probability_sideways REAL,
                    probability_reversal REAL,
                    timestamp TEXT
                );
            """)
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS closed_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    symbol TEXT,
                    side TEXT,
                    entry_price REAL,
                    exit_price REAL,
                    pnl_percent REAL,
                    duration_minutes INTEGER,
                    close_reason TEXT,
                    apex_score REAL,
                    iss_score REAL,
                    ai_score_at_entry REAL,
                    ai_score_at_close REAL,
                    external_score REAL,
                    recommendation_at_entry TEXT,
                    recommendation_at_close TEXT,
                    was_recommendation_correct BOOLEAN,
                    if_held_to_tp_pnl REAL,
                    if_held_to_sl_pnl REAL,
                    timestamp TEXT
                );
            """)
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS system_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    system_name TEXT,
                    total_trades INTEGER,
                    wins INTEGER,
                    loss INTEGER,
                    winrate REAL,
                    avg_profit REAL,
                    avg_rr REAL,
                    avg_hold_time REAL,
                    false_signals INTEGER,
                    canceled_signals INTEGER,
                    saved_losses INTEGER,
                    missed_profits INTEGER,
                    total_pnl REAL,
                    last_updated TEXT
                );
            """)
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS lessons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    symbol TEXT,
                    market_regime TEXT,
                    apex_decision TEXT,
                    ai_decision TEXT,
                    iss_decision TEXT,
                    actual_outcome TEXT,
                    apex_pnl REAL,
                    ai_pnl REAL,
                    iss_pnl REAL,
                    best_decision TEXT,
                    lesson TEXT,
                    timestamp TEXT
                );
            """)
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS rejected_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    apex_score REAL,
                    ai_score REAL,
                    iss_score REAL,
                    external_score REAL,
                    reason TEXT,
                    hypothetical_pnl REAL,
                    timestamp TEXT
                );
            """)
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS confusion_matrix (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    system_name TEXT,
                    true_positive INTEGER,
                    true_negative INTEGER,
                    false_positive INTEGER,
                    false_negative INTEGER,
                    precision REAL,
                    recall REAL,
                    f1_score REAL,
                    timestamp TEXT
                );
            """)
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS market_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    timestamp TEXT,
                    market_structure TEXT,
                    liquidity_levels TEXT,
                    order_flow_imbalance REAL,
                    delta_volume REAL,
                    cvd REAL,
                    whale_activity INTEGER,
                    fear_greed_index INTEGER,
                    correlation_btc REAL,
                    session TEXT
                );
            """)
            self.monitor_conn.commit()

    # ---- قراءة الصفقات المفتوحة من API فقط (لا SQLite) ----
    @circuit_breaker
    def get_open_trades(self) -> List[Dict[str, Any]]:
        """جلب الصفقات المفتوحة من البوت الأول عبر API"""
        try:
            resp = requests.get(f"{APEX_API_URL}/positions", timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    logger.info(f"Fetched {len(data)} open trades from API")
                    return data
                else:
                    logger.warning(f"API returned unexpected data: {type(data)}")
                    return []
            else:
                logger.warning(f"API returned status {resp.status_code}")
                return []
        except Exception as e:
            logger.error(f"API error in get_open_trades: {e}")
            return []

    # ---- جلب الإحصائيات من API ----
    def get_statistics(self) -> Dict[str, Any]:
        """جلب الإحصائيات من البوت الأول عبر /health"""
        try:
            resp = requests.get(f"{APEX_API_URL}/health", timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if 'performance' in data:
                    return data['performance']
                else:
                    return data
            else:
                logger.warning(f"Health API returned {resp.status_code}")
                return {}
        except Exception as e:
            logger.error(f"Health API error: {e}")
            return {}

    # ---- باقي دوال التخزين المحلي (للتحليلات الخاصة بالبوت الثاني) ----
    def save_open_analysis(self, data: Dict[str, Any]):
        with self.lock:
            self.monitor_conn.execute("""
                INSERT INTO open_analysis (
                    trade_id, symbol, side, entry_price, current_price,
                    profit_pct, time_open_minutes, target_progress,
                    trend_strength, momentum_score, funding_rate,
                    oi_change_1h, oi_trend, apex_score, iss_score,
                    ai_decision, ai_confidence, ai_explanation,
                    ai2_decision, ai2_confidence, ai2_explanation,
                    recommendation, probability_tp, probability_sl,
                    probability_sideways, probability_reversal, timestamp
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data['trade_id'], data['symbol'], data['side'],
                data['entry_price'], data['current_price'],
                data['profit_pct'], data['time_open_minutes'],
                data['target_progress'], data['trend_strength'],
                data['momentum_score'], data['funding_rate'],
                data['oi_change_1h'], data['oi_trend'],
                data['apex_score'], data['iss_score'],
                data['ai_decision'], data['ai_confidence'],
                data['ai_explanation'],
                data.get('ai2_decision', ''),
                data.get('ai2_confidence', 0),
                data.get('ai2_explanation', ''),
                data['recommendation'],
                data.get('probability_tp', 0),
                data.get('probability_sl', 0),
                data.get('probability_sideways', 0),
                data.get('probability_reversal', 0),
                data['timestamp']
            ))
            self.monitor_conn.commit()

    def save_market_analysis(self, data: Dict[str, Any]):
        with self.lock:
            self.monitor_conn.execute("""
                INSERT INTO market_analysis (
                    symbol, timestamp, market_structure,
                    liquidity_levels, order_flow_imbalance,
                    delta_volume, cvd, whale_activity,
                    fear_greed_index, correlation_btc, session
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data['symbol'], data['timestamp'], data['market_structure'],
                json.dumps(data['liquidity_levels']),
                data['order_flow_imbalance'], data['delta_volume'],
                data['cvd'], data['whale_activity'],
                data['fear_greed_index'], data['correlation_btc'],
                data['session']
            ))
            self.monitor_conn.commit()

    def save_closed_analysis(self, data: Dict[str, Any]):
        with self.lock:
            self.monitor_conn.execute("""
                INSERT INTO closed_analysis (
                    trade_id, symbol, side, entry_price, exit_price,
                    pnl_percent, duration_minutes, close_reason,
                    apex_score, iss_score, ai_score_at_entry,
                    ai_score_at_close, external_score,
                    recommendation_at_entry, recommendation_at_close,
                    was_recommendation_correct,
                    if_held_to_tp_pnl, if_held_to_sl_pnl,
                    timestamp
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data['trade_id'], data['symbol'], data['side'],
                data['entry_price'], data['exit_price'],
                data['pnl_percent'], data['duration_minutes'],
                data['close_reason'],
                data['apex_score'], data['iss_score'],
                data['ai_score_at_entry'], data['ai_score_at_close'],
                data['external_score'],
                data['recommendation_at_entry'],
                data['recommendation_at_close'],
                data['was_recommendation_correct'],
                data['if_held_to_tp_pnl'], data['if_held_to_sl_pnl'],
                data['timestamp']
            ))
            self.monitor_conn.commit()

    def update_system_performance(self, system_name: str, stats: Dict):
        with self.lock:
            self.monitor_conn.execute("""
                INSERT OR REPLACE INTO system_performance (
                    system_name, total_trades, wins, loss,
                    winrate, avg_profit, avg_rr, avg_hold_time,
                    false_signals, canceled_signals, saved_losses,
                    missed_profits, total_pnl, last_updated
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                system_name,
                stats.get('total_trades', 0),
                stats.get('wins', 0),
                stats.get('loss', 0),
                stats.get('winrate', 0),
                stats.get('avg_profit', 0),
                stats.get('avg_rr', 0),
                stats.get('avg_hold_time', 0),
                stats.get('false_signals', 0),
                stats.get('canceled_signals', 0),
                stats.get('saved_losses', 0),
                stats.get('missed_profits', 0),
                stats.get('total_pnl', 0),
                datetime.now(timezone.utc).isoformat()
            ))
            self.monitor_conn.commit()

    def save_lesson(self, data: Dict[str, Any]):
        with self.lock:
            self.monitor_conn.execute("""
                INSERT INTO lessons (
                    trade_id, symbol, market_regime,
                    apex_decision, ai_decision, iss_decision,
                    actual_outcome, apex_pnl, ai_pnl, iss_pnl,
                    best_decision, lesson, timestamp
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                data['trade_id'], data['symbol'], data['market_regime'],
                data['apex_decision'], data['ai_decision'], data['iss_decision'],
                data['actual_outcome'], data['apex_pnl'], data['ai_pnl'],
                data['iss_pnl'], data['best_decision'], data['lesson'],
                data['timestamp']
            ))
            self.monitor_conn.commit()

    def save_rejected_trade(self, data: Dict[str, Any]):
        with self.lock:
            self.monitor_conn.execute("""
                INSERT INTO rejected_trades (
                    symbol, apex_score, ai_score, iss_score,
                    external_score, reason, hypothetical_pnl, timestamp
                ) VALUES (?,?,?,?,?,?,?,?)
            """, (
                data['symbol'], data['apex_score'], data['ai_score'],
                data['iss_score'], data['external_score'],
                data['reason'], data['hypothetical_pnl'],
                data['timestamp']
            ))
            self.monitor_conn.commit()

    def get_all_performance(self) -> List[Dict]:
        with self.lock:
            cursor = self.monitor_conn.execute(
                "SELECT system_name, total_trades, wins, loss, winrate, "
                "avg_profit, avg_rr, avg_hold_time, total_pnl "
                "FROM system_performance ORDER BY total_pnl DESC"
            )
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_latest_open_analysis(self, trade_id: int) -> Dict:
        with self.lock:
            cursor = self.monitor_conn.execute(
                "SELECT * FROM open_analysis WHERE trade_id=? ORDER BY timestamp DESC LIMIT 1",
                (trade_id,)
            )
            row = cursor.fetchone()
            if row:
                cols = [desc[0] for desc in cursor.description]
                return dict(zip(cols, row))
            return {}

    def get_lessons(self, limit=500) -> List[Dict]:
        with self.lock:
            cursor = self.monitor_conn.execute(
                "SELECT * FROM lessons ORDER BY timestamp DESC LIMIT ?",
                (limit,)
            )
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def update_confusion_matrix(self, system_name: str, tp, tn, fp, fn):
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        with self.lock:
            self.monitor_conn.execute("""
                INSERT INTO confusion_matrix (
                    system_name, true_positive, true_negative,
                    false_positive, false_negative,
                    precision, recall, f1_score, timestamp
                ) VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                system_name, tp, tn, fp, fn,
                precision, recall, f1,
                datetime.now(timezone.utc).isoformat()
            ))
            self.monitor_conn.commit()
        return {"precision": precision, "recall": recall, "f1": f1}

# =============================================================================
# 📊 ADVANCED ANALYTICS ENGINE (نفس الكود السابق، يستخدم exchange_public)
# =============================================================================

class AdvancedAnalyticsEngine:
    def __init__(self, exchange_public, deriv_feed=None):
        self.exchange = exchange_public
        self.deriv_feed = deriv_feed
        self.cache = {}
        self.news_cache = {}
        self.liquidity_levels_cache = {}

    def fetch_market_data(self, symbol: str) -> Dict:
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            ohlcv = self.exchange.fetch_ohlcv(symbol, '1h', limit=100)
            orderbook = self.exchange.fetch_order_book(symbol, limit=50)
            return {
                'price': ticker['last'],
                'high_24h': ticker['high'],
                'low_24h': ticker['low'],
                'volume': ticker['quoteVolume'],
                'ohlcv': ohlcv,
                'orderbook': orderbook,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            return {'error': str(e)}

    def trend_strength(self, ohlcv: List) -> float:
        if len(ohlcv) < 50:
            return 0.0
        highs = [c[2] for c in ohlcv]
        lows = [c[3] for c in ohlcv]
        closes = [c[4] for c in ohlcv]
        sma20 = np.mean(closes[-20:])
        sma50 = np.mean(closes[-50:])
        trend = abs(sma20 - sma50) / sma50
        atr = self._atr(highs, lows, closes, 14)
        atr_pct = atr / closes[-1] if closes[-1] != 0 else 0
        score = trend * 0.7 + min(atr_pct * 5, 0.3) * 0.3
        return min(score * 100, 100)

    def _atr(self, highs, lows, closes, period=14):
        if len(closes) < period + 1:
            return 0.0
        trs = []
        for i in range(1, len(closes)):
            tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
            trs.append(tr)
        return np.mean(trs[-period:])

    def momentum_score(self, ohlcv: List) -> float:
        if len(ohlcv) < 30:
            return 50.0
        closes = [c[4] for c in ohlcv]
        highs = [c[2] for c in ohlcv]
        lows = [c[3] for c in ohlcv]
        rsi = self._rsi(closes)
        macd = self._macd(closes)
        cci = self._cci(highs, lows, closes, period=20)
        rsi_score = 50 + (rsi - 50) * 0.5
        macd_score = 50 + macd * 10
        cci_score = 50 + cci / 2
        score = rsi_score * 0.4 + macd_score * 0.3 + cci_score * 0.3
        return max(0, min(100, score))

    def _rsi(self, prices, period=14):
        if len(prices) < period:
            return 50.0
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[-period:])
        avg_loss = np.mean(losses[-period:])
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _macd(self, prices, fast=12, slow=26, signal=9):
        if len(prices) < slow:
            return 0.0
        ema_fast = self._ema(prices, fast)
        ema_slow = self._ema(prices, slow)
        macd_line = ema_fast[-1] - ema_slow[-1]
        max_val = max(abs(max(prices)), abs(min(prices)))
        return macd_line / max_val if max_val != 0 else 0.0

    def _ema(self, prices, period):
        if len(prices) < period:
            return prices
        k = 2.0 / (period + 1)
        ema = prices[0]
        for p in prices[1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def _cci(self, highs, lows, closes, period=20):
        if len(closes) < period:
            return 0.0
        tp = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(len(closes))]
        sma_tp = np.mean(tp[-period:])
        mad = np.mean([abs(tp[i] - sma_tp) for i in range(-period, 0)])
        if mad == 0:
            return 0.0
        cci = (tp[-1] - sma_tp) / (0.015 * mad)
        return max(-100, min(100, cci))

    def funding_rate(self, symbol: str) -> float:
        try:
            s = symbol.split('/')[0]
            url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={s}USDT"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return float(resp.json().get('lastFundingRate', 0))
        except:
            pass
        return 0.0

    def oi_change_1h(self, symbol: str) -> float:
        try:
            s = symbol.split('/')[0]
            url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={s}USDT&period=1h&limit=2"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) >= 2:
                    v1 = float(data[-1]['sumOpenInterestValue'])
                    v0 = float(data[-2]['sumOpenInterestValue'])
                    return (v1 - v0) / v0 if v0 != 0 else 0.0
        except:
            pass
        return 0.0

    def oi_trend(self, symbol: str) -> float:
        try:
            s = symbol.split('/')[0]
            url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={s}USDT&period=1h&limit=7"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                vals = [float(d['sumOpenInterestValue']) for d in data]
                if len(vals) >= 7:
                    slope = np.polyfit(range(len(vals)), vals, 1)[0]
                    return slope / (np.mean(vals) + 1e-10) * 100
        except:
            pass
        return 0.0

    def detect_market_regime(self, ohlcv: List) -> str:
        if len(ohlcv) < 50:
            return "UNKNOWN"
        closes = [c[4] for c in ohlcv]
        sma20 = np.mean(closes[-20:])
        sma50 = np.mean(closes[-50:])
        volatility = np.std(closes[-20:]) / np.mean(closes[-20:])
        if abs(sma20 - sma50) / sma50 > 0.02:
            return "TRENDING" if sma20 > sma50 else "TRENDING_DOWN"
        elif volatility > 0.03:
            return "HIGH_VOLATILITY"
        else:
            return "RANGING"

    def recent_news(self, symbol: str, limit=5) -> List[str]:
        if not NEWS_ENABLED:
            return []
        now = time.time()
        cache_key = symbol
        if cache_key in self.news_cache:
            data, timestamp = self.news_cache[cache_key]
            if now - timestamp < NEWS_CACHE_TTL:
                return data
        try:
            coin = symbol.split('/')[0].lower()
            url = f"https://newsapi.org/v2/everything?q={coin}&apiKey={NEWS_API_KEY}&language=en&sortBy=publishedAt&pageSize={limit}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                articles = data.get('articles', [])
                titles = [a['title'] for a in articles[:limit] if a.get('title')]
                if titles:
                    self.news_cache[cache_key] = (titles, now)
                    return titles
        except Exception as e:
            logger.debug(f"News API error: {e}")
        try:
            coin = symbol.split('/')[0].lower()
            url = f"https://cryptopanic.com/api/v1/posts/?auth_token={NEWS_API_KEY}&currencies={coin}"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                news = resp.json().get('results', [])
                titles = [n['title'] for n in news[:limit]]
                self.news_cache[cache_key] = (titles, now)
                return titles
        except:
            pass
        return []

    # ===== Market Structure =====
    def detect_market_structure(self, ohlcv: List) -> Dict:
        if len(ohlcv) < 50:
            return {"structure": "UNKNOWN", "breakout": False, "fakeout": False}
        highs = [c[2] for c in ohlcv]
        lows = [c[3] for c in ohlcv]
        closes = [c[4] for c in ohlcv]
        swing_highs = []
        swing_lows = []
        for i in range(2, len(highs)-2):
            if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                swing_highs.append((i, highs[i]))
            if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                swing_lows.append((i, lows[i]))
        last_high = max(highs[-10:])
        last_low = min(lows[-10:])
        prev_high = max(highs[-20:-10]) if len(highs) >= 20 else last_high
        prev_low = min(lows[-20:-10]) if len(lows) >= 20 else last_low
        breakout_up = closes[-1] > prev_high * 1.01
        breakout_down = closes[-1] < prev_low * 0.99
        fakeout_up = False
        fakeout_down = False
        if len(ohlcv) >= 30:
            recent_highs = highs[-15:]
            if max(recent_highs) > prev_high * 1.01 and closes[-1] < prev_high * 0.995:
                fakeout_up = True
            if min(lows[-15:]) < prev_low * 0.99 and closes[-1] > prev_low * 1.005:
                fakeout_down = True
        if breakout_up and not fakeout_up:
            structure = "BREAKOUT_UP"
        elif breakout_down and not fakeout_down:
            structure = "BREAKOUT_DOWN"
        elif fakeout_up:
            structure = "FAKEOUT_UP"
        elif fakeout_down:
            structure = "FAKEOUT_DOWN"
        elif len(swing_highs) >= 3 and len(swing_lows) >= 3:
            sh_trend = swing_highs[-1][1] > swing_highs[-2][1] > swing_highs[-3][1]
            sl_trend = swing_lows[-1][1] > swing_lows[-2][1] > swing_lows[-3][1]
            if sh_trend and sl_trend:
                structure = "TRENDING_UP"
            elif not sh_trend and not sl_trend:
                structure = "TRENDING_DOWN"
            else:
                structure = "RANGING"
        else:
            structure = "RANGING"
        return {
            "structure": structure,
            "breakout_up": breakout_up,
            "breakout_down": breakout_down,
            "fakeout_up": fakeout_up,
            "fakeout_down": fakeout_down,
            "swing_highs": swing_highs[-5:],
            "swing_lows": swing_lows[-5:],
            "prev_high": prev_high,
            "prev_low": prev_low
        }

    # ===== Liquidity Levels =====
    def detect_liquidity_levels(self, orderbook: Dict, ohlcv: List) -> Dict:
        if not orderbook:
            return {"high_liquidity": [], "low_liquidity": []}
        bids = orderbook.get('bids', [])
        asks = orderbook.get('asks', [])
        high_liquidity_bids = []
        high_liquidity_asks = []
        bid_clusters = defaultdict(float)
        ask_clusters = defaultdict(float)
        for price, amount in bids[:20]:
            if amount > 0:
                key = round(price / (price * 0.001), 3)
                bid_clusters[key] += amount * price
        for price, amount in asks[:20]:
            if amount > 0:
                key = round(price / (price * 0.001), 3)
                ask_clusters[key] += amount * price
        high_liquidity_bids = sorted(bid_clusters.items(), key=lambda x: x[1], reverse=True)[:3]
        high_liquidity_asks = sorted(ask_clusters.items(), key=lambda x: x[1], reverse=True)[:3]
        if len(ohlcv) >= 50:
            highs = [c[2] for c in ohlcv[-50:]]
            lows = [c[3] for c in ohlcv[-50:]]
            resistance_levels = []
            support_levels = []
            for i in range(5, len(highs)-5):
                if highs[i] > highs[i-1] and highs[i] > highs[i+1]:
                    count = sum(1 for j in range(i-10, i+10) if abs(highs[j] - highs[i]) / highs[i] < 0.005)
                    if count >= 3:
                        resistance_levels.append(highs[i])
                if lows[i] < lows[i-1] and lows[i] < lows[i+1]:
                    count = sum(1 for j in range(i-10, i+10) if abs(lows[j] - lows[i]) / lows[i] < 0.005)
                    if count >= 3:
                        support_levels.append(lows[i])
            high_liquidity_bids.extend([(f"SUPPORT_{i+1}", v) for i, v in enumerate(support_levels[:3])])
            high_liquidity_asks.extend([(f"RESISTANCE_{i+1}", v) for i, v in enumerate(resistance_levels[:3])])
        return {
            "high_liquidity_bids": high_liquidity_bids,
            "high_liquidity_asks": high_liquidity_asks,
            "liquidity_imbalance": sum([a[1] for a in high_liquidity_asks]) - sum([b[1] for b in high_liquidity_bids])
        }

    # ===== Order Flow =====
    def analyze_order_flow(self, symbol: str) -> Dict:
        try:
            s = symbol.split('/')[0]
            url = f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={s}USDT&period=5m&limit=24"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if len(data) >= 10:
                    buy_vol = sum(float(d['buyVol']) for d in data[-10:])
                    sell_vol = sum(float(d['sellVol']) for d in data[-10:])
                    total_vol = buy_vol + sell_vol
                    imbalance = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0
                    cvd = 0
                    cvd_values = []
                    for d in data:
                        delta = float(d['buyVol']) - float(d['sellVol'])
                        cvd += delta
                        cvd_values.append(cvd)
                    cvd_trend = 0
                    if len(cvd_values) >= 5:
                        slope = np.polyfit(range(len(cvd_values[-5:])), cvd_values[-5:], 1)[0]
                        cvd_trend = 1 if slope > 0 else (-1 if slope < 0 else 0)
                    return {
                        'buy_volume': buy_vol,
                        'sell_volume': sell_vol,
                        'imbalance': imbalance,
                        'cvd': cvd,
                        'cvd_trend': cvd_trend,
                        'delta': (buy_vol - sell_vol) / total_vol
                    }
        except Exception as e:
            logger.debug(f"Order flow error: {e}")
        return {
            'buy_volume': 0,
            'sell_volume': 0,
            'imbalance': 0,
            'cvd': 0,
            'cvd_trend': 0,
            'delta': 0
        }

    # ===== Whale Activity =====
    def detect_whale_activity(self, symbol: str) -> int:
        try:
            trades = self.exchange.fetch_trades(symbol, limit=100)
            if not trades:
                return 0
            avg_size = np.mean([t['amount'] for t in trades])
            std_size = np.std([t['amount'] for t in trades])
            whale_trades = [t for t in trades if t['amount'] > avg_size + 2 * std_size]
            if len(whale_trades) > 5:
                return 3
            elif len(whale_trades) > 3:
                return 2
            elif len(whale_trades) > 1:
                return 1
            else:
                return 0
        except:
            return 0

    # ===== Fear & Greed =====
    def get_fear_greed_index(self) -> int:
        try:
            resp = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return int(data['data'][0]['value'])
        except:
            pass
        return 50

    # ===== BTC Correlation =====
    def get_btc_correlation(self, symbol: str) -> float:
        if symbol.startswith('BTC'):
            return 1.0
        try:
            symbol_ohlcv = self.exchange.fetch_ohlcv(symbol, '15m', limit=96)
            btc_ohlcv = self.exchange.fetch_ohlcv('BTC/USDT:USDT', '15m', limit=96)
            if len(symbol_ohlcv) < 50 or len(btc_ohlcv) < 50:
                return 0.5
            s_close = np.array([c[4] for c in symbol_ohlcv])
            b_close = np.array([c[4] for c in btc_ohlcv])
            corr = np.corrcoef(s_close, b_close)[0, 1]
            return max(-1, min(1, corr))
        except:
            return 0.5

    # ===== Session =====
    def get_session(self) -> str:
        now = datetime.now(timezone.utc)
        hour = now.hour
        if 0 <= hour < 8:
            return "ASIA"
        elif 8 <= hour < 16:
            return "LONDON"
        else:
            return "NEW_YORK"

    # ===== Analyze Market =====
    def analyze_market(self, symbol: str) -> Dict:
        market = self.fetch_market_data(symbol)
        if 'error' in market:
            return {'error': market['error']}
        ohlcv = market.get('ohlcv', [])
        orderbook = market.get('orderbook', {})
        structure = self.detect_market_structure(ohlcv)
        liquidity = self.detect_liquidity_levels(orderbook, ohlcv)
        order_flow = self.analyze_order_flow(symbol)
        whale = self.detect_whale_activity(symbol)
        fng = self.get_fear_greed_index()
        corr = self.get_btc_correlation(symbol)
        session = self.get_session()
        closes = [c[4] for c in ohlcv] if ohlcv else []
        rsi = self._rsi(closes) if closes else 50
        liquidation_levels = self._estimate_liquidation_levels(market.get('price', 0))
        result = {
            'symbol': symbol,
            'price': market.get('price', 0),
            'market_structure': structure['structure'],
            'breakout': structure['breakout_up'] or structure['breakout_down'],
            'fakeout': structure['fakeout_up'] or structure['fakeout_down'],
            'liquidity_imbalance': liquidity.get('liquidity_imbalance', 0),
            'order_flow_imbalance': order_flow.get('imbalance', 0),
            'cvd': order_flow.get('cvd', 0),
            'cvd_trend': order_flow.get('cvd_trend', 0),
            'delta_volume': order_flow.get('delta', 0),
            'whale_activity': whale,
            'fear_greed': fng,
            'btc_correlation': corr,
            'session': session,
            'rsi': rsi,
            'liquidation_levels': liquidation_levels,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        if hasattr(self, 'db'):
            self.db.save_market_analysis(result)
        return result

    def _estimate_liquidation_levels(self, price):
        if price == 0:
            return {'long_liquidations': 0, 'short_liquidations': 0}
        return {
            'long_liquidations': price * 0.95,
            'short_liquidations': price * 1.05
        }

# =============================================================================
# 🤖 AI CLIENT (مع دمج الدروس)
# =============================================================================

logger = logging.getLogger(__name__)

class AIClient:
    def __init__(self, db=None):
        self.client_mistral = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=NVIDIA_API_KEY,
        )
        self.client_oss = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=NVIDIA_API_KEY_OSS,
        )
        self.db = db
        self.lessons_cache = None
        self.lessons_cache_time = 0

    def _get_relevant_lessons(self, market_regime, symbol, limit=10):
        if not self.db:
            return []
        if time.time() - self.lessons_cache_time > 3600:
            self.lessons_cache = self.db.get_lessons(limit=500)
            self.lessons_cache_time = time.time()
        lessons = self.lessons_cache or []
        filtered = [l for l in lessons if l['symbol'] == symbol or l['market_regime'] == market_regime]
        filtered = sorted(filtered, key=lambda x: x['timestamp'], reverse=True)[:limit]
        return filtered

    def _call_ai_with_probabilities(self, client, model, trade_data, market_data):
        lessons = self._get_relevant_lessons(trade_data.get('market_regime', 'UNKNOWN'), trade_data['symbol'])
        lessons_text = ""
        if lessons:
            lessons_text = "\nدروس مستفادة من صفقات سابقة:\n"
            for l in lessons[:5]:
                lessons_text += f"- {l['symbol']} ({l['market_regime']}): الأفضل كان {l['best_decision']}، الدرس: {l['lesson']}\n"

        prompt = f"""أنت محلل تداول خبير. بناءً على البيانات التالية، قم بتحليل احتمالات السيناريوهات المختلفة للصفقة المفتوحة.

═══════════════════════════════════════════
📊 بيانات الصفقة
═══════════════════════════════════════════
العملة: {trade_data['symbol']}
سعر الدخول: {trade_data['entry_price']}
السعر الحالي: {trade_data['current_price']}
الربح الحالي: {trade_data['profit_pct']:.2f}%
الوقت منذ الفتح: {trade_data['time_open_minutes']} دقيقة
نسبة تحقيق الهدف: {trade_data['target_progress']:.1f}%

📈 مؤشرات السوق
قوة الاتجاه: {trade_data['trend_strength']:.1f}
الزخم: {trade_data['momentum_score']:.1f}
معدل التمويل: {trade_data['funding_rate']:.6f}
تغير OI (ساعة): {trade_data['oi_change_1h']:.2%}
اتجاه OI: {trade_data['oi_trend']:.2f}

🎯 نتائج الأنظمة
APEX Score: {trade_data['apex_score']:.1f}
ISS Score: {trade_data['iss_score']:.1f}

🔬 تحليل السوق المتقدم
بنية السوق: {market_data.get('market_structure', 'UNKNOWN')}
الاختراق: {'نعم' if market_data.get('breakout', False) else 'لا'}
الفيك أوت: {'نعم' if market_data.get('fakeout', False) else 'لا'}
عدم توازن السيولة: {market_data.get('liquidity_imbalance', 0):.2f}
عدم توازن تدفق الأوامر: {market_data.get('order_flow_imbalance', 0):.2f}
CVD: {market_data.get('cvd', 0):.0f}
اتجاه CVD: {'صاعد' if market_data.get('cvd_trend', 0) > 0 else 'هابط' if market_data.get('cvd_trend', 0) < 0 else 'محايد'}
نشاط الحيتان: {market_data.get('whale_activity', 0)}/3
مؤشر الخوف والجشع: {market_data.get('fear_greed', 50)}
ارتباط البيتكوين: {market_data.get('btc_correlation', 0.5):.2f}
الجلسة الحالية: {market_data.get('session', 'UNKNOWN')}
RSI: {market_data.get('rsi', 50):.1f}
{lessons_text}

═══════════════════════════════════════════
📋 المطلوب
═══════════════════════════════════════════

بناءً على جميع المعطيات، قم بتقدير احتمالات السيناريوهات التالية (0-100%):

1. احتمال الوصول إلى الهدف (Take Profit) - TP%
2. احتمال ضرب وقف الخسارة (Stop Loss) - SL%
3. احتمال بقاء السعر في نطاق جانبي - SIDEWAYS%
4. احتمال انعكاس الاتجاه - REVERSAL%

ثم بناءً على هذه الاحتمالات، قرر:
- التوصية النهائية: HOLD أو CLOSE
- الثقة في التوصية (0-100)

أجب بصيغة JSON:
{{
    "tp_probability": 65,
    "sl_probability": 20,
    "sideways_probability": 10,
    "reversal_probability": 5,
    "recommendation": "HOLD",
    "confidence": 85,
    "reason": "سبب مختصر بالعربية يوضح التحليل"
}}
"""
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.4,
                max_tokens=500
            )
            raw = response.choices[0].message.content
            json_str = raw.strip()
            if json_str.startswith("```"):
                lines = json_str.split("\n")
                json_str = "\n".join([l for l in lines if not l.strip().startswith("```")])
            if json_str.startswith("{"):
                result = json.loads(json_str)
                result['recommendation'] = result.get('recommendation', 'HOLD').upper()
                result['confidence'] = min(100, max(0, float(result.get('confidence', 50))))
                result['reason'] = result.get('reason', '')
                return result
        except Exception as e:
            logger.error(f"AI ({model}) call failed: {e}")
        return {
            "tp_probability": 50,
            "sl_probability": 25,
            "sideways_probability": 15,
            "reversal_probability": 10,
            "recommendation": "HOLD",
            "confidence": 50,
            "reason": "تحليل غير متاح"
        }

    def get_recommendation(self, trade_data: Dict, market_data: Dict) -> Dict:
        result1 = self._call_ai_with_probabilities(
            self.client_mistral, AI_MODEL, trade_data, market_data
        )
        result = {
            'recommendation': result1.get('recommendation', 'HOLD'),
            'confidence': result1.get('confidence', 50),
            'reason': result1.get('reason', ''),
            'probability_tp': result1.get('tp_probability', 50),
            'probability_sl': result1.get('sl_probability', 25),
            'probability_sideways': result1.get('sideways_probability', 15),
            'probability_reversal': result1.get('reversal_probability', 10),
            'ai2_decision': '',
            'ai2_confidence': 0,
            'ai2_explanation': '',
            'dual_agreement': False
        }
        if DUAL_AI_ENABLED:
            result2 = self._call_ai_with_probabilities(
                self.client_oss, AI_MODEL_OSS, trade_data, market_data
            )
            result['ai2_decision'] = result2.get('recommendation', 'HOLD')
            result['ai2_confidence'] = result2.get('confidence', 50)
            result['ai2_explanation'] = result2.get('reason', '')
            if result1.get('recommendation') == result2.get('recommendation'):
                result['dual_agreement'] = True
                result['confidence'] = min(100, (result1.get('confidence', 50) + result2.get('confidence', 50)) / 2 + 10)
                result['reason'] = f"🤝 النموذجان متفقتان: {result1.get('reason', '')}"
                result['probability_tp'] = (result1.get('tp_probability', 50) + result2.get('tp_probability', 50)) / 2
                result['probability_sl'] = (result1.get('sl_probability', 25) + result2.get('sl_probability', 25)) / 2
                result['probability_sideways'] = (result1.get('sideways_probability', 15) + result2.get('sideways_probability', 15)) / 2
                result['probability_reversal'] = (result1.get('reversal_probability', 10) + result2.get('reversal_probability', 10)) / 2
            else:
                result['confidence'] = (result1.get('confidence', 50) + result2.get('confidence', 50)) / 2
                result['reason'] = f"⚠️ اختلاف بين النموذجين:\nالنموذج 1: {result1.get('reason', '')}\nالنموذج 2: {result2.get('reason', '')}"
        return result

# =============================================================================
# 📡 TELEGRAM BOT (موسع)
# =============================================================================

class TelegramBot:
    def __init__(self, token, admin_chat_id, monitor_db):
        self.token = token
        self.admin_chat_id = admin_chat_id
        self.db = monitor_db
        self.app = None

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 مرحباً! أنا بوت مراقبة APEX v4.0.\n"
            "✨ تحليل السوق المتقدم: بنية السوق، السيولة، تدفق الأوامر، الحيتان\n"
            "الأوامر المتاحة:\n"
            "/positions - عرض الصفقات المفتوحة مع التوصيات\n"
            "/advice - الحصول على توصية لصفقة محددة (أرسل المعرف)\n"
            "/statistics - إحصائيات الأداء\n"
            "/lessons - عرض الدروس المستفادة\n"
            "/confusion - عرض مصفوفة الارتباك\n"
            "/market - تحليل السوق الحالي لعملة معينة\n"
            "/help - هذه الرسالة"
        )

    async def market(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("يرجى إرسال اسم العملة: /market BTC")
            return
        symbol = context.args[0].upper()
        if not symbol.endswith('USDT'):
            symbol += 'USDT'
        symbol = f"{symbol}/USDT:USDT"
        try:
            market_data = context.bot_data.get('market_analysis', {}).get(symbol, {})
            if not market_data:
                await update.message.reply_text("لم يتم العثور على تحليل لهذه العملة.")
                return
            msg = f"📊 **تحليل السوق - {symbol}**\n\n"
            msg += f"**بنية السوق:** {market_data.get('market_structure', 'N/A')}\n"
            msg += f"**اختراق:** {'✅' if market_data.get('breakout', False) else '❌'}\n"
            msg += f"**فيك أوت:** {'⚠️' if market_data.get('fakeout', False) else '✅'}\n"
            msg += f"**عدم توازن السيولة:** {market_data.get('liquidity_imbalance', 0):.2f}\n"
            msg += f"**عدم توازن تدفق الأوامر:** {market_data.get('order_flow_imbalance', 0):.2f}\n"
            msg += f"**CVD:** {market_data.get('cvd', 0):.0f} ({'📈' if market_data.get('cvd_trend', 0) > 0 else '📉'})\n"
            msg += f"**نشاط الحيتان:** {'🐋' * market_data.get('whale_activity', 0)}\n"
            msg += f"**Fear & Greed:** {market_data.get('fear_greed', 50)}/100\n"
            msg += f"**ارتباط BTC:** {market_data.get('btc_correlation', 0.5):.2f}\n"
            msg += f"**الجلسة:** {market_data.get('session', 'N/A')}\n"
            msg += f"**RSI:** {market_data.get('rsi', 50):.1f}\n"
            await update.message.reply_text(msg, parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"خطأ في جلب التحليل: {e}")

    # =====================================================================
    # 🔹 الدالة المعدلة positions – تستخدم HTML بدلاً من Markdown مع تنظيف النصوص
    # =====================================================================
    async def positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        trades = self.db.get_open_trades()
        if not trades:
            await update.message.reply_text("لا توجد صفقات مفتوحة حالياً. 💤")
            return
            
        msg = "📊 <b>الصفقات المفتوحة</b>\n\n"
        for t in trades:
            symbol = str(t.get('symbol', 'UNKNOWN')).replace(':USDT', '')
            side = str(t.get('side', 'UNKNOWN'))
            
            entry_price = round(float(t.get('entry_price') or 0), 6)
            sl_price = round(float(t.get('sl_price') or 0), 6)
            tp_price = round(float(t.get('tp_price') or 0), 6)
            
            leverage = t.get('leverage_used', 1)
            slot = t.get('slot_used', '?')
            score = t.get('confidence', 0)
            regime = str(t.get('regime', 'غير محدد'))
            ai_exp = str(t.get('ai_explanation', '')).replace('<', '').replace('>', '') # تنظيف لحماية HTML
            
            direction_emoji = '🟢 LONG' if side == 'LONG' else '🔴 SHORT'

            # استخراج معرف الصفقة
            trade_id = t.get('id', '?')

            # إضافة المعرف (ID) بجانب اسم العملة
            msg += f"• <b>{symbol}</b> (ID: <code>{trade_id}</code>)\n"
            msg += f"الاتجاه: {direction_emoji} | الرافعة: x{leverage} (Slot {slot})\n"
            msg += f"الدخول: <code>{entry_price}</code>\n"
            msg += f"الوقف: <code>{sl_price}</code> | الهدف: <code>{tp_price}</code>\n"
            msg += f"قوة الدخول: {score:.1f}/100 | السوق: {regime}\n"
            
            if ai_exp:
                msg += f"💬 <b>سبب الدخول (AI):</b> <i>{ai_exp}</i>\n"

            analysis = self.db.get_latest_open_analysis(t.get('id', 0))
            if analysis:
                profit = analysis.get('profit_pct', 0)
                rec = str(analysis.get('recommendation', 'HOLD'))
                stars = '⭐' * min(5, int(analysis.get('target_progress', 0) / 20))
                msg += f"\n📈 <b>المتابعة الحية:</b>\n"
                msg += f"الربح الحالي: {profit:+.2f}% {stars}\n"
                msg += f"قرار الذكاء الاصطناعي الآن: <b>{rec}</b> (ثقة: {analysis.get('ai_confidence', 0):.0f}%)\n"
            else:
                msg += f"\n⏳ <i>جاري جمع بيانات الحيتان والسيولة للمتابعة الحية...</i>\n"

            msg += "\n" + "─"*25 + "\n\n"
            
        await update.message.reply_text(msg, parse_mode='HTML')

    # =====================================================================

    async def advice(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("يرجى إرسال معرف الصفقة: /advice <id>")
            return
        try:
            trade_id = int(context.args[0])
        except:
            await update.message.reply_text("المعرف غير صحيح.")
            return
        analysis = self.db.get_latest_open_analysis(trade_id)
        if not analysis:
            await update.message.reply_text("لم يتم العثور على تحليل لهذه الصفقة.")
            return
        msg = f"🔍 **توصية الصفقة #{trade_id}**\n"
        msg += f"العملة: {analysis['symbol']}\n"
        msg += f"الربح الحالي: {analysis['profit_pct']:+.2f}%\n"
        msg += f"الوقت: {analysis['time_open_minutes']} دقيقة\n"
        msg += f"نسبة تحقيق الهدف: {analysis['target_progress']:.1f}%\n"
        msg += f"قوة الاتجاه: {analysis['trend_strength']:.1f}\n"
        msg += f"الزخم: {analysis['momentum_score']:.1f}\n"
        msg += f"معدل التمويل: {analysis['funding_rate']:.6f}\n"
        msg += f"تغير OI: {analysis['oi_change_1h']:.2%}\n"
        msg += f"APEX: {analysis['apex_score']:.1f}\n"
        msg += f"ISS: {analysis['iss_score']:.1f}\n"
        msg += f"\n🎯 **الاحتمالات**\n"
        msg += f"  TP: {analysis.get('probability_tp', 0):.0f}%\n"
        msg += f"  SL: {analysis.get('probability_sl', 0):.0f}%\n"
        msg += f"  جانبي: {analysis.get('probability_sideways', 0):.0f}%\n"
        msg += f"  انعكاس: {analysis.get('probability_reversal', 0):.0f}%\n"
        msg += f"\n🤖 توصية الذكاء الاصطناعي: **{analysis['recommendation']}**\n"
        msg += f"الثقة: {analysis['ai_confidence']:.0f}%\n"
        msg += f"السبب: {analysis['ai_explanation']}\n"
        if analysis.get('ai2_decision'):
            msg += f"\n🔄 النموذج الثاني (GPT-OSS):\n"
            msg += f"  التوصية: {analysis['ai2_decision']}\n"
            msg += f"  الثقة: {analysis['ai2_confidence']:.0f}%\n"
            msg += f"  السبب: {analysis['ai2_explanation']}\n"
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # استخدام الدالة الجديدة get_statistics() التي تجلب من API
        stats = self.db.get_statistics()
        if not stats:
            await update.message.reply_text("لا توجد إحصائيات متاحة حالياً.")
            return
        msg = "📈 **إحصائيات الأداء**\n\n"
        msg += f"عدد الصفقات الكلي: {stats.get('total', 0)}\n"
        msg += f"عدد الصفقات الرابحة: {stats.get('wins', 0)}\n"
        msg += f"نسبة النجاح: {stats.get('winrate', 0):.1f}%\n"
        msg += f"إجمالي الربح: {stats.get('total_pnl', 0):.2f} USDT\n"
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def lessons(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        lessons = self.db.get_lessons(limit=20)
        if not lessons:
            await update.message.reply_text("لا توجد دروس مستفادة بعد.")
            return
        msg = "📚 **الدروس المستفادة**\n\n"
        for l in lessons[:10]:
            msg += f"• {l['symbol']} | {l['market_regime']}\n"
            msg += f"  APEX: {l['apex_decision']} ({l['apex_pnl']:+.2f}%)\n"
            msg += f"  AI: {l['ai_decision']} ({l['ai_pnl']:+.2f}%)\n"
            msg += f"  الأفضل: {l['best_decision']}\n"
            msg += f"  الدرس: {l['lesson'][:100]}...\n\n"
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def confusion(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        cursor = self.db.monitor_conn.execute("""
            SELECT system_name, precision, recall, f1_score, timestamp
            FROM confusion_matrix
            ORDER BY timestamp DESC
        """)
        rows = cursor.fetchall()
        if not rows:
            await update.message.reply_text("لا توجد بيانات كافية لمصفوفة الارتباك.")
            return
        msg = "📊 **مصفوفة الارتباك (أحدث القيم)**\n\n"
        for row in rows[:5]:
            msg += f"**{row[0]}**\n"
            msg += f"  Precision: {row[1]:.2f}\n"
            msg += f"  Recall: {row[2]:.2f}\n"
            msg += f"  F1 Score: {row[3]:.2f}\n"
            msg += f"  {row[4][:19]}\n\n"
        await update.message.reply_text(msg, parse_mode='Markdown')

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.start(update, context)

    def run(self):
        self.app = Application.builder().token(self.token).build()
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("positions", self.positions))
        self.app.add_handler(CommandHandler("advice", self.advice))
        self.app.add_handler(CommandHandler("statistics", self.statistics))
        self.app.add_handler(CommandHandler("lessons", self.lessons))
        self.app.add_handler(CommandHandler("confusion", self.confusion))
        self.app.add_handler(CommandHandler("market", self.market))
        self.app.add_handler(CommandHandler("help", self.help))
        self.app.run_polling()

# =============================================================================
# 🔁 MONITOR LOOP
# =============================================================================

class MonitorLoop:
    def __init__(self, db: MonitorDB, analytics: AdvancedAnalyticsEngine, ai: AIClient, telegram_bot):
        self.db = db
        self.analytics = analytics
        self.ai = ai
        self.telegram_bot = telegram_bot
        self.running = True
        self.thread = None
        self.analysis_history = {}
        self.ai_cache = {}
        self.market_cache = {}
        self.queue = Queue(maxsize=100)
        self.workers = []
        self.num_workers = 3

    def start(self):
        for _ in range(self.num_workers):
            worker = threading.Thread(target=self._worker, daemon=True)
            worker.start()
            self.workers.append(worker)
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()
        logger.info("Monitor loop started with Queue workers")

    def stop(self):
        self.running = False
        for _ in self.workers:
            self.queue.put(None)
        if self.thread:
            self.thread.join(timeout=2)
        for w in self.workers:
            w.join(timeout=1)

    def _worker(self):
        while True:
            item = self.queue.get()
            if item is None:
                break
            try:
                self._process_item(item)
            except Exception as e:
                logger.error(f"Worker error: {e}")
            finally:
                self.queue.task_done()

    def _process_item(self, item):
        trade = item['trade']
        self._analyze_trade(trade)

    def _loop(self):
        while self.running:
            try:
                open_trades = self.db.get_open_trades()
                logger.info(f"Monitoring {len(open_trades)} open trades")
                for trade in open_trades:
                    symbol = trade['symbol']
                    if symbol not in self.market_cache or time.time() - self.market_cache[symbol].get('time', 0) > 300:
                        market_data = self.analytics.analyze_market(symbol)
                        self.market_cache[symbol] = {'data': market_data, 'time': time.time()}
                    self.queue.put({'trade': trade})
                self._analyze_closed_trades()
                self._update_system_performance()
            except Exception as e:
                logger.error(f"Monitor cycle error: {e}")
            time.sleep(MONITOR_INTERVAL)

    def _should_update_ai(self, trade_id, current_data):
        if trade_id not in self.ai_cache:
            return True
        cached = self.ai_cache[trade_id]
        profit_change = abs(current_data['profit_pct'] - cached.get('profit_pct', 0))
        price_change = abs(current_data['current_price'] - cached.get('current_price', 0)) / cached.get('current_price', 1)
        if profit_change >= AI_MIN_PROFIT_CHANGE or price_change >= AI_MIN_PRICE_CHANGE:
            return True
        trend_change = abs(current_data['trend_strength'] - cached.get('trend_strength', 0))
        momentum_change = abs(current_data['momentum_score'] - cached.get('momentum_score', 0))
        if trend_change > 5 or momentum_change > 5:
            return True
        return False

    def _analyze_trade(self, trade):
        trade_id = trade['id']
        symbol = trade['symbol']
        entry_price = trade['entry_price']
        side = trade['side']

        market = self.analytics.fetch_market_data(symbol)
        if 'error' in market:
            logger.warning(f"Failed to fetch price for {symbol}")
            return
        current_price = market['price']

        if side == 'LONG':
            profit_pct = (current_price - entry_price) / entry_price * 100
        else:
            profit_pct = (entry_price - current_price) / entry_price * 100

        opened_at = datetime.fromisoformat(trade['timestamp'])
        time_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 60

        tp_price = trade.get('tp_price', 0)
        if tp_price and side == 'LONG':
            target_progress = (current_price - entry_price) / (tp_price - entry_price) * 100
        elif tp_price and side == 'SHORT':
            target_progress = (entry_price - current_price) / (entry_price - tp_price) * 100
        else:
            target_progress = 0.0
        target_progress = max(0, min(100, target_progress))

        ohlcv = market.get('ohlcv', [])
        # 🔴 أضف هذا السطر لحماية البوت من الانهيار إذا لم ترجع الشموع
        if not ohlcv or len(ohlcv) < 5:
            logger.warning(f"No sufficient OHLCV data for {symbol}, skipping analysis.")
            return

        trend_strength = self.analytics.trend_strength(ohlcv)
        momentum_score = self.analytics.momentum_score(ohlcv)
        funding_rate = self.analytics.funding_rate(symbol)
        oi_change = self.analytics.oi_change_1h(symbol)
        oi_trend = self.analytics.oi_trend(symbol)
        market_regime = self.analytics.detect_market_regime(ohlcv)

        apex_score = trade.get('confidence', 50)
        iss_score = 50

        news = self.analytics.recent_news(symbol)

        market_data = self.market_cache.get(symbol, {}).get('data', {})
        if not market_data:
            market_data = self.analytics.analyze_market(symbol)

        current_data = {
            'symbol': symbol,
            'entry_price': entry_price,
            'current_price': current_price,
            'profit_pct': profit_pct,
            'time_open_minutes': time_open,
            'target_progress': target_progress,
            'trend_strength': trend_strength,
            'momentum_score': momentum_score,
            'funding_rate': funding_rate,
            'oi_change_1h': oi_change,
            'oi_trend': oi_trend,
            'apex_score': apex_score,
            'iss_score': iss_score,
            'market_regime': market_regime,
            'news': news
        }

        need_ai = self._should_update_ai(trade_id, current_data)

        if need_ai:
            ai_result = self.ai.get_recommendation(current_data, market_data)
            self.ai_cache[trade_id] = {
                'profit_pct': profit_pct,
                'current_price': current_price,
                'trend_strength': trend_strength,
                'momentum_score': momentum_score,
                'ai_result': ai_result
            }
        else:
            ai_result = self.ai_cache[trade_id]['ai_result']

        recommendation = ai_result.get('recommendation', 'HOLD')
        confidence = ai_result.get('confidence', 50)
        reason = ai_result.get('reason', '')
        ai2_decision = ai_result.get('ai2_decision', '')
        ai2_confidence = ai_result.get('ai2_confidence', 0)
        ai2_explanation = ai_result.get('ai2_explanation', '')
        probability_tp = ai_result.get('probability_tp', 0)
        probability_sl = ai_result.get('probability_sl', 0)
        probability_sideways = ai_result.get('probability_sideways', 0)
        probability_reversal = ai_result.get('probability_reversal', 0)

        analysis_record = {
            'trade_id': trade_id,
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'current_price': current_price,
            'profit_pct': profit_pct,
            'time_open_minutes': int(time_open),
            'target_progress': target_progress,
            'trend_strength': trend_strength,
            'momentum_score': momentum_score,
            'funding_rate': funding_rate,
            'oi_change_1h': oi_change,
            'oi_trend': oi_trend,
            'apex_score': apex_score,
            'iss_score': iss_score,
            'ai_decision': recommendation,
            'ai_confidence': confidence,
            'ai_explanation': reason,
            'ai2_decision': ai2_decision,
            'ai2_confidence': ai2_confidence,
            'ai2_explanation': ai2_explanation,
            'recommendation': recommendation,
            'probability_tp': probability_tp,
            'probability_sl': probability_sl,
            'probability_sideways': probability_sideways,
            'probability_reversal': probability_reversal,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        self.db.save_open_analysis(analysis_record)

        if trade_id not in self.analysis_history:
            self.analysis_history[trade_id] = []
        self.analysis_history[trade_id].append({
            'time': datetime.now(timezone.utc),
            'profit': profit_pct,
            'recommendation': recommendation,
            'confidence': confidence,
            'prob_tp': probability_tp,
            'prob_sl': probability_sl
        })

        if recommendation == 'CLOSE' and confidence >= 70:
            self._send_alert(trade, profit_pct, reason, confidence, probability_tp, probability_sl)

    def _send_alert(self, trade, profit_pct, reason, confidence, prob_tp, prob_sl):
        msg = f"⚠️ **توصية إغلاق**\n"
        msg += f"الصفقة #{trade['id']} | {trade['symbol']}\n"
        msg += f"الربح الحالي: {profit_pct:+.2f}%\n"
        msg += f"احتمال TP: {prob_tp:.0f}% | احتمال SL: {prob_sl:.0f}%\n"
        msg += f"السبب: {reason}\n"
        msg += f"الثقة: {confidence:.0f}%"
        # يمكن إرسالها عبر التيليجرام هنا إذا أردت

    def _analyze_closed_trades(self):
        pass

    def _update_system_performance(self):
        pass

# =============================================================================
# 🚀 MAIN
# =============================================================================

def show_ip_on_startup():
    ip = get_public_ip()
    print("\n" + "=" * 70)
    print(f"🌐 CURRENT PUBLIC IP: {ip}")
    print("=" * 70)
    print("📌 Copy this IP and add to Binance Whitelist")
    print("=" * 70 + "\n")
    return ip

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    global logger
    logger = logging.getLogger("MONITOR")
    logger.info("🚀 Starting APEX Monitor Bot v4.0 (Lightweight)")
    logger.info("📊 Enhanced with: Market Structure, Liquidity Analysis, Order Flow, Whale Detection, Probability Engine")
    logger.info("📰 News API: ee6adc6bb00849d5bb0b1a29e62d5ed4")
    logger.info("🤖 AI Models: Mistral + GPT-OSS-120b")
    logger.info("🔗 Reading open trades from APEX API only (no local trades DB)")
    logger.info(f"📡 APEX API URL: {APEX_API_URL}")

    deployment_ip = show_ip_on_startup()
    logger.info(f"📌 Add this IP to Binance Whitelist: {deployment_ip}")

    # كائن exchange عام للتحليلات (بدون مفاتيح)
    exchange_public = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "swap"}
    })

    # قاعدة البيانات فقط للتحليلات الخاصة بالبوت الثاني
    db = MonitorDB(MONITOR_DB_PATH)

    analytics = AdvancedAnalyticsEngine(exchange_public)
    analytics.db = db

    ai = AIClient(db)

    telegram_bot = TelegramBot(TELEGRAM_TOKEN, ADMIN_CHAT_ID, db)

    monitor = MonitorLoop(db, analytics, ai, telegram_bot)
    monitor.start()

    # تشغيل Telegram Bot في الخيط الرئيسي
    telegram_bot.run()

    logger.info("Monitor is running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        monitor.stop()

if __name__ == "__main__":
    show_ip_on_startup()
    main()