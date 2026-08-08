#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║     APEX MONITOR BOT — Telegram AI Monitor v4.8            ║
║  Architecture: Hybrid Decision Engine + Execution Layer    ║
║  Single AI Model: poolside/laguna-xs-2.1                  ║
╚══════════════════════════════════════════════════════════════╝
"""

import os
import sys
import time
import json
import logging
import threading
import sqlite3
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from queue import Queue
from functools import wraps

import numpy as np
import requests
import ccxt
from openai import OpenAI

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# =============================================================================
# 🌐 عرض IP فور بدء التشغيل
# =============================================================================

def get_public_ip():
    try:
        urls = ["https://api.ipify.org", "https://ifconfig.me/ip"]
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

# =============================================================================
# 🔧 CONFIG
# =============================================================================

# 🔴 استخدم متغيرات البيئة في الإنتاج
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8122906116:AAHAWsXfaiymnvdeNO0BURyRVccJU8_gIco")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "6033203084"))

APEX_API_URL = os.getenv("APEX_API_URL", "https://binancetrading-production.up.railway.app")
MONITOR_DB_PATH = os.getenv("MONITOR_DB_PATH", "monitor.db")

# 🔴 المفتاح الجديد ونموذج poolside/laguna-xs-2.1
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "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")
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"

# -------------------------------------------------------------------------
# Binance Futures
# -------------------------------------------------------------------------
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "IX7kLH0ssWHP5TpYMUGcp0pzq4LX4Lqi7m4XtlqMkkq6DCZAsLhoeYZ3533jJFF4")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "LmICnpSpMxL1riv4RfIf0HBGRfhDTP5JhDUYdlPSukpqV7kDTonrZ0j3DWp1a7hU")

# 🔴 مهم جداً:
# true = لا يتم إرسال أي أمر إلى Binance
# false = يسمح بالتنفيذ الحقيقي
DRY_RUN = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes", "on")

# -------------------------------------------------------------------------
# Monitor
# -------------------------------------------------------------------------
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", "60"))
AI_MODEL = "poolside/laguna-xs-2.1"

# -------------------------------------------------------------------------
# Risk / Hard Guards
# -------------------------------------------------------------------------
MAX_LOSS_PCT = float(os.getenv("MAX_LOSS_PCT", "1.0"))
DECISION_COOLDOWN = int(os.getenv("DECISION_COOLDOWN", str(10 * 60)))
REDUCE_PERCENT = float(os.getenv("REDUCE_PERCENT", "50"))
MIN_ORDER_AMOUNT = float(os.getenv("MIN_ORDER_AMOUNT", "0"))

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
                else:
                    return None
            try:
                result = func(*args, **kwargs)
                if self.state == "HALF_OPEN":
                    self.state = "CLOSED"
                    self.failures = 0
                return result
            except Exception as e:
                self.failures += 1
                self.last_failure = time.time()
                if self.failures >= self.failure_threshold:
                    self.state = "OPEN"
                raise
        return wrapper

circuit_breaker = CircuitBreaker()

# =============================================================================
# 🗄️ DATABASE
# =============================================================================

class MonitorDB:
    def __init__(self, monitor_db_path):
        self.monitor_conn = sqlite3.connect(monitor_db_path, check_same_thread=False)
        self.lock = threading.Lock()
        self._init_tables()

    def _init_tables(self):
        with self.lock:
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS open_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER, symbol TEXT, side TEXT, entry_price REAL,
                    current_price REAL, profit_pct REAL, time_open_minutes INTEGER,
                    target_progress REAL, trend_strength REAL, momentum_score REAL,
                    funding_rate REAL, oi_change_1h REAL, oi_trend REAL, apex_score REAL,
                    iss_score REAL, ai_decision TEXT, ai_confidence REAL, ai_explanation TEXT,
                    recommendation TEXT, probability_tp REAL, probability_sl REAL,
                    probability_sideways REAL, probability_reversal REAL, timestamp TEXT
                );
            """)
            # 🔴 جدول قرارات الإدارة للتسجيل والتحليل
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS management_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    symbol TEXT,
                    decision TEXT,
                    confidence REAL,
                    price REAL,
                    profit_pct REAL,
                    adx REAL,
                    plus_di REAL,
                    minus_di REAL,
                    rsi REAL,
                    atr_pct REAL,
                    distance_sl REAL,
                    distance_tp REAL,
                    trend_alignment TEXT,
                    reversal_risk REAL,
                    management_score REAL,
                    ai_recommendation TEXT,
                    ai_confidence REAL,
                    ai_explanation TEXT,
                    timestamp TEXT
                );
            """)
            # 🔴 جدول تنفيذ الأوامر
            self.monitor_conn.execute("""
                CREATE TABLE IF NOT EXISTS executed_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    symbol TEXT,
                    action TEXT,
                    side TEXT,
                    amount REAL,
                    price REAL,
                    order_id TEXT,
                    dry_run BOOLEAN,
                    success BOOLEAN,
                    reason TEXT,
                    timestamp TEXT
                );
            """)
            self.monitor_conn.commit()

    @circuit_breaker
    def get_open_trades(self) -> List[Dict[str, Any]]:
        try:
            resp = requests.get(f"{APEX_API_URL}/positions", timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list):
                    return data
            return []
        except Exception as e:
            logging.error(f"API Fetch Error: {e}")
            return []

    def get_statistics(self) -> Dict[str, Any]:
        try:
            resp = requests.get(f"{APEX_API_URL}/health", timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                return data.get('performance', data)
            return {}
        except Exception:
            return {}

    def save_open_analysis(self, data: Dict[str, Any]):
        columns = [
            "trade_id", "symbol", "side", "entry_price", "current_price",
            "profit_pct", "time_open_minutes", "target_progress",
            "trend_strength", "momentum_score", "funding_rate",
            "oi_change_1h", "oi_trend", "apex_score", "iss_score",
            "ai_decision", "ai_confidence", "ai_explanation",
            "recommendation", "probability_tp", "probability_sl",
            "probability_sideways", "probability_reversal", "timestamp"
        ]
        values = [
            data.get("trade_id"),
            data.get("symbol"),
            data.get("side"),
            data.get("entry_price"),
            data.get("current_price"),
            data.get("profit_pct"),
            data.get("time_open_minutes"),
            data.get("target_progress"),
            data.get("trend_strength"),
            data.get("momentum_score"),
            data.get("funding_rate"),
            data.get("oi_change_1h"),
            data.get("oi_trend"),
            data.get("apex_score"),
            data.get("iss_score"),
            data.get("ai_decision"),
            data.get("ai_confidence"),
            data.get("ai_explanation"),
            data.get("recommendation"),
            data.get("probability_tp", 0),
            data.get("probability_sl", 0),
            data.get("probability_sideways", 0),
            data.get("probability_reversal", 0),
            data.get("timestamp")
        ]
        if len(columns) != len(values):
            error_msg = f"DB mapping mismatch: {len(columns)} columns vs {len(values)} values"
            logging.error(error_msg)
            raise RuntimeError(error_msg)
        placeholders = ",".join(["?"] * len(values))
        with self.lock:
            try:
                self.monitor_conn.execute(
                    f"""
                    INSERT INTO open_analysis (
                        {",".join(columns)}
                    ) VALUES ({placeholders})
                    """,
                    values
                )
                self.monitor_conn.commit()
            except Exception as e:
                logging.error(f"DB Save Error: {e}\nColumns: {len(columns)} | Values: {len(values)}\n{traceback.format_exc()}")

    def save_management_decision(self, data: Dict[str, Any]):
        with self.lock:
            try:
                self.monitor_conn.execute("""
                    INSERT INTO management_decisions (
                        trade_id, symbol, decision, confidence, price, profit_pct,
                        adx, plus_di, minus_di, rsi, atr_pct,
                        distance_sl, distance_tp, trend_alignment, reversal_risk,
                        management_score, ai_recommendation, ai_confidence,
                        ai_explanation, timestamp
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    data.get('trade_id'),
                    data.get('symbol'),
                    data.get('decision'),
                    data.get('confidence'),
                    data.get('price'),
                    data.get('profit_pct'),
                    data.get('adx'),
                    data.get('plus_di'),
                    data.get('minus_di'),
                    data.get('rsi'),
                    data.get('atr_pct'),
                    data.get('distance_sl'),
                    data.get('distance_tp'),
                    data.get('trend_alignment'),
                    data.get('reversal_risk'),
                    data.get('management_score'),
                    data.get('ai_recommendation'),
                    data.get('ai_confidence'),
                    data.get('ai_explanation'),
                    data.get('timestamp')
                ))
                self.monitor_conn.commit()
            except Exception as e:
                logging.error(f"Management Decision Save Error: {e}")

    def save_executed_action(self, data: Dict[str, Any]):
        with self.lock:
            try:
                self.monitor_conn.execute("""
                    INSERT INTO executed_actions (
                        trade_id, symbol, action, side, amount, price,
                        order_id, dry_run, success, reason, timestamp
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    data.get('trade_id'),
                    data.get('symbol'),
                    data.get('action'),
                    data.get('side'),
                    data.get('amount'),
                    data.get('price'),
                    data.get('order_id'),
                    data.get('dry_run', True),
                    data.get('success', False),
                    data.get('reason'),
                    data.get('timestamp')
                ))
                self.monitor_conn.commit()
            except Exception as e:
                logging.error(f"Executed Action Save Error: {e}")

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

    def get_last_executed_action(self, trade_id: int) -> Optional[Dict]:
        with self.lock:
            cursor = self.monitor_conn.execute(
                "SELECT * FROM executed_actions WHERE trade_id=? ORDER BY timestamp DESC LIMIT 1",
                (trade_id,)
            )
            row = cursor.fetchone()
            if row:
                cols = [desc[0] for desc in cursor.description]
                return dict(zip(cols, row))
            return None

# =============================================================================
# 📊 ADVANCED ANALYTICS ENGINE
# =============================================================================

class AdvancedAnalyticsEngine:
    def __init__(self, exchange_public):
        self.exchange = exchange_public

    def fetch_market_data(self, symbol: str) -> Dict:
        try:
            ticker = self.exchange.fetch_ticker(symbol)
            ohlcv = self.exchange.fetch_ohlcv(symbol, '1h', limit=100)
            return {
                'price': ticker['last'],
                'ohlcv': ohlcv,
            }
        except Exception as e:
            return {'error': str(e)}

    # 🔴 حساب الـ ADX الحقيقي مع التنعيم
    def adx_di(self, ohlcv: List, period: int = 14) -> Dict:
        if not ohlcv or len(ohlcv) < period + 5:
            return {'adx': 0.0, 'plus_di': 0.0, 'minus_di': 0.0}
        highs = [c[2] for c in ohlcv]
        lows = [c[3] for c in ohlcv]
        closes = [c[4] for c in ohlcv]
        
        plus_dms, minus_dms, trs = [], [], []
        for i in range(1, len(closes)):
            up_move = highs[i] - highs[i - 1]
            down_move = lows[i - 1] - lows[i]
            plus_dms.append(up_move if up_move > down_move and up_move > 0 else 0)
            minus_dms.append(down_move if down_move > up_move and down_move > 0 else 0)
            trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
        
        # حساب ATR
        atr = np.mean(trs[-period:]) or 1e-12
        
        # حساب Plus DI و Minus DI
        plus_di = 100 * np.mean(plus_dms[-period:]) / atr
        minus_di = 100 * np.mean(minus_dms[-period:]) / atr
        
        # حساب DX لكل فترة
        dx_values = []
        for i in range(period, len(plus_dms)):
            p_di = 100 * np.mean(plus_dms[i-period:i]) / atr
            m_di = 100 * np.mean(minus_dms[i-period:i]) / atr
            di_sum = p_di + m_di
            if di_sum > 0:
                dx_values.append(100 * abs(p_di - m_di) / di_sum)
        
        # تنعيم DX للحصول على ADX
        if len(dx_values) >= period:
            adx = np.mean(dx_values[-period:])
        else:
            adx = np.mean(dx_values) if dx_values else 0.0
        
        return {'adx': adx, 'plus_di': plus_di, 'minus_di': minus_di}

    # 🔴 حساب الـ RSI الحقيقي
    def real_rsi(self, ohlcv: List) -> float:
        if not ohlcv or len(ohlcv) < 15:
            return 50.0
        closes = [c[4] for c in ohlcv]
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[-14:])
        avg_loss = np.mean(losses[-14:])
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    # 🔴 حساب الـ ATR بالنسبة المئوية
    def get_atr_pct(self, ohlcv: List) -> float:
        if not ohlcv or len(ohlcv) < 15:
            return 0.0
        highs = [c[2] for c in ohlcv]
        lows = [c[3] for c in ohlcv]
        closes = [c[4] for c in ohlcv]
        trs = [max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])) for i in range(1, len(closes))]
        atr = np.mean(trs[-14:])
        return (atr / closes[-1]) * 100

    def funding_rate(self, symbol: str) -> float:
        try:
            s = symbol.split('/')[0]
            url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={s}USDT"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return float(resp.json().get('lastFundingRate', 0))
        except: pass
        return 0.0

    def detect_market_regime(self, ohlcv: List) -> str:
        if not ohlcv or len(ohlcv) < 50:
            return "UNKNOWN"
        di = self.adx_di(ohlcv)
        adx = di['adx']
        plus_di = di['plus_di']
        minus_di = di['minus_di']
        if adx < 15:
            return "RANGING"
        elif adx >= 25:
            if plus_di > minus_di:
                return "TRENDING_UP"
            else:
                return "TRENDING_DOWN"
        else:
            return "TRANSITION"

    def analyze_market(self, symbol: str) -> Dict:
        market = self.fetch_market_data(symbol)
        if 'error' in market:
            return {'error': market['error']}
        ohlcv = market.get('ohlcv', [])
        return {
            'symbol': symbol,
            'price': market.get('price', 0),
            'market_structure': self.detect_market_regime(ohlcv),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'ohlcv': ohlcv
        }

# =============================================================================
# 🤖 AI CLIENT (LAGUNA XS 2.1)
# =============================================================================

class AIClient:
    def __init__(self):
        self.client = OpenAI(
            base_url=NVIDIA_BASE_URL,
            api_key=NVIDIA_API_KEY
        )

    def get_recommendation(self, trade_data: Dict) -> Dict:
        tp_sl_context = ""
        if trade_data.get('tp_price', 0) == 0 or trade_data.get('sl_price', 0) == 0:
            tp_sl_context = "تنبيه: لا يوجد TP/SL محدد (يتم إدارتها يدوياً أو عبر التتبع). قم بتقييم الصفقة بناءً على هيكل السوق فقط ولا تعتبر غيابهم خطأ."
        else:
            tp_sl_context = f"مسافة الهدف: {trade_data.get('distance_to_tp_pct', 0):.2f}% | مسافة الوقف: {trade_data.get('distance_to_sl_pct', 0):.2f}% | نسبة المخاطرة للعائد (R:R): {trade_data.get('risk_reward', 0):.2f}"

        prompt = f"""أنت "مدير صفقات" (Position Manager) في صندوق استثماري محترف. مهمتك ليست فتح صفقات جديدة، بل تقييم صفقة "مفتوحة حالياً" وتحديد الإجراء الأنسب لها.

📊 [بيانات الصفقة المفتوحة]
العملة: {trade_data.get('symbol')} | نوع الصفقة: {trade_data.get('side')}
سعر الدخول: {trade_data.get('entry_price')} | السعر الحالي: {trade_data.get('current_price')}
الربح الحالي: {trade_data.get('profit_pct', 0):.2f}% | مدة الصفقة: {trade_data.get('time_open_minutes')} دقيقة

📏 [إدارة المخاطر والأهداف]
{tp_sl_context}
التقلبات (ATR%): {trade_data.get('atr_pct', 0):.2f}%

📈 [مؤشرات السوق الحية - 1H]
حالة السوق: {trade_data.get('market_regime')}
قوة الاتجاه (ADX): {trade_data.get('trend_strength_adx', 0):.1f}/100
مؤشر القوة النسبية RSI(14): {trade_data.get('rsi_14', 0):.1f}
معدل التمويل: {trade_data.get('funding_rate', 0):.6f}

يجب أن يكون التفسير (reason) باللغة العربية الفصحى حصراً وبدون أي كلمات إنجليزية. اشرح باحترافية مالية.

أجب بصيغة JSON فقط:
{{
    "action": "HOLD أو CLOSE أو REDUCE أو TRAIL_SL",
    "tp_probability": 60,
    "sl_probability": 10,
    "sideways_probability": 20,
    "reversal_probability": 10,
    "confidence": 85,
    "reason": "اشرح قرارك كمدير صفقات بالعربية الفصحى هنا"
}}
"""
        try:
            logging.info(f"🤖 Calling {AI_MODEL} for {trade_data.get('symbol')}")
            start_time = time.time()
            response = self.client.chat.completions.create(
                model=AI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a quantitative position manager. Return valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=1.0,
                top_p=0.95,
                max_tokens=8192,
                stream=False,
                extra_body={"chat_template_kwargs": {"enable_thinking": True}}
            )
            elapsed = time.time() - start_time
            logging.info(f"✅ {AI_MODEL} responded in {elapsed:.2f}s")

            raw = response.choices[0].message.content.strip()
            if raw.startswith("```"):
                raw = "\n".join([l for l in raw.split("\n") if not l.strip().startswith("```")])
            if raw.startswith("json"):
                raw = raw[4:].strip()
            result = json.loads(raw)
            return {
                'recommendation': result.get('action', result.get('recommendation', 'HOLD')).upper(),
                'confidence': float(result.get('confidence', 50)),
                'reason': result.get('reason', ''),
                'tp_probability': float(result.get('tp_probability', 50)),
                'sl_probability': float(result.get('sl_probability', 25)),
                'sideways_probability': float(result.get('sideways_probability', 15)),
                'reversal_probability': float(result.get('reversal_probability', 10)),
            }
        except Exception as e:
            logging.error(f"❌ {AI_MODEL} ERROR for {trade_data.get('symbol')}: {type(e).__name__}: {e}")
            return {
                'recommendation': 'HOLD',
                'confidence': 0,
                'reason': f'AI غير متاح: {type(e).__name__}',
                'tp_probability': 50,
                'sl_probability': 25,
                'sideways_probability': 15,
                'reversal_probability': 10,
            }

# =============================================================================
# 💱 BINANCE EXECUTION MANAGER
# =============================================================================

class BinanceExecutionManager:
    def __init__(self):
        self.enabled = bool(BINANCE_API_KEY and BINANCE_API_SECRET)
        self.dry_run = DRY_RUN
        self.exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
            }
        })
        self.lock = threading.Lock()
        
        if self.enabled:
            try:
                self.exchange.load_markets()
                logging.info("✅ Binance execution manager initialized")
            except Exception as e:
                logging.error(f"❌ Binance market loading failed: {e}")
        else:
            logging.warning("⚠️ Binance execution manager disabled (missing API keys)")
        
        if self.dry_run:
            logging.warning("🟡 DRY_RUN ENABLED — NO REAL ORDERS WILL BE SENT")
        else:
            logging.warning("🔴 LIVE TRADING MODE ENABLED")

    def status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "dry_run": self.dry_run,
            "live": self.enabled and not self.dry_run
        }

    def normalize_symbol(self, symbol: str) -> str:
        symbol = str(symbol).strip()
        if ":USDT" in symbol:
            return symbol
        if symbol.endswith("/USDT"):
            return symbol + ":USDT"
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            return f"{base}/USDT:USDT"
        return symbol

    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        symbol = self.normalize_symbol(symbol)
        try:
            positions = self.exchange.fetch_positions([symbol])
            for position in positions:
                contracts = float(position.get("contracts") or 0)
                if abs(contracts) <= 0:
                    continue
                side = str(position.get("side") or "").lower()
                return {
                    "symbol": symbol,
                    "side": side,
                    "contracts": abs(contracts),
                    "entry_price": float(position.get("entryPrice") or 0),
                    "mark_price": float(position.get("markPrice") or 0),
                    "leverage": float(position.get("leverage") or 1)
                }
        except Exception as e:
            logging.error(f"❌ Binance position fetch error {symbol}: {e}")
        return None

    def normalize_amount(self, symbol: str, amount: float) -> float:
        symbol = self.normalize_symbol(symbol)
        try:
            amount = float(self.exchange.amount_to_precision(symbol, amount))
            return amount
        except Exception as e:
            logging.error(f"Amount precision error {symbol}: {e}")
            return float(amount)

    def reduce_position(self, symbol: str, position_side: str, contracts: float, percent: float = 50) -> Dict[str, Any]:
        symbol = self.normalize_symbol(symbol)
        reduce_amount = contracts * (percent / 100.0)
        reduce_amount = self.normalize_amount(symbol, reduce_amount)
        
        if reduce_amount <= MIN_ORDER_AMOUNT:
            return {
                "success": False,
                "action": "REDUCE",
                "reason": "REDUCE amount too small",
                "amount": reduce_amount
            }

        side = "sell" if position_side.lower() == "long" else "buy"
        order_params = {"reduceOnly": True}

        if self.dry_run:
            logging.warning(f"🟡 DRY RUN REDUCE | {symbol} | {position_side} | {percent}% | amount={reduce_amount}")
            return {
                "success": True,
                "dry_run": True,
                "action": "REDUCE",
                "symbol": symbol,
                "side": side,
                "amount": reduce_amount
            }

        if not self.enabled:
            return {
                "success": False,
                "action": "REDUCE",
                "reason": "Binance API disabled"
            }

        try:
            with self.lock:
                order = self.exchange.create_order(
                    symbol=symbol,
                    type="market",
                    side=side,
                    amount=reduce_amount,
                    params=order_params
                )
                logging.warning(f"🔴 LIVE REDUCE EXECUTED | {symbol} | {percent}% | amount={reduce_amount} | order={order.get('id')}")
                return {
                    "success": True,
                    "dry_run": False,
                    "action": "REDUCE",
                    "symbol": symbol,
                    "side": side,
                    "amount": reduce_amount,
                    "order_id": order.get("id")
                }
        except Exception as e:
            logging.error(f"❌ REDUCE execution failed {symbol}: {e}")
            return {
                "success": False,
                "action": "REDUCE",
                "reason": str(e)
            }

    def close_position(self, symbol: str, position_side: str, contracts: float) -> Dict[str, Any]:
        symbol = self.normalize_symbol(symbol)
        amount = self.normalize_amount(symbol, contracts)
        
        if amount <= MIN_ORDER_AMOUNT:
            return {
                "success": False,
                "action": "CLOSE",
                "reason": "Position amount too small"
            }

        side = "sell" if position_side.lower() == "long" else "buy"
        params = {"reduceOnly": True}

        if self.dry_run:
            logging.warning(f"🟡 DRY RUN CLOSE | {symbol} | {position_side} | amount={amount}")
            return {
                "success": True,
                "dry_run": True,
                "action": "CLOSE",
                "symbol": symbol,
                "side": side,
                "amount": amount
            }

        if not self.enabled:
            return {
                "success": False,
                "action": "CLOSE",
                "reason": "Binance API disabled"
            }

        try:
            with self.lock:
                order = self.exchange.create_order(
                    symbol=symbol,
                    type="market",
                    side=side,
                    amount=amount,
                    params=params
                )
                logging.warning(f"🔴 LIVE CLOSE EXECUTED | {symbol} | amount={amount} | order={order.get('id')}")
                return {
                    "success": True,
                    "dry_run": False,
                    "action": "CLOSE",
                    "symbol": symbol,
                    "side": side,
                    "amount": amount,
                    "order_id": order.get("id")
                }
        except Exception as e:
            logging.error(f"❌ CLOSE execution failed {symbol}: {e}")
            return {
                "success": False,
                "action": "CLOSE",
                "reason": str(e)
            }

    def update_stop_loss(self, symbol: str, position_side: str, stop_price: float) -> Dict[str, Any]:
        symbol = self.normalize_symbol(symbol)
        if stop_price <= 0:
            return {
                "success": False,
                "action": "TRAIL_SL",
                "reason": "Invalid stop price"
            }

        try:
            stop_price = float(self.exchange.price_to_precision(symbol, stop_price))
        except Exception:
            pass

        if self.dry_run:
            logging.warning(f"🟡 DRY RUN TRAIL_SL | {symbol} | {position_side} | stop={stop_price}")
            return {
                "success": True,
                "dry_run": True,
                "action": "TRAIL_SL",
                "symbol": symbol,
                "stop_price": stop_price
            }

        if not self.enabled:
            return {
                "success": False,
                "action": "TRAIL_SL",
                "reason": "Binance API disabled"
            }

        try:
            side = "sell" if position_side.lower() == "long" else "buy"
            params = {
                "stopPrice": stop_price,
                "reduceOnly": True,
                "workingType": "MARK_PRICE"
            }
            with self.lock:
                order = self.exchange.create_order(
                    symbol=symbol,
                    type="STOP_MARKET",
                    side=side,
                    amount=None,
                    params=params
                )
                logging.warning(f"🔴 LIVE TRAIL_SL | {symbol} | stop={stop_price} | order={order.get('id')}")
                return {
                    "success": True,
                    "dry_run": False,
                    "action": "TRAIL_SL",
                    "symbol": symbol,
                    "stop_price": stop_price,
                    "order_id": order.get("id")
                }
        except Exception as e:
            logging.error(f"❌ TRAIL_SL failed {symbol}: {e}")
            return {
                "success": False,
                "action": "TRAIL_SL",
                "reason": str(e)
            }

# =============================================================================
# 📡 TELEGRAM BOT
# =============================================================================

class TelegramBot:
    def __init__(self, token, admin_chat_id, monitor_db, analytics_engine):
        self.token = token
        self.admin_chat_id = admin_chat_id
        self.db = monitor_db
        self.analytics = analytics_engine

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 مرحباً! أنا بوت مراقبة APEX v4.8 (Hybrid Decision + Execution).\n"
            "الأوامر:\n"
            "/positions - الصفقات المفتوحة مع قرارات الإدارة\n"
            "/advice <id> - تحليل مفصل لصفقة محددة\n"
            "/market <symbol> - تحليل سريع للسوق\n"
            "/status - حالة التنفيذ (DRY_RUN/LIVE)"
        )

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status_text = f"""
📊 <b>حالة البوت</b>

🔧 <b>الإعدادات</b>
• DRY_RUN: {'🟡 مفعل' if DRY_RUN else '🔴 غير مفعل'}
• MAX_LOSS_PCT: {MAX_LOSS_PCT}%
• DECISION_COOLDOWN: {DECISION_COOLDOWN}s
• REDUCE_PERCENT: {REDUCE_PERCENT}%

📈 <b>الإحصائيات</b>
• الصفقات المفتوحة: {len(self.db.get_open_trades())}

⚠️ {'🟡 وضع المحاكاة نشط - لن يتم تنفيذ أي أوامر حقيقية' if DRY_RUN else '🔴 الوضع المباشر نشط - يتم تنفيذ الأوامر'}
        """
        await update.message.reply_text(status_text, parse_mode='HTML')

    async def positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        trades = self.db.get_open_trades()
        if not trades:
            await update.message.reply_text("لا توجد صفقات مفتوحة حالياً. 💤")
            return

        msg = "📊 <b>الصفقات المفتوحة</b>\n\n"
        for t in trades:
            trade_id = t.get('id', '?')
            symbol = str(t.get('symbol', 'UNKNOWN')).replace(':USDT', '')
            side = str(t.get('side', 'UNKNOWN'))
            entry_price = round(float(t.get('entry_price') or 0), 6)
            sl_price = round(float(t.get('sl_price') or 0), 6)
            tp_price = round(float(t.get('tp_price') or 0), 6)
            leverage = t.get('leverage_used', 1)
            slot = t.get('slot_used', '?')
            score = t.get('confidence', 0)
            regime = str(t.get('regime', 'غير محدد'))
            direction_emoji = '🟢 LONG' if side == 'LONG' else '🔴 SHORT'

            msg += f"• <b>{symbol}</b> (ID: <code>{trade_id}</code>)\n"
            msg += f"الاتجاه: {direction_emoji} | الرافعة: x{leverage} (Slot {slot})\n"
            msg += f"الدخول: <code>{entry_price}</code>\n"
            msg += f"الوقف: <code>{sl_price}</code> | الهدف: <code>{tp_price}</code>\n"
            msg += f"قوة الدخول: {score:.1f}/100 | السوق: {regime}\n"

            analysis = self.db.get_latest_open_analysis(trade_id)
            if analysis:
                profit = analysis.get('profit_pct', 0)
                rec = str(analysis.get('recommendation', 'HOLD'))
                action_icon = "🛑" if rec == "CLOSE" else ("✂️" if rec == "REDUCE" else ("🎯" if rec == "TRAIL_SL" else "⏳"))
                msg += f"\n📈 <b>إدارة الصفقة (AI):</b>\n"
                msg += f"الربح الحالي: {profit:+.2f}%\n"
                msg += f"القرار الآن: {action_icon} <b>{rec}</b> (ثقة: {analysis.get('ai_confidence', 0):.0f}%)\n"
                
                # إظهار نتيجة التنفيذ السابق إن وجد
                last_action = self.db.get_last_executed_action(trade_id)
                if last_action:
                    status_icon = "✅" if last_action.get('success') else "❌"
                    msg += f"آخر تنفيذ: {status_icon} {last_action.get('action')} | {'🟡 محاكاة' if last_action.get('dry_run') else '🔴 حقيقي'}\n"
            else:
                msg += f"\n⏳ <i>جاري تحليل بيانات إدارة الصفقة...</i>\n"

            msg += "\n" + "─"*25 + "\n\n"

        await update.message.reply_text(msg, parse_mode='HTML')

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
            await update.message.reply_text("⏳ التحليل قيد التجهيز أو لم يتم العثور عليه، يرجى الانتظار دقيقة والمحاولة مجدداً.")
            return

        msg = f"🔍 <b>توصية مدير الصفقات #{trade_id}</b>\n"
        msg += f"العملة: {analysis.get('symbol', 'UNKNOWN')}\n"
        msg += f"الربح الحالي: {analysis.get('profit_pct', 0):+.2f}%\n"
        msg += f"\n🎯 <b>الاحتمالات المتوقعة</b>\n"
        msg += f" الهدف (TP): {analysis.get('probability_tp', 0):.0f}%\n"
        msg += f" الخسارة (SL): {analysis.get('probability_sl', 0):.0f}%\n"
        msg += f" عرضي: {analysis.get('probability_sideways', 0):.0f}%\n"
        msg += f" انعكاس: {analysis.get('probability_reversal', 0):.0f}%\n"

        rec = analysis.get('recommendation', 'HOLD')
        action_icon = "🛑" if rec == "CLOSE" else ("✂️" if rec == "REDUCE" else ("🎯" if rec == "TRAIL_SL" else "⏳"))
        msg += f"\n🤖 <b>القرار الإداري:</b> {action_icon} <b>{rec}</b>\n"
        msg += f"مستوى الثقة: {analysis.get('ai_confidence', 0):.0f}%\n"
        msg += f"التبرير: <i>{analysis.get('ai_explanation', '')}</i>\n"

        await update.message.reply_text(msg, parse_mode='HTML')

    async def market(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not context.args:
            await update.message.reply_text("يرجى إرسال اسم العملة: مثلاً /market BTC")
            return
        symbol = context.args[0].upper()
        if not symbol.endswith('USDT'):
            symbol += 'USDT'
        symbol = f"{symbol}/USDT:USDT"

        await update.message.reply_text(f"⏳ جاري الفحص المباشر لعملة {symbol}...")
        try:
            market_data = self.analytics.analyze_market(symbol)
            if 'error' in market_data:
                await update.message.reply_text(f"⚠️ خطأ في جلب بيانات {symbol}: {market_data['error']}")
                return
            msg = f"📊 <b>تحليل السوق المباشر - {symbol}</b>\n\n"
            msg += f"<b>بنية السوق:</b> {market_data.get('market_structure', 'N/A')}\n"
            msg += f"<b>السعر الحالي:</b> {market_data.get('price', 0)}\n"
            await update.message.reply_text(msg, parse_mode='HTML')
        except Exception as e:
            await update.message.reply_text(f"⚠️ حدث خطأ غير متوقع: {e}")

    def run(self):
        self.app = Application.builder().token(self.token).build()
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("status", self.status))
        self.app.add_handler(CommandHandler("positions", self.positions))
        self.app.add_handler(CommandHandler("advice", self.advice))
        self.app.add_handler(CommandHandler("market", self.market))
        self.app.run_polling()

# =============================================================================
# 🔁 MONITOR LOOP مع محرك القرار الهجين المتقدم + طبقة التنفيذ
# =============================================================================

class MonitorLoop:
    def __init__(self, db: MonitorDB, analytics: AdvancedAnalyticsEngine, ai: AIClient, execution_manager: BinanceExecutionManager):
        self.db = db
        self.analytics = analytics
        self.ai = ai
        self.execution = execution_manager
        self.running = True
        self.queue = Queue()
        self.market_cache = {}
        self.last_decision_time = {}  # trade_id -> timestamp
        self.last_executed_action = {}  # trade_id -> action
        self.execution_lock = threading.Lock()

    def start(self):
        for _ in range(3):
            threading.Thread(target=self._worker, daemon=True).start()
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self.running = False
        for _ in range(3): self.queue.put(None)

    def _worker(self):
        while True:
            trade = self.queue.get()
            if trade is None: break
            try:
                self._analyze_trade(trade)
            except Exception as e:
                logging.error(f"Worker Error for {trade.get('symbol', 'UNKNOWN')}:\n{traceback.format_exc()}")
            finally:
                self.queue.task_done()

    def _loop(self):
        while self.running:
            try:
                open_trades = self.db.get_open_trades()
                for trade in open_trades:
                    self.queue.put(trade)
            except Exception as e:
                logging.error(f"Monitor loop error: {e}")
            time.sleep(MONITOR_INTERVAL)

    def _get_trend_alignment(self, side: str, plus_di: float, minus_di: float) -> str:
        if side == "LONG":
            return "ALIGNED" if plus_di > minus_di else "CONFLICT"
        elif side == "SHORT":
            return "ALIGNED" if minus_di > plus_di else "CONFLICT"
        return "UNKNOWN"

    def _calculate_reversal_risk(self, data: Dict) -> float:
        risk = 0.0
        side = data.get('side', '')
        adx = data.get('adx', 0)
        rsi = data.get('rsi', 50)
        plus_di = data.get('plus_di', 0)
        minus_di = data.get('minus_di', 0)

        if side == "SHORT":
            if rsi < 30:
                risk += 20
            if plus_di > minus_di:
                risk += 25
        elif side == "LONG":
            if rsi > 70:
                risk += 20
            if minus_di > plus_di:
                risk += 25

        if adx < 15:
            risk += 15

        return min(100.0, risk)

    def _calculate_management_score(self, data: Dict) -> float:
        score = 50.0

        profit = data.get('profit_pct', 0.0)
        if profit > 0:
            score += 10
        else:
            score -= 10

        adx = data.get('adx', 0.0)
        if adx >= 25:
            score += 10
        elif adx < 15:
            score -= 5

        dist_sl = data.get('distance_to_sl_pct', 100.0)
        atr_pct = data.get('atr_pct', 1.0)
        if dist_sl < atr_pct * 0.5:
            score -= 20
        elif dist_sl < atr_pct:
            score -= 10

        dist_tp = data.get('distance_to_tp_pct', 0.0)
        if dist_tp < atr_pct:
            score += 10

        alignment = data.get('trend_alignment', 'UNKNOWN')
        if alignment == 'ALIGNED':
            score += 15
        elif alignment == 'CONFLICT':
            score -= 20

        reversal_risk = data.get('reversal_risk', 0)
        score -= reversal_risk * 0.2

        return max(0.0, min(100.0, score))

    def _apply_hard_guards(self, trade: Dict, current_price: float, profit_pct: float) -> str:
        side = trade.get('side', '')
        sl_price = float(trade.get('sl_price') or 0)

        if sl_price > 0:
            if side == 'LONG' and current_price <= sl_price:
                logging.info(f"🛑 Hard SL hit for {trade.get('symbol')} at {current_price}")
                return 'CLOSE'
            if side == 'SHORT' and current_price >= sl_price:
                logging.info(f"🛑 Hard SL hit for {trade.get('symbol')} at {current_price}")
                return 'CLOSE'

        if profit_pct <= -MAX_LOSS_PCT:
            logging.info(f"🚨 Max loss reached for {trade.get('symbol')}: {profit_pct:.2f}%")
            return 'CLOSE'

        return None

    def _decide_final(self, ai_recommendation: str, management_score: float, reversal_risk: float) -> str:
        if management_score < 30:
            return 'CLOSE'
        elif management_score < 45:
            if reversal_risk > 60:
                return 'CLOSE'
            elif ai_recommendation in ('CLOSE', 'REDUCE'):
                return ai_recommendation
            else:
                return 'REDUCE'
        elif management_score < 60:
            if reversal_risk > 70:
                return 'CLOSE'
            elif ai_recommendation == 'CLOSE':
                return 'CLOSE'
            else:
                return 'HOLD'
        else:
            if ai_recommendation in ('HOLD', 'TRAIL_SL'):
                return ai_recommendation
            else:
                if management_score > 75:
                    return 'HOLD'
                return ai_recommendation

    def _execute_management_action(self, trade: Dict, final_decision: str, current_price: float) -> Dict[str, Any]:
        trade_id = trade.get('id')
        symbol = trade.get('symbol', '')
        side = trade.get('side', '')

        final_decision = str(final_decision or "HOLD").upper()

        if final_decision == "HOLD":
            return {
                "success": True,
                "executed": False,
                "action": "HOLD",
                "reason": "No action required"
            }

        now = time.time()
        with self.execution_lock:
            last_time = self.last_decision_time.get(trade_id, 0)
            if now - last_time < DECISION_COOLDOWN:
                return {
                    "success": True,
                    "executed": False,
                    "action": final_decision,
                    "reason": "Decision cooldown"
                }
            self.last_decision_time[trade_id] = now

        if final_decision == "REDUCE":
            previous = self.last_executed_action.get(trade_id)
            if previous == "REDUCE":
                return {
                    "success": True,
                    "executed": False,
                    "action": "REDUCE",
                    "reason": "REDUCE already executed"
                }

        position = self.execution.get_position(symbol)
        if not position:
            logging.warning(f"⚠️ No Binance position found for {symbol}")
            return {
                "success": False,
                "executed": False,
                "action": final_decision,
                "reason": "Position not found"
            }

        contracts = float(position.get("contracts") or 0)
        actual_side = position.get("side", "")

        if contracts <= 0:
            return {
                "success": False,
                "executed": False,
                "action": final_decision,
                "reason": "Position size is zero"
            }

        if final_decision == "REDUCE":
            result = self.execution.reduce_position(
                symbol=symbol,
                position_side=actual_side,
                contracts=contracts,
                percent=REDUCE_PERCENT
            )
            if result.get("success"):
                self.last_executed_action[trade_id] = "REDUCE"
            return result

        if final_decision == "CLOSE":
            result = self.execution.close_position(
                symbol=symbol,
                position_side=actual_side,
                contracts=contracts
            )
            if result.get("success"):
                self.last_executed_action[trade_id] = "CLOSE"
            return result

        if final_decision == "TRAIL_SL":
            market_data = self.market_cache.get(symbol, {}).get("data", {})
            ohlcv = market_data.get("ohlcv", [])
            atr_pct = self.analytics.get_atr_pct(ohlcv)
            if atr_pct <= 0:
                return {
                    "success": False,
                    "executed": False,
                    "action": "TRAIL_SL",
                    "reason": "ATR unavailable"
                }

            if actual_side.lower() == "long":
                new_sl = current_price * (1 - (atr_pct * 1.5 / 100))
            else:
                new_sl = current_price * (1 + (atr_pct * 1.5 / 100))

            result = self.execution.update_stop_loss(
                symbol=symbol,
                position_side=actual_side,
                stop_price=new_sl
            )
            if result.get("success"):
                self.last_executed_action[trade_id] = "TRAIL_SL"
            return result

        return {
            "success": False,
            "executed": False,
            "action": final_decision,
            "reason": "Unknown management action"
        }

    def _analyze_trade(self, trade):
        trade_id = trade.get('id')
        symbol = trade.get('symbol', '')
        entry_price = float(trade.get('entry_price') or 0)
        side = trade.get('side', '')
        tp_price = float(trade.get('tp_price') or 0)
        sl_price = float(trade.get('sl_price') or 0)

        market_data = self.market_cache.get(symbol, {}).get('data', {})
        if not market_data or time.time() - self.market_cache.get(symbol, {}).get('time', 0) > 300:
            market_data = self.analytics.analyze_market(symbol)
            self.market_cache[symbol] = {'data': market_data, 'time': time.time()}

        if 'error' in market_data:
            return

        current_price = market_data.get('price', entry_price)
        if entry_price > 0:
            profit_pct = ((current_price - entry_price) / entry_price * 100) if side == 'LONG' else ((entry_price - current_price) / entry_price * 100)
        else:
            profit_pct = 0.0

        try:
            opened_at = datetime.fromisoformat(trade.get('timestamp', '').replace("Z", "+00:00"))
            time_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 60
        except:
            time_open = 0

        dist_tp_pct = 0.0
        dist_sl_pct = 0.0
        rr_ratio = 0.0
        target_progress = 0.0
        if tp_price > 0 and sl_price > 0 and entry_price > 0:
            if side == 'LONG':
                dist_tp_pct = ((tp_price - current_price) / current_price) * 100
                dist_sl_pct = ((current_price - sl_price) / current_price) * 100
                rr_ratio = abs(tp_price - entry_price) / abs(entry_price - sl_price) if abs(entry_price - sl_price) > 0 else 0
                target_progress = (current_price - entry_price) / (tp_price - entry_price) * 100
            else:
                dist_tp_pct = ((current_price - tp_price) / current_price) * 100
                dist_sl_pct = ((sl_price - current_price) / current_price) * 100
                rr_ratio = abs(entry_price - tp_price) / abs(sl_price - entry_price) if abs(sl_price - entry_price) > 0 else 0
                target_progress = (entry_price - current_price) / (entry_price - tp_price) * 100
            target_progress = max(0, min(100, target_progress))

        ohlcv = market_data.get('ohlcv', [])
        di = self.analytics.adx_di(ohlcv)
        adx = di['adx']
        plus_di = di['plus_di']
        minus_di = di['minus_di']
        rsi = self.analytics.real_rsi(ohlcv)
        atr_pct = self.analytics.get_atr_pct(ohlcv)
        funding = self.analytics.funding_rate(symbol)
        regime = market_data.get('market_structure', 'UNKNOWN')

        trend_alignment = self._get_trend_alignment(side, plus_di, minus_di)
        reversal_risk = self._calculate_reversal_risk({
            'side': side,
            'adx': adx,
            'rsi': rsi,
            'plus_di': plus_di,
            'minus_di': minus_di
        })

        current_data = {
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'current_price': current_price,
            'profit_pct': profit_pct,
            'time_open_minutes': int(time_open),
            'target_progress': target_progress,
            'trend_strength_adx': adx,
            'rsi_14': rsi,
            'atr_pct': atr_pct,
            'funding_rate': funding,
            'market_regime': regime,
            'tp_price': tp_price,
            'sl_price': sl_price,
            'distance_to_tp_pct': max(0, dist_tp_pct),
            'distance_to_sl_pct': max(0, dist_sl_pct),
            'risk_reward': rr_ratio,
            'adx': adx,
            'plus_di': plus_di,
            'minus_di': minus_di,
            'trend_alignment': trend_alignment,
            'reversal_risk': reversal_risk,
        }

        forced_decision = self._apply_hard_guards(trade, current_price, profit_pct)
        if forced_decision:
            final_decision = forced_decision
            ai_recommendation = forced_decision
            ai_confidence = 100
            ai_explanation = "إغلاق إجباري بسبب كسر وقف الخسارة أو الخسارة القصوى."
            management_score = 0
            tp_probability = sl_probability = sideways_probability = reversal_probability = 0
        else:
            ai_result = self.ai.get_recommendation(current_data)
            ai_recommendation = ai_result.get('recommendation', 'HOLD')
            ai_confidence = ai_result.get('confidence', 0)
            ai_explanation = ai_result.get('reason', '')
            tp_probability = ai_result.get('tp_probability', 0)
            sl_probability = ai_result.get('sl_probability', 0)
            sideways_probability = ai_result.get('sideways_probability', 0)
            reversal_probability = ai_result.get('reversal_probability', 0)

            management_score = self._calculate_management_score(current_data)
            final_decision = self._decide_final(ai_recommendation, management_score, reversal_risk)

            decision_record = {
                'trade_id': trade_id,
                'symbol': symbol,
                'decision': final_decision,
                'confidence': ai_confidence,
                'price': current_price,
                'profit_pct': profit_pct,
                'adx': adx,
                'plus_di': plus_di,
                'minus_di': minus_di,
                'rsi': rsi,
                'atr_pct': atr_pct,
                'distance_sl': dist_sl_pct,
                'distance_tp': dist_tp_pct,
                'trend_alignment': trend_alignment,
                'reversal_risk': reversal_risk,
                'management_score': management_score,
                'ai_recommendation': ai_recommendation,
                'ai_confidence': ai_confidence,
                'ai_explanation': ai_explanation,
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.db.save_management_decision(decision_record)

        # =========================================================================
        # ⚡ EXECUTION LAYER
        # =========================================================================
        execution_result = self._execute_management_action(
            trade=trade,
            final_decision=final_decision,
            current_price=current_price
        )
        logging.info(f"⚡ MANAGEMENT | {symbol} | Decision={final_decision} | Execution={execution_result}")

        # تسجيل التنفيذ
        if execution_result.get('executed', False) or not execution_result.get('success', True):
            self.db.save_executed_action({
                'trade_id': trade_id,
                'symbol': symbol,
                'action': final_decision,
                'side': execution_result.get('side', ''),
                'amount': execution_result.get('amount', 0),
                'price': current_price,
                'order_id': execution_result.get('order_id', ''),
                'dry_run': execution_result.get('dry_run', DRY_RUN),
                'success': execution_result.get('success', False),
                'reason': execution_result.get('reason', ''),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })

        # 🔴 5. حفظ التحليل الرئيسي مع الاحتمالات الصحيحة
        analysis_record = {
            'trade_id': trade_id,
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'current_price': current_price,
            'profit_pct': profit_pct,
            'time_open_minutes': int(time_open),
            'target_progress': target_progress,
            'trend_strength': adx,
            'momentum_score': rsi,
            'funding_rate': funding,
            'oi_change_1h': 0.0,
            'oi_trend': 0.0,
            'apex_score': float(trade.get('confidence', 50)),
            'iss_score': 50,
            'ai_decision': ai_recommendation,
            'ai_confidence': ai_confidence,
            'ai_explanation': ai_explanation,
            'recommendation': final_decision,
            'probability_tp': tp_probability if not forced_decision else 0,
            'probability_sl': sl_probability if not forced_decision else 0,
            'probability_sideways': sideways_probability if not forced_decision else 0,
            'probability_reversal': reversal_probability if not forced_decision else 0,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        self.db.save_open_analysis(analysis_record)
        logging.info(f"✅ Decision saved for {symbol}: {final_decision} (AI: {ai_recommendation}, Score: {management_score:.1f}, Alignment: {trend_alignment}, Reversal Risk: {reversal_risk:.1f})")

# =============================================================================
# 🚀 MAIN
# =============================================================================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    logging.info("🚀 Starting APEX Position Manager Bot v4.8 (Hybrid Decision + Execution)")

    exchange_public = ccxt.binance({
        "enableRateLimit": True,
        "options": {
            "defaultType": "swap",
            "adjustForTimeDifference": True
        }
    })

    try:
        exchange_public.load_markets()
    except Exception as e:
        logging.warning(f"Load markets warning: {e}")

    db = MonitorDB(MONITOR_DB_PATH)
    analytics = AdvancedAnalyticsEngine(exchange_public)
    ai = AIClient()
    execution_manager = BinanceExecutionManager()

    logging.info(f"💱 Binance execution status: {execution_manager.status()}")

    # اختبار الاتصال بالنموذج
    try:
        logging.info(f"🧪 Testing {AI_MODEL} connection...")
        test_response = ai.client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": "Reply with exactly: LAGUNA-OK"}],
            temperature=0,
            max_tokens=20,
            timeout=10.0,
            extra_body={"chat_template_kwargs": {"enable_thinking": False}}
        )
        test_content = test_response.choices[0].message.content.strip()
        logging.info(f"✅ TEST SUCCESS: {test_content}")
    except Exception as e:
        logging.error(f"❌ TEST FAILED: {type(e).__name__}: {e}")
        logging.warning("⚠️ AI test failed, but bot will continue. Advice may show fallback values.")

    monitor = MonitorLoop(db, analytics, ai, execution_manager)
    monitor.start()

    telegram_bot = TelegramBot(TELEGRAM_TOKEN, ADMIN_CHAT_ID, db, analytics)
    telegram_bot.run()

if __name__ == "__main__":
    main()