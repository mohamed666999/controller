#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════════════════╗
║                                                                                          ║
║     APEX KAF ADVISOR BOT v5.0 — استشاري الصفقات الذكي (بدون تنفيذ)                      ║
║                                                                                          ║
║  Architecture: Advisory-Only + AI Analysis + SP/TP from QUANTUM APEX v4.0               ║
║  AI Model: poolside/laguna-xs-2.1                                                       ║
║                                                                                          ║
║  ⚠️ هذا البوت لا ينفذ أي صفقات — مراقب ومستشار فقط                                     ║
║                                                                                          ║
╚══════════════════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import time
import json
import logging
import threading
import sqlite3
import traceback
import numpy as np
import requests
import ccxt
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from queue import Queue
from functools import wraps

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
print("🚀 APEX KAF ADVISOR BOT v5.0 — STARTING")
print("=" * 70)
DEPLOYMENT_IP = get_public_ip()
print(f"📌 PUBLIC IP: {DEPLOYMENT_IP}")
print("=" * 70)
print("⚠️  وضع الاستشارة فقط — لن يتم تنفيذ أي أوامر")
print("=" * 70)

# =============================================================================
# 🔧 CONFIG
# =============================================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8122906116:AAHAWsXfaiymnvdeNO0BURyRVccJU8_gIco")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "6033203084"))

APEX_API_URL = os.getenv("APEX_API_URL", "https://binancetrading-production.up.railway.app")
MONITOR_DB_PATH = os.getenv("MONITOR_DB_PATH", "kaf_advisor.db")

# AI Config
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "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")
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
AI_MODEL = "poolside/laguna-xs-2.1"

# Monitor
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", "60"))
AI_DECISION_INTERVAL = int(os.getenv("AI_DECISION_INTERVAL", "600"))  # 10 دقائق

# =============================================================================
# 🔢 ثوابت حساب SP/TP من QUANTUM APEX v4.0 (بدون تغيير)
# =============================================================================
# هذه القيم مأخوذة مباشرة من البوت الأول — QUANTUM APEX v4.0
# SL = entry ± (ATR × 2.5)
# TP = entry ± (ATR × 5.0)
ATR_PERIOD = 14
STOP_LOSS_ATR_MULTIPLE = 2.5      # من CFG.risk.default_stop_atr_multiple
TAKE_PROFIT_ATR_MULTIPLE = 5.0    # من CFG.risk.default_take_profit_atr_multiple
ATR_SAFETY_FLOOR_PCT = 0.0001     # أقل قيمة ممكنة للـ ATR (0.01% من السعر)

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
                CREATE TABLE IF NOT EXISTS kaf_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER, symbol TEXT, side TEXT,
                    entry_price REAL, current_price REAL,
                    sp_price REAL, tp_price REAL,
                    profit_if_tp REAL, loss_if_sp REAL,
                    profit_if_tp_pct REAL, loss_if_sp_pct REAL,
                    position_size_usdt REAL, leverage INTEGER,
                    profit_pct REAL, time_open_minutes INTEGER,
                    target_progress REAL, trend_strength REAL, momentum_score REAL,
                    funding_rate REAL, market_regime TEXT,
                    atr_value REAL, atr_pct REAL,
                    ai_decision TEXT, ai_confidence REAL, ai_explanation TEXT,
                    probability_tp REAL, probability_sl REAL,
                    probability_sideways REAL, probability_reversal REAL,
                    risk_reward_ratio REAL, trend_alignment TEXT, reversal_risk REAL,
                    management_score REAL, timestamp TEXT
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

    def save_kaf_analysis(self, data: Dict[str, Any]):
        columns = [
            "trade_id", "symbol", "side", "entry_price", "current_price",
            "sp_price", "tp_price", "profit_if_tp", "loss_if_sp",
            "profit_if_tp_pct", "loss_if_sp_pct",
            "position_size_usdt", "leverage",
            "profit_pct", "time_open_minutes", "target_progress",
            "trend_strength", "momentum_score", "funding_rate", "market_regime",
            "atr_value", "atr_pct",
            "ai_decision", "ai_confidence", "ai_explanation",
            "recommendation", "probability_tp", "probability_sl",
            "probability_sideways", "probability_reversal",
            "risk_reward_ratio", "trend_alignment", "reversal_risk",
            "management_score", "timestamp"
        ]
        values = [
            data.get("trade_id"), data.get("symbol"), data.get("side"),
            data.get("entry_price"), data.get("current_price"),
            data.get("sp_price"), data.get("tp_price"),
            data.get("profit_if_tp"), data.get("loss_if_sp"),
            data.get("profit_if_tp_pct"), data.get("loss_if_sp_pct"),
            data.get("position_size_usdt"), data.get("leverage"),
            data.get("profit_pct"), data.get("time_open_minutes"),
            data.get("target_progress"), data.get("trend_strength"),
            data.get("momentum_score"), data.get("funding_rate"),
            data.get("market_regime"), data.get("atr_value"), data.get("atr_pct"),
            data.get("ai_decision"), data.get("ai_confidence"),
            data.get("ai_explanation"), data.get("recommendation"),
            data.get("probability_tp", 0), data.get("probability_sl", 0),
            data.get("probability_sideways", 0), data.get("probability_reversal", 0),
            data.get("risk_reward_ratio", 0), data.get("trend_alignment"),
            data.get("reversal_risk", 0), data.get("management_score", 0),
            data.get("timestamp")
        ]
        if len(columns) != len(values):
            logging.error(f"DB mapping mismatch: {len(columns)} columns vs {len(values)} values")
            return
        placeholders = ",".join(["?"] * len(values))
        with self.lock:
            try:
                self.monitor_conn.execute(
                    f"INSERT INTO kaf_analysis ({','.join(columns)}) VALUES ({placeholders})",
                    values
                )
                self.monitor_conn.commit()
            except Exception as e:
                logging.error(f"DB Save Error: {e}\n{traceback.format_exc()}")

    def get_latest_analysis(self, trade_id: int) -> Optional[Dict]:
        with self.lock:
            try:
                row = self.monitor_conn.execute(
                    "SELECT * FROM kaf_analysis WHERE trade_id=? ORDER BY id DESC LIMIT 1",
                    (trade_id,)
                ).fetchone()
                if row:
                    cols = [desc[0] for desc in self.monitor_conn.execute("SELECT * FROM kaf_analysis LIMIT 0").description]
                    return dict(zip(cols, row))
            except Exception as e:
                logging.error(f"DB read error: {e}")
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

    def compute_atr(self, ohlcv: List, period: int = 14) -> float:
        """
        حساب ATR الحقيقي بنفس طريقة QUANTUM APEX v4.0:
        يستخدم High-Low-Close (True Range) مع فترة 14
        """
        if not ohlcv or len(ohlcv) < period + 1:
            return 0.0
        highs = np.array([c[2] for c in ohlcv[-(period+1):]])
        lows = np.array([c[3] for c in ohlcv[-(period+1):]])
        closes = np.array([c[4] for c in ohlcv[-(period+1):]])

        # True Range = max(H-L, |H-Cprev|, |L-Cprev|)
        trs = []
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i-1]),
                abs(lows[i] - closes[i-1])
            )
            trs.append(tr)

        # ATR = متوسط True Range على آخر `period` فترة
        atr = float(np.mean(trs[-period:])) if trs else 0.0
        return atr

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

        atr = np.mean(trs[-period:]) or 1e-12
        plus_di = 100 * np.mean(plus_dms[-period:]) / atr
        minus_di = 100 * np.mean(minus_dms[-period:]) / atr

        dx_values = []
        for i in range(period, len(plus_dms)):
            p_di = 100 * np.mean(plus_dms[i-period:i]) / atr
            m_di = 100 * np.mean(minus_dms[i-period:i]) / atr
            di_sum = p_di + m_di
            if di_sum > 0:
                dx_values.append(100 * abs(p_di - m_di) / di_sum)

        if len(dx_values) >= period:
            adx = np.mean(dx_values[-period:])
        else:
            adx = np.mean(dx_values) if dx_values else 0.0

        return {'adx': adx, 'plus_di': plus_di, 'minus_di': minus_di}

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
        except:
            pass
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
# 🤖 AI CLIENT — مستشار فقط (لا يصدر أوامر تنفيذ)
# =============================================================================

class AIClient:
    def __init__(self):
        self.client = OpenAI(
            base_url=NVIDIA_BASE_URL,
            api_key=NVIDIA_API_KEY
        )

    def get_advice(self, trade_data: Dict) -> Dict:
        """
        استشارة AI — القرارات هي HOLD / ALERT فقط
        لا يوجد CLOSE أو REDUCE أو TRAIL_SL لأن البوت لا ينفذ شيئاً
        """
        prompt = f"""أنت "مستشار صفقات" (Trade Advisor) في صندوق استثماري محترف. دورك هو الملاحظة والتوصية فقط — لا يمكنك تنفيذ أي إجراء.

📊 [بيانات الصفقة]
العملة: {trade_data.get('symbol')} | نوع الصفقة: {trade_data.get('side')}
سعر الدخول: {trade_data.get('entry_price')} | السعر الحالي: {trade_data.get('current_price')}
الربح/الخسارة الحالي: {trade_data.get('profit_pct', 0):.2f}% | مدة الصفقة: {trade_data.get('time_open_minutes')} دقيقة

📏 [وقف الخسارة والهدف — من خوارزمية QUANTUM APEX]
وقف الخسارة (SP): {trade_data.get('sp_price')} | مسافة SP: {trade_data.get('distance_to_sp_pct', 0):.2f}%
هدف الربح (TP): {trade_data.get('tp_price')} | مسافة TP: {trade_data.get('distance_to_tp_pct', 0):.2f}%
نسبة المخاطرة للعائد (R:R): {trade_data.get('risk_reward', 0):.2f}

💰 [حساب الربح والخسارة المتوقع]
الربح إذا تحقق الهدف (TP): +{trade_data.get('profit_if_tp', 0):.2f} USDT ({trade_data.get('profit_if_tp_pct', 0):.2f}%)
الخسارة إذا ضرب الوقف (SP): -{trade_data.get('loss_if_sp', 0):.2f} USDT ({trade_data.get('loss_if_sp_pct', 0):.2f}%)

📈 [مؤشرات السوق الحية - 1H]
حالة السوق: {trade_data.get('market_regime')}
قوة الاتجاه (ADX): {trade_data.get('trend_strength_adx', 0):.1f}/100
مؤشر القوة النسبية RSI(14): {trade_data.get('rsi_14', 0):.1f}
التقلبات (ATR%): {trade_data.get('atr_pct', 0):.2f}%
معدل التمويل: {trade_data.get('funding_rate', 0):.6f}

يجب أن يكون التفسير (reason) باللغة العربية الفصحى حصراً وبدون أي كلمات إنجليزية.

أجب بصيغة JSON فقط:
{{
    "action": "HOLD أو ALERT",
    "tp_probability": 60,
    "sl_probability": 10,
    "sideways_probability": 20,
    "reversal_probability": 10,
    "confidence": 85,
    "reason": "نصيحتك ك مستشار صفقات — اشرح وضع الصفقة بالعربية الفصحى"
}}
"""
        try:
            logging.info(f"🤖 Calling {AI_MODEL} for {trade_data.get('symbol')}")
            start_time = time.time()
            response = self.client.chat.completions.create(
                model=AI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a quantitative trade advisor. You observe and recommend only. Return valid JSON only."},
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
# 📡 TELEGRAM BOT — استشاري فقط
# =============================================================================

class TelegramBot:
    def __init__(self, token, admin_chat_id, monitor_db, analytics_engine):
        self.token = token
        self.admin_chat_id = admin_chat_id
        self.db = monitor_db
        self.analytics = analytics_engine

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 مرحباً! أنا بوت المستشار الذكي APEX KAF v5.0\n"
            "🔍 دوري: مراقبة الصفقات وتقديم SP و TP ونصائح فقط\n"
            "⚠️ لا أنفذ أي صفقات\n\n"
            "الأوامر:\n"
            "/positions - الصفقات المفتوحة مع SP/TP والربح/الخسارة المتوقع\n"
            "/advice <id> - تحليل AI مفصل لصفقة محددة\n"
            "/market <symbol> - تحليل سريع للسوق\n"
            "/status - حالة البوت"
        )

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        status_text = f"""
📊 <b>حالة بوت المستشار APEX KAF v5.0</b>

🔧 <b>الإعدادات</b>
• الوضع: 🟢 استشاري فقط (KAF) — لا ينفذ أي أوامر
• MONITOR_INTERVAL: {MONITOR_INTERVAL}s
• AI_DECISION_INTERVAL: {AI_DECISION_INTERVAL}s
• ATR Period: {ATR_PERIOD}
• SP Multiplier: {STOP_LOSS_ATR_MULTIPLE}x ATR
• TP Multiplier: {TAKE_PROFIT_ATR_MULTIPLE}x ATR

📈 <b>الصفقات المفتوحة:</b> {len(self.db.get_open_trades())}

⚠️ هذا البوت <b>لا ينفذ</b> أي صفقات — يقدم المشورة فقط
        """
        await update.message.reply_text(status_text, parse_mode='HTML')

    async def positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        trades = self.db.get_open_trades()
        if not trades:
            await update.message.reply_text("لا توجد صفقات مفتوحة حالياً. 💤")
            return

        msg = "📊 <b>الصفقات المفتوحة — المستشار الذكي</b>\n"
        msg += "⚠️ <i>هذا البوت لا ينفذ أي أوامر — مراقب فقط</i>\n\n"

        for t in trades:
            trade_id = t.get('id', '?')
            symbol = str(t.get('symbol', 'UNKNOWN')).replace(':USDT', '')
            side = str(t.get('side', 'UNKNOWN'))
            entry_price = round(float(t.get('entry_price') or 0), 6)
            leverage = t.get('leverage_used', 1)
            score = t.get('confidence', 0)

            # جلب آخر تحليل من قاعدة البيانات
            analysis = self.db.get_latest_analysis(trade_id)

            if analysis:
                sp_price = round(float(analysis.get('sp_price') or 0), 6)
                tp_price = round(float(analysis.get('tp_price') or 0), 6)
                current_price = round(float(analysis.get('current_price') or 0), 6)
                profit_pct = float(analysis.get('profit_pct') or 0)
                profit_if_tp = float(analysis.get('profit_if_tp') or 0)
                loss_if_sp = float(analysis.get('loss_if_sp') or 0)
                profit_if_tp_pct = float(analysis.get('profit_if_tp_pct') or 0)
                loss_if_sp_pct = float(analysis.get('loss_if_sp_pct') or 0)
                rr = float(analysis.get('risk_reward_ratio') or 0)
                ai_rec = analysis.get('ai_decision', 'HOLD')
                ai_conf = float(analysis.get('ai_confidence') or 0)
                ai_reason = analysis.get('ai_explanation', '')
                tp_prob = float(analysis.get('probability_tp') or 0)
                sl_prob = float(analysis.get('probability_sl') or 0)
            else:
                # لا يوجد تحليل بعد — نحسب SP/TP على السريع
                sp_price = 0
                tp_price = 0
                current_price = round(float(t.get('current_price') or t.get('mark_price') or 0), 6)
                profit_pct = 0
                profit_if_tp = 0
                loss_if_sp = 0
                profit_if_tp_pct = 0
                loss_if_sp_pct = 0
                rr = 0
                ai_rec = "⏳"
                ai_conf = 0
                ai_reason = "التحليل قيد التجهيز..."
                tp_prob = 0
                sl_prob = 0

            side_icon = "🟢" if side == "LONG" else ("🔴" if side == "SHORT" else "⚪")
            pnl_icon = "📈" if profit_pct >= 0 else "📉"

            msg += f"{side_icon} <b>#{trade_id} {symbol}</b> | {side} | {leverage}x\n"
            msg += f"   📥 الدخول: ${entry_price}\n"
            msg += f"   📍 الحالي: ${current_price} {pnl_icon} {profit_pct:+.2f}%\n"
            msg += f"   🔒 SP: ${sp_price} | 🎯 TP: ${tp_price}\n"
            msg += f"   📐 R:R = {rr:.2f}\n"
            msg += f"   💰 إذا حققت الهدف (TP): <b>+{profit_if_tp:.2f} USDT</b> ({profit_if_tp_pct:+.2f}%)\n"
            msg += f"   🛑 إذا ضرب الوقف (SP): <b>-{loss_if_sp:.2f} USDT</b> ({loss_if_sp_pct:+.2f}%)\n"
            msg += f"   🤖 AI: {ai_rec} | ثقة: {ai_conf:.0f}%\n"
            msg += f"   📊 احتمال TP: {tp_prob:.0f}% | احتمال SP: {sl_prob:.0f}%\n"
            if ai_reason:
                # تقصير النص إذا كان طويلاً
                if len(ai_reason) > 150:
                    ai_reason = ai_reason[:150] + "..."
                msg += f"   💡 <i>{ai_reason}</i>\n"
            msg += "─────────────────────\n"

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

        analysis = self.db.get_latest_analysis(trade_id)
        if not analysis:
            await update.message.reply_text("⏳ التحليل قيد التجهيز أو لم يتم العثور عليه، يرجى الانتظار دقيقة والمحاولة مجدداً.")
            return

        symbol = str(analysis.get('symbol', 'UNKNOWN')).replace(':USDT', '')
        side = analysis.get('side', 'UNKNOWN')
        entry = analysis.get('entry_price', 0)
        current = analysis.get('current_price', 0)
        sp = analysis.get('sp_price', 0)
        tp = analysis.get('tp_price', 0)
        profit_pct = analysis.get('profit_pct', 0)

        msg = f"🔍 <b>تقرير المستشار — صفقة #{trade_id}</b>\n\n"
        msg += f" العملة: <b>{symbol}</b> | {side}\n"
        msg += f" 📥 الدخول: ${entry:.6f}\n"
        msg += f" 📍 الحالي: ${current:.6f} ({profit_pct:+.2f}%)\n\n"

        msg += f"📏 <b>وقف الخسارة والهدف (من QUANTUM APEX)</b>\n"
        msg += f" 🔒 SP: ${sp:.6f}\n"
        msg += f" 🎯 TP: ${tp:.6f}\n"
        msg += f" 📐 R:R = {analysis.get('risk_reward_ratio', 0):.2f}\n\n"

        msg += f"💰 <b>حساب الربح والخسارة المتوقع</b>\n"
        msg += f" ✅ إذا حققت الهدف: <b>+{analysis.get('profit_if_tp', 0):.2f} USDT</b> ({analysis.get('profit_if_tp_pct', 0):+.2f}%)\n"
        msg += f" ❌ إذا ضرب الوقف: <b>-{analysis.get('loss_if_sp', 0):.2f} USDT</b> ({analysis.get('loss_if_sp_pct', 0):+.2f}%)\n\n"

        msg += f"📈 <b>مؤشرات السوق</b>\n"
        msg += f" حالة السوق: {analysis.get('market_regime', 'N/A')}\n"
        msg += f" ADX: {analysis.get('trend_strength', 0):.1f} | RSI: {analysis.get('momentum_score', 0):.1f}\n"
        msg += f" ATR: {analysis.get('atr_pct', 0):.2f}% | Funding: {analysis.get('funding_rate', 0):.6f}\n\n"

        msg += f"🎯 <b>احتمالات AI</b>\n"
        msg += f" الهدف (TP): {analysis.get('probability_tp', 0):.0f}%\n"
        msg += f" الوقف (SP): {analysis.get('probability_sl', 0):.0f}%\n"
        msg += f" عرضي: {analysis.get('probability_sideways', 0):.0f}%\n"
        msg += f" انعكاس: {analysis.get('probability_reversal', 0):.0f}%\n\n"

        rec = analysis.get('ai_decision', 'HOLD')
        conf = analysis.get('ai_confidence', 0)
        reason = analysis.get('ai_explanation', '')
        msg += f"🤖 <b>نصيحة المستشار:</b> {rec}\n"
        msg += f" مستوى الثقة: {conf:.0f}%\n"
        msg += f" 💡 <i>{reason}</i>\n\n"
        msg += "⚠️ <i>تذكر: هذا البوت لا ينفذ أي أوامر — أنت من يتخذ القرار</i>"

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
            ohlcv = market_data.get('ohlcv', [])
            atr = self.analytics.compute_atr(ohlcv, ATR_PERIOD)
            atr_pct = self.analytics.get_atr_pct(ohlcv)
            price = market_data.get('price', 0)

            # حساب SP و TP المرجعي (كما سيفعلها QUANTUM APEX)
            sp_long = price - (atr * STOP_LOSS_ATR_MULTIPLE)
            tp_long = price + (atr * TAKE_PROFIT_ATR_MULTIPLE)
            sp_short = price + (atr * STOP_LOSS_ATR_MULTIPLE)
            tp_short = price - (atr * TAKE_PROFIT_ATR_MULTIPLE)

            msg = f"📊 <b>تحليل السوق — {symbol.replace('/USDT:USDT', '')}</b>\n\n"
            msg += f"<b>السعر الحالي:</b> ${price}\n"
            msg += f"<b>بنية السوق:</b> {market_data.get('market_structure', 'N/A')}\n"
            msg += f"<b>ATR({ATR_PERIOD}):</b> {atr:.6f} ({atr_pct:.2f}%)\n\n"
            msg += f"📏 <b>SP/TP المرجعي (كما ستحسبهما QUANTUM APEX):</b>\n"
            msg += f"🟢 LONG: SP=${sp_long:.6f} | TP=${tp_long:.6f}\n"
            msg += f"🔴 SHORT: SP=${sp_short:.6f} | TP=${tp_short:.6f}\n"
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
# 🔁 ADVISOR LOOP — مراقبة وتحليل فقط (بدون تنفيذ)
# =============================================================================

class AdvisorLoop:
    def __init__(self, db: MonitorDB, analytics: AdvancedAnalyticsEngine, ai: AIClient):
        self.db = db
        self.analytics = analytics
        self.ai = ai
        self.running = True
        self.queue = Queue()
        self.market_cache = {}
        self.last_ai_time = {}
        self.queued_trades = set()
        self.queue_lock = threading.Lock()
        self.ai_lock = threading.Semaphore(1)

    def start(self):
        for _ in range(3):
            threading.Thread(target=self._worker, daemon=True).start()
        threading.Thread(target=self._loop, daemon=True).start()

    def stop(self):
        self.running = False
        for _ in range(3):
            self.queue.put(None)

    def _worker(self):
        while True:
            trade = self.queue.get()
            if trade is None:
                break
            try:
                self._analyze_trade(trade)
            except Exception as e:
                logging.error(f"Worker Error for {trade.get('symbol', 'UNKNOWN')}:\n{traceback.format_exc()}")
            finally:
                with self.queue_lock:
                    self.queued_trades.discard(trade.get('id'))
                self.queue.task_done()

    def _loop(self):
        while self.running:
            try:
                open_trades = self.db.get_open_trades()
                for trade in open_trades:
                    trade_id = trade.get('id')
                    with self.queue_lock:
                        if trade_id in self.queued_trades:
                            continue
                        self.queued_trades.add(trade_id)
                    self.queue.put(trade)
            except Exception as e:
                logging.error(f"Advisor loop error: {e}")
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

    def _calculate_advisory_score(self, data: Dict) -> float:
        """
        درجة تقييم الصفقة (للاستشارة فقط — لا تؤدي لتنفيذ)
        """
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

        dist_sp = data.get('distance_to_sp_pct', 100.0)
        atr_pct = data.get('atr_pct', 1.0)
        if dist_sp < atr_pct * 0.5:
            score -= 20
        elif dist_sp < atr_pct:
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

    def _calculate_profit_loss(self, entry_price: float, sp_price: float, tp_price: float,
                                 side: str, qty: float, leverage: int) -> Dict:
        """
        حساب الربح المتوقع إذا تحقق TP والخسارة المتوقعة إذا ضرب SP
        """
        position_size = entry_price * qty  # حجم المركز بالدولار

        if side == 'LONG':
            # ربح إذا وصل السعر لـ TP
            profit_usdt = (tp_price - entry_price) * qty
            # خسارة إذا وصل السعر لـ SP
            loss_usdt = (entry_price - sp_price) * qty
        else:  # SHORT
            # ربح إذا وصل السعر لـ TP (الذي هو أقل من الدخول)
            profit_usdt = (entry_price - tp_price) * qty
            # خسارة إذا وصل السعر لـ SP (الذي هو أعلى من الدخول)
            loss_usdt = (sp_price - entry_price) * qty

        # مع الرافعة المالية
        profit_usdt_leveraged = profit_usdt * leverage
        loss_usdt_leveraged = loss_usdt * leverage

        # نسبة الربح والخسارة من حجم المركز
        profit_pct = (profit_usdt / max(position_size, 1e-12)) * 100 * leverage
        loss_pct = (loss_usdt / max(position_size, 1e-12)) * 100 * leverage

        return {
            'profit_if_tp': profit_usdt_leveraged,
            'loss_if_sp': loss_usdt_leveraged,
            'profit_if_tp_pct': profit_pct,
            'loss_if_sp_pct': loss_pct,
            'position_size_usdt': position_size,
        }

    def _analyze_trade(self, trade):
        trade_id = trade.get('id')
        symbol = trade.get('symbol', '')
        entry_price = float(trade.get('entry_price') or 0)
        side = trade.get('side', '')
        qty = float(trade.get('qty') or 0)
        leverage = int(trade.get('leverage_used') or 1)

        # ─── جلب بيانات السوق ───
        market_data = self.market_cache.get(symbol, {}).get('data', {})
        if not market_data or time.time() - self.market_cache.get(symbol, {}).get('time', 0) > 300:
            market_data = self.analytics.analyze_market(symbol)
            self.market_cache[symbol] = {'data': market_data, 'time': time.time()}

        if 'error' in market_data:
            return

        current_price = market_data.get('price', entry_price)
        ohlcv = market_data.get('ohlcv', [])

        # ─── حساب ATR بنفس طريقة QUANTUM APEX v4.0 ───
        atr = self.analytics.compute_atr(ohlcv, ATR_PERIOD)
        atr = max(atr, entry_price * ATR_SAFETY_FLOOR_PCT)  # حد أدنى للسلامة

        # ─── حساب SP و TP بنفس خوارزمية QUANTUM APEX v4.0 ───
        if side == 'LONG':
            sp_price = entry_price - (atr * STOP_LOSS_ATR_MULTIPLE)
            tp_price = entry_price + (atr * TAKE_PROFIT_ATR_MULTIPLE)
        else:
            sp_price = entry_price + (atr * STOP_LOSS_ATR_MULTIPLE)
            tp_price = entry_price - (atr * TAKE_PROFIT_ATR_MULTIPLE)

        # ─── حساب الربح والخسارة المتوقع ───
        pnl_calc = self._calculate_profit_loss(
            entry_price, sp_price, tp_price, side, qty, leverage
        )

        # ─── حساب الربح الحالي ───
        if entry_price > 0:
            if side == 'LONG':
                profit_pct = ((current_price - entry_price) / entry_price) * 100
            else:
                profit_pct = ((entry_price - current_price) / entry_price) * 100
        else:
            profit_pct = 0.0

        # ─── مدة الصفقة ───
        try:
            opened_at = datetime.fromisoformat(trade.get('timestamp', '').replace("Z", "+00:00"))
            time_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 60
        except:
            time_open = 0

        # ─── مسافات SP و TP ───
        if sp_price > 0 and tp_price > 0 and entry_price > 0:
            if side == 'LONG':
                dist_tp_pct = ((tp_price - current_price) / current_price) * 100
                dist_sp_pct = ((current_price - sp_price) / current_price) * 100
                rr_ratio = abs(tp_price - entry_price) / abs(entry_price - sp_price) if abs(entry_price - sp_price) > 0 else 0
                target_progress = (current_price - entry_price) / (tp_price - entry_price) * 100 if tp_price != entry_price else 0
            else:
                dist_tp_pct = ((current_price - tp_price) / current_price) * 100
                dist_sp_pct = ((sp_price - current_price) / current_price) * 100
                rr_ratio = abs(entry_price - tp_price) / abs(sp_price - entry_price) if abs(sp_price - entry_price) > 0 else 0
                target_progress = (entry_price - current_price) / (entry_price - tp_price) * 100 if entry_price != tp_price else 0
            target_progress = max(0, min(100, target_progress))
        else:
            dist_tp_pct = 0.0
            dist_sp_pct = 0.0
            rr_ratio = 0.0
            target_progress = 0.0

        # ─── مؤشرات السوق ───
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
            'side': side, 'adx': adx, 'rsi': rsi,
            'plus_di': plus_di, 'minus_di': minus_di
        })

        # ─── إعداد بيانات AI ───
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
            'sp_price': sp_price,
            'tp_price': tp_price,
            'distance_to_tp_pct': max(0, dist_tp_pct),
            'distance_to_sp_pct': max(0, dist_sp_pct),
            'risk_reward': rr_ratio,
            'profit_if_tp': pnl_calc['profit_if_tp'],
            'loss_if_sp': pnl_calc['loss_if_sp'],
            'profit_if_tp_pct': pnl_calc['profit_if_tp_pct'],
            'loss_if_sp_pct': pnl_calc['loss_if_sp_pct'],
        }

        # ─── استدعاء AI (مع التحكم في المعدل) ───
        now = time.time()
        last_ai = self.last_ai_time.get(trade_id, 0)
        use_cached = (now - last_ai) < AI_DECISION_INTERVAL

        if use_cached:
            cached = self.db.get_latest_analysis(trade_id)
            if cached:
                ai_recommendation = cached.get('ai_decision', 'HOLD')
                ai_confidence = cached.get('ai_confidence', 50)
                ai_explanation = cached.get('ai_explanation', '')
                tp_probability = cached.get('probability_tp', 0)
                sl_probability = cached.get('probability_sl', 0)
                sideways_probability = cached.get('probability_sideways', 0)
                reversal_probability = cached.get('probability_reversal', 0)
            else:
                use_cached = False

        if not use_cached:
            with self.ai_lock:
                logging.info(f"🔄 Calling AI for {symbol} (last AI was {now - last_ai:.0f}s ago)")
                ai_result = self.ai.get_advice(current_data)
                self.last_ai_time[trade_id] = now
                ai_recommendation = ai_result.get('recommendation', 'HOLD')
                ai_confidence = ai_result.get('confidence', 0)
                ai_explanation = ai_result.get('reason', '')
                tp_probability = ai_result.get('tp_probability', 0)
                sl_probability = ai_result.get('sl_probability', 0)
                sideways_probability = ai_result.get('sideways_probability', 0)
                reversal_probability = ai_result.get('reversal_probability', 0)

        # ─── حساب درجة التقييم ───
        advisory_score = self._calculate_advisory_score({
            'profit_pct': profit_pct,
            'adx': adx,
            'atr_pct': atr_pct,
            'distance_to_sp_pct': dist_sp_pct,
            'distance_to_tp_pct': dist_tp_pct,
            'trend_alignment': trend_alignment,
            'reversal_risk': reversal_risk,
        })

        # ─── تسجيل التحليل في قاعدة البيانات ───
        analysis_record = {
            'trade_id': trade_id,
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'current_price': current_price,
            'sp_price': sp_price,
            'tp_price': tp_price,
            'profit_if_tp': pnl_calc['profit_if_tp'],
            'loss_if_sp': pnl_calc['loss_if_sp'],
            'profit_if_tp_pct': pnl_calc['profit_if_tp_pct'],
            'loss_if_sp_pct': pnl_calc['loss_if_sp_pct'],
            'position_size_usdt': pnl_calc['position_size_usdt'],
            'leverage': leverage,
            'profit_pct': profit_pct,
            'time_open_minutes': int(time_open),
            'target_progress': target_progress,
            'trend_strength': adx,
            'momentum_score': rsi,
            'funding_rate': funding,
            'market_regime': regime,
            'atr_value': atr,
            'atr_pct': atr_pct,
            'ai_decision': ai_recommendation,
            'ai_confidence': ai_confidence,
            'ai_explanation': ai_explanation,
            'recommendation': ai_recommendation,
            'probability_tp': tp_probability,
            'probability_sl': sl_probability,
            'probability_sideways': sideways_probability,
            'probability_reversal': reversal_probability,
            'risk_reward_ratio': rr_ratio,
            'trend_alignment': trend_alignment,
            'reversal_risk': reversal_risk,
            'management_score': advisory_score,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        self.db.save_kaf_analysis(analysis_record)

        logging.info(
            f"✅ KAF Analysis | {symbol} | SP={sp_price:.4f} | TP={tp_price:.4f} | "
            f"Profit_IF_TP=+{pnl_calc['profit_if_tp']:.2f}USDT | Loss_IF_SP=-{pnl_calc['loss_if_sp']:.2f}USDT | "
            f"AI={ai_recommendation} ({ai_confidence:.0f}%) | Score={advisory_score:.1f}"
        )

# =============================================================================
# 🚀 MAIN
# =============================================================================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    logging.info("🚀 Starting APEX KAF ADVISOR BOT v5.0 (Advisory Only — No Execution)")
    logging.info(f"📌 SP: ATR × {STOP_LOSS_ATR_MULTIPLE} | TP: ATR × {TAKE_PROFIT_ATR_MULTIPLE} (from QUANTUM APEX v4.0)")

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
        logging.info(f"✅ AI TEST SUCCESS: {test_content}")
    except Exception as e:
        logging.error(f"❌ AI TEST FAILED: {type(e).__name__}: {e}")
        logging.warning("⚠️ AI test failed, but bot will continue.")

    # بدء حلقة المراقبة الاستشارية
    advisor = AdvisorLoop(db, analytics, ai)
    advisor.start()

    # بدء بوت تيليجرام
    telegram_bot = TelegramBot(TELEGRAM_TOKEN, ADMIN_CHAT_ID, db, analytics)
    telegram_bot.run()

if __name__ == "__main__":
    main()
