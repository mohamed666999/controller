#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║     APEX MONITOR BOT — Telegram AI Monitor v4.3            ║
║  Architecture: Single AI Model (GPT-OSS-120B)              ║
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
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Any
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

# 🔴 استخدم متغيرات البيئة في الإنتاج، لا تضع المفاتيح هنا
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8122906116:AAHAWsXfaiymnvdeNO0BURyRVccJU8_gIco")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "6033203084"))

APEX_API_URL = os.getenv("APEX_API_URL", "https://binancetrading-production.up.railway.app")
MONITOR_DB_PATH = os.getenv("MONITOR_DB_PATH", "monitor.db")

# 🔴 المفتاح الوحيد المطلوب هو NVIDIA_API_KEY_OSS
NVIDIA_API_KEY_OSS = os.getenv("NVIDIA_API_KEY_OSS", "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")
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"

MONITOR_INTERVAL = 60
AI_MODEL = "openai/gpt-oss-120b"  # النموذج الوحيد المستخدم

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
        with self.lock:
            try:
                # 🔴 تم تصحيح عدد الـ ? إلى 24 (بعد إزالة عمود id)
                self.monitor_conn.execute("""
                    INSERT INTO open_analysis (
                        trade_id, symbol, side, entry_price, current_price,
                        profit_pct, time_open_minutes, target_progress,
                        trend_strength, momentum_score, funding_rate,
                        oi_change_1h, oi_trend, apex_score, iss_score,
                        ai_decision, ai_confidence, ai_explanation,
                        recommendation, probability_tp, probability_sl,
                        probability_sideways, probability_reversal, timestamp
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    data.get('trade_id'), data.get('symbol'), data.get('side'),
                    data.get('entry_price'), data.get('current_price'), data.get('profit_pct'),
                    data.get('time_open_minutes'), data.get('target_progress'),
                    data.get('trend_strength'), data.get('momentum_score'), data.get('funding_rate'),
                    data.get('oi_change_1h'), data.get('oi_trend'), data.get('apex_score'),
                    data.get('iss_score'), data.get('ai_decision'), data.get('ai_confidence'),
                    data.get('ai_explanation'), data.get('recommendation'),
                    data.get('probability_tp', 0), data.get('probability_sl', 0),
                    data.get('probability_sideways', 0), data.get('probability_reversal', 0),
                    data.get('timestamp')
                ))
                self.monitor_conn.commit()
            except Exception as e:
                logging.error(f"DB Save Error: {e}\n{traceback.format_exc()}")

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

    def get_trade_by_id(self, trade_id: int) -> Dict:
        """جلب تفاصيل الصفقة من قاعدة البيانات (في حال الحاجة لتحليل فوري)"""
        trades = self.get_open_trades()
        for t in trades:
            if t.get('id') == trade_id:
                return t
        return {}

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
            orderbook = self.exchange.fetch_order_book(symbol, limit=50)
            return {
                'price': ticker['last'],
                'ohlcv': ohlcv,
                'orderbook': orderbook,
            }
        except Exception as e:
            return {'error': str(e)}

    def trend_strength(self, ohlcv: List) -> float:
        if not ohlcv or len(ohlcv) < 50: return 0.0
        closes = [c[4] for c in ohlcv]
        sma20 = np.mean(closes[-20:])
        sma50 = np.mean(closes[-50:])
        trend = abs(sma20 - sma50) / sma50
        return min(trend * 70 * 100, 100)

    def momentum_score(self, ohlcv: List) -> float:
        if not ohlcv or len(ohlcv) < 30: return 50.0
        closes = [c[4] for c in ohlcv]
        if len(closes) < 14: return 50.0
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_loss = np.mean(losses[-14:])
        if avg_loss == 0: return 100.0
        rs = np.mean(gains[-14:]) / avg_loss
        return 100 - (100 / (1 + rs))

    def funding_rate(self, symbol: str) -> float:
        try:
            s = symbol.split('/')[0]
            url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={s}USDT"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return float(resp.json().get('lastFundingRate', 0))
        except: pass
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
        except: pass
        return 0.0

    def detect_market_regime(self, ohlcv: List) -> str:
        if not ohlcv or len(ohlcv) < 50: return "UNKNOWN"
        closes = [c[4] for c in ohlcv]
        sma20 = np.mean(closes[-20:])
        sma50 = np.mean(closes[-50:])
        if abs(sma20 - sma50) / sma50 > 0.02:
            return "TRENDING_UP" if sma20 > sma50 else "TRENDING_DOWN"
        return "RANGING"

    def analyze_market(self, symbol: str) -> Dict:
        market = self.fetch_market_data(symbol)
        if 'error' in market: return {'error': market['error']}
        
        ohlcv = market.get('ohlcv', [])
        return {
            'symbol': symbol,
            'price': market.get('price', 0),
            'market_structure': self.detect_market_regime(ohlcv),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

# =============================================================================
# 🤖 AI CLIENT (GPT-OSS-120B فقط)
# =============================================================================

class AIClient:
    def __init__(self):
        self.client = OpenAI(
            base_url=NVIDIA_BASE_URL,
            api_key=NVIDIA_API_KEY_OSS
        )

    def get_recommendation(self, trade_data: Dict) -> Dict:
        prompt = f"""أنت محلل تداول كمي. حلل الصفقة التالية بناءً على البيانات الحالية فقط.

العملة: {trade_data.get('symbol')}
الاتجاه: {trade_data.get('side', 'UNKNOWN')}
سعر الدخول: {trade_data.get('entry_price', 0)}
السعر الحالي: {trade_data.get('current_price', 0)}
هدف الربح (TP): {trade_data.get('tp_price', 0)}
وقف الخسارة (SL): {trade_data.get('sl_price', 0)}
الربح الحالي: {trade_data.get('profit_pct', 0):.2f}%
نسبة تحقيق الهدف: {trade_data.get('target_progress', 0):.1f}%
قوة الاتجاه: {trade_data.get('trend_strength', 0):.1f}
الزخم / RSI التقريبي: {trade_data.get('momentum_score', 50):.1f}
Funding Rate: {trade_data.get('funding_rate', 0):.6f}
تغير Open Interest خلال ساعة: {trade_data.get('oi_change_1h', 0):.4f}
حالة السوق: {trade_data.get('market_regime', 'UNKNOWN')}

أعطني JSON فقط، بدون Markdown وبدون أي نص خارجه:
{{
    "tp_probability": 0,
    "sl_probability": 0,
    "sideways_probability": 0,
    "reversal_probability": 0,
    "recommendation": "HOLD",
    "confidence": 0,
    "reason": "..."
}}

قواعد:
- مجموع الاحتمالات يجب أن يكون 100.
- recommendation يجب أن تكون واحدة من: BUY, SELL, HOLD, EXIT
- لا تخترع بيانات غير موجودة. إذا كانت البيانات غير كافية، استخدم HOLD وثقة منخفضة.
"""
        try:
            logging.info(f"🤖 Calling GPT-OSS-120B for {trade_data.get('symbol')}")
            start_time = time.time()
            
            response = self.client.chat.completions.create(
                model=AI_MODEL,
                messages=[
                    {"role": "system", "content": "You are a quantitative trading analysis assistant. Return valid JSON only."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=500,
                timeout=30.0
            )
            elapsed = time.time() - start_time
            logging.info(f"✅ GPT-OSS responded in {elapsed:.2f}s")

            message = response.choices[0].message
            reasoning = getattr(message, "reasoning_content", None)
            if reasoning:
                logging.info(f"🧠 GPT-OSS reasoning: {str(reasoning)[:200]}")

            raw = (message.content or "").strip()
            if raw.startswith("```"):
                lines = raw.splitlines()
                raw = "\n".join(line for line in lines if not line.strip().startswith("```")).strip()

            result = json.loads(raw)

            # التحقق من القيم
            tp = float(result.get("tp_probability", 0))
            sl = float(result.get("sl_probability", 0))
            sideways = float(result.get("sideways_probability", 0))
            reversal = float(result.get("reversal_probability", 0))
            total = tp + sl + sideways + reversal
            if total <= 0:
                raise ValueError("AI returned zero probabilities")

            # تطبيع الاحتمالات إلى 100
            result["tp_probability"] = tp / total * 100
            result["sl_probability"] = sl / total * 100
            result["sideways_probability"] = sideways / total * 100
            result["reversal_probability"] = reversal / total * 100
            result["recommendation"] = str(result.get("recommendation", "HOLD")).upper()
            result["confidence"] = float(result.get("confidence", 50))
            result["reason"] = str(result.get("reason", ""))

            logging.info(f"🎯 GPT-OSS result {trade_data.get('symbol')} | {result['recommendation']} | confidence={result['confidence']:.0f}%")
            return result

        except Exception as e:
            logging.error(f"❌ GPT-OSS ERROR for {trade_data.get('symbol')}: {type(e).__name__}: {e}")
            return {
                "tp_probability": 50,
                "sl_probability": 25,
                "sideways_probability": 15,
                "reversal_probability": 10,
                "recommendation": "HOLD",
                "confidence": 0,
                "reason": f"GPT-OSS unavailable: {type(e).__name__}"
            }

# =============================================================================
# 📡 TELEGRAM BOT
# =============================================================================

class TelegramBot:
    def __init__(self, token, admin_chat_id, monitor_db, analytics_engine, ai_client):
        self.token = token
        self.admin_chat_id = admin_chat_id
        self.db = monitor_db
        self.analytics = analytics_engine
        self.ai = ai_client

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "👋 مرحباً! أنا بوت مراقبة APEX v4.3.\n"
            "الأوامر:\n"
            "/positions - عرض الصفقات المفتوحة\n"
            "/advice <id> - تحليل فوري للصفقة (معرفها من /positions)\n"
            "/market <symbol> - تحليل سريع لعملة\n"
            "/statistics - إحصائيات الأداء"
        )

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
            ai_exp = str(t.get('ai_explanation', '')).replace('<', '').replace('>', '')
            direction_emoji = '🟢 LONG' if side == 'LONG' else '🔴 SHORT'

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
                msg += f"القرار الآن: <b>{rec}</b> (ثقة: {analysis.get('ai_confidence', 0):.0f}%)\n"
            else:
                msg += f"\n⏳ <i>جاري جمع بيانات الحيتان والسيولة للمتابعة الحية...</i>\n"

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

        # 1. حاول جلب التحليل المخزن
        analysis = self.db.get_latest_open_analysis(trade_id)
        if analysis:
            # يوجد تحليل مخزن، اعرضه فوراً
            msg = self._format_analysis_response(trade_id, analysis)
            await update.message.reply_text(msg, parse_mode='HTML')
            return

        # 2. لا يوجد تحليل مخزن، قم بتحليل فوري
        await update.message.reply_text("⏳ جاري تحليل الصفقة فوراً...")
        trade = self.db.get_trade_by_id(trade_id)
        if not trade:
            await update.message.reply_text("⚠️ لم يتم العثور على الصفقة.")
            return

        # تحليل فوري
        try:
            # جمع بيانات السوق
            symbol = trade.get('symbol')
            market_data = self.analytics.fetch_market_data(symbol)
            if 'error' in market_data:
                await update.message.reply_text(f"⚠️ خطأ في جلب بيانات السوق: {market_data['error']}")
                return

            # بناء بيانات trade_data مشابهة لما يفعله MonitorLoop
            entry_price = float(trade.get('entry_price') or 0)
            side = trade.get('side', '')
            current_price = market_data.get('price', entry_price)
            profit_pct = 0.0
            if entry_price > 0:
                if side == 'LONG':
                    profit_pct = (current_price - entry_price) / entry_price * 100
                else:
                    profit_pct = (entry_price - current_price) / entry_price * 100

            tp_price = float(trade.get('tp_price') or 0)
            sl_price = float(trade.get('sl_price') or 0)
            target_progress = 0.0
            if tp_price > 0 and entry_price > 0 and (tp_price - entry_price) != 0:
                if side == 'LONG':
                    target_progress = (current_price - entry_price) / (tp_price - entry_price) * 100
                else:
                    target_progress = (entry_price - current_price) / (entry_price - tp_price) * 100
                target_progress = max(0, min(100, target_progress))

            ohlcv = market_data.get('ohlcv', [])
            current_data = {
                'symbol': symbol,
                'side': side,
                'entry_price': entry_price,
                'current_price': current_price,
                'tp_price': tp_price,
                'sl_price': sl_price,
                'profit_pct': profit_pct,
                'target_progress': target_progress,
                'trend_strength': self.analytics.trend_strength(ohlcv),
                'momentum_score': self.analytics.momentum_score(ohlcv),
                'funding_rate': self.analytics.funding_rate(symbol),
                'oi_change_1h': self.analytics.oi_change_1h(symbol),
                'market_regime': self.analytics.detect_market_regime(ohlcv)
            }

            # استدعاء AI
            ai_result = self.ai.get_recommendation(current_data)

            # حفظ التحليل في قاعدة البيانات
            analysis_record = {
                'trade_id': trade_id,
                'symbol': symbol,
                'side': side,
                'entry_price': entry_price,
                'current_price': current_price,
                'profit_pct': profit_pct,
                'time_open_minutes': 0,  # غير معروف هنا
                'target_progress': target_progress,
                'trend_strength': current_data['trend_strength'],
                'momentum_score': current_data['momentum_score'],
                'funding_rate': current_data['funding_rate'],
                'oi_change_1h': current_data['oi_change_1h'],
                'oi_trend': 0.0,
                'apex_score': float(trade.get('confidence', 50)),
                'iss_score': 50,
                'ai_decision': ai_result.get('recommendation', 'HOLD'),
                'ai_confidence': ai_result.get('confidence', 0),
                'ai_explanation': ai_result.get('reason', ''),
                'recommendation': ai_result.get('recommendation', 'HOLD'),
                # 🔴 تصحيح أسماء الحقول لتطابق ما يخرجه الـ AI
                'probability_tp': ai_result.get('tp_probability', 0),
                'probability_sl': ai_result.get('sl_probability', 0),
                'probability_sideways': ai_result.get('sideways_probability', 0),
                'probability_reversal': ai_result.get('reversal_probability', 0),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            self.db.save_open_analysis(analysis_record)

            # عرض النتيجة
            analysis = self.db.get_latest_open_analysis(trade_id)
            if analysis:
                msg = self._format_analysis_response(trade_id, analysis)
            else:
                msg = "⚠️ تعذر حفظ التحليل، حاول مرة أخرى."
            await update.message.reply_text(msg, parse_mode='HTML')

        except Exception as e:
            logging.error(f"Advice error: {e}")
            await update.message.reply_text(f"⚠️ حدث خطأ أثناء التحليل: {e}")

    def _format_analysis_response(self, trade_id, analysis):
        msg = f"🔍 <b>توصية الصفقة #{trade_id}</b>\n"
        msg += f"العملة: {analysis.get('symbol', 'UNKNOWN')}\n"
        msg += f"الربح الحالي: {analysis.get('profit_pct', 0):+.2f}%\n"
        msg += f"نسبة تحقيق الهدف: {analysis.get('target_progress', 0):.1f}%\n"
        msg += f"\n🎯 <b>الاحتمالات</b>\n"
        msg += f"  TP: {analysis.get('probability_tp', 0):.0f}%\n"
        msg += f"  SL: {analysis.get('probability_sl', 0):.0f}%\n"
        msg += f"  جانبي: {analysis.get('probability_sideways', 0):.0f}%\n"
        msg += f"  انعكاس: {analysis.get('probability_reversal', 0):.0f}%\n"
        msg += f"\n🤖 توصية الذكاء الاصطناعي: <b>{analysis.get('recommendation', 'HOLD')}</b>\n"
        msg += f"الثقة: {analysis.get('ai_confidence', 0):.0f}%\n"
        msg += f"السبب: <i>{analysis.get('ai_explanation', '')}</i>"
        return msg

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

    async def statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        stats = self.db.get_statistics()
        msg = "📈 <b>إحصائيات الأداء</b>\n\n"
        msg += f"إجمالي الربح (المغلقة): {stats.get('total_pnl', 0):.2f} USDT\n"
        await update.message.reply_text(msg, parse_mode='HTML')

    def run(self):
        self.app = Application.builder().token(self.token).build()
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("positions", self.positions))
        self.app.add_handler(CommandHandler("advice", self.advice))
        self.app.add_handler(CommandHandler("market", self.market))
        self.app.add_handler(CommandHandler("statistics", self.statistics))
        self.app.run_polling()

# =============================================================================
# 🔁 MONITOR LOOP
# =============================================================================

class MonitorLoop:
    def __init__(self, db: MonitorDB, analytics: AdvancedAnalyticsEngine, ai: AIClient):
        self.db = db
        self.analytics = analytics
        self.ai = ai
        self.running = True
        self.queue = Queue()
        self.market_cache = {}

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

    def _analyze_trade(self, trade):
        trade_id = trade.get('id')
        symbol = trade.get('symbol', '')
        entry_price = float(trade.get('entry_price') or 0)
        side = trade.get('side', '')
        tp_price = float(trade.get('tp_price') or 0)
        sl_price = float(trade.get('sl_price') or 0)

        # جلب بيانات السوق
        market_data = self.market_cache.get(symbol, {}).get('data', {})
        if not market_data or time.time() - self.market_cache.get(symbol, {}).get('time', 0) > 300:
            market_data = self.analytics.analyze_market(symbol)
            self.market_cache[symbol] = {'data': market_data, 'time': time.time()}

        if 'error' in market_data:
            logging.error(f"⛔ Skipping AI for {symbol}: {market_data['error']}")
            return

        current_price = market_data.get('price', entry_price)
        if entry_price > 0:
            if side == 'LONG':
                profit_pct = (current_price - entry_price) / entry_price * 100
            else:
                profit_pct = (entry_price - current_price) / entry_price * 100
        else:
            profit_pct = 0.0

        try:
            opened_at = datetime.fromisoformat(trade.get('timestamp', '').replace("Z", "+00:00"))
            time_open = (datetime.now(timezone.utc) - opened_at).total_seconds() / 60
        except:
            time_open = 0

        target_progress = 0.0
        if tp_price > 0 and entry_price > 0 and (tp_price - entry_price) != 0:
            if side == 'LONG':
                target_progress = (current_price - entry_price) / (tp_price - entry_price) * 100
            else:
                target_progress = (entry_price - current_price) / (entry_price - tp_price) * 100
            target_progress = max(0, min(100, target_progress))

        market_regime = market_data.get('market_structure', 'UNKNOWN')
        ohlcv = market_data.get('ohlcv', [])

        current_data = {
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'current_price': current_price,
            'tp_price': tp_price,
            'sl_price': sl_price,
            'profit_pct': profit_pct,
            'time_open_minutes': int(time_open),
            'target_progress': target_progress,
            'trend_strength': self.analytics.trend_strength(ohlcv),
            'momentum_score': self.analytics.momentum_score(ohlcv),
            'funding_rate': self.analytics.funding_rate(symbol),
            'oi_change_1h': self.analytics.oi_change_1h(symbol),
            'oi_trend': 0.0,
            'apex_score': float(trade.get('confidence', 50)),
            'iss_score': 50,
            'market_regime': market_regime
        }

        ai_result = self.ai.get_recommendation(current_data)

        analysis_record = {
            'trade_id': trade_id,
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'current_price': current_price,
            'profit_pct': profit_pct,
            'time_open_minutes': int(time_open),
            'target_progress': target_progress,
            'trend_strength': current_data['trend_strength'],
            'momentum_score': current_data['momentum_score'],
            'funding_rate': current_data['funding_rate'],
            'oi_change_1h': current_data['oi_change_1h'],
            'oi_trend': 0.0,
            'apex_score': current_data['apex_score'],
            'iss_score': 50,
            'ai_decision': ai_result.get('recommendation', 'HOLD'),
            'ai_confidence': ai_result.get('confidence', 0),
            'ai_explanation': ai_result.get('reason', ''),
            'recommendation': ai_result.get('recommendation', 'HOLD'),
            # 🔴 تصحيح أسماء الحقول لتطابق ما يخرجه الـ AI
            'probability_tp': ai_result.get('tp_probability', 0),
            'probability_sl': ai_result.get('sl_probability', 0),
            'probability_sideways': ai_result.get('sideways_probability', 0),
            'probability_reversal': ai_result.get('reversal_probability', 0),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        self.db.save_open_analysis(analysis_record)
        logging.info(f"✅ Analysis Saved for {symbol}")

# =============================================================================
# 🚀 MAIN
# =============================================================================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    logging.info("🚀 Starting APEX Monitor Bot v4.3 (Single AI Model)")

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

    # ===================================================================
    # 🧪 اختبار GPT-OSS-120B قبل بدء التشغيل الكامل
    # ===================================================================
    try:
        logging.info("🧪 Testing GPT-OSS-120B connection...")
        test_response = ai.client.chat.completions.create(
            model=AI_MODEL,
            messages=[
                {"role": "user", "content": "Reply with exactly: GPT-OSS-OK"}
            ],
            temperature=0,
            max_tokens=20,
            timeout=10.0
        )
        test_content = test_response.choices[0].message.content.strip()
        logging.info(f"✅ GPT TEST SUCCESS: {test_content}")
        if "GPT-OSS-OK" not in test_content:
            logging.warning(f"⚠️ Unexpected test response: {test_content}")
    except Exception as e:
        logging.error(f"❌ GPT TEST FAILED: {type(e).__name__}: {e}")
        # نستمر في التشغيل ولكن مع تحذير، قد يعمل لاحقاً
        logging.warning("⚠️ GPT-OSS-120B test failed, but bot will continue. Advice may show fallback values.")

    monitor = MonitorLoop(db, analytics, ai)
    monitor.start()

    telegram_bot = TelegramBot(TELEGRAM_TOKEN, ADMIN_CHAT_ID, db, analytics, ai)
    telegram_bot.run()

if __name__ == "__main__":
    main()