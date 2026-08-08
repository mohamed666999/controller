#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║     APEX MONITOR BOT — Telegram AI Monitor v4.5            ║
║  Architecture: Institutional Position Manager (AI)         ║
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

# 🔴 استخدم متغيرات البيئة في الإنتاج
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8122906116:AAHAWsXfaiymnvdeNO0BURyRVccJU8_gIco")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "6033203084"))

APEX_API_URL = os.getenv("APEX_API_URL", "https://binancetrading-production.up.railway.app")
MONITOR_DB_PATH = os.getenv("MONITOR_DB_PATH", "monitor.db")

# 🔴 المفتاح الجديد ونموذج poolside/laguna-xs-2.1
NVIDIA_API_KEY = os.getenv("NVIDIA_API_KEY", "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")
NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"

MONITOR_INTERVAL = 60
AI_MODEL = "poolside/laguna-xs-2.1"  # النموذج الوحيد

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
                self.monitor_conn.execute("""
                    INSERT INTO open_analysis (
                        trade_id, symbol, side, entry_price, current_price,
                        profit_pct, time_open_minutes, target_progress,
                        trend_strength, momentum_score, funding_rate,
                        oi_change_1h, oi_trend, apex_score, iss_score,
                        ai_decision, ai_confidence, ai_explanation,
                        recommendation, probability_tp, probability_sl,
                        probability_sideways, probability_reversal, timestamp
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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

    # 🔴 حساب الـ ADX الحقيقي
    def trend_strength_adx(self, ohlcv: List) -> float:
        if not ohlcv or len(ohlcv) < 30:
            return 0.0
        highs = [c[2] for c in ohlcv]
        lows = [c[3] for c in ohlcv]
        closes = [c[4] for c in ohlcv]
        period = 14
        plus_dms, minus_dms, trs = [], [], []
        for i in range(1, len(closes)):
            up_move = highs[i] - highs[i - 1]
            down_move = lows[i - 1] - lows[i]
            plus_dms.append(up_move if up_move > down_move and up_move > 0 else 0)
            minus_dms.append(down_move if down_move > up_move and down_move > 0 else 0)
            trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1])))
        atr_val = np.mean(trs[-period:]) or 1e-12
        plus_di = 100 * np.mean(plus_dms[-period:]) / atr_val
        minus_di = 100 * np.mean(minus_dms[-period:]) / atr_val
        di_sum = plus_di + minus_di
        dx = 100 * abs(plus_di - minus_di) / di_sum if di_sum > 0 else 0
        return dx

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
        closes = [c[4] for c in ohlcv]
        sma20 = np.mean(closes[-20:])
        sma50 = np.mean(closes[-50:])
        if abs(sma20 - sma50) / sma50 > 0.015:
            return "TRENDING_UP" if sma20 > sma50 else "TRENDING_DOWN"
        return "RANGING"

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
        # بناء السياق لـ TP/SL
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

            # توحيد المفاتيح
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
            "👋 مرحباً! أنا بوت مراقبة APEX v4.5 (AI Position Manager).\n"
            "الأوامر:\n"
            "/positions - الصفقات المفتوحة مع قرارات الإدارة\n"
            "/advice <id> - تحليل مفصل لصفقة محددة\n"
            "/market <symbol> - تحليل سريع للسوق"
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
        self.app.add_handler(CommandHandler("positions", self.positions))
        self.app.add_handler(CommandHandler("advice", self.advice))
        self.app.add_handler(CommandHandler("market", self.market))
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

        # حسابات المسافات
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

        current_data = {
            'symbol': symbol,
            'side': side,
            'entry_price': entry_price,
            'current_price': current_price,
            'profit_pct': profit_pct,
            'time_open_minutes': int(time_open),
            'target_progress': target_progress,
            'trend_strength_adx': self.analytics.trend_strength_adx(ohlcv),
            'rsi_14': self.analytics.real_rsi(ohlcv),
            'atr_pct': self.analytics.get_atr_pct(ohlcv),
            'funding_rate': self.analytics.funding_rate(symbol),
            'market_regime': market_data.get('market_structure', 'UNKNOWN'),
            'tp_price': tp_price,
            'sl_price': sl_price,
            'distance_to_tp_pct': max(0, dist_tp_pct),
            'distance_to_sl_pct': max(0, dist_sl_pct),
            'risk_reward': rr_ratio,
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
            'trend_strength': current_data['trend_strength_adx'],
            'momentum_score': current_data['rsi_14'],
            'funding_rate': current_data['funding_rate'],
            'oi_change_1h': 0.0,
            'oi_trend': 0.0,
            'apex_score': float(trade.get('confidence', 50)),
            'iss_score': 50,
            'ai_decision': ai_result.get('recommendation', 'HOLD'),
            'ai_confidence': ai_result.get('confidence', 0),
            'ai_explanation': ai_result.get('reason', ''),
            'recommendation': ai_result.get('recommendation', 'HOLD'),
            'probability_tp': ai_result.get('tp_probability', 0),
            'probability_sl': ai_result.get('sl_probability', 0),
            'probability_sideways': ai_result.get('sideways_probability', 0),
            'probability_reversal': ai_result.get('reversal_probability', 0),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

        self.db.save_open_analysis(analysis_record)
        logging.info(f"✅ Position Manager Analysis Saved for {symbol}")

# =============================================================================
# 🚀 MAIN
# =============================================================================

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
    logging.info("🚀 Starting APEX Position Manager Bot v4.5 (Laguna XS 2.1)")

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

    # 🧪 اختبار الاتصال بالنموذج
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

    monitor = MonitorLoop(db, analytics, ai)
    monitor.start()

    telegram_bot = TelegramBot(TELEGRAM_TOKEN, ADMIN_CHAT_ID, db, analytics)
    telegram_bot.run()

if __name__ == "__main__":
    main()