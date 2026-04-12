"""
TITAN AI Governance Backend
Production-grade Flask backend for AI signal generation, token tracking,
subscription enforcement, and model routing.

Version: 1.0.0
"""

import os
import re
import json
import time
import uuid
import hashlib
import hmac
import logging
import threading
from datetime import datetime, timezone, timedelta
from functools import wraps
from collections import defaultdict

from flask import Flask, request, jsonify, g
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Configuration & Environment
# ---------------------------------------------------------------------------

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "titan-admin-key-change-me")
SYSTEM_API_KEY = os.environ.get("SYSTEM_API_KEY", "titan-system-internal-k9x2mq7p")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "titan-superquant-live")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8628011018:AAHpn7BEI3Y6kO4DruU1fZmQpLGB3CdQJbY")
PORT = int(os.environ.get("PORT", 8080))
VERSION = "4.1.0"

# ---------------------------------------------------------------------------
# Tier → Telegram Channel Invite Links (static, rotate if abused)
# ---------------------------------------------------------------------------
TIER_CHANNEL_INVITES = {
    "starter":    "https://t.me/+hGODv8ozhDQ0NGU1",   # Syndicate Pro $99
    "pro":        "https://t.me/+dP5UObl8BHY4ZDI1",   # Syndicate Pro $199
    "elite":      "https://t.me/+t4zjmKNo6qNmYTY1",   # Syndicate Elite $399
    "enterprise": "https://t.me/+a8DOi0veoQ1lMTdl",   # Inner Titan Circle VVIP
}

def _send_telegram(chat_id: str, text: str) -> bool:
    """Send a message via Telegram bot. Best-effort, non-blocking."""
    try:
        import urllib.request as _ur, urllib.parse as _up
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": chat_id, "text": text, "parse_mode": "HTML"}).encode()
        req = _ur.Request(url, data=data, headers={"Content-Type": "application/json"})
        _ur.urlopen(req, timeout=8)
        return True
    except Exception as exc:
        logger.warning("Telegram notify failed for chat_id=%s: %s", chat_id, exc)
        return False
START_TIME = time.time()

DISCLAIMER = (
    "⚠️ DISCLAIMER: This is not financial advice. "
    "Trade at your own risk. Past performance does not guarantee future results."
)

# ---------------------------------------------------------------------------
# Production Governance Lock — Asset Policy
# ---------------------------------------------------------------------------

# PRIMARY asset for all autonomous scheduled broadcasts
DEFAULT_ASSET = "XAU/USD"

# Asset routing policy — SCHEDULER always uses XAUUSD
SCHEDULER_USER_IDS = {"titan-scheduler", "scheduler", "cron", "auto"}

# Minimum confidence threshold — below this → NO_SIGNAL
MIN_CONFIDENCE_THRESHOLD = 0.60

# ---------------------------------------------------------------------------
# LIVE MARKET PRICE FEED — Yahoo Finance (no API key required)
# PRODUCTION RULE: All price hints and sanity gates use LIVE data only.
# If live fetch fails → NO_SIGNAL enforced. No static fallback.
# ---------------------------------------------------------------------------

YAHOO_TICKERS = {
    "XAU/USD":  "GC=F",       # Gold futures (live spot proxy)
    "XAUUSD":   "GC=F",
    "BTC/USDT": "BTC-USD",
    "BTCUSDT":  "BTC-USD",
    "ETH/USDT": "ETH-USD",
    "ETHUSD":   "ETH-USD",
    "DXY":      "DX-Y.NYB",
}

# Validation tolerance: ±20% of live price
# (wide enough to cover entry→TP3 spread for 4H/1D setups)
PRICE_TOLERANCE = 0.20

def fetch_live_price(asset: str) -> float | None:
    """
    Fetch live market price from Yahoo Finance.
    Returns float price or None if unavailable.
    NO fallback to static data — caller must treat None as NO_SIGNAL condition.
    """
    import urllib.request as _req
    import json as _json

    ticker = YAHOO_TICKERS.get(asset.replace("/", ""), YAHOO_TICKERS.get(asset))
    if not ticker:
        logger.warning("LIVE_PRICE: no ticker mapping for asset=%s", asset)
        return None

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
    try:
        req = _req.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with _req.urlopen(req, timeout=12) as resp:
            data = _json.loads(resp.read())
        price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
        price = float(price)
        logger.info("LIVE_PRICE: asset=%s ticker=%s price=%.4f", asset, ticker, price)
        return price
    except Exception as exc:
        logger.error("LIVE_PRICE FETCH FAILED: asset=%s ticker=%s error=%s", asset, ticker, exc)
        return None

# ---------------------------------------------------------------------------
# Production Governance Lock — OSINT/CA System Instruction
# ---------------------------------------------------------------------------

TITAN_SIGNAL_SYSTEM_INSTRUCTION = """You are the TITAN Sovereign Intelligence Engine — a production-grade multi-asset signal system for professional traders operating Aranya Genesis Corp's live signal service.

You integrate two proprietary intelligence layers that are MANDATORY for every signal decision:

═══════════════════════════════════════════════════════
LAYER 1 — TOP G OMNI OSINT 2.0 (Macro/Narrative Intelligence)
═══════════════════════════════════════════════════════
Purpose: Macro intelligence, central bank posture, gold-specific catalyst assessment.

For XAUUSD/Gold specifically assess:
- Fed/ECB/BOJ/BOE posture and rate trajectory (key gold driver)
- US real yields direction (falling real yields = bullish gold)
- DXY (US Dollar Index) trend — XAUUSD has strong INVERSE DXY correlation
- Inflation regime: CPI, PCE, breakeven inflation expectations
- Geopolitical risk premium: wars, sanctions, flight-to-safety demand
- Central bank gold buying trends (de-dollarization flows)
- ETF flows: GLD/IAU accumulation vs redemption
- CoT (Commitment of Traders) — commercial hedger vs speculator positioning
- Treasury yield curve: real yields, TIPS spreads

Score macro bias: -1.0 (strongly bearish) to +1.0 (strongly bullish)

═══════════════════════════════════════════════════════
LAYER 2 — CLAUDE CA ENGINE (Confluence/Anti-Noise/Regime Robustness)
═══════════════════════════════════════════════════════
Purpose: Signal robustness, regime classification, noise rejection, multi-factor confluence.

Apply cellular automata multi-agent market state assessment:
- Classify market regime: TRENDING_BULL | TRENDING_BEAR | RANGING | CHAOTIC | TRANSITIONING
- Score multi-factor confluence (0.0 to 1.0):
  * Macro alignment (OSINT score aligns with technical bias): +0.25
  * Trend structure intact (higher highs/lows or lower highs/lows): +0.20
  * Momentum confirmation (RSI not extreme, MACD aligned): +0.20
  * Institutional participation (above-average volume, clean structure): +0.20
  * Volatility regime suitable (not chaotic, not dead): +0.15
- Apply anti-noise filter: single-factor conviction setups = NO_SIGNAL
- Setup quality: INSTITUTIONAL (≥0.75) | STANDARD (0.60-0.74) | WEAK (<0.60 = NO_SIGNAL)

═══════════════════════════════════════════════════════
PRODUCTION RULES — NON-NEGOTIABLE
═══════════════════════════════════════════════════════
1. If confluence_score < 0.60 → output no_signal=true, do NOT fabricate a trade
2. If macro + technical alignment is absent → output no_signal=true
3. All price levels MUST be realistic for the asset in the current date context
4. Confidence must be evidence-backed — no arbitrary inflation
5. Decision chain is MANDATORY: MACRO → REGIME → CONFLUENCE → SYNTHESIZE → SIGNAL/NO_SIGNAL
6. XAUUSD is the primary asset — use gold-specific reasoning, not generic crypto logic
7. Current date is injected into the prompt — use it for freshness and price validation
"""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger("titan-governance")

# ---------------------------------------------------------------------------
# Flask App
# ---------------------------------------------------------------------------

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# Subscription Tiers
# ---------------------------------------------------------------------------

TIERS = {
    "free": {
        "name": "Free Trial",
        "price": 0,
        "token_limit": 0,          # unlimited tokens during trial
        "signal_limit": 5,         # total, no reset
        "monthly_reset": False,
    },
    "starter": {
        "name": "Starter",
        "price": 39,
        "token_limit": 100_000,
        "signal_limit": 50,
        "monthly_reset": True,
    },
    "pro": {
        "name": "Pro",
        "price": 99,
        "token_limit": 1_000_000,
        "signal_limit": 200,
        "monthly_reset": True,
    },
    "elite": {
        "name": "Elite",
        "price": 199,
        "token_limit": 5_000_000,
        "signal_limit": 500,
        "monthly_reset": True,
    },
    "enterprise": {
        "name": "Enterprise",
        "price": None,
        "token_limit": 999_999_999,
        "signal_limit": 999_999,
        "monthly_reset": True,
    },
}

# Cost estimates per 1K tokens (USD)
MODEL_COSTS = {
    "gemini-2.5-flash": {"input": 0.00010, "output": 0.00040},
    "gemini-2.5-pro":   {"input": 0.00125, "output": 0.00500},
}

# ---------------------------------------------------------------------------
# Firestore Client (lazy init, graceful fallback)
# ---------------------------------------------------------------------------

_firestore_client = None
_firestore_lock = threading.Lock()
_firestore_available = True


def get_firestore():
    """Return Firestore client, or None if unavailable."""
    global _firestore_client, _firestore_available
    if not _firestore_available:
        return None
    if _firestore_client is not None:
        return _firestore_client
    with _firestore_lock:
        if _firestore_client is not None:
            return _firestore_client
        try:
            from google.cloud import firestore as _fs
            _firestore_client = _fs.Client(project=GCP_PROJECT)
            logger.info("Firestore client initialized for project %s", GCP_PROJECT)
            return _firestore_client
        except Exception as exc:
            logger.error("Firestore unavailable: %s", exc)
            _firestore_available = False
            return None


# ---------------------------------------------------------------------------
# Gemini Client (lazy init)
# ---------------------------------------------------------------------------

_genai = None
_genai_lock = threading.Lock()


def get_genai():
    global _genai
    if _genai is not None:
        return _genai
    with _genai_lock:
        if _genai is not None:
            return _genai
        try:
            import google.generativeai as genai
            genai.configure(api_key=GEMINI_API_KEY)
            _genai = genai
            logger.info("Gemini API client initialized")
            return _genai
        except Exception as exc:
            logger.error("Gemini init failed: %s", exc)
            return None


# ---------------------------------------------------------------------------
# Stripe Client (lazy init)
# ---------------------------------------------------------------------------

_stripe = None


def get_stripe():
    global _stripe
    if _stripe is not None:
        return _stripe
    try:
        import stripe as _s
        _s.api_key = STRIPE_SECRET_KEY
        _stripe = _s
        return _stripe
    except Exception as exc:
        logger.error("Stripe init failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# In-Memory Rate Limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Thread-safe sliding-window rate limiter."""

    def __init__(self):
        self._lock = threading.Lock()
        # per-user: {user_id: [timestamps]}
        self._user_second = defaultdict(list)   # 1 req/s
        self._user_hour = defaultdict(list)      # 60 req/hr
        self._global = []                        # 100 req/min

    def _prune(self, bucket: list, window_seconds: float):
        cutoff = time.time() - window_seconds
        while bucket and bucket[0] < cutoff:
            bucket.pop(0)

    def check(self, user_id: str) -> str | None:
        """Return error message if rate limited, else None."""
        now = time.time()
        with self._lock:
            # Global: 100 req/min
            self._prune(self._global, 60)
            if len(self._global) >= 100:
                return "Global rate limit exceeded. Please try again in a minute."
            # Per-user: 1 req/s
            self._prune(self._user_second[user_id], 1)
            if len(self._user_second[user_id]) >= 1:
                return "Too many requests. Please wait 1 second."
            # Per-user: 60 req/hr
            self._prune(self._user_hour[user_id], 3600)
            if len(self._user_hour[user_id]) >= 60:
                return "Hourly request limit reached (60/hr). Please try again later."
            # Record
            self._global.append(now)
            self._user_second[user_id].append(now)
            self._user_hour[user_id].append(now)
            return None


rate_limiter = RateLimiter()

# ---------------------------------------------------------------------------
# In-Memory Cache (graceful degradation when Firestore is down)
# ---------------------------------------------------------------------------

_cache = {
    "subscriptions": {},   # user_id -> {tier, status, ...}
    "usage": {},           # user_id -> {tokens_used, signals_used, period_start}
}
_cache_lock = threading.Lock()


def cache_set(ns: str, key: str, value: dict):
    with _cache_lock:
        _cache[ns][key] = value


def cache_get(ns: str, key: str) -> dict | None:
    with _cache_lock:
        return _cache[ns].get(key)


# ---------------------------------------------------------------------------
# Firestore Helpers
# ---------------------------------------------------------------------------

def fs_get_subscription(user_id: str) -> dict:
    """Get subscription data from Firestore (with cache fallback)."""
    db = get_firestore()
    if db:
        try:
            doc = db.collection("subscriptions").document(user_id).get()
            if doc.exists:
                data = doc.to_dict()
                cache_set("subscriptions", user_id, data)
                return data
        except Exception as exc:
            logger.warning("Firestore read (subscriptions) failed: %s", exc)

    # Fallback to cache
    cached = cache_get("subscriptions", user_id)
    if cached:
        return cached

    # Default: free trial
    return {
        "tier": "free",
        "status": "active",
        "signals_used": 0,
        "tokens_used": 0,
        "period_start": datetime.now(timezone.utc).isoformat(),
    }


def fs_get_usage(user_id: str) -> dict:
    """Get current-period usage from Firestore."""
    db = get_firestore()
    if db:
        try:
            doc = db.collection("users").document(user_id).get()
            if doc.exists:
                data = doc.to_dict()
                cache_set("usage", user_id, data)
                return data
        except Exception as exc:
            logger.warning("Firestore read (users) failed: %s", exc)

    cached = cache_get("usage", user_id)
    if cached:
        return cached

    return {"tokens_used": 0, "signals_used": 0}


def fs_log_usage(user_id: str, endpoint: str, model: str,
                 tokens_in: int, tokens_out: int, cost: float):
    """Log an AI usage event to Firestore."""
    db = get_firestore()
    record = {
        "user_id": user_id,
        "endpoint": endpoint,
        "model_used": model,
        "tokens_input": tokens_in,
        "tokens_output": tokens_out,
        "cost_estimate": round(cost, 6),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "request_id": g.get("request_id", "unknown"),
    }
    if db:
        try:
            db.collection("ai_usage").add(record)
        except Exception as exc:
            logger.error("Failed to log usage to Firestore: %s", exc)
    else:
        logger.info("Usage log (no Firestore): %s", json.dumps(record))


def fs_increment_usage(user_id: str, tokens: int, signals: int = 0):
    """Increment token and signal counters in Firestore."""
    db = get_firestore()
    if db:
        try:
            from google.cloud.firestore_v1 import Increment
            ref = db.collection("users").document(user_id)
            ref.set({
                "tokens_used": Increment(tokens),
                "signals_used": Increment(signals),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }, merge=True)

            sub_ref = db.collection("subscriptions").document(user_id)
            sub_ref.set({
                "tokens_used": Increment(tokens),
                "signals_used": Increment(signals),
            }, merge=True)
        except Exception as exc:
            logger.error("Failed to increment usage: %s", exc)

    # Update cache too
    cached = cache_get("usage", user_id) or {"tokens_used": 0, "signals_used": 0}
    cached["tokens_used"] = cached.get("tokens_used", 0) + tokens
    cached["signals_used"] = cached.get("signals_used", 0) + signals
    cache_set("usage", user_id, cached)

    cached_sub = cache_get("subscriptions", user_id)
    if cached_sub:
        cached_sub["tokens_used"] = cached_sub.get("tokens_used", 0) + tokens
        cached_sub["signals_used"] = cached_sub.get("signals_used", 0) + signals
        cache_set("subscriptions", user_id, cached_sub)


def fs_update_subscription(user_id: str, data: dict):
    """Write subscription data to Firestore."""
    db = get_firestore()
    if db:
        try:
            db.collection("subscriptions").document(user_id).set(data, merge=True)
        except Exception as exc:
            logger.error("Failed to update subscription: %s", exc)
    cache_set("subscriptions", user_id, {**fs_get_subscription(user_id), **data})


# ---------------------------------------------------------------------------
# Subscription Enforcement
# ---------------------------------------------------------------------------

def enforce_subscription(user_id: str, is_signal: bool = False) -> str | None:
    """Return error string if user cannot proceed, else None."""
    sub = fs_get_subscription(user_id)
    tier_key = sub.get("tier", "free")
    status = sub.get("status", "active")
    tier = TIERS.get(tier_key, TIERS["free"])

    # Check expiry
    if status in ("expired", "canceled", "past_due") and tier_key != "free":
        return "Your subscription has expired. Please renew to continue."

    tokens_used = sub.get("tokens_used", 0)
    signals_used = sub.get("signals_used", 0)

    # Check signal limit
    if is_signal and signals_used >= tier["signal_limit"]:
        if tier_key == "free":
            return "Free trial exhausted (5 signals). Please subscribe to continue."
        return f"Signal limit reached ({tier['signal_limit']}/{tier['name']}). Upgrade your plan."

    # Check token limit (skip for free trial which has no token limit)
    if tier_key != "free" and tier["token_limit"] > 0 and tokens_used >= tier["token_limit"]:
        return f"Monthly token limit reached ({tier['token_limit']:,} tokens). Upgrade your plan."

    return None


# ---------------------------------------------------------------------------
# Model Routing Engine
# ---------------------------------------------------------------------------

TASK_COMPLEXITY = {
    "fix-code":           "simple",
    "explain-trade":      "simple",
    "generate-signal":    "medium",
    "validate-strategy":  "medium",
    "convert-script":     "complex",
}


def select_model(task: str, user_tier: str = "free") -> str:
    """Choose cheapest viable model for the task."""
    complexity = TASK_COMPLEXITY.get(task, "medium")
    if complexity in ("simple", "medium"):
        return "gemini-2.5-flash"
    else:
        return "gemini-2.5-pro"


def estimate_cost(model: str, tokens_in: int, tokens_out: int) -> float:
    costs = MODEL_COSTS.get(model, MODEL_COSTS["gemini-2.5-flash"])
    return (tokens_in / 1000 * costs["input"]) + (tokens_out / 1000 * costs["output"])


def call_gemini(prompt: str, model_name: str, system_instruction: str = "") -> dict:
    """Call Gemini API with fallback chain. Returns {text, model, tokens_in, tokens_out}."""
    genai = get_genai()
    if not genai:
        return {"error": "AI service temporarily unavailable. Please try again later."}

    models_to_try = [model_name]
    if model_name == "gemini-2.5-pro":
        models_to_try.append("gemini-2.5-flash")
    elif model_name == "gemini-2.5-flash":
        models_to_try.append("gemini-2.5-pro")

    last_error = None
    for m in models_to_try:
        try:
            model_kwargs = {}
            if system_instruction:
                model_kwargs["system_instruction"] = system_instruction
            model = genai.GenerativeModel(m, **model_kwargs)
            response = model.generate_content(prompt)

            # Extract token counts from usage metadata
            tokens_in = 0
            tokens_out = 0
            if hasattr(response, "usage_metadata") and response.usage_metadata:
                tokens_in = getattr(response.usage_metadata, "prompt_token_count", 0) or 0
                tokens_out = getattr(response.usage_metadata, "candidates_token_count", 0) or 0

            # Estimate if metadata unavailable
            if tokens_in == 0:
                tokens_in = len(prompt) // 4
            if tokens_out == 0:
                tokens_out = len(response.text) // 4 if response.text else 0

            return {
                "text": response.text,
                "model": m,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
            }
        except Exception as exc:
            last_error = str(exc)
            logger.warning("Gemini call failed (%s): %s", m, exc)
            continue

    return {"error": f"All AI models unavailable. Last error: {last_error}"}


# ---------------------------------------------------------------------------
# Validation Engine
# ---------------------------------------------------------------------------

def validate_pinescript(code: str) -> dict:
    """Validate PineScript structure and risk rules. Returns {score, issues, auto_fixes}."""
    issues = []
    auto_fixes = []
    score = 100

    if not code or not code.strip():
        return {"score": 0, "issues": ["Empty output"], "auto_fixes": [], "valid": False}

    text_lower = code.lower()

    # Syntax checks
    if "//@version=" not in code.replace(" ", "") and "indicator(" not in text_lower and "strategy(" not in text_lower:
        # Might not be PineScript, just informational
        pass
    else:
        # It looks like PineScript
        if "//@version=" not in code.replace(" ", ""):
            issues.append("Missing //@version directive")
            auto_fixes.append("Add //@version=5 at the top")
            score -= 10

        paren_open = code.count("(")
        paren_close = code.count(")")
        if paren_open != paren_close:
            issues.append(f"Unbalanced parentheses: {paren_open} open vs {paren_close} close")
            score -= 15

        bracket_open = code.count("[")
        bracket_close = code.count("]")
        if bracket_open != bracket_close:
            issues.append(f"Unbalanced brackets: {bracket_open} open vs {bracket_close} close")
            score -= 10

    # Risk rule checks
    if "stop_loss" not in text_lower and "stop loss" not in text_lower and "stoploss" not in text_lower:
        issues.append("No stop_loss detected — high risk strategy")
        auto_fixes.append("Consider adding stop_loss logic")
        score -= 20

    if "risk_reward" not in text_lower and "risk reward" not in text_lower and "risk/reward" not in text_lower:
        issues.append("No risk/reward ratio mentioned")
        score -= 10

    score = max(0, min(100, score))
    return {
        "score": score,
        "issues": issues,
        "auto_fixes": auto_fixes,
        "valid": score >= 40,
    }


def validate_signal_output(text: str) -> dict:
    """Validate any AI signal/strategy output."""
    issues = []
    score = 100

    if not text or len(text.strip()) < 20:
        return {"score": 0, "issues": ["Output too short or empty"], "valid": False}

    text_lower = text.lower()

    # Check for risk mentions
    risk_terms = ["stop loss", "stop_loss", "stoploss", "risk", "sl"]
    if not any(t in text_lower for t in risk_terms):
        issues.append("No risk management mentioned")
        score -= 25

    reward_terms = ["take profit", "take_profit", "tp", "target", "reward"]
    if not any(t in text_lower for t in reward_terms):
        issues.append("No profit target mentioned")
        score -= 15

    # Check for actionable content
    action_terms = ["buy", "sell", "long", "short", "entry", "signal", "strategy"]
    if not any(t in text_lower for t in action_terms):
        issues.append("No actionable trading terms found")
        score -= 10

    score = max(0, min(100, score))
    return {"score": score, "issues": issues, "valid": score >= 30}

# ---------------------------------------------------------------------------
# CANONICAL SIGNAL SCHEMA (v2) — Deterministic Response Contract
# ---------------------------------------------------------------------------

CANONICAL_SIGNAL_REQUIRED_KEYS = {
    "signal_id", "timestamp", "asset", "signal", "confidence",
    "entry_zone", "stop_loss", "take_profit", "timeframe",
    "rationale", "risk_rating"
}

VALID_SIGNALS = {"BUY", "SELL", "HOLD"}
VALID_RISK_RATINGS = {"low", "medium", "high", "extreme"}


def parse_signal_from_text(raw_text: str, asset: str, timeframe: str) -> dict:
    """
    Parse free-form AI text into canonical signal object.
    Uses regex extraction with fallbacks for maximum reliability.
    """
    import re as _re
    text = raw_text.strip()
    text_lower = text.lower()

    # --- Signal Direction ---
    signal = "HOLD"
    if any(w in text_lower for w in ["long", "buy", "bullish"]):
        signal = "BUY"
    elif any(w in text_lower for w in ["short", "sell", "bearish"]):
        signal = "SELL"

    # --- Price extraction helper ---
    def find_prices(pattern, txt):
        """Find all price-like numbers near a pattern."""
        matches = []
        for m in _re.finditer(pattern, txt, _re.IGNORECASE):
            # Look for numbers within 100 chars after the match
            region = txt[m.start():m.start()+200]
            nums = _re.findall(r'\$?([\d,]+\.?\d*)', region)
            for n in nums:
                try:
                    val = float(n.replace(',', ''))
                    if val > 0:
                        matches.append(val)
                except:
                    pass
        return matches

    # --- Entry Zone ---
    entry_prices = find_prices(r'entry\s*(?:price|zone|range|level|:)', text)
    if not entry_prices:
        entry_prices = find_prices(r'(?:enter|entering)\s+(?:at|around|near)', text)
    if len(entry_prices) >= 2:
        entry_zone = {"min": min(entry_prices[:2]), "max": max(entry_prices[:2])}
    elif len(entry_prices) == 1:
        p = entry_prices[0]
        entry_zone = {"min": round(p * 0.998, 2), "max": round(p * 1.002, 2)}
    else:
        # Try to find any prominent price
        all_nums = _re.findall(r'\$([\d,]+\.?\d*)', text)
        prices = []
        for n in all_nums:
            try:
                prices.append(float(n.replace(',','')))
            except:
                pass
        if prices:
            mid = prices[len(prices)//2]
            entry_zone = {"min": round(mid * 0.998, 2), "max": round(mid * 1.002, 2)}
        else:
            entry_zone = {"min": 0, "max": 0}

    # --- Stop Loss ---
    sl_prices = find_prices(r'stop[\s_-]*loss', text)
    if not sl_prices:
        sl_prices = find_prices(r'\bSL\b', text)
    stop_loss = sl_prices[0] if sl_prices else 0

    # --- Take Profit ---
    tp_prices = []
    for tp_label in [r'TP\s*1', r'take[\s_-]*profit[\s_-]*1', r'target[\s_-]*1']:
        tp_prices.extend(find_prices(tp_label, text))
    for tp_label in [r'TP\s*2', r'take[\s_-]*profit[\s_-]*2', r'target[\s_-]*2']:
        tp_prices.extend(find_prices(tp_label, text))
    for tp_label in [r'TP\s*3', r'take[\s_-]*profit[\s_-]*3', r'target[\s_-]*3']:
        tp_prices.extend(find_prices(tp_label, text))
    if not tp_prices:
        tp_prices = find_prices(r'take[\s_-]*profit', text)
    if not tp_prices:
        tp_prices = find_prices(r'target', text)
    # Deduplicate and sort
    tp_prices = sorted(set(tp_prices))[:3]

    # --- Confidence ---
    confidence = 0.5
    conf_match = _re.search(r'confidence[:\s]+(?:level[:\s]+)?([\d.]+)(?:\s*/\s*10)?', text_lower)
    if conf_match:
        val = float(conf_match.group(1))
        confidence = val / 10 if val > 1 else val
    else:
        conf_match = _re.search(r'(\d+)\s*/\s*10', text)
        if conf_match:
            confidence = int(conf_match.group(1)) / 10

    # --- Risk Rating ---
    risk_rating = "medium"
    if "high risk" in text_lower or "extreme" in text_lower:
        risk_rating = "high"
    elif "low risk" in text_lower or "conservative" in text_lower:
        risk_rating = "low"

    # --- Rationale ---
    # Extract first 2-3 sentences that explain the signal
    sentences = _re.split(r'[.!\n]', text)
    rationale_parts = []
    for s in sentences:
        s = s.strip()
        if len(s) > 20 and any(w in s.lower() for w in 
            ["because", "due to", "support", "resistance", "breakout", "trend", 
             "momentum", "rsi", "macd", "ema", "volume", "pattern", "indicator",
             "bullish", "bearish", "divergence", "fibonacci", "moving average"]):
            rationale_parts.append(s)
        if len(rationale_parts) >= 3:
            break
    rationale = ". ".join(rationale_parts) if rationale_parts else "AI-generated signal based on technical analysis"

    return {
        "signal_id": f"SIG-{uuid.uuid4().hex[:8].upper()}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "asset": asset,
        "signal": signal,
        "confidence": round(min(1.0, max(0.0, confidence)), 2),
        "entry_zone": entry_zone,
        "stop_loss": stop_loss,
        "take_profit": tp_prices if tp_prices else [0],
        "timeframe": timeframe,
        "rationale": rationale[:500],
        "risk_rating": risk_rating,
        "status_label": "LIVE"
    }



def format_signal_telegram(sig: dict) -> str:
    """
    Format canonical signal object into Telegram message.
    This is the ONLY place Telegram formatting happens.
    """
    direction_emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(sig["signal"], "⚪")
    conf_pct = int(sig["confidence"] * 100)
    
    # Format take profits
    tp_lines = ""
    for i, tp in enumerate(sig.get("take_profit", []), 1):
        if tp > 0:
            tp_lines += f"  🎯 TP{i}: ${tp:,.2f}\n"
    
    entry = sig.get("entry_zone", {})
    entry_str = f"${entry.get('min', 0):,.2f} — ${entry.get('max', 0):,.2f}"
    
    sl = sig.get("stop_loss", 0)
    
    msg = (
        f"🔱 TITAN AI SIGNAL — {sig['asset']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{direction_emoji} Signal: {sig['signal']}\n"
        f"📊 Confidence: {conf_pct}%\n"
        f"⏱ Timeframe: {sig['timeframe']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 Entry Zone: {entry_str}\n"
        f"🛑 Stop Loss: ${sl:,.2f}\n"
        f"{tp_lines}"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚖️ Risk: {sig.get('risk_rating', 'medium').upper()}\n"
        f"💡 {sig.get('rationale', 'N/A')[:200]}\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 {sig.get('signal_id', 'N/A')}\n"
        f"💰 Powered by Aranya Genesis Corp\n"
        f"⚠️ Not financial advice. Trade at your own risk."
    )
    return msg




# ---------------------------------------------------------------------------
# Request Middleware
# ---------------------------------------------------------------------------

@app.before_request
def before_request():
    g.request_id = str(uuid.uuid4())[:12]
    g.start_time = time.time()


@app.after_request
def after_request(response):
    duration = time.time() - g.get("start_time", time.time())
    logger.info(
        "request_id=%s method=%s path=%s status=%s duration=%.3fs",
        g.get("request_id", "-"), request.method, request.path,
        response.status_code, duration,
    )
    response.headers["X-Request-ID"] = g.get("request_id", "")
    return response


# ---------------------------------------------------------------------------
# Auth / Admin Decorators
# ---------------------------------------------------------------------------

def require_user(f):
    """Require user_id in request body or query string."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user_id = None
        if request.is_json and request.json:
            user_id = request.json.get("user_id")
        if not user_id:
            user_id = request.args.get("user_id")
        if not user_id:
            return jsonify({"error": "user_id is required", "request_id": g.request_id}), 400
        g.user_id = user_id
        return f(*args, **kwargs)
    return wrapper


def require_admin(f):
    """Require admin API key."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        key = request.headers.get("X-Admin-Key") or request.args.get("admin_key")
        if not key or key != ADMIN_API_KEY:
            return jsonify({"error": "Unauthorized"}), 403
        return f(*args, **kwargs)
    return wrapper


def protected_ai_endpoint(task_name: str, is_signal: bool = False):
    """Decorator for AI endpoints: rate limit → subscription check → proceed."""
    def decorator(f):
        @wraps(f)
        @require_user
        def wrapper(*args, **kwargs):
            user_id = g.user_id
            # SYSTEM BYPASS: internal/scheduled signals from owner backend skip all limits
            system_key = request.headers.get("X-System-Key") or request.args.get("system_key")
            if system_key and system_key == SYSTEM_API_KEY:
                g.task_name = task_name
                g.system_bypass = True
                return f(*args, **kwargs)
            # Rate limit
            rl_error = rate_limiter.check(user_id)
            if rl_error:
                return jsonify({"error": rl_error, "request_id": g.request_id}), 429
            # Subscription enforcement
            sub_error = enforce_subscription(user_id, is_signal=is_signal)
            if sub_error:
                return jsonify({"error": sub_error, "request_id": g.request_id}), 403
            g.task_name = task_name
            g.system_bypass = False
            return f(*args, **kwargs)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Helper: Run AI Task
# ---------------------------------------------------------------------------

def run_ai_task(user_id: str, task: str, prompt: str,
                system_instruction: str = "", is_signal: bool = False) -> tuple:
    """Execute AI call with full governance. Returns (response_dict, status_code)."""
    sub = fs_get_subscription(user_id)
    tier_key = sub.get("tier", "free")
    model = select_model(task, tier_key)

    # Pre-estimate tokens (rough)
    est_tokens_in = len(prompt) // 4
    est_tokens_out = est_tokens_in  # rough guess

    # Check if estimated usage would exceed limit
    tier = TIERS.get(tier_key, TIERS["free"])
    if tier_key != "free" and tier["token_limit"] > 0:
        current_used = sub.get("tokens_used", 0)
        if current_used + est_tokens_in > tier["token_limit"]:
            # Try downgrading model
            if model == "gemini-2.5-pro":
                model = "gemini-2.5-flash"
                logger.info("Downgraded model for user %s due to token budget", user_id)

    # Call Gemini
    result = call_gemini(prompt, model, system_instruction)
    if "error" in result:
        return {"error": result["error"], "request_id": g.request_id}, 503

    tokens_in = result["tokens_in"]
    tokens_out = result["tokens_out"]
    total_tokens = tokens_in + tokens_out
    cost = estimate_cost(result["model"], tokens_in, tokens_out)

    # Log and increment usage
    fs_log_usage(user_id, task, result["model"], tokens_in, tokens_out, cost)
    fs_increment_usage(user_id, total_tokens, signals=1 if is_signal else 0)

    return {
        "output": result["text"],
        "model_used": result["model"],
        "tokens": {"input": tokens_in, "output": tokens_out, "total": total_tokens},
        "cost_estimate": round(cost, 6),
        "request_id": g.request_id,
        "disclaimer": DISCLAIMER,
    }, 200


# ---------------------------------------------------------------------------
# Endpoints: Health
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    try:
        uptime = time.time() - START_TIME
        db_status = "connected" if get_firestore() else "unavailable"
        return jsonify({
            "status": "healthy",
            "version": VERSION,
            "uptime_seconds": round(uptime, 1),
            "firestore": db_status,
            "gemini": "configured" if GEMINI_API_KEY else "not configured",
        })
    except Exception as exc:
        logger.error("Health check error: %s", exc)
        return jsonify({"status": "degraded", "error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Endpoints: Usage
# ---------------------------------------------------------------------------

@app.route("/usage/check", methods=["GET"])
@require_user
def usage_check():
    try:
        user_id = g.user_id
        sub = fs_get_subscription(user_id)
        usage = fs_get_usage(user_id)
        tier_key = sub.get("tier", "free")
        tier = TIERS.get(tier_key, TIERS["free"])

        tokens_used = sub.get("tokens_used", 0) or usage.get("tokens_used", 0)
        signals_used = sub.get("signals_used", 0) or usage.get("signals_used", 0)

        return jsonify({
            "user_id": user_id,
            "tier": tier_key,
            "tier_name": tier["name"],
            "status": sub.get("status", "active"),
            "tokens_used": tokens_used,
            "token_limit": tier["token_limit"],
            "tokens_remaining": max(0, tier["token_limit"] - tokens_used) if tier["token_limit"] > 0 else "unlimited",
            "signals_used": signals_used,
            "signal_limit": tier["signal_limit"],
            "signals_remaining": max(0, tier["signal_limit"] - signals_used),
            "request_id": g.request_id,
        })
    except Exception as exc:
        logger.error("Usage check error: %s", exc)
        return jsonify({"error": "Failed to check usage", "request_id": g.request_id}), 500


@app.route("/usage/log", methods=["POST"])
@require_user
def usage_log():
    try:
        data = request.json or {}
        user_id = g.user_id
        fs_log_usage(
            user_id=user_id,
            endpoint=data.get("endpoint", "unknown"),
            model=data.get("model_used", "unknown"),
            tokens_in=data.get("tokens_input", 0),
            tokens_out=data.get("tokens_output", 0),
            cost=data.get("cost_estimate", 0.0),
        )
        return jsonify({"status": "logged", "request_id": g.request_id})
    except Exception as exc:
        logger.error("Usage log error: %s", exc)
        return jsonify({"error": "Failed to log usage", "request_id": g.request_id}), 500


# ---------------------------------------------------------------------------
# Endpoints: AI - Generate Signal
# ---------------------------------------------------------------------------

@app.route("/generate-signal", methods=["POST"])
@protected_ai_endpoint("generate-signal", is_signal=True)
def generate_signal():
    """
    Generate AI trading signal with CANONICAL response contract.
    
    Pipeline: Gemini AI → Text Parser → Canonical Schema → Validation → Response
    
    Returns deterministic JSON matching the canonical signal schema.
    """
    try:
        data = request.json or {}
        # ── Production Governance Lock: Asset Policy ──────────────────────────
        # Scheduler/autonomous calls always get XAUUSD regardless of payload
        requester_id = data.get("user_id", "")
        requested_asset = data.get("asset", None)
        if requester_id in SCHEDULER_USER_IDS or requested_asset is None:
            asset = DEFAULT_ASSET  # XAU/USD — XAUUSD priority enforced
        else:
            asset = requested_asset

        timeframe = data.get("timeframe", "4H")
        context = data.get("context", "")
        format_telegram = data.get("format_telegram", False)
        dry_run = data.get("dry_run", False)

        import uuid
        from datetime import datetime, timezone
        now_utc = datetime.now(timezone.utc)
        signal_id = f"T-{asset.replace('/', '-')}-{timeframe}-{now_utc.strftime('%Y%m%d%H%M%S')}"
        current_datetime_str = now_utc.strftime("%A, %d %B %Y %H:%M UTC")
        current_date_str = now_utc.strftime("%d %B %Y")

        # ── LIVE price fetch — NO static fallback allowed ─────────────────────
        live_price = fetch_live_price(asset)
        if live_price is None:
            logger.error("LIVE_PRICE UNAVAILABLE for %s — enforcing NO_SIGNAL", asset)
            return jsonify({
                "status": "no_signal",
                "no_signal": True,
                "no_signal_reason": f"Live market price unavailable for {asset}. Cannot generate a valid signal without real-time data. Try again shortly.",
                "asset": asset,
                "signal_id": signal_id,
                "timestamp": now_utc.isoformat(),
                "request_id": g.request_id
            }), 200

        tol = PRICE_TOLERANCE
        live_price_min = round(live_price * (1 - tol), 2)
        live_price_max = round(live_price * (1 + tol), 2)
        price_hint = (
            f"\nLIVE MARKET PRICE for {asset} as of {current_datetime_str}: ${live_price:,.2f}."
            f"\nAll entry zones, stop losses, and take profits MUST be anchored to this live price."
            f"\nAcceptable price range for this signal: ${live_price_min:,.2f} – ${live_price_max:,.2f}."
            f"\nDo NOT use outdated, cached, or training-data prices. Use ONLY the live price provided above."
        )

        # ---- STAGE 1: Call Gemini with NATIVE JSON MODE ────────────────────
        # Full 8-step TITAN Intelligence Pipeline:
        # 1. Freshness Gate → 2. OSINT/Macro → 3. Technical State
        # 4. CA Confluence → 5. Synthesis → 6. Validation → 7. Eligibility → 8. Output
        json_prompt = f"""{TITAN_SIGNAL_SYSTEM_INSTRUCTION}

═══════════════════════════════════════════════════════
SIGNAL REQUEST — {current_datetime_str}
═══════════════════════════════════════════════════════
Asset: {asset}
Timeframe: {timeframe}
{f'Additional context: {context}' if context else ''}
{price_hint}

MANDATORY 8-STEP DECISION PIPELINE:
You MUST reason through each step before outputting JSON.

STEP 1 — FRESHNESS GATE:
Confirm today is {current_date_str}. Anchor all analysis to current market conditions.
Reject any stale/cached reasoning that doesn't reflect today's macro environment.

STEP 2 — OSINT MACRO LAYER (Top G Omni OSINT 2.0):
For {asset}, assess RIGHT NOW:
- Fed/ECB/BOJ/BOE rate posture and forward guidance (critical for gold)
- US real yields trajectory (TIPS spread) — falling real yields = bullish gold
- DXY direction and momentum — gold has INVERSE DXY correlation
- Inflation regime: CPI/PCE trends and market expectations
- Geopolitical risk premium currently in market
- Central bank gold demand (ongoing de-dollarization flows)
- ETF flows (GLD/IAU) and CoT positioning
- Assign macro_bias score: -1.0 (strongly bearish) to +1.0 (strongly bullish)

STEP 3 — TECHNICAL/STRUCTURAL STATE:
- Identify dominant trend on {timeframe}: BULLISH / BEARISH / SIDEWAYS
- Key support/resistance zones
- Momentum state (RSI, MACD alignment)
- Volatility regime: EXPANDING / NORMAL / CONTRACTING
- Liquidity pockets and stop-hunt zones
- Assign technical_bias score: -1.0 to +1.0

STEP 4 — CA ENGINE CONFLUENCE (Claude CA Cellular Automata):
Score each confluence factor (0–1):
- Macro ↔ Technical alignment: [score]
- Trend structure integrity: [score]
- Momentum confirmation: [score]
- Institutional participation evidence: [score]
- Volatility regime suitability: [score]
Compute confluence_score = weighted average (0.0 to 1.0)

STEP 5 — SYNTHESIS DECISION:
IF confluence_score < 0.60: output no_signal = true
IF macro and technical biases CONFLICT without strong catalyst: output no_signal = true
ELSE: synthesize entry_zone, stop_loss, take_profit from analysis

STEP 6 — PRICE VALIDATION:
All price levels MUST be realistic for {asset} as of {current_date_str}.
{price_hint}

OUTPUT — Return EXACTLY this JSON structure:
{{
  "asset": "{asset}",
  "signal": "BUY or SELL or HOLD or NO_SIGNAL",
  "no_signal": false,
  "no_signal_reason": null,
  "confidence": 0.85,
  "confluence_score": 0.78,
  "macro_bias": 0.6,
  "regime": "TRENDING_BULL or TRENDING_BEAR or RANGING or CHAOTIC or TRANSITIONING",
  "setup_quality": "INSTITUTIONAL or STANDARD or WEAK",
  "entry_zone": {{"min": 0.0, "max": 0.0}},
  "stop_loss": 0.0,
  "take_profit": [0.0, 0.0, 0.0],
  "timeframe": "{timeframe}",
  "rationale": "3-4 sentences covering macro context, technical setup, and CA confluence evidence",
  "macro_context": "1-2 sentences on macro/OSINT drivers",
  "risk_rating": "low or medium or high",
  "freshness_date": "{current_date_str}"
}}

HARD RULES:
- All numeric values must be actual numbers, not strings
- confidence must be between 0.0 and 1.0
- take_profit must be an array of exactly 3 numbers (TP1, TP2, TP3)
- signal must be exactly one of: BUY, SELL, HOLD, NO_SIGNAL
- If no_signal is true: set signal to "NO_SIGNAL", set entry_zone/stop_loss/take_profit to 0
- confidence < {MIN_CONFIDENCE_THRESHOLD} MUST result in no_signal=true
- DO NOT fabricate setups — if evidence is insufficient, output NO_SIGNAL
- Use realistic prices anchored to {current_date_str}
"""

        # Use direct Gemini API with responseMimeType for deterministic JSON
        import urllib.request as _urlreq
        gemini_key = os.environ.get("GEMINI_API_KEY", "")
        gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
        
        req_payload = json.dumps({
            "contents": [{"parts": [{"text": json_prompt}]}],
            "generationConfig": {
                "temperature": 0.3,
                "responseMimeType": "application/json"
            }
        }).encode()
        
        api_req = _urlreq.Request(gemini_url, data=req_payload, method='POST')
        api_req.add_header('Content-Type', 'application/json')
        api_resp = _urlreq.urlopen(api_req, timeout=30)
        api_result = json.loads(api_resp.read())
        
        raw_text = api_result['candidates'][0]['content']['parts'][0]['text']
        tokens_in = api_result.get('usageMetadata', {}).get('promptTokenCount', 0)
        tokens_out = api_result.get('usageMetadata', {}).get('candidatesTokenCount', 0)
        total_tokens = tokens_in + tokens_out

        # ---- STAGE 2: Parse JSON (with fallback) ----
        import re as _re
        parsed = None
        try:
            parsed = json.loads(raw_text)
        except json.JSONDecodeError:
            json_match = _re.search(r'```(?:json)?\s*({.*?})\s*```', raw_text, _re.DOTALL)
            if json_match:
                parsed = json.loads(json_match.group(1))
            else:
                json_match = _re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', raw_text)
                if json_match:
                    parsed = json.loads(json_match.group(0))

        if not parsed:
            return jsonify({
                "status": "no_signal",
                "no_signal": True,
                "no_signal_reason": "Failed to parse structured JSON from AI response — cannot broadcast unvalidated data",
                "signal_id": signal_id,
                "raw_snippet": raw_text[:300],
                "request_id": g.request_id
            }), 200  # 200 so broadcasters handle it gracefully

        # ── Production Governance Lock: NO_SIGNAL Gate ─────────────────────
        ai_no_signal = parsed.get("no_signal", False)
        ai_signal_str = str(parsed.get("signal", "HOLD")).upper()
        ai_confidence = float(parsed.get("confidence", 0.5))
        if ai_confidence > 1:
            ai_confidence = ai_confidence / 100.0

        # Reject if AI itself says no signal
        if ai_no_signal or ai_signal_str == "NO_SIGNAL":
            reason = parsed.get("no_signal_reason") or "Insufficient confluence or macro context for a validated signal"
            logger.info("NO_SIGNAL gate triggered for %s: %s", asset, reason)
            return jsonify({
                "status": "no_signal",
                "no_signal": True,
                "no_signal_reason": reason,
                "asset": asset,
                "signal_id": signal_id,
                "confluence_score": parsed.get("confluence_score", 0),
                "macro_bias": parsed.get("macro_bias", 0),
                "regime": parsed.get("regime", "UNKNOWN"),
                "timestamp": now_utc.isoformat(),
                "request_id": g.request_id
            }), 200

        # Reject if confidence below minimum threshold
        if ai_confidence < MIN_CONFIDENCE_THRESHOLD:
            logger.info("LOW_CONFIDENCE gate triggered for %s: %.2f < %.2f", asset, ai_confidence, MIN_CONFIDENCE_THRESHOLD)
            return jsonify({
                "status": "no_signal",
                "no_signal": True,
                "no_signal_reason": f"Confidence {ai_confidence:.0%} below minimum threshold {MIN_CONFIDENCE_THRESHOLD:.0%} — signal rejected",
                "asset": asset,
                "signal_id": signal_id,
                "confidence": ai_confidence,
                "timestamp": now_utc.isoformat(),
                "request_id": g.request_id
            }), 200

        # ── LIVE Price Sanity Gate — anchored to real-time fetched price ───────
        # live_price was fetched above; if somehow None here, reject signal
        if live_price is None:
            return jsonify({
                "status": "no_signal",
                "no_signal": True,
                "no_signal_reason": f"Live price unavailable at validation stage for {asset}. Signal rejected.",
                "asset": asset,
                "signal_id": signal_id,
                "timestamp": now_utc.isoformat(),
                "request_id": g.request_id
            }), 200

        entry_min = float(parsed.get("entry_zone", {}).get("min", 0))
        entry_max = float(parsed.get("entry_zone", {}).get("max", 0))
        pmin = live_price * (1 - PRICE_TOLERANCE)
        pmax = live_price * (1 + PRICE_TOLERANCE)
        if entry_min > 0 and (entry_min < pmin or entry_max > pmax):
            logger.warning(
                "PRICE_SANITY FAIL for %s: entry %.2f-%.2f outside live range %.2f-%.2f (live=%.2f)",
                asset, entry_min, entry_max, pmin, pmax, live_price
            )
            return jsonify({
                "status": "no_signal",
                "no_signal": True,
                "no_signal_reason": (
                    f"Price sanity check failed: entry zone ${entry_min:,.2f}–${entry_max:,.2f} "
                    f"is outside live market range ${pmin:,.2f}–${pmax:,.2f} "
                    f"(live {asset} price: ${live_price:,.2f}). Stale/hallucinated price detected."
                ),
                "asset": asset,
                "signal_id": signal_id,
                "timestamp": now_utc.isoformat(),
                "request_id": g.request_id
            }), 200

        # ---- STAGE 3: Build canonical signal object ----
        canonical = {
            "status": "success",
            "signal_id": signal_id,
            "timestamp": now_utc.isoformat(),
            "asset": parsed.get("asset", asset),
            "signal": ai_signal_str,
            "confidence": ai_confidence,
            "confluence_score": float(parsed.get("confluence_score", 0)),
            "macro_bias": float(parsed.get("macro_bias", 0)),
            "regime": str(parsed.get("regime", "UNKNOWN")),
            "setup_quality": str(parsed.get("setup_quality", "STANDARD")),
            "entry_zone": {
                "min": float(parsed.get("entry_zone", {}).get("min", 0)),
                "max": float(parsed.get("entry_zone", {}).get("max", 0)),
            },
            "stop_loss": float(parsed.get("stop_loss", 0)),
            "take_profit": [float(x) for x in parsed.get("take_profit", [0, 0, 0])[:3]],
            "timeframe": parsed.get("timeframe", timeframe),
            "rationale": str(parsed.get("rationale", "No rationale provided")),
            "macro_context": str(parsed.get("macro_context", "")),
            "risk_rating": str(parsed.get("risk_rating", "medium")).lower(),
            "freshness_date": str(parsed.get("freshness_date", current_date_str)),
            "status_label": "DRY_RUN" if dry_run else "LIVE"
        }

        # ---- STAGE 4: Validate canonical object ----
        is_valid, issues = validate_canonical_signal(canonical)

        if not is_valid:
            return jsonify({
                "status": "validation_failed",
                "signal_id": signal_id,
                "issues": issues,
                "raw_parsed": parsed,
                "request_id": g.request_id
            }), 422

        # Track usage through governance layer
        model_name = "gemini-2.5-flash"
        cost = estimate_cost(model_name, tokens_in, tokens_out)
        fs_log_usage(g.user_id, "generate-signal", model_name, tokens_in, tokens_out, cost)
        fs_increment_usage(g.user_id, total_tokens, signals=1)

        # Build Telegram message
        telegram_text = format_telegram_signal(canonical)

        return jsonify({
            "canonical_signal": canonical,
            "telegram_formatted": telegram_text,
            "validation": {"valid": is_valid, "issues": issues, "score": 100},
            "tokens": {"input": tokens_in, "output": tokens_out, "total": total_tokens},
            "cost_estimate": round(cost, 6),
            "model_used": model_name,
            "request_id": g.request_id,
            "disclaimer": DISCLAIMER,
        }), 200

    except Exception as exc:
        logger.error("generate-signal error: %s", exc)
        return jsonify({
            "status": "error",
            "error": "Signal generation failed",
            "detail": str(exc),
            "request_id": g.request_id
        }), 500




# ---------------------------------------------------------------------------
# Endpoints: AI - Fix Code
# ---------------------------------------------------------------------------

@app.route("/fix-code", methods=["POST"])
@protected_ai_endpoint("fix-code", is_signal=False)
def fix_code():
    try:
        data = request.json or {}
        code = data.get("code", "")
        language = data.get("language", "PineScript")
        issue = data.get("issue", "")

        if not code:
            return jsonify({"error": "code is required", "request_id": g.request_id}), 400

        prompt = (
            f"Fix the following {language} code.\n"
            f"Issue described by user: {issue}\n\n"
            f"```\n{code}\n```\n\n"
            "Return the corrected code with comments explaining what was fixed."
        )
        system_inst = (
            "You are a PineScript and trading code expert. Fix code bugs and improve quality. "
            "Always preserve the original strategy logic unless it's clearly wrong."
        )

        result, status = run_ai_task(g.user_id, "fix-code", prompt, system_inst)
        if status == 200 and language.lower() == "pinescript":
            validation = validate_pinescript(result.get("output", ""))
            result["validation"] = validation
        return jsonify(result), status
    except Exception as exc:
        logger.error("fix-code error: %s", exc)
        return jsonify({"error": "Code fix failed", "request_id": g.request_id}), 500


# ---------------------------------------------------------------------------
# Endpoints: AI - Validate Strategy
# ---------------------------------------------------------------------------

@app.route("/validate-strategy", methods=["POST"])
@protected_ai_endpoint("validate-strategy", is_signal=False)
def validate_strategy():
    try:
        data = request.json or {}
        strategy = data.get("strategy", "")
        if not strategy:
            return jsonify({"error": "strategy is required", "request_id": g.request_id}), 400

        prompt = (
            "Analyze and validate the following trading strategy. Evaluate:\n"
            "1. Logical correctness\n"
            "2. Risk management (stop_loss, position sizing)\n"
            "3. Edge conditions and failure modes\n"
            "4. Suggested improvements\n"
            "5. Overall score (0-100)\n\n"
            f"Strategy:\n```\n{strategy}\n```"
        )
        system_inst = (
            "You are a professional strategy auditor for algorithmic trading. "
            "Be thorough and critical. Identify risks, missing stop_loss logic, "
            "and potential issues. Score strategies honestly."
        )

        result, status = run_ai_task(g.user_id, "validate-strategy", prompt, system_inst)
        if status == 200:
            code_val = validate_pinescript(strategy)
            signal_val = validate_signal_output(result.get("output", ""))
            result["code_validation"] = code_val
            result["output_validation"] = signal_val
        return jsonify(result), status
    except Exception as exc:
        logger.error("validate-strategy error: %s", exc)
        return jsonify({"error": "Validation failed", "request_id": g.request_id}), 500


# ---------------------------------------------------------------------------
# Endpoints: AI - Convert Script
# ---------------------------------------------------------------------------

@app.route("/convert-script", methods=["POST"])
@protected_ai_endpoint("convert-script", is_signal=False)
def convert_script():
    try:
        data = request.json or {}
        code = data.get("code", "")
        source_lang = data.get("from", "TradingView PineScript")
        target_lang = data.get("to", "Python")

        if not code:
            return jsonify({"error": "code is required", "request_id": g.request_id}), 400

        prompt = (
            f"Convert the following {source_lang} code to {target_lang}.\n"
            "Preserve all strategy logic, risk management rules, and indicator calculations.\n"
            "Add comments explaining any translation decisions.\n\n"
            f"```\n{code}\n```"
        )
        system_inst = (
            "You are an expert at converting trading scripts between languages "
            "(PineScript, Python, MQL4/5, ThinkScript, etc). Maintain exact logic fidelity."
        )

        result, status = run_ai_task(g.user_id, "convert-script", prompt, system_inst)
        return jsonify(result), status
    except Exception as exc:
        logger.error("convert-script error: %s", exc)
        return jsonify({"error": "Conversion failed", "request_id": g.request_id}), 500


# ---------------------------------------------------------------------------
# Endpoints: AI - Explain Trade
# ---------------------------------------------------------------------------

@app.route("/explain-trade", methods=["POST"])
@protected_ai_endpoint("explain-trade", is_signal=False)
def explain_trade():
    try:
        data = request.json or {}
        trade = data.get("trade", "")
        if not trade:
            return jsonify({"error": "trade description is required", "request_id": g.request_id}), 400

        prompt = (
            "Explain the following trade setup in clear, educational terms:\n\n"
            f"{trade}\n\n"
            "Cover:\n"
            "1. What the trade is (direction, asset, timeframe)\n"
            "2. Why this setup was identified (technical reasoning)\n"
            "3. Risk management (stop_loss, risk/reward)\n"
            "4. Key levels to watch\n"
            "5. What could go wrong (invalidation)\n"
        )
        system_inst = (
            "You are a trading educator. Explain trades clearly for all skill levels. "
            "Always emphasize risk management and that no trade is guaranteed."
        )

        result, status = run_ai_task(g.user_id, "explain-trade", prompt, system_inst)
        return jsonify(result), status
    except Exception as exc:
        logger.error("explain-trade error: %s", exc)
        return jsonify({"error": "Explanation failed", "request_id": g.request_id}), 500


# ---------------------------------------------------------------------------
# Endpoints: Stripe Webhook
# ---------------------------------------------------------------------------

@app.route("/webhook/stripe", methods=["POST"])
def stripe_webhook():
    try:
        payload = request.get_data(as_text=True)
        sig_header = request.headers.get("Stripe-Signature", "")

        stripe_mod = get_stripe()
        if not stripe_mod:
            logger.error("Stripe module not available")
            return jsonify({"error": "Webhook handler unavailable"}), 500

        # Verify signature
        try:
            event = stripe_mod.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        except stripe_mod.error.SignatureVerificationError:
            logger.warning("Invalid Stripe webhook signature")
            return jsonify({"error": "Invalid signature"}), 400
        except Exception as exc:
            logger.warning("Stripe signature verification failed: %s", exc)
            return jsonify({"error": "Signature verification failed"}), 400

        event_type = event.get("type", "")
        data_obj = event.get("data", {}).get("object", {})
        logger.info("Stripe webhook: type=%s id=%s", event_type, event.get("id"))

        if event_type == "payment_intent.succeeded":
            customer_id = data_obj.get("customer", "")
            amount = data_obj.get("amount", 0)
            logger.info("Payment succeeded: customer=%s amount=%s", customer_id, amount)
            # Map amount to tier
            amount_dollars = amount / 100
            tier = "starter"
            if amount_dollars >= 199:
                tier = "elite"
            elif amount_dollars >= 99:
                tier = "pro"
            elif amount_dollars >= 39:
                tier = "starter"

            if customer_id:
                fs_update_subscription(customer_id, {
                    "tier": tier,
                    "status": "active",
                    "stripe_customer_id": customer_id,
                    "last_payment": datetime.now(timezone.utc).isoformat(),
                    "tokens_used": 0,
                    "signals_used": 0,
                    "period_start": datetime.now(timezone.utc).isoformat(),
                })

        elif event_type == "customer.subscription.updated":
            customer_id = data_obj.get("customer", "")
            status = data_obj.get("status", "active")
            cancel_at = data_obj.get("cancel_at_period_end", False)

            if customer_id:
                update_data = {
                    "status": "active" if status == "active" else status,
                    "stripe_subscription_id": data_obj.get("id", ""),
                    "cancel_at_period_end": cancel_at,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                fs_update_subscription(customer_id, update_data)

        elif event_type == "customer.subscription.deleted":
            customer_id = data_obj.get("customer", "")
            if customer_id:
                fs_update_subscription(customer_id, {
                    "status": "canceled",
                    "tier": "free",
                    "canceled_at": datetime.now(timezone.utc).isoformat(),
                })

        return jsonify({"status": "received"}), 200

    except Exception as exc:
        logger.error("Stripe webhook error: %s", exc)
        return jsonify({"error": "Webhook processing failed"}), 500


# ---------------------------------------------------------------------------
# Endpoints: Admin
# ---------------------------------------------------------------------------

@app.route("/admin/stats", methods=["GET"])
@require_admin
def admin_stats():
    try:
        stats = {
            "version": VERSION,
            "uptime_seconds": round(time.time() - START_TIME, 1),
            "firestore_status": "connected" if get_firestore() else "unavailable",
            "gemini_status": "configured" if GEMINI_API_KEY else "not configured",
            "rate_limiter": {
                "active_users": len(rate_limiter._user_hour),
                "global_requests_last_min": len(rate_limiter._global),
            },
            "cache": {
                "subscriptions_cached": len(_cache["subscriptions"]),
                "usage_cached": len(_cache["usage"]),
            },
        }

        # Try to get Firestore stats
        db = get_firestore()
        if db:
            try:
                # Get recent usage count (last 24h)
                from datetime import datetime, timezone, timedelta
                yesterday = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
                docs = list(db.collection("ai_usage")
                           .where("timestamp", ">=", yesterday)
                           .limit(1000)
                           .stream())
                stats["usage_last_24h"] = len(docs)

                total_cost = sum(d.to_dict().get("cost_estimate", 0) for d in docs)
                stats["cost_last_24h"] = round(total_cost, 4)
            except Exception as exc:
                stats["usage_last_24h"] = "error: " + str(exc)

        return jsonify(stats)
    except Exception as exc:
        logger.error("Admin stats error: %s", exc)
        return jsonify({"error": "Failed to get stats"}), 500


@app.route("/admin/revenue", methods=["GET"])
@require_admin
def admin_revenue():
    try:
        revenue = {
            "tiers": {},
            "total_monthly_estimate": 0,
        }

        db = get_firestore()
        if db:
            try:
                subs = list(db.collection("subscriptions")
                           .where("status", "==", "active")
                           .stream())
                tier_counts = defaultdict(int)
                for doc in subs:
                    d = doc.to_dict()
                    tier_key = d.get("tier", "free")
                    tier_counts[tier_key] += 1

                total = 0
                for tier_key, count in tier_counts.items():
                    tier = TIERS.get(tier_key, TIERS["free"])
                    price = tier["price"] or 0
                    monthly = price * count
                    total += monthly
                    revenue["tiers"][tier_key] = {
                        "subscribers": count,
                        "price": price,
                        "monthly_revenue": monthly,
                    }
                revenue["total_monthly_estimate"] = total
            except Exception as exc:
                revenue["error"] = str(exc)
        else:
            revenue["error"] = "Firestore unavailable"

        return jsonify(revenue)
    except Exception as exc:
        logger.error("Admin revenue error: %s", exc)
        return jsonify({"error": "Failed to get revenue data"}), 500


# ---------------------------------------------------------------------------
# Error Handlers
# ---------------------------------------------------------------------------

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Endpoint not found", "hint": "GET /health for API info"}), 404


@app.errorhandler(405)
def method_not_allowed(e):
    return jsonify({"error": "Method not allowed"}), 405


@app.errorhandler(500)
def internal_error(e):
    logger.error("Unhandled 500: %s", e)
    return jsonify({"error": "Internal server error"}), 500


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Canonical Signal Validation (Owner Directive Compliant)
# ---------------------------------------------------------------------------

def validate_canonical_signal(signal: dict) -> tuple:
    """Validate a canonical signal object. Returns (is_valid, issues_list)."""
    issues = []
    
    # Required fields
    required = ["asset", "signal", "confidence", "entry_zone", "stop_loss", "take_profit", "timeframe"]
    for field in required:
        if field not in signal or signal[field] is None:
            issues.append(f"Missing required field: {field}")
    
    # Signal must be BUY/SELL/HOLD
    sig = signal.get("signal", "")
    if sig not in ("BUY", "SELL", "HOLD"):
        issues.append(f"Invalid signal direction: {sig} (must be BUY/SELL/HOLD)")
    
    # Confidence 0-1
    conf = signal.get("confidence", -1)
    if not isinstance(conf, (int, float)) or conf < 0 or conf > 1:
        issues.append(f"Invalid confidence: {conf} (must be 0-1)")
    
    # Entry zone
    ez = signal.get("entry_zone", {})
    if not isinstance(ez, dict) or "min" not in ez or "max" not in ez:
        issues.append("entry_zone must have min and max")
    elif not isinstance(ez.get("min"), (int, float)) or not isinstance(ez.get("max"), (int, float)):
        issues.append("entry_zone min/max must be numeric")
    
    # Stop loss
    sl = signal.get("stop_loss", None)
    if not isinstance(sl, (int, float)):
        issues.append(f"stop_loss must be numeric, got {type(sl)}")
    
    # Take profit array
    tp = signal.get("take_profit", [])
    if not isinstance(tp, list) or len(tp) < 1:
        issues.append("take_profit must be non-empty array")
    else:
        for i, t in enumerate(tp):
            if not isinstance(t, (int, float)):
                issues.append(f"take_profit[{i}] must be numeric")
    
    # Asset non-empty
    if not signal.get("asset"):
        issues.append("asset is empty")
    
    # Risk rating
    rr = signal.get("risk_rating", "")
    if rr not in ("low", "medium", "high"):
        issues.append(f"Invalid risk_rating: {rr}")
    
    return (len(issues) == 0, issues)


def format_telegram_signal(signal: dict) -> str:
    """Format canonical signal into Telegram-ready message (v4.0 — OSINT/CA fields included)."""
    # Handle NO_SIGNAL case — never format a broadcast for no-signal events
    if signal.get("no_signal") or signal.get("signal", "").upper() == "NO_SIGNAL":
        return ""  # Empty string = caller must NOT broadcast

    direction_emoji = {"BUY": "🟢", "SELL": "🔴", "HOLD": "🟡"}.get(signal.get("signal", ""), "⚪")
    risk_emoji = {"low": "🟢", "medium": "🟡", "high": "🔴"}.get(signal.get("risk_rating", ""), "⚪")
    regime = signal.get("regime", "")
    regime_str = f" · {regime}" if regime and regime != "UNKNOWN" else ""
    setup_quality = signal.get("setup_quality", "STANDARD")
    quality_emoji = {"INSTITUTIONAL": "🏦", "STANDARD": "📊", "WEAK": "⚠️"}.get(setup_quality, "📊")
    macro_context = signal.get("macro_context", "")
    confluence_pct = int(float(signal.get("confluence_score", 0)) * 100)
    freshness = signal.get("freshness_date", "")

    ez = signal.get("entry_zone", {})
    tps = signal.get("take_profit", [0, 0, 0])
    conf_pct = int(signal.get("confidence", 0) * 100)

    tp1 = f"${tps[0]:,.2f}" if len(tps) > 0 else "N/A"
    tp2 = f"${tps[1]:,.2f}" if len(tps) > 1 else "N/A"
    tp3 = f"${tps[2]:,.2f}" if len(tps) > 2 else "N/A"
    entry_min = f"${ez.get('min', 0):,.2f}"
    entry_max = f"${ez.get('max', 0):,.2f}"
    sl = f"${signal.get('stop_loss', 0):,.2f}"

    msg = (
        f"🔱 *TITAN SOVEREIGN SIGNAL* 🔱\n"
        f"📅 {freshness}\n\n"
        f"{direction_emoji} *{signal.get('signal', 'N/A')}* — {signal.get('asset', 'N/A')}\n"
        f"⏱ Timeframe: {signal.get('timeframe', 'N/A')}{regime_str}\n"
        f"📊 Confidence: *{conf_pct}%* | Confluence: *{confluence_pct}%*\n"
        f"{quality_emoji} Setup: {setup_quality} | Risk: {risk_emoji} {signal.get('risk_rating', 'N/A').upper()}\n\n"
        f"📍 *Entry Zone:* {entry_min} – {entry_max}\n"
        f"🛑 *Stop Loss:* {sl}\n\n"
        f"🎯 *Take Profit:*\n"
        f"  TP1: {tp1}\n"
        f"  TP2: {tp2}\n"
        f"  TP3: {tp3}\n\n"
    )
    if macro_context:
        msg += f"🌐 *Macro:* {macro_context}\n\n"
    msg += (
        f"📝 {signal.get('rationale', '')}\n\n"
        f"⚠️ _Not financial advice. Trade at your own risk._\n"
        f"🆔 `{signal.get('signal_id', '')}`\n"
        f"_Powered by TITAN AI · Aranya Genesis Corp_"
    )
    return msg


if __name__ == "__main__":
    logger.info("Starting TITAN AI Governance Backend v%s on port %s", VERSION, PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False)

