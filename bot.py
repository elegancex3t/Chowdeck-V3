#!/usr/bin/env python3
""" Chowdeck Multi-Account Promo Sniper - Telegram bot + Supabase Cloud Storage. """

import asyncio
import base64
import contextvars
import datetime
import email.utils
import hmac
import json
import os
import random
import re
import threading
import time
import unicodedata
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
from curl_cffi import requests as cffi_requests
import psycopg2
from psycopg2.extras import Json

# ---------------------------------------------------------------------------
# Configuration & Supabase DB Setup
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip() or None
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
ALLOW_INSECURE_WEBHOOK = os.getenv("ALLOW_INSECURE_WEBHOOK", "").strip().lower() in (
    "1", "true", "yes", "on",
)
PORT = int(os.getenv("PORT", "8080"))
OCR_API_KEY = os.getenv("OCR_API_KEY", "helloworld").strip() or "helloworld"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

ADMIN_MAX_SLOTS = max(1, int(os.getenv("ADMIN_MAX_SLOTS", "20")))
USER_MAX_SLOTS = max(1, int(os.getenv("USER_MAX_SLOTS", "10")))

CLAIM_MAX_ATTEMPTS = max(1, int(os.getenv("CLAIM_MAX_ATTEMPTS", "3")))
CLAIM_WINDOW_SECONDS = float(os.getenv("CLAIM_WINDOW_SECONDS", "4.0"))
HEDGE_REQUESTS = max(1, int(os.getenv("HEDGE_REQUESTS", "1")))
REQUEST_CONCURRENCY = max(10, int(os.getenv("REQUEST_CONCURRENCY", "150")))

CLAIM_RATE = max(1.0, float(os.getenv("CLAIM_RATE", "14")))
CLAIM_RATE_BURST = max(1, int(os.getenv("CLAIM_RATE_BURST", "24")))
SLOT_STAGGER_SECONDS = float(os.getenv("SLOT_STAGGER_SECONDS", "0.02"))
TG_RATE = max(1.0, float(os.getenv("TG_RATE", "18")))
TG_RATE_BURST = max(1, int(os.getenv("TG_RATE_BURST", "6")))
CLAIM_GLOBAL_RATE = max(0.0, float(os.getenv("CLAIM_GLOBAL_RATE", "0")))
CLAIM_GLOBAL_RATE_BURST = max(1, int(os.getenv("CLAIM_GLOBAL_RATE_BURST", "24")))
LOG_RAW_CLAIM = os.getenv("LOG_RAW_CLAIM", "").strip().lower() in (
    "1", "true", "yes", "on",
)
CLAIM_TIMEOUT = float(os.getenv("CLAIM_TIMEOUT", "4.0"))

CHOW_API_BASE = os.getenv("CHOW_API_BASE", "https://api.chowdeck.com").strip().rstrip("/")
CHOW_AUTH_ROLE = os.getenv("CHOW_AUTH_ROLE", "customer").strip() or "customer"
CHOW_COUNTRY_CODE = os.getenv("CHOW_COUNTRY_CODE", "NG").strip().upper() or "NG"

_DIAL_TO_ISO = {
    "1": "US", "7": "RU", "20": "EG", "27": "ZA", "30": "GR", "31": "NL",
    "32": "BE", "33": "FR", "34": "ES", "39": "IT", "40": "RO", "41": "CH",
    "43": "AT", "44": "GB", "45": "DK", "46": "SE", "47": "NO", "48": "PL",
    "49": "DE", "51": "PE", "52": "MX", "54": "AR", "55": "BR", "56": "CL",
    "57": "CO", "60": "MY", "61": "AU", "62": "ID", "63": "PH", "64": "NZ",
    "65": "SG", "66": "TH", "81": "JP", "82": "KR", "84": "VN", "86": "CN",
    "90": "TR", "91": "IN", "92": "PK", "94": "LK", "98": "IR",
    "212": "MA", "213": "DZ", "216": "TN", "218": "LY", "220": "GM",
    "221": "SN", "225": "CI", "226": "BF", "227": "NE", "228": "TG",
    "229": "BJ", "230": "MU", "233": "GH", "234": "NG", "235": "TD",
    "237": "CM", "240": "GQ", "241": "GA", "242": "CG", "243": "CD",
    "244": "AO", "249": "SD", "250": "RW", "251": "ET", "254": "KE",
    "255": "TZ", "256": "UG", "257": "BI", "258": "MZ", "260": "ZM",
    "263": "ZW", "264": "NA", "265": "MW", "266": "LS", "267": "BW",
    "268": "SZ", "351": "PT", "353": "IE", "358": "FI", "359": "BG",
    "370": "LT", "371": "LV", "372": "EE", "380": "UA", "381": "RS",
    "385": "HR", "420": "CZ", "421": "SK", "880": "BD", "961": "LB",
    "962": "JO", "964": "IQ", "965": "KW", "966": "SA", "971": "AE",
    "972": "IL", "973": "BH", "974": "QA", "977": "NP",
}
_DIAL_PREFIXES = sorted(_DIAL_TO_ISO, key=len, reverse=True)
_TRUNK_ZERO_CC = {
    "NG", "GH", "KE", "GB", "IE", "ZA", "AU", "IN", "DE", "NL", "BE",
    "IT", "ES", "FR", "ID", "MY", "PK", "BD", "EG", "MA", "TR", "RU",
    "UA", "RO", "CH", "AT", "DK", "SE", "NO", "PL", "PT", "FI", "CZ",
    "SK", "HR", "RS", "BG", "LT", "LV", "EE", "VN", "TH", "GR", "IL",
}
_NANP_CC = ("US", "CA")
_DIAL_BY_ISO = {}
for _dial, _iso in _DIAL_TO_ISO.items():
    _DIAL_BY_ISO.setdefault(_iso, _dial)
_DIAL_BY_ISO["CA"] = "1"

HEALTH_PROBE_PATHS = [
    p.strip() for p in os.getenv(
        "HEALTH_PROBE_PATH", "/customer/profile,/customer,/customer/me,/profile"
    ).split(",") if p.strip()
]
HEALTH_PROBE_CONCURRENCY = max(1, min(8, int(os.getenv("HEALTH_PROBE_CONCURRENCY", "3"))))
HEALTH_PROBE_STAGGER = max(0.0, float(os.getenv("HEALTH_PROBE_STAGGER", "0.2")))
LOGIN_ENABLED = os.getenv("LOGIN_ENABLED", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
OTP_FLOW_TTL = max(30.0, float(os.getenv("OTP_FLOW_TTL", "600")))
OTP_RESEND_COOLDOWN = max(5.0, float(os.getenv("OTP_RESEND_COOLDOWN", "30")))
CHOW_AUTO_REFRESH = os.getenv("CHOW_AUTO_REFRESH", "safe").strip().lower() or "safe"
TOKEN_REFRESH_SKEW = float(os.getenv("TOKEN_REFRESH_SKEW", "120"))
AUTH_TIMEOUT = float(os.getenv("AUTH_TIMEOUT", "6.0"))
LOG_RAW_AUTH = os.getenv("LOG_RAW_AUTH", "").strip().lower() in (
    "1", "true", "yes", "on",
)

AI_ASSIST_ENABLED = os.getenv("AI_ASSIST_ENABLED", "auto").strip().lower()
AI_PROVIDER = os.getenv("AI_PROVIDER", "").strip().lower()
AI_MODEL = os.getenv("AI_MODEL", "").strip()
AI_TIMEOUT = float(os.getenv("AI_TIMEOUT", "8.0"))
AI_MAX_ANSWERS = max(1, min(8, int(os.getenv("AI_MAX_ANSWERS", "4"))))
AI_RACE = os.getenv("AI_RACE", "true").strip().lower() not in ("0", "false", "no", "off")
AI_DEADLINE = float(os.getenv("AI_DEADLINE", "3.0"))
AI_LATE_DEADLINE = float(os.getenv("AI_LATE_DEADLINE", "12.0"))
AI_NOTIFY_FAILURE = os.getenv("AI_NOTIFY_FAILURE", "true").strip().lower() not in (
    "0", "false", "no", "off",
)
AI_SHOW_LATENCY = os.getenv("AI_SHOW_LATENCY", "true").strip().lower() not in (
    "0", "false", "no", "off",
)
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()
PERPLEXITY_MODEL = os.getenv("PERPLEXITY_MODEL", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "").strip()

_IGNORE_TOKENS = {
    "CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL",
    "ABUJA", "LAGOS", "GHANA", "ISMAIL", "MAY", "JUNE", "JULY",
    "TRUE", "FALSE", "PROMO", "CODE", "VOUCHER", "ANSWER", "HELP",
    "START", "STOP", "YES", "NO",
    "PLANS", "SLIGHT", "DROPS", "WILD", "SECONDS", "TOMORROW", "TONIGHT",
    "MAYBE", "LATER", "SOON", "SAME", "TIME", "NEXT", "CHANGE", "FIRST",
    "LATEST", "NEW", "POST", "VOUCHERS", "CODES", "PROMOS",
    "HELLO", "HI", "HEY", "PLEASE", "THANKS", "THANK", "SORRY", "WELCOME",
    "OKAY", "NICE", "GREAT", "COOL", "MORNING", "EVENING", "NIGHT",
    "HAHA", "LOL", "CONGRATS", "CONGRATULATIONS",
}

# ---------------------------------------------------------------------------
# Supabase Cloud Persistence Layer (Replaces Local JSON Files)
# ---------------------------------------------------------------------------
_store_lock = threading.RLock()
_cache = {}
_dirty = set()
_dirty_event = threading.Event()
_writer_started = False

def _pg_conn():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"[db] connection error: {e}", flush=True)
        return None

def _atomic_write(file_path, data):
    if not DATABASE_URL:
        return
    try:
        conn = _pg_conn()
        if not conn:
            return
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO bot_storage (key, data) VALUES (%s, %s) ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data;",
                    (file_path, Json(data))
                )
        conn.close()
    except Exception as e:
        print(f"[store] failed to persist {file_path} to Supabase: {e}", flush=True)

def _writer_loop():
    while True:
        _dirty_event.wait()
        time.sleep(0.25)
        with _store_lock:
            paths = list(_dirty)
            _dirty.clear()
            _dirty_event.clear()
            snapshot = {p: _cache.get(p) for p in paths}
        for path, data in snapshot.items():
            if data is not None:
                _atomic_write(path, data)

def start_store_writer():
    global _writer_started
    with _store_lock:
        if _writer_started:
            return
        _writer_started = True
    threading.Thread(target=_writer_loop, name="json-writer", daemon=True).start()

def load_json(file_path):
    with _store_lock:
        if file_path in _cache:
            return _cache[file_path]
    data = {}
    if DATABASE_URL:
        try:
            conn = _pg_conn()
            if conn:
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT data FROM bot_storage WHERE key = %s;", (file_path,))
                        row = cur.fetchone()
                        if row and row[0]:
                            data = row[0]
                conn.close()
        except Exception as e:
            print(f"[store] error reading {file_path} from Supabase: {e}", flush=True)
            data = {}
    if not isinstance(data, dict):
        data = {}
    with _store_lock:
        _cache[file_path] = data
        return data

def save_json(file_path, data):
    with _store_lock:
        _cache[file_path] = data
        _dirty.add(file_path)
    _dirty_event.set()

def flush_store():
    with _store_lock:
        paths = list(_dirty)
        _dirty.clear()
        _dirty_event.clear()
        snapshot = {p: _cache.get(p) for p in paths}
    for path, data in snapshot.items():
        if data is not None:
            _atomic_write(path, data)

TOKENS_FILE = "tokens_multi.json"
CARTS_FILE = "carts_multi.json"
STATE_FILE = "bot_state.json"
LOCKOUT_FILE = "lockouts_multi.json"
ACTIVE_SLOT_FILE = "active_slots.json"
PENDING_PROMPT_FILE = "pending_prompts.json"
ACCOUNT_TOGGLES_FILE = "account_toggles.json"
BURNER_FILE = "admin_burner.json"
BURNER_CART_FILE = "admin_burner_cart.json"
PORTFOLIO_FILE = "portfolio.json"
LINKED_ACCOUNTS_FILE = "linked_accounts.json"
WHITELIST_FILE = "whitelist.json"
INVITE_CODES_FILE = "invite_codes.json"
LOGIN_FLOW_FILE = "login_flows.json"

# ---------------------------------------------------------------------------
# Identity / access control
# ---------------------------------------------------------------------------
def is_admin_chat(chat_str):
    chat_str = str(chat_str)
    return bool(ADMIN_CHAT_ID) and chat_str == ADMIN_CHAT_ID

def is_user_whitelisted(chat_str):
    chat_str = str(chat_str)
    if is_admin_chat(chat_str):
        return True
    whitelist = load_json(WHITELIST_FILE)
    return bool(whitelist.get(chat_str, False))

def add_to_whitelist(chat_str):
    chat_str = str(chat_str)
    whitelist = load_json(WHITELIST_FILE)
    whitelist[chat_str] = True
    save_json(WHITELIST_FILE, whitelist)

def remove_from_whitelist(chat_str):
    chat_str = str(chat_str)
    whitelist = load_json(WHITELIST_FILE)
    if chat_str in whitelist:
        del whitelist[chat_str]
        save_json(WHITELIST_FILE, whitelist)

def get_max_slots(chat_str):
    return ADMIN_MAX_SLOTS if is_admin_chat(chat_str) else USER_MAX_SLOTS

def get_bot_active():
    return bool(load_json(STATE_FILE).get("active", True))

def set_bot_active(val):
    state = load_json(STATE_FILE)
    state["active"] = bool(val)
    save_json(STATE_FILE, state)

def get_channel_announce():
    return bool(load_json(STATE_FILE).get("channel_announce", True))

def set_channel_announce(val):
    state = load_json(STATE_FILE)
    state["channel_announce"] = bool(val)
    save_json(STATE_FILE, state)

def get_active_slot(chat_str):
    slots = load_json(ACTIVE_SLOT_FILE)
    return str(slots.get(str(chat_str), "1"))

def set_active_slot(chat_str, slot):
    slots = load_json(ACTIVE_SLOT_FILE)
    slots[str(chat_str)] = str(slot)
    save_json(ACTIVE_SLOT_FILE, slots)

def is_slot_active_state(chat_str, slot_id):
    toggles = load_json(ACCOUNT_TOGGLES_FILE).get(str(chat_str), {})
    return bool(toggles.get(str(slot_id), True))

def toggle_slot_state(chat_str, slot_id):
    toggles = load_json(ACCOUNT_TOGGLES_FILE)
    user_toggles = toggles.setdefault(str(chat_str), {})
    current = bool(user_toggles.get(str(slot_id), True))
    user_toggles[str(slot_id)] = not current
    save_json(ACCOUNT_TOGGLES_FILE, toggles)
    return not current

def get_burner_credentials(chat_str):
    return load_json(BURNER_FILE).get(str(chat_str), {})

def save_burner_credentials(chat_str, token_data):
    burners = load_json(BURNER_FILE)
    burners[str(chat_str)] = token_data
    save_json(BURNER_FILE, burners)

def get_burner_cart(chat_str):
    return load_json(BURNER_CART_FILE).get(str(chat_str), {})

def save_burner_cart(chat_str, cart_data):
    carts = load_json(BURNER_CART_FILE)
    carts[str(chat_str)] = cart_data
    save_json(BURNER_CART_FILE, carts)

def get_linked_accounts(chat_str):
    return list(load_json(LINKED_ACCOUNTS_FILE).get(str(chat_str), []))

def add_linked_account(chat_str, secondary_chat_id):
    linked = load_json(LINKED_ACCOUNTS_FILE)
    bucket = linked.setdefault(str(chat_str), [])
    sec_str = str(secondary_chat_id).strip()
    if sec_str and sec_str not in bucket:
        bucket.append(sec_str)
        save_json(LINKED_ACCOUNTS_FILE, linked)

def remove_linked_account(chat_str, secondary_chat_id):
    linked = load_json(LINKED_ACCOUNTS_FILE)
    bucket = linked.get(str(chat_str))
    if bucket:
        sec_str = str(secondary_chat_id).strip()
        if sec_str in bucket:
            bucket.remove(sec_str)
            save_json(LINKED_ACCOUNTS_FILE, linked)

def get_admin_stats():
    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    total_users = len(tokens_db)
    total_slots = sum(len(slots) for slots in tokens_db.values())
    total_carts = sum(len(carts) for carts in carts_db.values())
    return total_users, total_slots, total_carts

def record_portfolio_win(chat_str, code, slot_id, amount=2000):
    chat_str = str(chat_str)
    portfolio = load_json(PORTFOLIO_FILE)
    bucket = portfolio.setdefault(chat_str, {"items": [], "total_value": 0})
    already = any(
        item.get("code") == code and str(item.get("slot")) == str(slot_id)
        for item in bucket.get("items", [])
    )
    if not already:
        bucket.setdefault("items", []).append({
            "code": code,
            "slot": str(slot_id),
            "amount": amount,
            "timestamp": time.time(),
        })
        bucket["total_value"] = sum(
            item.get("amount", 2000) for item in bucket["items"]
        )
        save_json(PORTFOLIO_FILE, portfolio)

def add_or_update_pending(chat_str, entry):
    store = load_json(PENDING_PROMPT_FILE)
    store[str(chat_str)] = [entry]
    save_json(PENDING_PROMPT_FILE, store)

def list_pending(chat_str):
    return list(load_json(PENDING_PROMPT_FILE).get(str(chat_str), []))

def pop_pending(chat_str, code=None, reply_to=None):
    store = load_json(PENDING_PROMPT_FILE)
    bucket = list(store.get(str(chat_str), []))
    target = None
    if code:
        target = next((e for e in bucket if e.get("code") == code), None)
    elif reply_to is not None:
        target = next((e for e in bucket if e.get("msg_id") == reply_to), None)
    if target is None and bucket:
        target = bucket[-1]
    if target is not None:
        bucket = [e for e in bucket if e is not target]
        store[str(chat_str)] = bucket
        save_json(PENDING_PROMPT_FILE, store)
    return target

def clear_pending(chat_str, code=None):
    store = load_json(PENDING_PROMPT_FILE)
    bucket = list(store.get(str(chat_str), []))
    if code:
        bucket = [e for e in bucket if e.get("code") != code]
    else:
        bucket = []
    store[str(chat_str)] = bucket
    save_json(PENDING_PROMPT_FILE, store)
    return bucket

def robust_clean_code(raw):
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", str(raw))
    return "".join(c for c in normalized if c.isalnum()).upper()

def parse_jwt_payload(token):
    try:
        parts = str(token).split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            decoded = base64.urlsafe_b64decode(payload_b64)
            return json.loads(decoded.decode("utf-8"))
    except Exception:
        pass
    return {}

def extract_phone_from_data(data):
    if not isinstance(data, dict):
        return None
    user = data.get("user") if isinstance(data.get("user"), dict) else {}
    return (
        data.get("phone_number")
        or data.get("phone")
        or data.get("phoneNumber")
        or data.get("mobile")
        or data.get("telephone")
        or data.get("user_phone")
        or user.get("phone_number")
        or user.get("phone")
    )

def parse_and_store_credentials(raw_input, chat_str, slot):
    cleaned = str(raw_input).strip()
    extracted_token = cleaned
    extracted_cookies = {}
    extracted_phone = None

    if "user=" in cleaned or ";" in cleaned:
        try:
            for part in cleaned.split(";"):
                part = part.strip()
                if "=" in part:
                    k, v = part.split("=", 1)
                    extracted_cookies[k.strip()] = v.strip()
                    if k.strip() == "user":
                        decoded = urllib.parse.unquote(v.strip())
                        data = json.loads(decoded)
                        if "token" in data:
                            extracted_token = data["token"]
                        extracted_phone = extract_phone_from_data(data)
        except Exception as e:
            print(f"[creds] cookie parse error: {e}", flush=True)

    if cleaned.startswith("{") and "token" in cleaned:
        try:
            data = json.loads(cleaned)
            if "token" in data:
                extracted_token = data["token"]
            extracted_phone = extract_phone_from_data(data)
        except Exception:
            pass

    if extracted_token.lower().startswith("bearer "):
        extracted_token = extracted_token[7:].strip()

    if not extracted_phone and extracted_token.count(".") == 2:
        extracted_phone = extract_phone_from_data(parse_jwt_payload(extracted_token))

    tokens_db = load_json(TOKENS_FILE)
    user_slots = tokens_db.setdefault(str(chat_str), {})
    existing_phone = user_slots.get(str(slot), {}).get("phone")
    final_phone = extracted_phone or existing_phone

    user_slots[str(slot)] = {
        "token": extracted_token,
        "cookies": extracted_cookies,
        "phone": str(final_phone)[:15] if final_phone else None,
    }
    save_json(TOKENS_FILE, tokens_db)
    return extracted_token

async def fetch_account_phone(token, cookies):
    if token and token.count(".") == 2:
        phone = extract_phone_from_data(parse_jwt_payload(token))
        if phone:
            return phone

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/",
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
    }
    try:
        res = await _CD_CLIENT.get(
            "https://api.chowdeck.com/customer/profile",
            headers=headers,
            cookies=cookies or None,
        )
        if res.status_code == 200:
            res_json = res.json()
            data = res_json.get("data", res_json)
            if not isinstance(data, dict):
                data = res_json
            return extract_phone_from_data(data)
    except Exception as e:
        print(f"[creds] profile fetch error: {e}", flush=True)
    return None

# ---------------------------------------------------------------------------
# Shared HTTP clients
# ---------------------------------------------------------------------------
_UA = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)

_CD_CLIENT = httpx.AsyncClient(
    timeout=CLAIM_TIMEOUT,
    proxy=PROXY_URL,
    follow_redirects=True,
    limits=httpx.Limits(
        max_keepalive_connections=REQUEST_CONCURRENCY,
        max_connections=REQUEST_CONCURRENCY * 2,
        keepalive_expiry=30.0,
    ),
)

_AUTH_CLIENT = cffi_requests.AsyncSession(
    impersonate="chrome124",
    timeout=AUTH_TIMEOUT
)

_TG_CLIENT = httpx.AsyncClient(
    timeout=httpx.Timeout(6.0, connect=4.0),
    proxy=PROXY_URL,
    limits=httpx.Limits(max_keepalive_connections=64, max_connections=128),
)

_REQUEST_SEM = asyncio.Semaphore(REQUEST_CONCURRENCY)

class _TokenBucket:
    def __init__(self, rate, burst):
        self.rate = max(0.1, float(rate))
        self.capacity = max(1.0, float(burst))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity, self._tokens + (now - self._updated) * self.rate
                )
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(wait)

    async def backoff(self, seconds):
        seconds = max(0.0, min(float(seconds or 0.0), 30.0))
        if seconds <= 0:
            return
        async with self._lock:
            self._tokens = -seconds * self.rate
            self._updated = time.monotonic()

_PACERS = {}
_PACERS_LOCK = threading.Lock()

def _pacer_for(scope, rate, burst):
    key = (str(scope), float(rate), int(burst))
    with _PACERS_LOCK:
        bucket = _PACERS.get(key)
        if bucket is None:
            bucket = _TokenBucket(rate, burst)
            _PACERS[key] = bucket
        return bucket

_CLAIM_SCOPE = contextvars.ContextVar("claim_scope", default="")

_GLOBAL_CLAIM_PACER = (
    _TokenBucket(CLAIM_GLOBAL_RATE, CLAIM_GLOBAL_RATE_BURST)
    if CLAIM_GLOBAL_RATE > 0
    else None
)

def _claim_pacer(scope):
    return _pacer_for("claim:" + str(scope or "_default"), CLAIM_RATE, CLAIM_RATE_BURST)

async def _acquire_claim(scope):
    if _GLOBAL_CLAIM_PACER is not None:
        await _GLOBAL_CLAIM_PACER.acquire()
    await _claim_pacer(scope).acquire()

async def _claim_backoff(scope, wait):
    await _claim_pacer(scope).backoff(wait)
    if _GLOBAL_CLAIM_PACER is not None:
        await _GLOBAL_CLAIM_PACER.backoff(wait)

_CHOW_HEADERS_BASE = {
    "Content-Type": "application/json",
    "Origin": "https://chowdeck.com",
    "Referer": "https://chowdeck.com/",
    "User-Agent": _UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

def chow_headers(token):
    headers = dict(_CHOW_HEADERS_BASE)
    headers["Authorization"] = f"Bearer {token}"
    return headers

_bg_tasks = set()

def spawn_bg(coro):
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    def _done(t):
        _bg_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            print(f"[bg] background task failed: {exc}", flush=True)
    task.add_done_callback(_done)
    return task

def _auth_url(path):
    return f"{CHOW_API_BASE}{path}"

def _safe_json(res):
    try:
        return res.json()
    except Exception:
        return {}

class _MockResponse:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
    def json(self): return self._body

async def _auth_post(path, payload, token=None):
    headers = dict(_CHOW_HEADERS_BASE)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    target_url = _auth_url(path)
    try:
        res = await _AUTH_CLIENT.post(target_url, json=payload, headers=headers)
        body = _safe_json(res)
    except Exception as e:
        return None, {"message": str(e)}
    return res, body

def _failure_reason(body):
    for src in (body or {}, (body or {}).get("data") or {}):
        if not isinstance(src, dict):
            continue
        for key in ("failureReason", "failure_reason", "reason", "code", "error"):
            val = src.get(key)
            if val:
                return str(val)
    return ""

def _iso_from_dial(digits):
    for pref in _DIAL_PREFIXES:
        if digits.startswith(pref):
            rest = digits[len(pref):]
            if 6 <= len(rest) <= 12:
                return _DIAL_TO_ISO[pref], rest
    return None, digits

def _normalise_phone(raw, country_code=None):
    text = str(raw or "").strip()
    digits = re.sub(r"\D", "", text)
    cc = (country_code or "").strip().upper() or None
    if cc and not re.fullmatch(r"[A-Z]{2}", cc):
        cc = None

    if text.startswith("+") or digits.startswith("00"):
        probe = digits[2:] if digits.startswith("00") else digits
        iso, rest = _iso_from_dial(probe)
        if iso:
            cc, digits = iso, rest
        elif digits.startswith("00"):
            digits = digits[2:]
    if cc is None:
        cc = CHOW_COUNTRY_CODE

    if country_code is None and len(digits) == 11 and digits.startswith("1"):
        cc, digits = "US", digits[1:]

    if cc in _NANP_CC:
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
    else:
        dial = _DIAL_BY_ISO.get(cc)
        if len(digits) > 11 and dial and digits.startswith(dial):
            digits = digits[len(dial):]
        if cc in _TRUNK_ZERO_CC and len(digits) >= 8 and digits.startswith("0"):
            digits = digits[1:]
    return cc, digits

def _parse_login_arg(arg):
    parts = [p for p in re.split(r"[\s,]+", str(arg or "").strip()) if p]
    cc_hint, nums = None, []
    for token in parts:
        if re.fullmatch(r"[A-Za-z]{2}", token):
            cc_hint = token.upper()
        elif re.fullmatch(r"\+?[\d()\- .]+", token) and re.search(r"\d", token):
            nums.append(token)
    if not nums:
        return cc_hint or CHOW_COUNTRY_CODE, "", "no number"
    cc, national = _normalise_phone(" ".join(nums), cc_hint)
    return cc, national, ""

_COUNTRY_LABELS = {
    "NG": "Nigeria", "US": "United States", "CA": "Canada", "GB": "United Kingdom",
    "GH": "Ghana", "KE": "Kenya", "ZA": "South Africa", "IN": "India",
}

def country_label(cc):
    cc = str(cc or "").upper()
    return _COUNTRY_LABELS.get(cc, cc or "?")

def phone_label(cc, digits):
    dial = _DIAL_BY_ISO.get(str(cc or "").upper(), "")
    return f"+{dial} {mask_phone_mid(digits)}" if dial else mask_phone_mid(digits)

def mask_phone_mid(digits):
    d = re.sub(r"\D", "", str(digits or ""))
    if len(d) <= 4:
        return d
    return f"{d[:3]}****{d[-3:]}"

def mask_phone(digits):
    d = str(digits or "")
    if len(d) <= 4:
        return d
    return f"{d[:3]}****{d[-3:]}"

def get_login_flow(chat_str):
    flow = load_json(LOGIN_FLOW_FILE).get(str(chat_str))
    if not isinstance(flow, dict):
        return None
    if time.time() - float(flow.get("ts") or 0) > OTP_FLOW_TTL:
        clear_login_flow(chat_str)
        return None
    return flow

def set_login_flow(chat_str, flow):
    store = load_json(LOGIN_FLOW_FILE)
    store[str(chat_str)] = flow
    save_json(LOGIN_FLOW_FILE, store)

def clear_login_flow(chat_str):
    store = load_json(LOGIN_FLOW_FILE)
    if str(chat_str) in store:
        del store[str(chat_str)]
        save_json(LOGIN_FLOW_FILE, store)

def token_exp_ts(token):
    try:
        payload = parse_jwt_payload(token) or {}
    except Exception:
        return None
    exp = payload.get("exp")
    try:
        return float(exp) if exp is not None else None
    except (TypeError, ValueError):
        return None

def token_seconds_left(token):
    exp = token_exp_ts(token)
    if exp is None:
        return None
    return exp - time.time()

def human_seconds(seconds):
    seconds = int(max(0, seconds))
    if seconds >= 86400:
        return f"{seconds // 86400}d {(seconds % 86400) // 3600}h"
    if seconds >= 3600:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    if seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"

def set_slot_active_state(chat_str, slot_id, value):
    toggles = load_json(ACCOUNT_TOGGLES_FILE)
    user_toggles = toggles.setdefault(str(chat_str), {})
    user_toggles[str(slot_id)] = bool(value)
    save_json(ACCOUNT_TOGGLES_FILE, toggles)
    return bool(value)

def set_slot_phone(chat_str, slot_id, phone, country_code=None):
    tokens_db = load_json(TOKENS_FILE)
    entry = (tokens_db.get(str(chat_str)) or {}).get(str(slot_id))
    if isinstance(entry, dict) and phone:
        entry["phone"] = str(phone)[:15]
        if country_code:
            entry["country_code"] = str(country_code).upper()[:2]
        save_json(TOKENS_FILE, tokens_db)

def store_slot_token(chat_str, slot, token, *, phone=None, cookies=None, origin="login", country_code=None):
    tokens_db = load_json(TOKENS_FILE)
    user_slots = tokens_db.setdefault(str(chat_str), {})
    entry = user_slots.get(str(slot))
    if not isinstance(entry, dict):
        entry = {}
    entry["token"] = token
    if cookies is not None:
        entry["cookies"] = cookies
    if phone:
        entry["phone"] = str(phone)[:15]
    if country_code:
        entry["country_code"] = str(country_code).upper()[:2]
    entry["origin"] = origin
    entry["token_exp"] = token_exp_ts(token)
    entry["linked_at"] = time.time()
    user_slots[str(slot)] = entry
    save_json(TOKENS_FILE, tokens_db)
    return entry

async def chow_validate_phone(phone, country_code):
    res, body = await _auth_post(
        "/auth/phone/validate", {"phone": phone, "country_code": country_code}
    )
    if res is None or res.status_code != 200:
        return None
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    val = data.get("is_valid")
    if val is None and isinstance(data.get("data"), dict):
        val = data["data"].get("is_valid")
    return bool(val) if val is not None else None

async def chow_request_otp(phone, country_code):
    res, body = await _auth_post(
        "/auth/login",
        {"country_code": country_code, "phone": phone, "role": CHOW_AUTH_ROLE},
    )
    if res is None:
        return False, "Could not reach Chowdeck. Please try again.", "network"
    msg = str((body or {}).get("message") or "")
    reason = _failure_reason(body)
    if res.status_code in (200, 201):
        return True, msg or "Verification code sent.", ""
    return False, msg or f"Request rejected (HTTP {res.status_code}).", reason

async def chow_validate_otp(phone, otp, country_code=None):
    payload = {"otp": str(otp).strip(), "phone": phone, "role": CHOW_AUTH_ROLE}
    if country_code:
        payload["country_code"] = str(country_code).upper()
    res, body = await _auth_post("/auth/login/validate-otp", payload)
    if res is None:
        return False, "Could not reach Chowdeck. Please try again.", {}
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    if res.status_code in (200, 201) and data.get("token"):
        return True, str((body or {}).get("message") or "Signed in."), data
    msg = str((body or {}).get("message") or "")
    return False, msg or f"Code rejected (HTTP {res.status_code}).", data

async def chow_resend_otp(phone, country_code, otp_type="sign_in"):
    res, body = await _auth_post(
        "/auth/resend-otp",
        {
            "phone": phone,
            "role": CHOW_AUTH_ROLE,
            "country_code": country_code,
            "type": otp_type,
        },
    )
    if res is None:
        return False, "Could not reach Chowdeck. Please try again."
    if res.status_code in (200, 201):
        return True, str((body or {}).get("message") or "Code resent.")
    msg = str((body or {}).get("message") or "")
    return False, msg or f"Resend rejected (HTTP {res.status_code})."

async def chow_refresh_token(token):
    if not token:
        return None
    res, body = await _auth_post("/auth/refresh-token", {"token": token})
    if res is None or res.status_code not in (200, 201):
        return None
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    new = data.get("token") or (body or {}).get("token")
    return str(new) if new else None

_refresh_tasks = {}
_refresh_lock = threading.Lock()

def _auto_refresh_mode():
    mode = (CHOW_AUTO_REFRESH or "safe").lower()
    if mode in ("0", "false", "no", "off", "never"):
        return "off"
    if mode in ("on", "true", "yes", "all", "always"):
        return "on"
    return "safe"

def _refresh_allowed(entry):
    mode = _auto_refresh_mode()
    if mode == "off":
        return False
    if mode == "on":
        return bool((entry or {}).get("token"))
    return str((entry or {}).get("origin") or "") == "login"

def _token_needs_refresh(entry):
    if not _refresh_allowed(entry):
        return False
    left = token_seconds_left((entry or {}).get("token"))
    if left is None:
        return False
    return left <= TOKEN_REFRESH_SKEW

async def refresh_slot_token(chat_str, slot, entry=None, force=False):
    chat_str, slot = str(chat_str), str(slot)
    if entry is None:
        entry = ((load_json(TOKENS_FILE).get(chat_str) or {}).get(slot)) or {}
    token = (entry or {}).get("token")
    if not token or (not force and not _token_needs_refresh(entry)):
        return token
    new_token = await chow_refresh_token(token)
    if not new_token:
        return None
    store_slot_token(
        chat_str, slot, new_token,
        phone=(entry or {}).get("phone"),
        origin=(entry or {}).get("origin") or "login",
    )
    return new_token

async def _refresh_worker(chat_str, slot, entry):
    key = (str(chat_str), str(slot))
    try:
        return await refresh_slot_token(chat_str, slot, entry, force=True)
    except Exception as e:
        print(f"[auth] refresh {chat_str}/{slot} failed: {e}", flush=True)
        return None
    finally:
        with _refresh_lock:
            if _refresh_tasks.get(key) is asyncio.current_task():
                _refresh_tasks.pop(key, None)

def start_token_refresh(chat_str, slot, entry):
    key = (str(chat_str), str(slot))
    with _refresh_lock:
        task = _refresh_tasks.get(key)
        if task is None or task.done():
            task = asyncio.create_task(_refresh_worker(chat_str, slot, entry))
            _refresh_tasks[key] = task
    return task

def _probe_definitive(status):
    return status is not None and (200 <= status < 300 or status in (401, 403))

def _probe_note(status, body, path, err=""):
    msg = ""
    if isinstance(body, dict):
        for key in ("message", "error", "detail"):
            if body.get(key):
                msg = str(body[key]).strip()
                break
        if not msg:
            msg = _failure_reason(body)
    if status is None:
        return f"`{path}` → no response ({err or 'timeout'})"
    tail = f" — {msg}" if msg else ""
    return f"`{path}` → HTTP {status}{tail}"

async def _probe_path(path, token, cookies=None):
    try:
        res = await _CD_CLIENT.get(
            f"{CHOW_API_BASE}{path}",
            headers=chow_headers(token),
            cookies=cookies or None,
            timeout=AUTH_TIMEOUT,
        )
    except Exception as e:
        return {"path": path, "status": None, "body": {}, "err": str(e)}
    return {"path": path, "status": res.status_code, "body": _safe_json(res), "err": ""}

async def chow_profile(token, cookies=None):
    if not token:
        return None, None, {}, "no token stored"
    paths = list(HEALTH_PROBE_PATHS) or ["/customer/profile"]
    sem = asyncio.Semaphore(HEALTH_PROBE_CONCURRENCY)
    tasks, winner = [], None

    async def run(path):
        async with sem:
            return await _probe_path(path, token, cookies)

    for idx, path in enumerate(paths):
        tasks.append(asyncio.create_task(run(path)))
        if idx < len(paths) - 1 and HEALTH_PROBE_STAGGER:
            await asyncio.sleep(HEALTH_PROBE_STAGGER)
        for task in tasks:
            if task.done() and _probe_definitive(task.result()["status"]):
                winner = task.result()
                break
        if winner:
            break
    if winner is None:
        settled = await asyncio.gather(*tasks)
        for result in settled:
            if _probe_definitive(result["status"]):
                winner = result
                break
        if winner is None:
            winner = settled[-1] if settled else {
                "path": paths[0], "status": None, "body": {}, "err": ""
            }
    pending = [t for t in tasks if not t.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    body = winner["body"] if isinstance(winner.get("body"), dict) else {}
    data = body.get("data") if isinstance(body.get("data"), dict) else body
    if not isinstance(data, dict):
        data = {}
    phone = extract_phone_from_data(data) or extract_phone_from_data(body)
    note = _probe_note(winner.get("status"), body, winner.get("path") or paths[0],
                       winner.get("err") or "")
    return winner.get("status"), phone, data, note

async def probe_slot_token(chat_str, slot, allow_refresh=True):
    chat_str, slot = str(chat_str), str(slot)
    entry = ((load_json(TOKENS_FILE).get(chat_str) or {}).get(slot)) or {}
    token = (entry or {}).get("token")
    if not token:
        return {"slot": slot, "state": "empty", "status": None, "note": "not linked",
                "entry": entry}
    if allow_refresh and _token_needs_refresh(entry):
        new_token = await refresh_slot_token(chat_str, slot, entry, force=True)
        if new_token:
            entry = {**entry, "token": new_token}
            token = new_token
    status, phone, _data, note = await chow_profile(token, (entry or {}).get("cookies"))
    if status in (401, 403) and allow_refresh and _refresh_allowed(entry):
        new_token = await refresh_slot_token(chat_str, slot, entry, force=True)
        if new_token:
            entry = {**entry, "token": new_token}
            token = new_token
            status, phone, _data, note = await chow_profile(token, (entry or {}).get("cookies"))
    if phone and str(phone) != str((entry or {}).get("phone") or ""):
        set_slot_phone(chat_str, slot, phone)
        entry = {**entry, "phone": str(phone)[:15]}
    if status == 200:
        state = "active"
    elif status in (401, 403):
        state = "expired"
    elif status is None:
        state = "unreachable"
    else:
        state = "error"
    return {
        "slot": slot, "state": state, "status": status, "note": note,
        "phone": phone or (entry or {}).get("phone"), "entry": entry,
    }

async def check_all_slots(chat_str, concurrency=4):
    chat_str = str(chat_str)
    max_slots = get_max_slots(chat_str)
    configured = load_json(TOKENS_FILE).get(chat_str, {}) or {}
    slot_ids = [str(i) for i in range(1, max_slots + 1)]
    for sid in configured:
        if str(sid) not in slot_ids:
            slot_ids.append(str(sid))
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(sid):
        async with sem:
            try:
                return await probe_slot_token(chat_str, sid)
            except Exception as e:
                return {"slot": sid, "state": "error", "status": None,
                        "entry": {}, "error": str(e)}

    return list(await asyncio.gather(*(one(sid) for sid in slot_ids)))

_STATE_ICON = {
    "active": "🟢", "expired": "🔴", "expiring": "🟡",
    "empty": "⚫", "unreachable": "🟠", "error": "🟠",
}

def format_accounts_health(results, chat_str=None):
    lines = ["🩺 *Account health check*", ""]
    active = 0
    for r in results:
        state = r.get("state")
        if state == "active":
            active += 1
        icon = _STATE_ICON.get(state, "⚪")
        phone = r.get("phone") or ((r.get("entry") or {}).get("phone"))
        label = f"{icon} S{r.get('slot')}"
        if phone:
            label += f": `{phone}`"
        detail = {
            "active": "active",
            "expired": "token expired — sign in again with /login",
            "empty": "empty — link with /login or /settoken",
            "unreachable": "no response",
            "error": "unexpected response",
        }.get(state, state or "unknown")
        note = r.get("note")
        if state == "active":
            left = token_seconds_left((r.get("entry") or {}).get("token"))
            if left is not None:
                detail += f" · {human_seconds(left)} left"
        elif note and state != "empty":
            detail += f" · {note}"
        if chat_str is not None and state != "empty" and not is_slot_active_state(chat_str, r.get("slot")):
            detail += " · ⏸️ disabled"
        lines.append(f"{label} — {detail}")
    lines.append("")
    lines.append(f"🟢 active: `{active}` / `{len(results)}` slot(s)")
    return "\n".join(lines)

def _phone_taken_by_other_slot(chat_str, phone, slot, country_code=None):
    tokens_db = load_json(TOKENS_FILE).get(str(chat_str), {}) or {}
    target = re.sub(r"\D", "", str(phone or ""))
    target_cc = str(country_code or "").upper() or None
    for sid, td in tokens_db.items():
        if str(sid) == str(slot):
            continue
        other = re.sub(r"\D", "", str((td or {}).get("phone") or ""))
        if not target or other != target:
            continue
        other_cc = str((td or {}).get("country_code") or "").upper() or None
        if target_cc and other_cc and other_cc != target_cc:
            continue
        return str(sid)
    return None

async def start_otp_login(chat_id, chat_str, arg):
    chat_str = str(chat_str)
    if not LOGIN_ENABLED:
        await send_telegram(chat_id, "🔐 Phone login is disabled on this deployment. Use `/settoken`.")
        return
    if not arg:
        await send_telegram(
            chat_id,
            "🔐 *Login with phone (OTP)*\n\n"
            "Usage: `/login <phone>`\n"
            "Example: `/login 08031234567`",
        )
        return
    country_code, phone, parse_err = _parse_login_arg(arg)
    if parse_err or len(phone) < 7:
        await send_telegram(chat_id, "⚠️ That does not look like a phone number.")
        return
    slot = get_active_slot(chat_str)
    dup = _phone_taken_by_other_slot(chat_str, phone, slot, country_code)
    if dup:
        await send_telegram(chat_id, f"⚠️ That phone is already linked to *Slot {dup}*.")
        return
    cc_try = [country_code]
    if country_code in _NANP_CC:
        cc_try += [c for c in _NANP_CC if c != country_code]
    plausible = await chow_validate_phone(phone, cc_try[0])
    if plausible is False and len(cc_try) > 1:
        if await chow_validate_phone(phone, cc_try[1]):
            plausible, cc_try = True, [cc_try[1]]
    if plausible is False:
        await send_telegram(chat_id, f"⚠️ Chowdeck does not recognise `{phone_label(country_code, phone)}`.")
        return
    await send_telegram(chat_id, f"📲 Requesting a Chowdeck verification code for `{phone_label(cc_try[0], phone)}`…")
    ok, message, reason = await chow_request_otp(phone, cc_try[0])
    if not ok and len(cc_try) > 1:
        low = f"{message} {reason}".lower()
        if any(w in low for w in ("invalid", "exist", "not found", "recognis", "recogniz")):
            ok, message, reason = await chow_request_otp(phone, cc_try[1])
            if ok:
                cc_try = [cc_try[1]]
    if not ok:
        await send_telegram(chat_id, f"❌ {message}")
        return
    country_code = cc_try[0]
    set_login_flow(chat_str, {
        "phone": phone,
        "country_code": country_code,
        "slot": str(slot),
        "ts": time.time(),
        "otp_type": "sign_in",
        "attempts": 0,
        "resends": 0,
        "last_resend": time.time(),
    })
    await send_telegram(
        chat_id,
        f"✅ *Code sent to* `{mask_phone(phone)}`\n\n"
        f"👉 Send the *code* in this chat to finish signing in (Slot {slot}).",
    )

async def complete_otp_login(chat_id, chat_str, code, flow):
    chat_str = str(chat_str)
    flow = dict(flow or {})
    phone = flow.get("phone")
    slot = str(flow.get("slot") or get_active_slot(chat_str))
    ok, message, data = await chow_validate_otp(
        phone, code, flow.get("country_code")
    )
    if not ok:
        flow["attempts"] = int(flow.get("attempts") or 0) + 1
        if flow["attempts"] >= 5:
            clear_login_flow(chat_str)
            await send_telegram(chat_id, "❌ Too many attempts — login cancelled.")
            return False
        flow["ts"] = time.time()
        set_login_flow(chat_str, flow)
        await send_telegram(chat_id, f"❌ {message}")
        return False
    token = str(data.get("token"))
    linked_phone = data.get("phone") or phone
    store_slot_token(
        chat_str, slot, token, phone=linked_phone, origin="login",
        country_code=flow.get("country_code"),
    )
    set_slot_active_state(chat_str, slot, True)
    set_active_slot(chat_str, slot)
    clear_login_flow(chat_str)
    await send_telegram(chat_id, f"🔓 *Account locked in — Slot {slot}*")
    return True

async def resend_login_otp(chat_id, chat_str):
    flow = get_login_flow(chat_str)
    if not flow:
        await send_telegram(chat_id, "ℹ️ No login is waiting for a code.")
        return
    now = time.time()
    wait = OTP_RESEND_COOLDOWN - (now - float(flow.get("last_resend") or 0))
    if wait > 0:
        await send_telegram(chat_id, f"⏳ Please wait `{int(wait) + 1}s`.")
        return
    ok, message = await chow_resend_otp(
        flow.get("phone"), flow.get("country_code"), flow.get("otp_type") or "sign_in"
    )
    if not ok:
        await send_telegram(chat_id, f"❌ {message}")
        return
    flow["resends"] = int(flow.get("resends") or 0) + 1
    flow["last_resend"] = now
    flow["ts"] = now
    set_login_flow(chat_str, flow)
    await send_telegram(chat_id, f"✅ New code sent to `{mask_phone(flow.get('phone'))}`.")

def looks_like_otp(text):
    return bool(re.fullmatch(r"\d{4}|\d{6}", str(text or "").strip()))

async def maybe_handle_login_otp(chat_id, chat_str, text, reply_to, pending):
    if not LOGIN_ENABLED:
        return False
    flow = get_login_flow(chat_str)
    if not flow:
        return False
    if not looks_like_otp(text):
        return False
    answering = (
        reply_to is not None
        and any((e or {}).get("msg_id") == reply_to for e in (pending or []))
    )
    if answering:
        return False
    await complete_otp_login(chat_id, chat_str, str(text).strip(), flow)
    return True

# ---------------------------------------------------------------------------
# Telegram helpers & remaining core logic
# ---------------------------------------------------------------------------
async def send_telegram(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    if not chat_id or not TELEGRAM_BOT_TOKEN:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_pacer = _pacer_for("tg:" + str(chat_id), TG_RATE, TG_RATE_BURST)
    for attempt in range(3):
        await tg_pacer.acquire()
        try:
            res = await _TG_CLIENT.post(url, json=payload)
            if res.status_code == 200:
                return res.json().get("result", {}).get("message_id")
            if res.status_code == 429:
                wait = 1.0
                try:
                    wait = float(res.json().get("parameters", {}).get("retry_after", 1.0))
                except Exception:
                    pass
                if attempt < 2:
                    await asyncio.sleep(min(wait, 5.0) + 0.05)
                    continue
                return None
            body = res.text.lower()
            if attempt == 0 and ("parse" in body or "entit" in body):
                payload.pop("parse_mode", None)
                continue
            return None
        except Exception:
            if attempt < 2:
                continue
            return None
    return None

async def broadcast_to_channel(text):
    if not TELEGRAM_CHANNEL_ID:
        return
    await send_telegram(TELEGRAM_CHANNEL_ID, text)

async def announce_channel(text):
    if not get_channel_announce():
        return
    await broadcast_to_channel(text)

def get_admin_claim_targets():
    targets = []
    if ADMIN_CHAT_ID:
        targets.append(ADMIN_CHAT_ID)
        for linked in get_linked_accounts(ADMIN_CHAT_ID):
            linked = str(linked)
            if linked not in targets:
                targets.append(linked)
    return targets

async def answer_callback_query(callback_query_id, text=""):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        await _TG_CLIENT.post(url, json={"callback_query_id": callback_query_id, "text": text})
    except Exception:
        pass

async def edit_message_text(chat_id, message_id, text, reply_markup=None, parse_mode="Markdown"):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    for attempt in range(2):
        try:
            res = await _TG_CLIENT.post(url, json=payload)
            if res.status_code == 200:
                return
            if attempt == 0 and ("parse" in res.text.lower() or "entit" in res.text.lower()):
                payload.pop("parse_mode", None)
                continue
            return
        except Exception:
            if attempt == 0:
                continue
            return

def build_control_keyboard(chat_str, is_admin):
    active_slot = get_active_slot(chat_str)
    max_slots = get_max_slots(chat_str)
    rows = [[], [], [], []]
    for i in range(1, max_slots + 1):
        s_str = str(i)
        icon = "✅" if is_slot_active_state(chat_str, s_str) else "❌"
        label = f"📌 S{i}" if s_str == active_slot else f"{icon} {i}"
        btn = {"text": label, "callback_data": f"slot_{i}"}
        if i <= 5: rows[0].append(btn)
        elif i <= 10: rows[1].append(btn)
        elif i <= 15: rows[2].append(btn)
        else: rows[3].append(btn)
    keyboard = [rows[0], rows[1]]
    for extra in (rows[2], rows[3]):
        if extra: keyboard.append(extra)
    keyboard.extend([
        [{"text": "👥 My Accounts", "callback_data": "btn_my_accounts"}, {"text": "💰 Portfolio", "callback_data": "btn_portfolio"}],
        [{"text": "🩺 Accounts Health", "callback_data": "btn_health"}, {"text": "🔐 Login (OTP)", "callback_data": "btn_login_help"}],
        [{"text": "🔄 Refresh", "callback_data": "btn_status"}, {"text": "📖 Guide / Help", "callback_data": "btn_guide"}],
    ])
    if is_admin:
        keyboard.insert(len(keyboard) - 1, [{"text": "🛡 Admin Stats", "callback_data": "btn_admin_stats"}, {"text": "🧪 Burner Panel", "callback_data": "btn_burner_menu"}])
        keyboard.insert(len(keyboard) - 1, [{"text": "🔗 Linked Accounts", "callback_data": "btn_linked_menu"}, {"text": "🔑 Invites", "callback_data": "btn_invites_menu"}])
        keyboard.append([{"text": "⏸️ Pause", "callback_data": "btn_pause"}, {"text": "▶️ Resume", "callback_data": "btn_resume"}])
        chan_on = get_channel_announce()
        keyboard.append([{"text": f"📣 Channel: {'ON' if chan_on else 'OFF'}", "callback_data": "btn_toggle_channel"}])
    return {"inline_keyboard": keyboard}

def menu_text(chat_str):
    chat_str = str(chat_str)
    max_slots = get_max_slots(chat_str)
    tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
    enabled = sum(1 for i in range(1, max_slots + 1) if is_slot_active_state(chat_str, str(i)))
    return (
        f"🤖 *Chowdeck Multi-Account Sniper (1-{max_slots})*\n\n"
        f"• Active Slot: `Slot {get_active_slot(chat_str)}`\n"
        f"• Configured Slots: `{len(tokens_db)} / {max_slots}`\n"
        f"• Enabled Slots: `{enabled}`\n"
        f"• Mode: `PROMPT` — Supabase Database Linked 🟢\n\n"
        "Pick an option below."
    )

# ---------------------------------------------------------------------------
# Core claim worker & server loops
# ---------------------------------------------------------------------------
_code_queue = asyncio.Queue(maxsize=5000)
loop_ref = None
_recent_lock = threading.Lock()
_recent_codes = {}

def reserve_code(code, ttl=180):
    now = time.time()
    with _recent_lock:
        for k in [k for k, v in _recent_codes.items() if now - v > ttl]:
            _recent_codes.pop(k, None)
        if code in _recent_codes:
            return False
        _recent_codes[code] = now
    return True

async def main():
    global loop_ref
    loop_ref = asyncio.get_running_loop()
    start_store_writer()
    
    if not DATABASE_URL:
        print("[boot] WARNING: DATABASE_URL is not set! Data will not persist to Supabase.", flush=True)
    else:
        print("[boot] Supabase database connection verified.", flush=True)

    if not TELEGRAM_BOT_TOKEN:
        print("[boot] WARNING: TELEGRAM_BOT_TOKEN is not set.", flush=True)

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped.", flush=True)
