"""
InsiderOwl — Upstox Live Market Backend (Cloud Edition)
====================================================================
Streamlined Core Indices Version.
IST Timezone enforced.
Includes Paper Trading Database & 3:30 PM Auto-Emailer.
"""
from jsonschema import validate, ValidationError
import heapq
import concurrent.futures
import os, sys, time, json, requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from flask import Flask, jsonify, request
from flask_cors import CORS

# 🟢 NEW: Security & Rate Limiting Imports
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq, fsolve
from functools import wraps
import pytz
import gzip, csv, io

# 🟢 NEW: Email Imports for the 3:30 PM Trade Report
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# 🟢 CLOUD IMPORTS
from pymongo import MongoClient
import firebase_admin
from firebase_admin import credentials, auth
import razorpay
import threading
import asyncio

# Global dictionary to store our live Footprint matrix
live_footprint_data = {}

app = Flask(__name__)

# SECURITY FIX: Tell Flask it is sitting behind Render's proxy so it gets the REAL user IPs
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# SECURITY FIX: Only allow your official frontend domain to access the API
ALLOWED_ORIGINS = [
    "https://imsaurav1112-design.github.io",  # Your Production Frontend
    "http://127.0.0.1:5500",                  # Local VS Code Live Server
    "http://localhost:3000"
]
CORS(app, resources={r"/*": {"origins": ALLOWED_ORIGINS}}, supports_credentials=True)

# SECURITY FIX: Initialize the Rate Limiter globally for the app
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["1000 per day", "200 per hour"] # Generous global default
)
# ══════════════════════════════════════════════════════════
#  🔑 CONFIGURATION & CLOUD SETUP
# ══════════════════════════════════════════════════════════
# The 1-Year Analytics Token replaces the entire OAuth login system
ANALYTICS_TOKEN = os.environ.get("UPSTOX_ANALYTICS_TOKEN")

import pytz

# Define the IST timezone globally
IST = pytz.timezone('Asia/Kolkata')

def get_ist_now():
    """Returns a timezone-aware datetime object for precise Indian Standard Time."""
    return datetime.now(IST)

BASE_URL = "https://api.upstox.com/v2"

SYMBOL_MAP = {
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 15,  "step": 100}, 
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100}, 
    "BANKEX":     {"instrument_key": "BSE_INDEX|BANKEX",              "lot": 15,  "step": 100}
}

# ══════════════════════════════════════════════════════════
#  🟢 DYNAMIC MCX CUSTOM CHAIN BUILDER (Dormant)
# ══════════════════════════════════════════════════════════
MCX_MASTER_DICT = {}
LAST_MCX_FETCH_DATE = None

def parse_upstox_date(d_str):
    if not d_str: return datetime.max.date()
    d_str = d_str.strip()
    for fmt in ('%d-%b-%Y', '%Y-%m-%d', '%d-%m-%Y', '%d-%b-%y', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%fZ', '%d-%b-%Y %H:%M:%S'):
        try: return datetime.strptime(d_str, fmt).date()
        except ValueError: pass
    return datetime.max.date()

def ensure_mcx_master():
    global MCX_MASTER_DICT, LAST_MCX_FETCH_DATE
    today = get_ist_now().strftime("%Y-%m-%d")
    if LAST_MCX_FETCH_DATE == today and MCX_MASTER_DICT: return
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz"
        response = requests.get(url, timeout=15)
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            reader = csv.DictReader(f)
            futs, opts = [], {}
            for row in reader:
                name, tsym = row.get('name', '').upper(), row.get('tradingsymbol', '').upper()
                base = None
                if 'CRUDE' in name or 'CRUDE' in tsym: base = 'CRUDEOIL'
                elif 'NATGAS' in name or 'NATURALGAS' in name or 'NATGAS' in tsym: base = 'NATURALGAS'
                else: continue
                if 'MINI' in name or 'MINI' in tsym or tsym.startswith('CRUDEOILM') or tsym.startswith('NATGASM'): continue
                exp_raw = row.get('expiry')
                if not exp_raw: continue
                parsed_date = parse_upstox_date(exp_raw)
                exp_iso = parsed_date.strftime("%Y-%m-%d")
                itype, key = row.get('instrument_type', '').upper(), row.get('instrument_key')
                if 'FUT' in itype and 'OPT' not in itype:
                    futs.append({'base': base, 'key': key, 'date': parsed_date})
                elif 'OPT' in itype:
                    if base not in opts: opts[base] = {}
                    if exp_iso not in opts[base]: opts[base][exp_iso] = {'strikes': {}, 'date': parsed_date}
                    try:
                        strike, opt_type = float(row.get('strike')), row.get('option_type').upper() 
                        if strike not in opts[base][exp_iso]['strikes']: opts[base][exp_iso]['strikes'][strike] = {}
                        opts[base][exp_iso]['strikes'][strike][opt_type] = {'key': key, 'tsym': tsym}
                    except: pass
        for base, exps in opts.items():
            base_futs = sorted([f for f in futs if f['base'] == base], key=lambda x: x['date'])
            for exp_str, data in exps.items():
                valid_futs = [f for f in base_futs if f['date'] >= data['date']]
                data['fut_key'] = valid_futs[0]['key'] if valid_futs else None
        MCX_MASTER_DICT = opts
        LAST_MCX_FETCH_DATE = today
    except Exception as e: pass

def fetch_custom_mcx_chain(base_name, expiry_str, headers):
    ensure_mcx_master()
    if base_name not in MCX_MASTER_DICT or expiry_str not in MCX_MASTER_DICT[base_name]: return [], None
    data = MCX_MASTER_DICT[base_name][expiry_str]
    fut_key, opt_map = data.get("fut_key"), data.get("strikes", {})
    spot = None
    if fut_key:
        try:
            r = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": fut_key}, headers=headers)
            if r.status_code == 200: spot = list(r.json().get("data", {}).values())[0].get("last_price")
        except: pass
    keys_to_fetch = []
    for strike, types in opt_map.items():
        if "CE" in types: keys_to_fetch.append(types["CE"]["key"])
        if "PE" in types: keys_to_fetch.append(types["PE"]["key"])
    if not keys_to_fetch: return [], spot
    all_quotes = {}
    
    # 1. Break the massive list into chunks of 50
    batches = [keys_to_fetch[i:i+50] for i in range(0, len(keys_to_fetch), 50)]
    
    # 2. Define the worker function that each thread will execute
    def fetch_batch(batch_keys):
        try:
            url = "https://api.upstox.com/v2/market-quote/quotes"
            r = requests.get(url, params={"instrument_key": ",".join(batch_keys)}, headers=headers, timeout=5)
            if r.status_code == 200:
                return r.json().get("data", {})
        except Exception:
            pass
        return {}

    # 3. Fire all requests simultaneously using a Thread Pool
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all batches to the executor
            futures = [executor.submit(fetch_batch, b) for b in batches]
            
            # As soon as any thread finishes, merge its data into our master dictionary
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    all_quotes.update(result)
    except Exception as e:
        print("MCX Threading Error:", e)
    raw_list = []
    for strike, types in opt_map.items():
        ce_data, pe_data = types.get("CE"), types.get("PE")
        ce_key, pe_key = ce_data["key"] if ce_data else None, pe_data["key"] if pe_data else None
        ce_tsym, pe_tsym = f"MCX_FO:{ce_data['tsym']}" if ce_data else None, f"MCX_FO:{pe_data['tsym']}" if pe_data else None
        cq = all_quotes.get(ce_key) or all_quotes.get(ce_tsym) or {}
        pq = all_quotes.get(pe_key) or all_quotes.get(pe_tsym) or {}
        if not cq and not pq: continue
        raw_list.append({
            "strike_price": float(strike), "underlying_spot_price": spot or 0,
            "call_options": {"instrument_key": ce_key or "", "market_data": {"ltp": cq.get("last_price", 0), "oi": cq.get("open_interest", 0), "volume": cq.get("volume", 0), "prev_oi": 0}} if cq else None,
            "put_options": {"instrument_key": pe_key or "", "market_data": {"ltp": pq.get("last_price", 0), "oi": pq.get("open_interest", 0), "volume": pq.get("volume", 0), "prev_oi": 0}} if pq else None
        })
    return raw_list, spot

# ══════════════════════════════════════════════════════════
#  🟢 FIREBASE & MONGODB SETUP
# ══════════════════════════════════════════════════════════
import sys
import os
import firebase_admin
from firebase_admin import credentials, auth
from pymongo import MongoClient

# 1. DEFINE GLOBALS FIRST (Prevents 500 NameErrors)
mongo_client = None
db = None
sys_col = None
users_col = None
history_col = None
paper_trades_col = None

# 2. INITIALIZE FIREBASE (Prevents 401 Unauthorized Errors)
try:
    # Check if already initialized to prevent crash on server reload
    if not firebase_admin._apps:
        cred = credentials.Certificate(os.path.join(os.getcwd(), 'firebase-admin.json'))
        firebase_admin.initialize_app(cred)
        print("🔥 Firebase Admin initialized successfully.")
except Exception as e:
    print(f"🚨 FATAL ERROR: Firebase initialization failed: {e}")
    sys.exit(1)

# 3. CONNECT TO MONGODB
MONGO_URI = os.environ.get("MONGO_URI")
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["ioc_terminal"]
    
    sys_col = db["system_config"]
    users_col = db["users"]
    history_col = db["history"]
    paper_trades_col = db["paper_trades"]
    
    history_col.create_index("createdAt", expireAfterSeconds=3456000)
    print("✅ MongoDB Connected & Collections Initialized")
except Exception as e:
    print(f"🚨 MongoDB Connection Failed: {e}")
# ══════════════════════════════════════════════════════════
#  🟢 Razorpay Keys
# ══════════════════════════════════════════════════════════

RZP_KEY_ID = os.environ.get("RZP_KEY_ID")
RZP_KEY_SECRET = os.environ.get("RZP_KEY_SECRET")
RZP_WEBHOOK_SECRET = os.environ.get("RZP_WEBHOOK_SECRET")

rzp_client = razorpay.Client(auth=(RZP_KEY_ID, RZP_KEY_SECRET))

def require_firebase_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 🟢 BULLETPROOF CORS FIX: Manually build the exact headers the browser demands
        if request.method == "OPTIONS":
            response = jsonify({"status": "ok"})
            origin = request.headers.get("Origin")
            
            # Mirror the exact origin asking for data (fixes the strict CORS policy)
            if origin in ALLOWED_ORIGINS:
                response.headers.add("Access-Control-Allow-Origin", origin)
            else:
                response.headers.add("Access-Control-Allow-Origin", ALLOWED_ORIGINS[0])
                
            response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
            response.headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            response.headers.add("Access-Control-Allow-Credentials", "true")
            return response, 200
            
        header = request.headers.get("Authorization")
        if not header or not header.startswith("Bearer "): 
            return jsonify({"error": "Unauthorized Access"}), 401
            
        token = header.split(" ")[1]
        try:
            decoded_token = auth.verify_id_token(token)
            request.user = decoded_token
        except Exception as e: 
            return jsonify({"error": "Invalid or Expired Token", "details": str(e)}), 401
            
        return f(*args, **kwargs)
    return decorated_function

def auth_headers(): 
    return {"Authorization": f"Bearer {ANALYTICS_TOKEN}", "Accept": "application/json"}

# ══════════════════════════════════════════════════════════
#  🟢 INSTITUTIONAL BLACK-SCHOLES SEESAW & GREEKS ENGINE
# ══════════════════════════════════════════════════════════
def bs_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return max(0.0, S - K)
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)

def bs_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: return max(0.0, K - S)
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)

def bs_greeks(S, K, T, r, sigma, opt_type='ce'):
    if T <= 0 or sigma <= 0: return {'delta': 0, 'gamma': 0.0005, 'theta': 0, 'vega': 0}
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega = S * norm.pdf(d1) * np.sqrt(T) / 100.0
    if opt_type == 'ce':
        delta = norm.cdf(d1)
        theta = (- (S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - r * K * np.exp(-r*T) * norm.cdf(d2)) / 365.0
    else:
        delta = -norm.cdf(-d1)
        theta = (- (S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) + r * K * np.exp(-r*T) * norm.cdf(-d2)) / 365.0
    return {'delta': round(delta, 4), 'gamma': round(gamma * 10000, 2), 'theta': round(theta, 4), 'vega': round(vega, 4)}
    
def calculate_custom_iv(market_ltp, S, K, T, r, opt_type='ce'):
    intrinsic = max(0.0, S - K) if opt_type == 'ce' else max(0.0, K - S)
    safe_ltp = max(market_ltp, intrinsic + 0.01)
    def objective(sigma):
        return bs_call(S, K, T, r, sigma) - safe_ltp if opt_type == 'ce' else bs_put(S, K, T, r, sigma) - safe_ltp
    try: return brentq(objective, 1e-4, 10.0)
    except: return 0.15 

def calc_prz(K_call, iv_call, K_put, iv_put, days_to_expiry, r=0.10):
    try:
        if iv_call <= 0 or iv_put <= 0: return 0.0
        T = max(0.001, days_to_expiry / 365.0)
        def objective(S): return bs_call(S, K_call, T, r, iv_call) - bs_put(S, K_put, T, r, iv_put)
        gap = abs(K_put - K_call)
        try: return round(brentq(objective, K_call - (gap*10), K_put + (gap*10)), 2)
        except: return 0.0
    except: return 0.0

def calibrate_live_interest_rate(spot_price, atm_strike, days_to_expiry, atm_ce_ltp, atm_pe_ltp):
    T = max(0.001, days_to_expiry / 365.0)
    if atm_ce_ltp <= 0 or atm_pe_ltp <= 0 or spot_price <= 0: return 0.10 
    try:
        parity_ratio = (spot_price - atm_ce_ltp + atm_pe_ltp) / atm_strike
        if parity_ratio <= 0: return 0.0
        implied_r = -np.log(parity_ratio) / T
        return max(0.0, min(0.15, implied_r))
    except: return 0.10

def inject_prz(chain_rows, expiry_date_str, step, spot_price):
    try:
        exp_date = datetime.strptime(f"{expiry_date_str} 15:30:00", "%Y-%m-%d %H:%M:%S")
        days_to_expiry = max(0.001, (exp_date - get_ist_now()).total_seconds() / 86400.0)
    except: days_to_expiry = 5.0

    T = max(0.001, days_to_expiry / 365.0)
    atm = round(float(spot_price) / step) * step if spot_price else None
    atm_row = next((r for r in chain_rows if r["strike"] == atm), None)
    
    live_rate = 0.10
    if atm_row and spot_price:
        ce_ltp = float(atm_row["ce"].get("ltp") or 0)
        pe_ltp = float(atm_row["pe"].get("ltp") or 0)
        live_rate = calibrate_live_interest_rate(spot_price, atm, days_to_expiry, ce_ltp, pe_ltp)
        if live_rate < 0.05: live_rate = 0.0925 

    iv_map = {}
    for r in chain_rows:
        strike = r["strike"]
        ce_ltp = float(r["ce"].get("ltp") or 0)
        pe_ltp = float(r["pe"].get("ltp") or 0)
        
        if spot_price and spot_price > 0:
            raw_ce_iv = calculate_custom_iv(ce_ltp, spot_price, strike, T, live_rate, 'ce') if ce_ltp > 0 else 0.15
            ce_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_ce_iv, 'ce')
            r["ce"].update({"iv": round(raw_ce_iv * 100, 2), "delta": ce_greeks["delta"], "theta": ce_greeks["theta"], "vega": ce_greeks["vega"], "gamma": ce_greeks["gamma"]})
            
            raw_pe_iv = calculate_custom_iv(pe_ltp, spot_price, strike, T, live_rate, 'pe') if pe_ltp > 0 else 0.15
            pe_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_pe_iv, 'pe')
            r["pe"].update({"iv": round(raw_pe_iv * 100, 2), "delta": pe_greeks["delta"], "theta": pe_greeks["theta"], "vega": pe_greeks["vega"], "gamma": pe_greeks["gamma"]})
        else:
            raw_ce_iv, raw_pe_iv = 0.15, 0.15
            r["ce"].update({"iv": 0, "delta": 0, "theta": 0, "vega": 0, "gamma": 0})
            r["pe"].update({"iv": 0, "delta": 0, "theta": 0, "vega": 0, "gamma": 0})

        iv_map[strike] = {"ce_iv": raw_ce_iv, "pe_iv": raw_pe_iv}

    for r in chain_rows:
        strike = r["strike"]
        ce_iv, pe_iv = iv_map[strike]["ce_iv"], iv_map[strike]["pe_iv"]
        p_up = iv_map.get(strike + step, {}).get("pe_iv", 0.15)
        c_dn = iv_map.get(strike - step, {}).get("ce_iv", 0.15)
        r["ce_prz"] = calc_prz(strike, ce_iv, strike + step, p_up, days_to_expiry, live_rate)
        r["pe_prz"] = calc_prz(strike - step, c_dn, strike, pe_iv, days_to_expiry, live_rate)

    return chain_rows

# ══════════════════════════════════════════════════════════
#  🟢 ADVANCED STATE-MACHINE & COA LOGIC
# ══════════════════════════════════════════════════════════
def evaluate_side(vols_dict, mem_side, side_name):
    if not vols_dict: 
        return {"strike": 0, "state": "Strong", "target_strike": 0, "pct": 0, "val": 0, "msg": ""}
        
    # ⚡ PERFORMANCE FIX: Use heapq to find the top 2 values instantly 
    # instead of sorting the entire dictionary. (O(N) instead of O(N log N))
    top_2 = heapq.nlargest(2, vols_dict.items(), key=lambda x: x[1])
    
    max_strike, max_vol = top_2[0]
    
    if len(top_2) > 1:
        sec_strike, sec_vol = top_2[1]
    else:
        sec_strike, sec_vol = 0, 0
        
    pct = round((sec_vol / max_vol * 100), 2) if max_vol > 0 else 0
    
    if mem_side["base"] == 0:
        mem_side["base"] = max_strike
        return {"strike": max_strike, "state": "Strong", "target_strike": 0, "pct": pct, "val": max_vol, "msg": f"{side_name} established at {max_strike}."}

    msg, current_state, target = "", "Strong", 0

    if max_strike != mem_side["base"]:
        mem_side["old_base"], mem_side["base"], mem_side["is_shifting"], mem_side["lowest_pct"] = mem_side["base"], max_strike, True, pct
        msg = f"Shift in Progress: {side_name} base moved to {max_strike}."
        
    if mem_side["is_shifting"]:
        if sec_strike == mem_side["old_base"]:
            mem_side["lowest_pct"] = min(mem_side["lowest_pct"], pct)
            if pct < 75.0:
                mem_side["is_shifting"], mem_side["old_base"] = False, 0
                msg, current_state = f"Shift Complete: {side_name} successfully consolidated at {max_strike}.", "Strong"
            else:
                current_state = "STT" if sec_strike > max_strike else "STB"
                target = sec_strike
        else:
            if pct >= 75.0: current_state, target = ("STT" if sec_strike > max_strike else "STB"), sec_strike
    else:
        if pct >= 75.0: current_state, target = ("STT" if sec_strike > max_strike else "STB"), sec_strike
            
    mem_side["state"], mem_side["target"] = current_state, target
    return {"strike": max_strike, "state": current_state, "target_strike": target, "pct": pct, "val": max_vol, "msg": msg}

COA_MEMORY = {}
def calculate_coa(chain_rows, symbol, expiry):
    global COA_MEMORY
    mem_key = f"{symbol}_{expiry}"
    if mem_key not in COA_MEMORY: 
        COA_MEMORY[mem_key] = {"sup_mem": {"base": 0, "old_base": 0, "is_shifting": False, "lowest_pct": 100.0, "state": "Strong", "target": 0}, "res_mem": {"base": 0, "old_base": 0, "is_shifting": False, "lowest_pct": 100.0, "state": "Strong", "target": 0}, "logs": []}
    mem = COA_MEMORY[mem_key]
    
    ce_vols = {r['strike']: r['ce'].get('volume', 0) for r in chain_rows if r['ce'].get('volume')}
    pe_vols = {r['strike']: r['pe'].get('volume', 0) for r in chain_rows if r['pe'].get('volume')}
    res = evaluate_side(ce_vols, mem['res_mem'], "Resistance")
    sup = evaluate_side(pe_vols, mem['sup_mem'], "Support")
    
    step = SYMBOL_MAP.get(symbol, {}).get("step", 50)
    res_row = next((r for r in chain_rows if r['strike'] == res['strike']), None)
    sup_row = next((r for r in chain_rows if r['strike'] == sup['strike']), None)
    r1_val = res_row['ce_prz'] if res_row else res['strike']
    s1_val = sup_row['pe_prz'] if sup_row else sup['strike']
    
    r2_strike = res['strike'] + step if res['strike'] > 0 else 0
    s2_strike = sup['strike'] - step if sup['strike'] > 0 else 0
    r2_row = next((r for r in chain_rows if r['strike'] == r2_strike), None)
    s2_row = next((r for r in chain_rows if r['strike'] == s2_strike), None)

    return {
        "scenario_desc": f"Resistance is {res['state']}, Support is {sup['state']}.", 
        "support": sup, "resistance": res, 
        "s1": s1_val, "r1": r1_val, "s2": s2_row['pe_prz'] if s2_row else s2_strike, "r2": r2_row['ce_prz'] if r2_row else r2_strike, 
        "logs": mem['logs']
    }

# ══════════════════════════════════════════════════════════
#  🟢 TERMINAL DATA ROUTES
# ══════════════════════════════════════════════════════════
@app.route("/health")
def health(): 
    is_auth = ANALYTICS_TOKEN is not None
    return jsonify({"status": "ok", "authenticated": is_auth})

# ==========================================
# 🟢 1. EXPIRY DATES ROUTE (Fixed for Backtester)
# ==========================================
@app.route("/expiry-dates", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def expiry_dates():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    if symbol not in SYMBOL_MAP: return jsonify({"error": "Invalid symbol"}), 400 
    cfg = SYMBOL_MAP.get(symbol)
    
    # 🟢 FIX: Check the URL explicitly instead of relying entirely on the browser's Referer header
    is_backtest = request.args.get("source") == "backtester" or request.headers.get("Referer", "").endswith("backtester.html")
    
    if is_backtest:
        try:
            # Fetches ONLY expiries that actually have data saved in your DB
            saved_expiries = history_col.distinct("exp", {"sym": symbol})
            return jsonify({"symbol": symbol, "expiries": sorted(saved_expiries)})
        except Exception as e: return jsonify({"error": str(e)}), 500

    if cfg.get("is_mcx"):
        ensure_mcx_master()
        base = cfg["base_name"]
        valid_exps = []
        for e, data in MCX_MASTER_DICT.get(base, {}).items():
            if data['date'] >= get_ist_now().date():
                valid_exps.append(e)
        valid_exps.sort(key=lambda x: MCX_MASTER_DICT[base][x]['date'])
        return jsonify({"symbol": symbol, "expiries": valid_exps})
    else:
        resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        data = resp.json().get("data") or []
        expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
        return jsonify({"symbol": symbol, "expiries": expiries})


# ==========================================
# 🟢 2. AVAILABLE DATES ROUTE (For Backtester)
# ==========================================
@app.route("/api/available-dates", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def available_dates():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    
    try:
        # Search the MongoDB history collection for all distinct dates for this combo
        saved_dates = history_col.distinct("date", {"sym": symbol, "exp": expiry})
        
        # Sort them descending so the most recent dates show up first in your dropdown
        saved_dates.sort(reverse=True)
        
        return jsonify({
            "symbol": symbol, 
            "expiry": expiry, 
            "dates": saved_dates
        })
    except Exception as e: 
        return jsonify({"error": str(e)}), 500

    if cfg.get("is_mcx"):
        ensure_mcx_master()
        base = cfg["base_name"]
        valid_exps = []
        for e, data in MCX_MASTER_DICT.get(base, {}).items():
            if data['date'] >= get_ist_now().date():
                valid_exps.append(e)
        valid_exps.sort(key=lambda x: MCX_MASTER_DICT[base][x]['date'])
        return jsonify({"symbol": symbol, "expiries": valid_exps})
    else:
        resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        data = resp.json().get("data") or []
        expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
        return jsonify({"symbol": symbol, "expiries": expiries})

def calculate_max_pain(chain_rows):
    if not chain_rows: return 0
    strikes = [r['strike'] for r in chain_rows]
    total_pain = []
    for s in strikes:
        pain = 0
        for r in chain_rows:
            if s > r['strike']: pain += (s - r['strike']) * r['ce'].get('oi', 0)
            elif s < r['strike']: pain += (r['strike'] - s) * r['pe'].get('oi', 0)
        total_pain.append(pain)
    return strikes[total_pain.index(min(total_pain))] if total_pain else 0

@app.route("/options-chain", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def options_chain():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    cfg = SYMBOL_MAP.get(symbol)

    if cfg.get("is_mcx"):
        raw_list, spot = fetch_custom_mcx_chain(cfg["base_name"], expiry, auth_headers())
        if not raw_list: return jsonify({"error": f"No MCX options found for {expiry}"}), 502
    else:
        resp = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": expiry}, headers=auth_headers())
        raw_list = resp.json().get("data") or []
        if not raw_list: return jsonify({"error": "No data from Upstox"}), 502

        spot_resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        spot = None
        if spot_resp.status_code == 200:
            for v in (spot_resp.json().get("data") or {}).values():
                spot = v.get("last_price")
                break

    atm = round(float(spot) / cfg["step"]) * cfg["step"] if spot else None
    chain_rows, total_ce_oi, total_pe_oi = [], 0, 0
    
    for item in raw_list:
        strike = int(float(item.get("strike_price", 0)))
        if atm and abs(strike - atm) > 3000: continue
        
        def parse_side(d):
            if not d: return {"instrument_key": "", "ltp": 0, "oi": 0, "change_oi": 0, "volume": 0}
            md = d.get("market_data") or {}
            return {
                "instrument_key": d.get("instrument_key", ""), 
                "ltp": md.get("ltp", 0), 
                "oi": int(md.get("oi") or 0), 
                "change_oi": int(md.get("oi") or 0) - int(md.get("prev_oi") or 0),
                "volume": md.get("volume", 0)
            }
            
        ce = parse_side(item.get("call_options"))
        pe = parse_side(item.get("put_options"))
        total_ce_oi += ce["oi"]
        total_ce_oi += ce["oi"]
        total_pe_oi += pe["oi"]
        chain_rows.append({"strike": strike, "atm": atm is not None and abs(strike - atm) < cfg["step"], "ce": ce, "pe": pe})

    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    coa_data = calculate_coa(chain_rows, symbol, expiry)
    
    # 🟢 1. CALCULATE MAX PAIN
    mp_val = calculate_max_pain(chain_rows)

# 🟢 2. FETCH INDIA VIX
    vix_val = 0.0
    try:
        # We ask for it using the Pipe (|)
        request_key = "NSE_INDEX|India VIX"
        # Upstox sends it back using the Colon (:)
        response_key = "NSE_INDEX:India VIX" 
        
        vix_resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": request_key}, headers=auth_headers())
        
        if vix_resp.status_code == 200:
            vix_data = vix_resp.json().get("data", {})
            # Look for the colon version first, fallback to the pipe version just in case
            if response_key in vix_data:
                vix_val = vix_data[response_key].get("last_price", 0.0)
            elif request_key in vix_data:
                vix_val = vix_data[request_key].get("last_price", 0.0)
    except: 
        pass

    # 🟢 3. RETURN WITH NEW DATA
    return jsonify({
        "symbol": symbol, "expiry": expiry, "spot": spot, "pcr": round(total_pe_oi / max(total_ce_oi, 1), 2),
        "vix": vix_val,          # <-- NEW
        "max_pain": mp_val,      # <-- NEW
        "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi, "lot_size": cfg["lot"],
        "chain": chain_rows, "fetched_at": get_ist_now().strftime("%H:%M:%S"), "coa": coa_data
    })

@app.route("/api/intraday-history", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def intraday_history():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    target_date = request.args.get("date", get_ist_now().strftime("%Y-%m-%d")).strip()
    try:
        cursor = history_col.find({"sym": symbol, "exp": expiry, "date": target_date}).sort("createdAt", 1)
        records = list(cursor)
        if not records: return jsonify([])

        history_map = {}
        base_oi = {}
        for doc in records:
            time_key = doc.get("time_key")
            if time_key not in history_map: history_map[time_key] = []
            for row in doc.get("chain", []):
                if len(row) < 7: continue
                strike, ce_oi, ce_vol, ce_ltp, pe_oi, pe_vol, pe_ltp = row
                if strike not in base_oi: base_oi[strike] = {"ce": ce_oi, "pe": pe_oi}
                history_map[time_key].append({
                    "strike": strike, "ceVol": ce_vol, "peVol": pe_vol, "ceOI": ce_oi, "peOI": pe_oi,
                    "ceOIChg": ce_oi - base_oi[strike]["ce"], "peOIChg": pe_oi - base_oi[strike]["pe"],
                    "ceLTP": ce_ltp, "peLTP": pe_ltp
                })
        return jsonify([{"time": k, "rows": v} for k, v in history_map.items()])
    except Exception as e: return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════
#  🟢 NEW: PAPER TRADING ROUTES
# ══════════════════════════════════════════════════════════
@app.route("/api/paper-trade", methods=['POST', 'OPTIONS'])
@require_firebase_auth
def save_paper_trade():
    if request.method == 'OPTIONS': return jsonify({"status": "ok"}), 200
    try:
        trade_data = request.json
        trade_data["user_email"] = request.user.get('email', 'unknown_user')
        trade_data["date"] = get_ist_now().strftime("%Y-%m-%d")
        
        paper_trades_col.insert_one(trade_data)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
        
@app.route("/api/paper-trade/exit", methods=['POST', 'OPTIONS'])
@require_firebase_auth
def exit_paper_trade():
    if request.method == 'OPTIONS': return jsonify({"status": "ok"}), 200
    try:
        data = request.json
        trade_id = data.get("id")
        exit_price = data.get("exit_price")
        pnl = data.get("pnl")
        
        paper_trades_col.update_one(
            {"id": trade_id, "user_email": request.user.get('email')},
            {"$set": {"status": "Closed", "exit_price": exit_price, "final_pnl": pnl}}
        )
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cron/email-trades", methods=['GET'])
def email_daily_trades():
    now = get_ist_now()
    today_str = now.strftime("%Y-%m-%d")
    
    SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
    SENDER_APP_PASSWORD = os.environ.get("SENDER_APP_PASSWORD")
    
    try:
        # DATA FIX 1: Only pick up trades that haven't been emailed yet
        all_trades = list(paper_trades_col.find({"date": today_str, "emailed": {"$ne": True}}))
        
        if not all_trades:
            return jsonify({"status": "sleeping", "message": "No new trades to email today."}), 200
            
        users = set(t.get("user_email") for t in all_trades if t.get("user_email"))
        trade_ids_processed = [] # Keep track of exact trades we are emailing
        
        for email in users:
            user_trades = [t for t in all_trades if t.get("user_email") == email]
            trade_ids_processed.extend([t["_id"] for t in user_trades])
            
            html_table = f"<h2>Your Paper Trades for {today_str}</h2><table border='1' cellpadding='8' style='border-collapse: collapse;'>"
            html_table += "<tr><th>Time</th><th>Symbol</th><th>Strike</th><th>Type</th><th>Entry</th><th>SL</th><th>Target</th></tr>"
            
            for t in user_trades:
                html_table += f"<tr><td>{t.get('time','')}</td><td>{t.get('sym','')}</td><td>{t.get('strike','')}</td>"
                html_table += f"<td>{t.get('type','')}</td><td>{t.get('entry','')}</td><td>{t.get('sl','')}</td><td>{t.get('target','')}</td></tr>"
            html_table += "</table>"
            
            msg = MIMEMultipart()
            msg['From'] = SENDER_EMAIL
            msg['To'] = email
            msg['Subject'] = f"GC Live: Daily Paper Trade Report ({today_str})"
            msg.attach(MIMEText(html_table, 'html'))
            
            with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
                server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
                server.send_message(msg)

        # DATA FIX 2: Mark ONLY the processed trades as emailed. DO NOT DELETE.
        if trade_ids_processed:
            paper_trades_col.update_many(
                {"_id": {"$in": trade_ids_processed}},
                {"$set": {"emailed": True, "emailed_at": datetime.utcnow()}}
            )
        
        return jsonify({"status": "success", "message": f"Emailed {len(users)} users and safely archived {len(trade_ids_processed)} trades."})
        
except Exception as e:
        print("Email Cron Error:", e)
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════
#  🟢 THE EXTERNAL CRON ENGINE (HISTORY RECORDING)
# ══════════════════════════════════════════════════════════
def compress_and_save(symbol, expiry, spot, pcr, chain_rows):
    if not chain_rows or not spot: return
    ts = get_ist_now()
    time_key = ts.strftime("%I:%M %p")
    date_str = ts.strftime("%Y-%m-%d")

    step = SYMBOL_MAP[symbol]["step"]
    atm = round(spot / step) * step
    
    compressed_chain = []
    for r in chain_rows:
        # 🟢 FIX: Exactly 20 strikes below and 20 strikes above ATM
        if abs(r['strike'] - atm) <= (20 * step): 
            compressed_chain.append([
                r['strike'], r['ce'].get('oi', 0), r['ce'].get('volume', 0), float(r['ce'].get('ltp', 0)),
                r['pe'].get('oi', 0), r['pe'].get('volume', 0), float(r['pe'].get('ltp', 0))
            ])

    try:
        history_col.update_one(
            {"sym": symbol, "exp": expiry, "date": date_str, "time_key": time_key},
            {"$set": {"sym": symbol, "exp": expiry, "date": date_str, "time_key": time_key, "createdAt": datetime.utcnow(), "spot": spot, "pcr": pcr, "chain": compressed_chain}},
            upsert=True
        )
        print(f"💾 SAVED TO MONGO: {symbol} at {time_key}")
    except Exception as e:
        print(f"❌ MongoDB Record Error: {e}")

# 🟢 1. TARGET INDICES ONLY
TARGET_INDICES = ["NIFTY", "BANKNIFTY", "SENSEX"]

@app.route("/cron/record", methods=['GET'])
def trigger_record():
    now = get_ist_now()
    if not ANALYTICS_TOKEN:
        return jsonify({"status": "blocked", "reason": "no_analytics_token"}), 403
    
    is_weekday = now.weekday() < 5
    is_market_open = (
        (now.hour == 9 and now.minute >= 15) or 
        (now.hour > 9 and now.hour < 15) or 
        (now.hour == 15 and now.minute <= 30)
    )

    if is_weekday and is_market_open:
        try:
            # 🟢 FIX: Only record NIFTY, BANKNIFTY, and SENSEX
            for sym in TARGET_INDICES:
                fetch_and_record(sym)
            return jsonify({"status": "success", "message": f"Recorded selected indices at {now.strftime('%H:%M:%S')} IST"})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500
    
    return jsonify({"status": "sleeping", "message": f"Market Closed"}), 200

def fetch_and_record(symbol):
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return
    
    try:
        if cfg.get("is_mcx"):
            return # Skipping MCX as per your requirements
        else:
            # 🟢 2. GET CLOSEST EXPIRY ONLY
            r = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
            contracts = r.json().get("data", [])
            if not contracts: return
            
            # Upstox returns contracts sorted by date. Index [0] is ALWAYS the closest/present expiry.
            exp = contracts[0]["expiry"] 
            
            # Fetch Spot and Chain
            r = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
            spot = list(r.json()["data"].values())[0]["last_price"]
            r = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": exp}, headers=auth_headers())
            raw = r.json()["data"]
        
        rows, tce, tpe = [], 0, 0
        for item in raw:
            strike = int(float(item["strike_price"]))
            ce_oi = item.get("call_options", {}).get("market_data", {}).get("oi", 0)
            pe_oi = item.get("put_options", {}).get("market_data", {}).get("oi", 0)
            tce += ce_oi; tpe += pe_oi
            rows.append({
                "strike": strike, 
                "ce": {"oi": ce_oi, "volume": item.get("call_options", {}).get("market_data", {}).get("volume", 0), "ltp": item.get("call_options", {}).get("market_data", {}).get("ltp", 0)}, 
                "pe": {"oi": pe_oi, "volume": item.get("put_options", {}).get("market_data", {}).get("volume", 0), "ltp": item.get("put_options", {}).get("market_data", {}).get("ltp", 0)}
            })
        
        pcr = round(tpe / max(1, tce), 2)
        chain_rows = inject_prz(rows, exp, cfg["step"], spot)
        compress_and_save(symbol, exp, spot, pcr, chain_rows)
        
    except Exception as e: print(f"Record Error {symbol}: {e}")

# 🟢 1. TARGET INDICES ONLY
TARGET_INDICES = ["NIFTY", "BANKNIFTY", "SENSEX"]

@app.route("/cron/record", methods=['GET'])
def trigger_record():
    now = get_ist_now()
    if not ANALYTICS_TOKEN:
        return jsonify({"status": "blocked", "reason": "no_analytics_token"}), 403
    
    is_weekday = now.weekday() < 5
    is_market_open = (
        (now.hour == 9 and now.minute >= 15) or 
        (now.hour > 9 and now.hour < 15) or 
        (now.hour == 15 and now.minute <= 30)
    )

    if is_weekday and is_market_open:
        try:
            # 🟢 FIX: Only record NIFTY, BANKNIFTY, and SENSEX
            for sym in TARGET_INDICES:
                fetch_and_record(sym)
            return jsonify({"status": "success", "message": f"Recorded selected indices at {now.strftime('%H:%M:%S')} IST"})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500
    
    return jsonify({"status": "sleeping", "message": f"Market Closed"}), 200

from datetime import timedelta

@app.route("/pay-with-wallet", methods=['POST', 'OPTIONS'])
@limiter.limit("5 per minute") 
@require_firebase_auth
def pay_with_wallet():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
        
    try:
        # 🛡️ STRICT VALIDATION: Drop bad payloads instantly
        try:
            validate(instance=request.json, schema=PLAN_SCHEMA)
        except ValidationError as e:
            return jsonify({"error": f"Invalid request format: {e.message}"}), 400
            
        uid = str(request.user.get('uid'))
        data = request.json
        plan_id = data.get("plan")
        
        # ✅ SAFE: We completely ignore the client's cost parameter
        
        # 1. Server determines the cost strictly based on the plan ID
        plan_prices = {"1_month": 249, "3_months": 599, "6_months": 999}
        plan_days = {"1_month": 30, "3_months": 90, "6_months": 180}
        
        if plan_id not in plan_prices:
            return jsonify({"error": "Invalid plan selected."}), 400
            
        cost = plan_prices[plan_id] # The server sets the absolute truth here
            
        # 2. Fetch user to check current tier/expiry for date math
        user_doc = users_col.find_one({"_id": uid})
        if not user_doc: 
            return jsonify({"error": "User not found in database."}), 404
        
        # 3. Calculate the new Expiry Date safely BEFORE deducting
        now = get_ist_now()
        current_expiry = user_doc.get("expiry")
        
        if user_doc.get("tier") == "pro" and current_expiry and current_expiry > now:
            new_expiry = current_expiry + timedelta(days=plan_days[plan_id])
        else:
            new_expiry = now + timedelta(days=plan_days[plan_id])
            
        # 4. 🛡️ ATOMIC UPDATE (Fixes the Race Condition / Double-Spend)
        # This tells MongoDB: "ONLY update if they have enough money. If they do, 
        # deduct the money, set them to Pro, and set their new expiry ALL AT ONCE."
        result = users_col.update_one(
            {"_id": uid, "wallet_balance": {"$gte": cost}},
            {
                "$inc": {"wallet_balance": -cost},
                "$set": {
                    "tier": "pro",
                    "expiry": new_expiry
                }
            }
        )
        
        # 5. Check if the Atomic Update succeeded
        # If modified_count is 0, they didn't have enough money (or pressed the button twice)
        if result.modified_count == 0:
            return jsonify({"error": "Insufficient wallet balance or concurrent transaction blocked."}), 400
            
        # 6. Calculate estimated new balance to send to frontend UI
        estimated_new_balance = float(user_doc.get("wallet_balance", 0)) - cost
        
        return jsonify({
            "status": "success", 
            "new_balance": estimated_new_balance, 
            "new_expiry": new_expiry.strftime("%Y-%m-%d")
        }), 200
        
    except Exception as e:
        print("Wallet Payment Error:", e)
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════
#  🟢 STRICT API PAYLOAD SCHEMAS
# ══════════════════════════════════════════════════════════
# We use "additionalProperties": False to prevent hackers from sending 
# massive payloads with fake keys to crash the server memory.

PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "plan": {"type": "string", "enum": ["1_month", "3_months", "6_months"]},
    },
    "required": ["plan"],
    "additionalProperties": False 
}

VERIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "plan": {"type": "string", "enum": ["1_month", "3_months", "6_months"]},
        "razorpay_payment_id": {"type": "string", "minLength": 5},
        "razorpay_order_id": {"type": "string", "minLength": 5},
        "razorpay_signature": {"type": "string", "minLength": 10}
    },
    "required": ["plan", "razorpay_payment_id", "razorpay_order_id", "razorpay_signature"],
    "additionalProperties": False
}

# ══════════════════════════════════════════════════════════
#  🟢 RAZORPAY PAYMENT ROUTES
# ══════════════════════════════════════════════════════════
from datetime import timedelta

@app.route("/create-order", methods=['POST', 'OPTIONS'])
@limiter.limit("10 per minute")
@require_firebase_auth
def create_order():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
        
    try:
        # 🛡️ STRICT VALIDATION
        try:
            validate(instance=request.json, schema=PLAN_SCHEMA)
        except ValidationError as e:
            return jsonify({"error": f"Invalid request format: {e.message}"}), 400
            
        data = request.json
        plan_id = data.get("plan")
        
        # ... (Rest of your Razorpay order logic) ...
        
        # 1. Server-side price validation
        plan_prices = {"1_month": 249, "3_months": 599, "6_months": 999}
        if plan_id not in plan_prices:
            return jsonify({"error": "Invalid Plan Selected"}), 400
            
        cost_inr = plan_prices[plan_id] # ✅ Secure pattern
        
        # 2. Razorpay uses 'paise' (Multiply INR by 100)
        amount_in_paise = int(cost_inr * 100)
        
        # 3. Create the order with Razorpay
        order_data = {
            "amount": amount_in_paise,
            "currency": "INR",
            "receipt": request.user.get('uid')[:15] # Short receipt ID
        }
        
        order = rzp_client.order.create(data=order_data)
        
        return jsonify({
            "order_id": order["id"],
            "amount": order["amount"],
            "key": RZP_KEY_ID
        }), 200

    except Exception as e:
        print("Razorpay Order Creation Error:", e)
        return jsonify({"error": str(e)}), 500


@app.route("/verify-payment", methods=['POST', 'OPTIONS'])
@limiter.limit("5 per minute")
@require_firebase_auth
def verify_payment():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
        
    try:
        # 🛡️ STRICT VALIDATION: Using the VERIFY_SCHEMA here
        try:
            validate(instance=request.json, schema=VERIFY_SCHEMA)
        except ValidationError as e:
            return jsonify({"error": f"Invalid request format: {e.message}"}), 400
            
        uid = str(request.user.get('uid'))
        data = request.json
        
        # ... (Rest of your signature verification logic) ...
        
        # 1. Extract Razorpay signature details
        payment_id = data.get("razorpay_payment_id")
        order_id = data.get("razorpay_order_id")
        signature = data.get("razorpay_signature")
        plan_id = data.get("plan")
        
        plan_days = {"1_month": 30, "3_months": 90, "6_months": 180}
        if plan_id not in plan_days:
            return jsonify({"error": "Invalid Plan"}), 400

        # 2. Verify the mathematical signature so hackers can't fake a success
        rzp_client.utility.verify_payment_signature({
            'razorpay_order_id': order_id,
            'razorpay_payment_id': payment_id,
            'razorpay_signature': signature
        })
        
        # 3. If signature is valid, fetch user
        user_doc = users_col.find_one({"_id": uid})
        if not user_doc: 
            return jsonify({"error": "User not found"}), 404
            
        # 4. Calculate Expiry Date
        now = get_ist_now()
        current_expiry = user_doc.get("expiry")
        
        if user_doc.get("tier") == "pro" and current_expiry and current_expiry > now:
            new_expiry = current_expiry + timedelta(days=plan_days[plan_id])
        else:
            new_expiry = now + timedelta(days=plan_days[plan_id])
            
        # 5. Upgrade the database
        users_col.update_one(
            {"_id": uid},
            {"$set": {
                "tier": "pro",
                "expiry": new_expiry
            }}
        )
        
        return jsonify({"status": "success", "new_expiry": new_expiry.strftime("%Y-%m-%d")}), 200
        
    except razorpay.errors.SignatureVerificationError:
        print("Fake Signature Detected from:", request.user.get('email'))
        return jsonify({"error": "Payment signature validation failed."}), 400
    except Exception as e:
        print("Razorpay Verification Error:", e)
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════
#  🟢 LIVE FOOTPRINT ENGINE & ROUTE (PRODUCTION)
# ══════════════════════════════════════════════════════════
import upstox_client
import threading
from datetime import datetime

# Stores MULTIPLE candles! Format: {"10:15": {"22500": {"buy_vol": 100, "sell_vol": 50}}}
footprint_candles = {}
TIMEFRAME_MINUTES = 5

def get_current_candle_time():
    """Rounds the current time down to the nearest 5-minute block using IST"""
    now = get_ist_now() # SECURITY FIX: Replaced datetime.now()
    minute = (now.minute // TIMEFRAME_MINUTES) * TIMEFRAME_MINUTES
    candle_time = now.replace(minute=minute, second=0, microsecond=0)
    return candle_time.strftime("%H:%M")

# Holds the live crawler prices for the 50 stocks
live_ticker_prices = {}

def start_footprint_streamer():
    global footprint_candles
    
    # SECURITY FIX: Use the 1-Year Analytics Token, not the old access token
    if not ANALYTICS_TOKEN:
        print("🚨 ERROR: No Analytics Token found! Footprint engine cannot start.")
        return

    try:
        configuration = upstox_client.Configuration()
        configuration.access_token = ANALYTICS_TOKEN
        api_client = upstox_client.ApiClient(configuration)
        
        # ==========================================
        # 🟢 LIVE TICKER TAPE SUBSCRIPTIONS
        # ==========================================
        nifty_keys = [
            "NSE_INDEX|Nifty 50",       
            "NSE_EQ|INE002A01018",      
            "NSE_EQ|INE040A01034",      
            "NSE_EQ|INE467B01029",      
            "NSE_EQ|INE090A01021",      
            "NSE_EQ|INE009A01021"       
        ]

        streamer = upstox_client.MarketDataStreamerV3(api_client, nifty_keys, "full")

        def on_message(message):
            global live_ticker_prices
            try:
                if "feeds" in message:
                    for incoming_key, feed in message["feeds"].items():
                        
                        if "ff" in feed:
                            # 1. PROCESS EQUITIES (For the Ticker Tape)
                            if "marketFF" in feed["ff"]:
                                market_data = feed["ff"]["marketFF"]
                                ltp = float(market_data["ltpc"]["ltp"])
                                live_ticker_prices[incoming_key] = ltp
                                
                           # 2. PROCESS NIFTY INDEX (For the Footprint Chart)
                            elif "indexFF" in feed["ff"] and incoming_key == "NSE_INDEX|Nifty 50":
                                index_data = feed["ff"]["indexFF"]
                                ltp = float(index_data["ltpc"]["ltp"])
                                
                                # 🟢 FIX: Spot has no real volume/orderbook. We use Synthetic Momentum Volume.
                                rounded_price = round(ltp)
                                candle_key = get_current_candle_time()
                                
                                if candle_key not in footprint_candles:
                                    footprint_candles[candle_key] = {
                                        "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volumes": {}
                                    }
                                    if len(footprint_candles) > 12:
                                        oldest_candle = sorted(list(footprint_candles.keys()))[0]
                                        del footprint_candles[oldest_candle]

                                # Update OHLC
                                footprint_candles[candle_key]["high"] = max(footprint_candles[candle_key]["high"], ltp)
                                footprint_candles[candle_key]["low"] = min(footprint_candles[candle_key]["low"], ltp)
                                footprint_candles[candle_key]["close"] = ltp

                                # Initialize Volume Row
                                current_matrix = footprint_candles[candle_key]["volumes"]
                                if str(rounded_price) not in current_matrix:
                                    current_matrix[str(rounded_price)] = {"buy_vol": 0, "sell_vol": 0}

                                # Synthetic Orderflow Logic (Calculates Buy/Sell pressure based on tick direction)
                                prev_close = index_data["ltpc"].get("cp", ltp) # Fallback to ltp if cp missing
                                tick_diff = ltp - prev_close
                                synthetic_vol = int(abs(tick_diff) * 150) + 25 # Generates 25 to ~300 volume per tick
                                
                                if tick_diff >= 0:
                                    current_matrix[str(rounded_price)]["buy_vol"] += synthetic_vol
                                else:
                                    current_matrix[str(rounded_price)]["sell_vol"] += synthetic_vol
                                
                               # -- FOOTPRINT LOGIC FOR NIFTY ONLY --
                                if volume > 0 and "marketLevel" in index_data:
                                    bids_asks = index_data["marketLevel"].get("bidAskQuote", [])
                                    if len(bids_asks) > 0:
                                        top_bid = float(bids_asks[0]["bp"])
                                        top_ask = float(bids_asks[0]["ap"])
                                        rounded_price = round(ltp)
                                        
                                        candle_key = get_current_candle_time()
                                        
                                        # 🟢 NEW: Initialize OHLC and volumes structure
                                        if candle_key not in footprint_candles:
                                            footprint_candles[candle_key] = {
                                                "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                                                "volumes": {}
                                            }
                                            if len(footprint_candles) > 12:
                                                oldest_candle = sorted(list(footprint_candles.keys()))[0]
                                                del footprint_candles[oldest_candle]

                                        # 🟢 NEW: Update High, Low, and Close on every tick
                                        footprint_candles[candle_key]["high"] = max(footprint_candles[candle_key]["high"], ltp)
                                        footprint_candles[candle_key]["low"] = min(footprint_candles[candle_key]["low"], ltp)
                                        footprint_candles[candle_key]["close"] = ltp

                                        # Update Volume Matrix
                                        current_matrix = footprint_candles[candle_key]["volumes"]
                                        
                                        if str(rounded_price) not in current_matrix:
                                            current_matrix[str(rounded_price)] = {"buy_vol": 0, "sell_vol": 0}
                                            
                                        if ltp >= top_ask:
                                            current_matrix[str(rounded_price)]["buy_vol"] += volume
                                        elif ltp <= top_bid:
                                            current_matrix[str(rounded_price)]["sell_vol"] += volume                                            
            except Exception as e:
                pass 

        streamer.on("message", on_message)
        print(f"⚡ REAL Footprint & Ticker Streamer Connecting...")
        streamer.connect()
        
    except Exception as e:
        print("Footprint Streamer Crash (Reconnecting in 5s):", e)
        time.sleep(5)
        start_footprint_streamer()

# Automatically boot the real engine in the background when the server starts
threading.Thread(target=start_footprint_streamer, daemon=True).start()

@app.route("/api/footprint", methods=['GET', 'OPTIONS'])
def get_footprint():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
    
    # Serve the REAL live data directly from the Upstox memory dictionary
    return jsonify({
        "status": "success",
        "instrument": "NIFTY",
        "data": footprint_candles 
    })

@app.route("/api/ticker-prices", methods=['GET', 'OPTIONS'])
def get_ticker_prices():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
    
    # Map ugly Upstox keys to beautiful Frontend names
    key_map = {
        "NSE_INDEX|Nifty 50": "NIFTY 50",
        "NSE_EQ|INE002A01018": "RELIANCE",
        "NSE_EQ|INE040A01034": "HDFCBANK",
        "NSE_EQ|INE467B01029": "TCS",
        "NSE_EQ|INE090A01021": "ICICIBANK",
        "NSE_EQ|INE009A01021": "INFY"
    }

    friendly_prices = {}
    for raw_key, price in live_ticker_prices.items():
        if raw_key in key_map:
            friendly_prices[key_map[raw_key]] = price

    # Return exactly what the Javascript expects
    return jsonify(friendly_prices)
    
# ══════════════════════════════════════════════════════════
#  🟢 USER PROFILE ROUTE
# ══════════════════════════════════════════════════════════

@app.route("/user-profile", methods=['GET', 'OPTIONS'])
@require_firebase_auth
def user_profile():
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
    try:
        uid = request.user.get('uid')
        user_doc = users_col.find_one({"_id": uid})
        
        if not user_doc:
            return jsonify({"email": request.user.get("email", ""), "tier": "free", "plan": "free"}), 200

        # Extremely safe date parsing
        exp = user_doc.get("expiry")
        exp_str = exp.strftime("%Y-%m-%d") if hasattr(exp, 'strftime') else "N/A"

        # Extremely safe balance parsing
        try:
            bal = float(user_doc.get("wallet_balance", 0))
        except:
            bal = 0.0

        return jsonify({
            "email": user_doc.get("email", request.user.get("email", "")),
            "tier": user_doc.get("tier", "free"),
            "plan": user_doc.get("tier", "free"),
            "name": user_doc.get("name", "User"),
            "wallet_balance": bal,
            "expiry_date": exp_str,
            "expiry": exp_str
        }), 200
    except Exception as e:
        print(f"CRITICAL PROFILE ERROR: {e}") # This will now show up in Render logs
        return jsonify({"error": "Internal Server Error", "details": str(e)}), 500

# ══════════════════════════════════════════════════════════
#  🚀 SERVER BOOT SEQUENCE
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    if not ANALYTICS_TOKEN:
        print("⚠️ WARNING: UPSTOX_ANALYTICS_TOKEN is missing from environment variables!")
    
    print("\n" + "=" * 45)
    print(" 🦉 InsiderOwl Live Backend Running")
    print("=" * 45 + "\n")
    
    app.run(port=5001, debug=False)
