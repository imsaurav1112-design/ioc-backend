"""
InsiderOwl — Upstox Live Market Backend (Cloud Edition)
====================================================================
"""

import os, sys, time, json, requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from flask import Flask, jsonify, request
from flask_cors import CORS
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq, fsolve
from functools import wraps
import pytz

# 🟢 CLOUD IMPORTS
from pymongo import MongoClient
import firebase_admin
from firebase_admin import credentials, auth
import razorpay

app = Flask(__name__)
# 🟢 UPDATED: Specifically allow all origins and headers for GitHub Pages compatibility
CORS(app, resources={r"/*": {
    "origins": "*",
    "allow_headers": ["Authorization", "Content-Type"],
    "methods": ["GET", "POST", "OPTIONS"]
}}, supports_credentials=True)
# ══════════════════════════════════════════════════════════
#  🔑 CONFIGURATION & CLOUD SETUP
# ══════════════════════════════════════════════════════════
API_KEY      = "3e51765a-3794-41ab-b3c9-4a88e0d55e30"
API_SECRET   = "1ky9l299rf"
REDIRECT_URI = "https://ioc-backend-kq9x.onrender.com/callback"

BASE_URL   = "https://api.upstox.com/v2"
_access_token = None

def auth_headers(): 
    return {"Authorization": f"Bearer {_access_token}", "Accept": "application/json"}

# 🟢 FIXED: Added MCX Commodity Support
SYMBOL_MAP = {
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 30,  "step": 100},
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    "MIDCPNIFTY": {"instrument_key": "NSE_INDEX|Nifty Midcap Select", "lot": 50,  "step": 25},
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100}, 
    "CRUDEOIL":   {"instrument_key": "MCX_FO|CRUDEOIL",               "lot": 100, "step": 10},
    "NATURALGAS": {"instrument_key": "MCX_FO|NATURALGAS",             "lot": 1250,"step": 5}
}

# 🟢 FIREBASE ADMIN SETUP
try:
    cred = credentials.Certificate(os.path.join(os.getcwd(), 'firebase-admin.json'))
    firebase_admin.initialize_app(cred)
except Exception as e:
    print(f"⚠️ Firebase Admin Init Error: {e}")

# 🟢 MONGODB ATLAS SETUP
from urllib.parse import quote_plus
DB_USERNAME = quote_plus("insideowl")
DB_PASSWORD = quote_plus("K@vy4120422")
MONGO_URI = f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}@ioc.ecqcgvo.mongodb.net/?appName=ioc"

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["ioc_terminal"]
    sys_col = db["system_config"]        
    users_col = db["users"]
    history_col = db["history"]
    history_col.create_index("createdAt", expireAfterSeconds=3456000)
    print("✅ MongoDB Connected")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")

# 🟢 RAZORPAY SETUP
RZP_KEY_ID = "rzp_test_ShbvbudW5LV1v3"
RZP_KEY_SECRET = "Yz6P5jckKk6OyfuqvZ21YCXG"
RZP_WEBHOOK_SECRET = "ioc_secure_webhook_2026" 
rzp_client = razorpay.Client(auth=(RZP_KEY_ID, RZP_KEY_SECRET))

# ══════════════════════════════════════════════════════════
#  🛡️ SECURITY MIDDLEWARE
# ══════════════════════════════════════════════════════════
def require_firebase_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 🟢 THE FIX: Handle the browser's "Preflight" check
        if request.method == "OPTIONS":
            return jsonify({"status": "ok"}), 200
            
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

# ══════════════════════════════════════════════════════════
#  🟢 OPTIONS MATH ENGINE (BS & PRZ)
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
        except:
            guess = (K_call + K_put) / 2.0
            ans, _, ier, _ = fsolve(objective, guess, full_output=True)
            if ier == 1: return round(ans[0], 2)
            return 0.0
    except: return 0.0

def inject_prz(chain_rows, expiry_date_str, step, spot_price):
    try:
        exp_date = datetime.strptime(f"{expiry_date_str} 15:30:00", "%Y-%m-%d %H:%M:%S")
        days_to_expiry = max(0.001, (exp_date - datetime.now()).total_seconds() / 86400.0)
    except: days_to_expiry = 5.0

    T = max(0.001, days_to_expiry / 365.0)
    live_rate = 0.09 # Standard Repo Rate Baseline

    iv_map = {}
    for r in chain_rows:
        strike = r["strike"]
        ce_ltp = float(r["ce"].get("ltp") or 0)
        pe_ltp = float(r["pe"].get("ltp") or 0)
        
        if spot_price and spot_price > 0:
            if ce_ltp > 0:
                raw_ce_iv = calculate_custom_iv(ce_ltp, spot_price, strike, T, live_rate, 'ce')
                ce_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_ce_iv, 'ce')
                r["ce"].update({"iv": round(raw_ce_iv * 100, 2), **ce_greeks})
            if pe_ltp > 0:
                raw_pe_iv = calculate_custom_iv(pe_ltp, spot_price, strike, T, live_rate, 'pe')
                pe_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_pe_iv, 'pe')
                r["pe"].update({"iv": round(raw_pe_iv * 100, 2), **pe_greeks})

        iv_map[strike] = {"ce_iv": r["ce"].get("iv", 15)/100, "pe_iv": r["pe"].get("iv", 15)/100}

    for r in chain_rows:
        strike = r["strike"]
        next_up = strike + step
        next_dn = strike - step
        r["ce_prz"] = calc_prz(strike, iv_map[strike]["ce_iv"], next_up, iv_map.get(next_up, {}).get("pe_iv", 0.15), days_to_expiry, live_rate)
        r["pe_prz"] = calc_prz(next_dn, iv_map.get(next_dn, {}).get("ce_iv", 0.15), strike, iv_map[strike]["pe_iv"], days_to_expiry, live_rate)
    return chain_rows

# ══════════════════════════════════════════════════════════
#  🟢 COA STATE MACHINE logic
# ══════════════════════════════════════════════════════════
COA_MEMORY = {}

def evaluate_side(vols_dict, mem_side, side_name):
    if not vols_dict: return {"strike": 0, "state": "Strong", "target_strike": 0, "msg": ""}
    sorted_vols = sorted(vols_dict.items(), key=lambda x: x[1], reverse=True)
    max_strike, max_vol = sorted_vols[0]
    pct = round((sorted_vols[1][1] / max_vol * 100), 2) if len(sorted_vols) > 1 else 0
    
    if mem_side["base"] == 0: mem_side["base"] = max_strike
    
    current_state = "Strong"
    target = 0
    msg = ""

    if max_strike != mem_side["base"]:
        mem_side["old_base"] = mem_side["base"]
        mem_side["base"] = max_strike
        mem_side["is_shifting"] = True
        msg = f"Shift: {side_name} base moved to {max_strike}."

    if pct >= 75.0:
        sec_strike = sorted_vols[1][0]
        current_state = "STT" if sec_strike > max_strike else "STB"
        target = sec_strike
        msg = f"{side_name} {current_state} @ {target} ({pct}%)"
    
    mem_side["state"] = current_state
    return {"strike": max_strike, "state": current_state, "target_strike": target, "msg": msg}

def calculate_coa(chain_rows, symbol, expiry):
    mem_key = f"{symbol}_{expiry}"
    if mem_key not in COA_MEMORY: 
        COA_MEMORY[mem_key] = {"sup_mem": {"base": 0, "old_base": 0, "is_shifting": False, "state": "Strong"}, "res_mem": {"base": 0, "old_base": 0, "is_shifting": False, "state": "Strong"}, "logs": []}
    mem = COA_MEMORY[mem_key]
    
    res = evaluate_side({r['strike']: r['ce'].get('volume', 0) for r in chain_rows}, mem['res_mem'], "Resistance")
    sup = evaluate_side({r['strike']: r['pe'].get('volume', 0) for r in chain_rows}, mem['sup_mem'], "Support")
    
    if res['msg'] or sup['msg']:
        mem['logs'] = ([f"{datetime.now().strftime('%I:%M %p')} - {res['msg'] or sup['msg']}"] + mem['logs'])[:50]

    return {"scenario_desc": f"Resistance {res['state']}, Support {sup['state']}", "support": sup, "resistance": res, "logs": mem['logs'], "s2": sup['strike'] - 50, "r2": res['strike'] + 50}

# ══════════════════════════════════════════════════════════
#  🟢 STORAGE & AUTH FIXED
# ══════════════════════════════════════════════════════════
def load_saved_token():
    global _access_token
    try:
        token_doc = sys_col.find_one({"_id": "upstox_auth"})
        if token_doc and token_doc.get("access_token"):
            _access_token = token_doc["access_token"]
            print("✅ Token loaded from MongoDB")
            return True
        return False
    except: return False

def save_token(token):
    global _access_token
    if not token or token == "None": return False
    _access_token = token
    try:
        sys_col.update_one({"_id": "upstox_auth"}, {"$set": {"access_token": token, "date": datetime.now().strftime("%Y-%m-%d"), "last_updated": datetime.now().strftime("%H:%M:%S")}}, upsert=True)
        return True
    except: return False

def compress_and_save(symbol, expiry, spot, pcr, chain_rows):
    if not chain_rows or not spot: return
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    # 🟢 FIXED: High-Res seconds to prevent overwriting
    time_key = now.strftime("%I:%M:%S %p") 
    
    step = SYMBOL_MAP.get(symbol, {}).get("step", 50)
    atm = round(spot / step) * step
    
    compressed = []
    for r in chain_rows:
        if abs(r['strike'] - atm) <= (20 * step):
            compressed.append([r['strike'], r['ce'].get('oi', 0), r['ce'].get('volume', 0), float(r['ce'].get('ltp', 0)), r['pe'].get('oi', 0), r['pe'].get('volume', 0), float(r['pe'].get('ltp', 0))])

    snapshot = {"sym": symbol, "exp": expiry, "date": now.strftime("%Y-%m-%d"), "time_key": time_key, "createdAt": datetime.utcnow(), "spot": spot, "pcr": pcr, "chain": compressed}
    try: history_col.insert_one(snapshot)
    except: pass

def fetch_and_record(symbol):
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return
    try:
        # Get expiry
        r = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        exp = r.json()["data"][0]["expiry"]
        # Get spot
        r = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        spot = list(r.json()["data"].values())[0]["last_price"]
        # Get Chain
        r = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": exp}, headers=auth_headers())
        raw = r.json()["data"]
        
        rows, tce, tpe = [], 0, 0
        for item in raw:
            strike = int(float(item["strike_price"]))
            ce_oi = item.get("call_options", {}).get("market_data", {}).get("oi", 0)
            pe_oi = item.get("put_options", {}).get("market_data", {}).get("oi", 0)
            tce += ce_oi; tpe += pe_oi
            rows.append({"strike": strike, "ce": {"oi": ce_oi, "volume": item.get("call_options", {}).get("market_data", {}).get("volume", 0), "ltp": item.get("call_options", {}).get("market_data", {}).get("ltp", 0)}, "pe": {"oi": pe_oi, "volume": item.get("put_options", {}).get("market_data", {}).get("volume", 0), "ltp": item.get("put_options", {}).get("market_data", {}).get("ltp", 0)}})
        
        compress_and_save(symbol, exp, spot, round(tpe/max(1, tce), 2), rows)
    except Exception as e: print(f"Record Error {symbol}: {e}")

# ══════════════════════════════════════════════════════════
#  🟢 API ROUTES FIXED
# ══════════════════════════════════════════════════════════
@app.route("/cron/record", methods=['GET'])
def trigger_record():
    global _access_token
    if not _access_token: load_saved_token()
    if not _access_token: return jsonify({"error": "no_token"}), 403
    
    for sym in SYMBOL_MAP.keys(): fetch_and_record(sym)
    return jsonify({"status": "success"})

@app.route("/options-chain", methods=['GET', 'OPTIONS'])
@require_firebase_auth
def options_chain():
    symbol = request.args.get("symbol", "NIFTY").upper()
    expiry = request.args.get("expiry", "")
    cfg = SYMBOL_MAP.get(symbol)
    
    r = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": expiry}, headers=auth_headers())
    data = r.json().get("data", [])
    
    spot_r = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
    spot = list(spot_r.json()["data"].values())[0]["last_price"]

    chain_rows = []
    for item in data:
        def p_side(side):
            m = side.get("market_data", {})
            g = side.get("option_greeks", {})
            return {"ltp": m.get("ltp", 0), "oi": m.get("oi", 0), "volume": m.get("volume", 0), "prev_oi": m.get("prev_oi", 0), "iv": g.get("iv", 0)}
        chain_rows.append({"strike": int(float(item["strike_price"])), "ce": p_side(item.get("call_options", {})), "pe": p_side(item.get("put_options", {}))})

    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    coa = calculate_coa(chain_rows, symbol, expiry)
    
    # 🟢 AUTO-SAVE on manual fetch
    compress_and_save(symbol, expiry, spot, 1.0, chain_rows)
    
    return jsonify({"symbol": symbol, "expiry": expiry, "spot": spot, "chain": chain_rows, "coa": coa})

@app.route("/api/intraday-history")
@require_firebase_auth
def intraday_history():
    sym, exp, date = request.args.get("symbol"), request.args.get("expiry"), request.args.get("date")
    recs = list(history_col.find({"sym": sym, "exp": exp, "date": date}).sort("createdAt", 1))
    
    formatted = []
    for r in recs:
        formatted.append({"time": r["time_key"], "spot": r["spot"], "rows": [{"strike": c[0], "ceOI": c[1], "ceVol": c[2], "ceLTP": c[3], "peOI": c[4], "peVol": c[5], "peLTP": c[6]} for c in r["chain"]]})
    return jsonify(formatted)

@app.route("/login")
def login():
    url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI}"
    return f'<a href="{url}">Login to Upstox</a>'

@app.route("/callback")
def callback():
    code = request.args.get("code")
    r = requests.post(f"{BASE_URL}/login/authorization/token", data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"})
    if r.status_code == 200:
        token = r.json()["access_token"]
        save_token(token)
        return "✅ Success"
    return "❌ Error"

if __name__ == "__main__":
    load_saved_token()
    app.run(port=5001)
