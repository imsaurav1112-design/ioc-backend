"""
InsiderOwl — Upstox Live Market Backend (Cloud Edition)
====================================================================
Fixed Syntax Errors & Enhanced Split-Brain Routing.
"""

import os, sys, time, json, requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from flask import Flask, jsonify, request # 🟢 FIXED: Removed the 'f' from requestf
from flask_cors import CORS
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq, fsolve
from functools import wraps
import pytz
import gzip, csv, io

# 🟢 CLOUD IMPORTS
from pymongo import MongoClient
import firebase_admin
from firebase_admin import credentials, auth
import razorpay

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

API_KEY      = "3e51765a-3794-41ab-b3c9-4a88e0d55e30"
API_SECRET   = "1ky9l299rf"
REDIRECT_URI = "https://ioc-backend-kq9x.onrender.com/callback"

BASE_URL   = "https://api.upstox.com/v2"
_access_token = None

# ══════════════════════════════════════════════════════════
#  🟢 SYMBOL MAP
# ══════════════════════════════════════════════════════════
SYMBOL_MAP = {
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 15,  "step": 100}, 
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    "MIDCPNIFTY": {"instrument_key": "NSE_INDEX|Nifty Mid Select",    "lot": 50,  "step": 25}, 
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100}, 
    "BANKEX":     {"instrument_key": "BSE_INDEX|BANKEX",              "lot": 15,  "step": 100},
    "CRUDEOIL":   {"is_mcx": True, "base_name": "CRUDEOIL",   "lot": 100, "step": 10},
    "NATURALGAS": {"is_mcx": True, "base_name": "NATURALGAS", "lot": 1250,"step": 5}
}

# ══════════════════════════════════════════════════════════
#  🟢 THE MCX CUSTOM CHAIN BUILDER
# ══════════════════════════════════════════════════════════
MCX_MASTER_DICT = {}
LAST_MCX_FETCH_DATE = None

def ensure_mcx_master():
    global MCX_MASTER_DICT, LAST_MCX_FETCH_DATE
    today = datetime.now().strftime("%Y-%m-%d")
    if LAST_MCX_FETCH_DATE == today and MCX_MASTER_DICT: return
    
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz"
        response = requests.get(url, timeout=15)
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            reader = csv.DictReader(f)
            new_dict = {}
            for row in reader:
                name = row.get('name', '').upper()
                base = None
                if 'CRUDEOIL' in name: base = 'CRUDEOIL'
                elif 'NATURALGAS' in name or 'NATGAS' in name: base = 'NATURALGAS'
                else: continue
                
                exp = row.get('expiry')
                if not exp: continue
                itype = row.get('instrument_type', '').upper()
                key = row.get('instrument_key')
                
                if base not in new_dict: new_dict[base] = {}
                if exp not in new_dict[base]: new_dict[base][exp] = {"FUT": None, "OPT": {}}
                
                if 'FUT' in itype and 'OPT' not in itype:
                    if 'MINI' not in name: new_dict[base][exp]["FUT"] = key
                elif itype == 'OPTFUT':
                    try:
                        strike = float(row.get('strike'))
                        opt_type = row.get('option_type')
                        if strike not in new_dict[base][exp]["OPT"]: new_dict[base][exp]["OPT"][strike] = {}
                        new_dict[base][exp]["OPT"][strike][opt_type] = key
                    except: pass
            
        MCX_MASTER_DICT = new_dict
        LAST_MCX_FETCH_DATE = today
    except Exception as e:
        print(f"❌ Failed to build MCX Dictionary: {e}")

def fetch_custom_mcx_chain(base_name, expiry_str, headers):
    ensure_mcx_master()
    if base_name not in MCX_MASTER_DICT or expiry_str not in MCX_MASTER_DICT[base_name]:
        return [], None
        
    data = MCX_MASTER_DICT[base_name][expiry_str]
    fut_key = data.get("FUT")
    opt_map = data.get("OPT", {})
    spot = None
    if fut_key:
        try:
            r = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": fut_key}, headers=headers)
            if r.status_code == 200:
                spot = list(r.json().get("data", {}).values())[0].get("last_price")
        except: pass
        
    keys_to_fetch = []
    for strike, types in opt_map.items():
        if "CE" in types: keys_to_fetch.append(types["CE"])
        if "PE" in types: keys_to_fetch.append(types["PE"])
        
    if not keys_to_fetch: return [], spot
    
    raw_list = []
    try:
        # Split keys into batches of 50 to avoid API limits
        for i in range(0, len(keys_to_fetch), 50):
            batch = keys_to_fetch[i:i+50]
            r = requests.get(f"{BASE_URL}/market-quote/quotes", params={"instrument_key": ",".join(batch)}, headers=headers)
            quotes = r.json().get("data", {})
            
            for strike, types in opt_map.items():
                ce_key, pe_key = types.get("CE"), types.get("PE")
                cq, pq = quotes.get(ce_key, {}), quotes.get(pe_key, {})
                if not cq and not pq: continue
                
                raw_list.append({
                    "strike_price": float(strike),
                    "underlying_spot_price": spot or 0,
                    "call_options": {"market_data": {"ltp": cq.get("last_price", 0), "oi": cq.get("open_interest", 0), "volume": cq.get("volume", 0), "prev_oi": 0}},
                    "put_options": {"market_data": {"ltp": pq.get("last_price", 0), "oi": pq.get("open_interest", 0), "volume": pq.get("volume", 0), "prev_oi": 0}}
                })
    except Exception as e:
        print(f"MCX custom fetch error: {e}")
    return raw_list, spot

# ══════════════════════════════════════════════════════════
#  🟢 DATABASE & AUTH SETUP
# ══════════════════════════════════════════════════════════
try:
    cred = credentials.Certificate(os.path.join(os.getcwd(), 'firebase-admin.json'))
    firebase_admin.initialize_app(cred)
except: pass

from urllib.parse import quote_plus
DB_USERNAME, DB_PASSWORD = quote_plus("insideowl"), quote_plus("K@vy4120422")
MONGO_URI = f"mongodb+srv://{DB_USERNAME}:{DB_PASSWORD}@ioc.ecqcgvo.mongodb.net/?appName=ioc"

try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["ioc_terminal"]
    sys_col, users_col, history_col = db["system_config"], db["users"], db["history"]
    history_col.create_index("createdAt", expireAfterSeconds=3456000)
except: pass

def require_firebase_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method == "OPTIONS": return jsonify({"status": "ok"}), 200
        header = request.headers.get("Authorization")
        if not header or not header.startswith("Bearer "): return jsonify({"error": "Unauthorized"}), 401
        token = header.split(" ")[1]
        try:
            decoded_token = auth.verify_id_token(token)
            request.user = decoded_token
        except: return jsonify({"error": "Invalid Token"}), 401
        return f(*args, **kwargs)
    return decorated_function

def auth_headers(): return {"Authorization": f"Bearer {_access_token}", "Accept": "application/json"}

# ══════════════════════════════════════════════════════════
#  🟢 GREEKS & PRZ MATH ENGINE
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
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    vega = S * norm.pdf(d1) * np.sqrt(T) / 100.0
    if opt_type == 'ce':
        delta = norm.cdf(d1)
        theta = (- (S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) - r * K * np.exp(-r*T) * norm.cdf(d1 - sigma*np.sqrt(T))) / 365.0
    else:
        delta = -norm.cdf(-d1)
        theta = (- (S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T)) + r * K * np.exp(-r*T) * norm.cdf(-(d1 - sigma*np.sqrt(T)))) / 365.0
    return {'delta': round(delta, 4), 'gamma': round(gamma * 10000, 2), 'theta': round(theta, 4), 'vega': round(vega, 4)}

def calculate_custom_iv(market_ltp, S, K, T, r, opt_type='ce'):
    intrinsic = max(0.0, S - K) if opt_type == 'ce' else max(0.0, K - S)
    safe_ltp = max(market_ltp, intrinsic + 0.01)
    def objective(sigma): return bs_call(S, K, T, r, sigma) - safe_ltp if opt_type == 'ce' else bs_put(S, K, T, r, sigma) - safe_ltp
    try: return brentq(objective, 1e-4, 10.0)
    except: return 0.15

def inject_prz(chain_rows, expiry_date_str, step, spot_price):
    try:
        exp_date = datetime.strptime(f"{expiry_date_str} 15:30:00", "%Y-%m-%d %H:%M:%S")
        days_to_expiry = max(0.001, (exp_date - datetime.now()).total_seconds() / 86400.0)
    except: days_to_expiry = 5.0
    T, live_rate = max(0.001, days_to_expiry / 365.0), 0.10
    iv_map = {}
    for r in chain_rows:
        strike, ce_ltp, pe_ltp = r["strike"], float(r["ce"].get("ltp") or 0), float(r["pe"].get("ltp") or 0)
        if spot_price and spot_price > 0:
            civ = calculate_custom_iv(ce_ltp, spot_price, strike, T, live_rate, 'ce') if ce_ltp > 0 else 0.15
            piv = calculate_custom_iv(pe_ltp, spot_price, strike, T, live_rate, 'pe') if pe_ltp > 0 else 0.15
            cg, pg = bs_greeks(spot_price, strike, T, live_rate, civ, 'ce'), bs_greeks(spot_price, strike, T, live_rate, piv, 'pe')
            r["ce"].update({"iv": round(civ*100,2), **cg}); r["pe"].update({"iv": round(piv*100,2), **pg})
            iv_map[strike] = {"ce_iv": civ, "pe_iv": piv}
    
    # PRZ Logic
    for r in chain_rows:
        strike = r["strike"]
        ce_iv, pe_iv = iv_map.get(strike, {}).get("ce_iv", 0.15), iv_map.get(strike, {}).get("pe_iv", 0.15)
        p_up = iv_map.get(strike+step, {}).get("pe_iv", 0.15)
        c_dn = iv_map.get(strike-step, {}).get("ce_iv", 0.15)
        
        def calc_obj_prz(K1, iv1, K2, iv2, mode):
            def obj(S): return bs_call(S, K1, T, live_rate, iv1) - bs_put(S, K2, T, live_rate, iv2)
            try: return round(brentq(obj, K1-step*10, K2+step*10), 2)
            except: return 0.0
            
        r["ce_prz"] = calc_obj_prz(strike, ce_iv, strike+step, p_up, 'ce')
        r["pe_prz"] = calc_obj_prz(strike-step, c_dn, strike, pe_iv, 'pe')
    return chain_rows

# ══════════════════════════════════════════════════════════
#  🟢 COA & ROUTES
# ══════════════════════════════════════════════════════════
def calculate_coa(chain_rows, symbol, expiry):
    # Simplified COA returning strong status
    return {
        "scenario_desc": "Market Neutral", "support": {"state": "Strong", "strike": 0}, "resistance": {"state": "Strong", "strike": 0},
        "s1": 0, "r1": 0, "s2": 0, "r2": 0, "logs": []
    }

@app.route("/expiry-dates", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def expiry_dates():
    symbol = request.args.get("symbol", "NIFTY").upper()
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return jsonify({"error": "Invalid symbol"}), 400
    
    if cfg.get("is_mcx"):
        ensure_mcx_master()
        base = cfg["base_name"]
        exps = sorted([e for e in MCX_MASTER_DICT.get(base, {}).keys()])
        return jsonify({"symbol": symbol, "expiries": exps})
    else:
        resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        data = resp.json().get("data") or []
        expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
        return jsonify({"symbol": symbol, "expiries": expiries})

@app.route("/options-chain", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def options_chain():
    symbol, expiry = request.args.get("symbol", "NIFTY").upper(), request.args.get("expiry", "")
    cfg = SYMBOL_MAP.get(symbol)
    if cfg.get("is_mcx"):
        raw_list, spot = fetch_custom_mcx_chain(cfg["base_name"], expiry, auth_headers())
    else:
        resp = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": expiry}, headers=auth_headers())
        raw_list = resp.json().get("data") or []
        s_resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        spot = list(s_resp.json().get("data", {}).values())[0].get("last_price") if s_resp.status_code==200 else None

    chain_rows = []
    for item in raw_list:
        strike = int(float(item["strike_price"]))
        def p_side(d):
            md, og = d.get("market_data") or {}, d.get("option_greeks") or {}
            return {"ltp": md.get("ltp", 0), "oi": md.get("oi", 0), "volume": md.get("volume", 0), "change_oi": md.get("oi",0)-md.get("prev_oi",0)}
        chain_rows.append({"strike": strike, "ce": p_side(item.get("call_options", {})), "pe": p_side(item.get("put_options", {}))})

    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    return jsonify({"symbol": symbol, "expiry": expiry, "spot": spot, "chain": chain_rows, "coa": calculate_coa(chain_rows, symbol, expiry)})

# 🟢 Firebase User Logic
@app.route("/user-profile", methods=['GET', 'OPTIONS'])
@require_firebase_auth
def user_profile():
    uid = request.user['uid']
    user_doc = users_col.find_one({"_id": uid})
    if not user_doc:
        user_doc = {"_id": uid, "tier": "pro", "email": request.user.get('email')}
        users_col.insert_one(user_doc)
    return jsonify({"tier": user_doc.get("tier", "pro"), "email": user_doc.get("email")})

def load_saved_token():
    global _access_token
    doc = sys_col.find_one({"_id": "upstox_auth"})
    if doc: _access_token = doc.get("access_token")

if __name__ == "__main__":
    load_saved_token()
    app.run(port=5001, debug=False)
