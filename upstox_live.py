"""
InsiderOwl — Upstox Live Market Backend (Cloud Edition)
====================================================================
Includes Split-Brain Routing for native Equities vs Custom MCX Option Chains.
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
    # NSE / BSE INDICES (Native Option Chain API)
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 15,  "step": 100}, 
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    "MIDCPNIFTY": {"instrument_key": "NSE_INDEX|Nifty Mid Select",    "lot": 50,  "step": 25}, 
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100}, 
    "BANKEX":     {"instrument_key": "BSE_INDEX|BANKEX",              "lot": 15,  "step": 100},
    
    # 🟢 COMMODITIES (Custom Quotes API Chain Builder)
    "CRUDEOIL":   {"is_mcx": True, "base_name": "CRUDEOIL",   "lot": 100, "step": 10},
    "NATURALGAS": {"is_mcx": True, "base_name": "NATURALGAS", "lot": 1250,"step": 5}
}

# ══════════════════════════════════════════════════════════
#  🟢 THE MCX CUSTOM CHAIN BUILDER
# ══════════════════════════════════════════════════════════
MCX_MASTER_DICT = {}
LAST_MCX_FETCH_DATE = None

def ensure_mcx_master():
    """Downloads MCX.csv.gz and maps every Strike to its specific Call/Put Upstox Key"""
    global MCX_MASTER_DICT, LAST_MCX_FETCH_DATE
    today = datetime.now().strftime("%Y-%m-%d")
    if LAST_MCX_FETCH_DATE == today and MCX_MASTER_DICT: return
    
    try:
        print("⏳ Downloading MCX Master CSV for Custom Options Chain Mapping...")
        url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz"
        response = requests.get(url, timeout=15)
        
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            reader = csv.DictReader(f)
            new_dict = {}
            
            for row in reader:
                name = row.get('name', '').upper()
                if 'MINI' in name: continue 
                
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
                    new_dict[base][exp]["FUT"] = key
                elif itype == 'OPTFUT':
                    try:
                        strike = float(row.get('strike'))
                        opt_type = row.get('option_type') # CE or PE
                        if strike not in new_dict[base][exp]["OPT"]:
                            new_dict[base][exp]["OPT"][strike] = {}
                        new_dict[base][exp]["OPT"][strike][opt_type] = key
                    except: pass
            
        MCX_MASTER_DICT = new_dict
        LAST_MCX_FETCH_DATE = today
        print("✅ MCX Master Dictionary Cached Successfully.")
    except Exception as e:
        print(f"❌ Failed to build MCX Dictionary: {e}")

def fetch_custom_mcx_chain(base_name, expiry_str, headers):
    """Bypasses native API: Uses Quotes API to manually construct a commodity option chain"""
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
    
    # Batch max 500 keys for Upstox limit
    keys_to_fetch = keys_to_fetch[:500]
    
    raw_list = []
    try:
        r = requests.get(f"{BASE_URL}/market-quote/quotes", params={"instrument_key": ",".join(keys_to_fetch)}, headers=headers)
        quotes = r.json().get("data", {})
        
        for strike, types in opt_map.items():
            ce_key = types.get("CE")
            pe_key = types.get("PE")
            
            cq = quotes.get(ce_key, {})
            pq = quotes.get(pe_key, {})
            
            if not cq and not pq: continue
            
            raw_list.append({
                "strike_price": float(strike),
                "underlying_spot_price": spot or 0,
                "call_options": {
                    "market_data": {
                        "ltp": cq.get("last_price", 0),
                        "oi": cq.get("open_interest", 0),
                        "volume": cq.get("volume", 0),
                        "prev_oi": 0
                    }
                },
                "put_options": {
                    "market_data": {
                        "ltp": pq.get("last_price", 0),
                        "oi": pq.get("open_interest", 0),
                        "volume": pq.get("volume", 0),
                        "prev_oi": 0
                    }
                }
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
except Exception as e:
    print(f"⚠️ Firebase Admin Init Error (Auth will fail): {e}")

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
    print("✅ Connected to MongoDB Atlas")
except Exception as e:
    print(f"❌ MongoDB Connection Error: {e}")

RZP_KEY_ID = "rzp_test_ShbvbudW5LV1v3"
RZP_KEY_SECRET = "Yz6P5jckKk6OyfuqvZ21YCXG"
RZP_WEBHOOK_SECRET = "ioc_secure_webhook_2026" 
rzp_client = razorpay.Client(auth=(RZP_KEY_ID, RZP_KEY_SECRET))

def require_firebase_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if request.method == "OPTIONS":
            response = jsonify({"status": "ok"})
            response.headers.add("Access-Control-Allow-Origin", "*")
            response.headers.add("Access-Control-Allow-Headers", "Content-Type, Authorization")
            response.headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
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
    return {"Authorization": f"Bearer {_access_token}", "Accept": "application/json"}

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
        days_to_expiry = max(0.001, (exp_date - datetime.now()).total_seconds() / 86400.0)
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
        raw_ce_iv = 0.15
        raw_pe_iv = 0.15
        
        if spot_price and spot_price > 0:
            if ce_ltp > 0:
                raw_ce_iv = calculate_custom_iv(ce_ltp, spot_price, strike, T, live_rate, 'ce')
                ce_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_ce_iv, 'ce')
                r["ce"]["iv"] = round(raw_ce_iv * 100, 2)
                r["ce"]["delta"] = ce_greeks["delta"]
                r["ce"]["theta"] = ce_greeks["theta"]
                r["ce"]["vega"] = ce_greeks["vega"]
                r["ce"]["gamma"] = ce_greeks["gamma"]
            else:
                r["ce"]["iv"] = 0; r["ce"]["delta"] = 0; r["ce"]["theta"] = 0; r["ce"]["vega"] = 0; r["ce"]["gamma"] = 0
                
            if pe_ltp > 0:
                raw_pe_iv = calculate_custom_iv(pe_ltp, spot_price, strike, T, live_rate, 'pe')
                pe_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_pe_iv, 'pe')
                r["pe"]["iv"] = round(raw_pe_iv * 100, 2)
                r["pe"]["delta"] = pe_greeks["delta"]
                r["pe"]["theta"] = pe_greeks["theta"]
                r["pe"]["vega"] = pe_greeks["vega"]
                r["pe"]["gamma"] = pe_greeks["gamma"]
            else:
                r["pe"]["iv"] = 0; r["pe"]["delta"] = 0; r["pe"]["theta"] = 0; r["pe"]["vega"] = 0; r["pe"]["gamma"] = 0

        iv_map[strike] = {"ce_iv": raw_ce_iv if raw_ce_iv > 0 else 0.15, "pe_iv": raw_pe_iv if raw_pe_iv > 0 else 0.15}

    for r in chain_rows:
        strike = r["strike"]
        ce_iv = iv_map[strike]["ce_iv"]
        pe_iv = iv_map[strike]["pe_iv"]
        next_strike_up = strike + step
        next_strike_down = strike - step
        pe_iv_up = iv_map.get(next_strike_up, {}).get("pe_iv", 0.15)
        ce_iv_down = iv_map.get(next_strike_down, {}).get("ce_iv", 0.15)

        r["ce_prz"] = calc_prz(strike, ce_iv, next_strike_up, pe_iv_up, days_to_expiry, live_rate)
        r["pe_prz"] = calc_prz(next_strike_down, ce_iv_down, strike, pe_iv, days_to_expiry, live_rate)

    return chain_rows

# ══════════════════════════════════════════════════════════
#  🟢 COA STATE MACHINE LOGIC
# ══════════════════════════════════════════════════════════
COA_MEMORY = {}

def evaluate_side(vols_dict, mem_side, side_name):
    if not vols_dict: 
        return {"strike": 0, "state": "Strong", "target_strike": 0, "pct": 0, "val": 0, "msg": ""}
        
    sorted_vols = sorted(vols_dict.items(), key=lambda x: x[1], reverse=True)
    max_strike, max_vol = sorted_vols[0]
    sec_strike, sec_vol = 0, 0
    pct = 0.0
    
    if len(sorted_vols) > 1:
        sec_strike, sec_vol = sorted_vols[1]
        pct = round((sec_vol / max_vol * 100), 2) if max_vol > 0 else 0
        
    if mem_side["base"] == 0:
        mem_side["base"] = max_strike
        return {"strike": max_strike, "state": "Strong", "target_strike": 0, "pct": pct, "val": max_vol, "msg": f"{side_name} established at {max_strike}."}

    msg = ""
    current_state = "Strong"
    target = 0

    if max_strike != mem_side["base"]:
        mem_side["old_base"] = mem_side["base"]
        mem_side["base"] = max_strike
        mem_side["is_shifting"] = True
        mem_side["lowest_pct"] = pct
        msg = f"Shift in Progress: {side_name} base moved to {max_strike}."
        
    if mem_side["is_shifting"]:
        if sec_strike == mem_side["old_base"]:
            mem_side["lowest_pct"] = min(mem_side["lowest_pct"], pct)
            if pct < 75.0:
                mem_side["is_shifting"] = False
                mem_side["old_base"] = 0
                msg = f"Shift Complete: {side_name} has successfully consolidated at {max_strike}."
                current_state = "Strong"
            else:
                if mem_side["lowest_pct"] < 75.0:
                    current_state = "STT" if sec_strike > max_strike else "STB"
                    target = sec_strike
                else: current_state = "Strong"
        else:
            if pct >= 75.0:
                current_state = "STT" if sec_strike > max_strike else "STB"
                target = sec_strike
            else: current_state = "Strong"
    else:
        if pct >= 75.0:
            current_state = "STT" if sec_strike > max_strike else "STB"
            target = sec_strike
            
    if msg == "" and current_state != mem_side["state"]:
        if current_state == "STT": msg = f"Bullish Pressure: {side_name} is Sliding Towards Top ({target})."
        elif current_state == "STB": msg = f"Bearish Pressure: {side_name} is Sliding Towards Bottom ({target})."
        elif current_state == "Strong": msg = f"{side_name} has become Strong at {max_strike}."
        
    mem_side["state"] = current_state
    mem_side["target"] = target
    return {"strike": max_strike, "state": current_state, "target_strike": target, "pct": pct, "val": max_vol, "msg": msg}

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
    
    ct = datetime.now().strftime("%I:%M %p")
    new_logs = []
    if res['msg']: new_logs.append(f"{ct} - {res['msg']}")
    if sup['msg']: new_logs.append(f"{ct} - {sup['msg']}")
    if new_logs: mem['logs'] = (new_logs + mem['logs'])[:100]

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
        "scenario_desc": f"Currently, Resistance is {res['state']} and Support is {sup['state']}.", 
        "support": sup, "resistance": res, 
        "s1": s1_val, "r1": r1_val, "s2": s2_row['pe_prz'] if s2_row else s2_strike, "r2": r2_row['ce_prz'] if r2_row else r2_strike, 
        "logs": mem['logs']
    }

# ══════════════════════════════════════════════════════════
#  🟢 TERMINAL DATA ROUTES (SPLIT-BRAIN)
# ══════════════════════════════════════════════════════════
@app.route("/health")
def health(): return jsonify({"status": "ok", "authenticated": _access_token is not None})

@app.route("/expiry-dates", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def expiry_dates():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    if symbol not in SYMBOL_MAP: return jsonify({"error": "Invalid symbol"}), 400 
    cfg = SYMBOL_MAP.get(symbol)
    
    is_backtest = request.headers.get("Referer", "").endswith("backtester.html")
    if is_backtest:
        try:
            saved_expiries = history_col.distinct("exp", {"sym": symbol})
            return jsonify({"symbol": symbol, "expiries": sorted(saved_expiries)})
        except Exception as e: return jsonify({"error": str(e)}), 500

    # 🟢 SPLIT-BRAIN EXPIRY FETCH
    if cfg.get("is_mcx"):
        ensure_mcx_master()
        base = cfg["base_name"]
        valid_exps = []
        for e in MCX_MASTER_DICT.get(base, {}).keys():
            try:
                d = datetime.strptime(e, "%Y-%m-%d").date()
                if d >= datetime.now().date(): valid_exps.append(e)
            except: valid_exps.append(e)
        return jsonify({"symbol": symbol, "expiries": sorted(valid_exps)})
    else:
        resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        data = resp.json().get("data") or []
        expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
        return jsonify({"symbol": symbol, "expiries": expiries})

@app.route("/api/intraday-history", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def intraday_history():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    target_date = request.args.get("date", (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")).strip()
    
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

@app.route("/options-chain", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def options_chain():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    cfg = SYMBOL_MAP.get(symbol)
    
    # 🟢 SPLIT-BRAIN CHAIN FETCH
    if cfg.get("is_mcx"):
        raw_list, spot = fetch_custom_mcx_chain(cfg["base_name"], expiry, auth_headers())
        if not raw_list: return jsonify({"error": "No MCX options found for this expiry"}), 502
    else:
        resp = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": expiry}, headers=auth_headers())
        raw_list = resp.json().get("data") or []
        if not raw_list: return jsonify({"error": "No data from Upstox API"}), 502

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
            if not d: return {"ltp": 0, "oi": 0, "change_oi": 0, "volume": 0, "iv": 0, "delta": 0, "theta": 0, "vega": 0, "gamma": 0}
            md, og = d.get("market_data") or {}, d.get("option_greeks") or {}
            raw_iv = float(og.get("iv") or 0)
            return {
                "ltp": md.get("ltp", 0), "oi": int(md.get("oi") or 0), 
                "change_oi": int(md.get("oi") or 0) - int(md.get("prev_oi") or 0),
                "volume": md.get("volume", 0), 
                "iv": round(raw_iv * 100, 2) if 0 < raw_iv < 1.0 else round(raw_iv, 2), 
                "delta": round(float(og.get("delta") or 0), 4), "theta": round(float(og.get("theta") or 0), 4), 
                "vega": round(float(og.get("vega") or 0), 4), "gamma": round(float(og.get("gamma") or 0.0005) * 10000, 2)
            }
            
        ce = parse_side(item.get("call_options"))
        pe = parse_side(item.get("put_options"))
        total_ce_oi += ce["oi"] or 0
        total_pe_oi += pe["oi"] or 0
        chain_rows.append({"strike": strike, "atm": atm is not None and abs(strike - atm) < cfg["step"], "ce": ce, "pe": pe})

    # The Custom Math Engine natively calculates MCX Greeks!
    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    coa_data = calculate_coa(chain_rows, symbol, expiry)

    return jsonify({
        "symbol": symbol, "expiry": expiry, "spot": spot, "pcr": round(total_pe_oi / max(total_ce_oi, 1), 2),
        "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi, "lot_size": cfg["lot"],
        "chain": chain_rows, "fetched_at": datetime.now().strftime("%H:%M:%S"), "coa": coa_data
    })

# ══════════════════════════════════════════════════════════
#  🟢 THE EXTERNAL CRON ENGINE
# ══════════════════════════════════════════════════════════
def compress_and_save(symbol, expiry, spot, pcr, chain_rows):
    if not chain_rows or not spot: return
    ts = datetime.utcnow() + timedelta(hours=5, minutes=30)
    time_key = ts.strftime("%I:%M %p")
    date_str = ts.strftime("%Y-%m-%d")

    step = SYMBOL_MAP[symbol]["step"]
    atm = round(spot / step) * step
    
    compressed_chain = []
    for r in chain_rows:
        if abs(r['strike'] - atm) <= (20 * step): 
            compressed_chain.append([
                r['strike'], r['ce'].get('oi', 0), r['ce'].get('volume', 0), float(r['ce'].get('ltp', 0)),
                r['pe'].get('oi', 0), r['pe'].get('volume', 0), float(r['pe'].get('ltp', 0))
            ])

    try:
        history_col.update_one({"sym": symbol, "exp": expiry, "date": date_str, "time_key": time_key},
            {"$set": {"sym": symbol, "exp": expiry, "date": date_str, "time_key": time_key, "createdAt": datetime.utcnow(), "spot": spot, "pcr": pcr, "chain": compressed_chain}}, upsert=True)
        print(f"💾 SAVED TO MONGO: {symbol} at {time_key}")
    except Exception as e: print(f"❌ MongoDB Record Error: {e}")

def fetch_and_record(symbol):
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return
    
    try:
        # 🟢 SPLIT-BRAIN CRON ENGINE
        if cfg.get("is_mcx"):
            ensure_mcx_master()
            base = cfg["base_name"]
            valid_exps = []
            for e in MCX_MASTER_DICT.get(base, {}).keys():
                try:
                    d = datetime.strptime(e, "%Y-%m-%d").date()
                    if d >= datetime.now().date(): valid_exps.append((e, d))
                except: pass
            if not valid_exps: return
            valid_exps.sort(key=lambda x: x[1])
            exp = valid_exps[0][0]
            
            raw, spot = fetch_custom_mcx_chain(base, exp, auth_headers())
        else:
            r = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
            exp = r.json()["data"][0]["expiry"]
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

@app.route("/cron/record", methods=['GET'])
def trigger_record():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    global _access_token
    if not _access_token: load_saved_token()
    
    is_weekday = now.weekday() < 5
    is_market_open = ((now.hour == 9 and now.minute >= 15) or (now.hour > 9 and now.hour < 23) or (now.hour == 23 and now.minute <= 30))
    
    if not _access_token: return jsonify({"status": "blocked", "reason": "no_token"}), 403

    if is_weekday and is_market_open:
        try:
            for sym in SYMBOL_MAP.keys(): fetch_and_record(sym)
            return jsonify({"status": "success", "message": f"Recorded all indices at {now.strftime('%H:%M:%S')} IST"})
        except Exception as e: return jsonify({"status": "error", "message": str(e)}), 500
    
    return jsonify({"status": "sleeping", "message": "Market Closed"}), 200

# ══════════════════════════════════════════════════════════
#  🟢 SERVER AUTH (Login, Tokens, Razorpay logic truncated for clarity, it remains unchanged)
# ══════════════════════════════════════════════════════════
def load_saved_token():
    global _access_token
    try:
        token_doc = sys_col.find_one({"_id": "upstox_auth"})
        if not token_doc: return False
        token = token_doc.get("access_token", "")
        if token:
            _access_token = token
            return True
        return False
    except: return False

def save_token(token):
    global _access_token
    if not token or token == "None": return False
    _access_token = token
    try:
        sys_col.update_one({"_id": "upstox_auth"}, {"$set": {"access_token": token, "date": datetime.now().strftime("%Y-%m-%d")}}, upsert=True)
        return True
    except: return False

@app.route("/login")
def login_route():
    params = {"response_type": "code", "client_id": API_KEY, "redirect_uri": REDIRECT_URI}
    login_url = f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(params)}"
    return f'<a href="{login_url}">Click here to Login</a>'

@app.route("/callback")
def callback_route():
    code = request.args.get("code")
    resp = requests.post("https://api.upstox.com/v2/login/authorization/token", data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"}, headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    if resp.status_code == 200:
        save_token(resp.json().get("access_token"))
        return '✅ Login Successful!'
    return f'❌ Failed: <p>{resp.text}</p>'

@app.route("/user-profile", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def user_profile():
    return jsonify({"tier": "pro", "email": request.user.get('email', '')})

if __name__ == "__main__":
    load_saved_token()
    print("\n Server Running\n" + "-" * 45)
    app.run(port=5001, debug=False)
