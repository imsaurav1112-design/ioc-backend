"""
InsiderOwl — Upstox Live Market Backend (Cloud Edition)
====================================================================
Includes 9:15-3:30 Automated Background Recorder (MongoDB Atlas).
Includes Split-Brain Routing to bypass Upstox MCX Option Chain limits.
Provides raw instrument_keys to the frontend for WebSocket live feed.
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
# Allow cross-origin requests
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# ══════════════════════════════════════════════════════════
#  🔑 CONFIGURATION & CLOUD SETUP
# ══════════════════════════════════════════════════════════
API_KEY      = "3e51765a-3794-41ab-b3c9-4a88e0d55e30"
API_SECRET   = "1ky9l299rf"
REDIRECT_URI = "https://ioc-backend-kq9x.onrender.com/callback"

BASE_URL   = "https://api.upstox.com/v2"
_access_token = None

# 🟢 SYMBOL MAP
SYMBOL_MAP = {
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 15,  "step": 100}, 
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    # 🟢 FIXED: Permanent Numeric Token for Midcap
    "MIDCPNIFTY": {"instrument_key": "NSE_INDEX|288009",              "lot": 50,  "step": 25}, 
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100}, 
    "BANKEX":     {"instrument_key": "BSE_INDEX|BANKEX",              "lot": 15,  "step": 100},
    
    # 🟢 COMMODITIES: Flagged to trigger Custom Chain Builder
    "CRUDEOIL":   {"is_mcx": True, "base_name": "CRUDEOIL",   "lot": 100, "step": 10},
    "NATURALGAS": {"is_mcx": True, "base_name": "NATURALGAS", "lot": 1250,"step": 5}
}

# ══════════════════════════════════════════════════════════
#  🟢 DYNAMIC MCX CUSTOM CHAIN BUILDER
# ══════════════════════════════════════════════════════════
MCX_MASTER_DICT = {}
LAST_MCX_FETCH_DATE = None

def ensure_mcx_master():
    """Downloads MCX.csv.gz and maps every Strike to its specific Call/Put Upstox Key"""
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
                
                # Check for Futures or Options
                if itype in ['FUTCOM', 'FUTENR', 'FUT']:
                    if 'MINI' not in name: new_dict[base][exp]["FUT"] = key
                elif itype == 'OPTFUT':
                    try:
                        strike = float(row.get('strike'))
                        opt_type = row.get('option_type')
                        if strike not in new_dict[base][exp]["OPT"]:
                            new_dict[base][exp]["OPT"][strike] = {}
                        new_dict[base][exp]["OPT"][strike][opt_type] = key
                    except: pass            
        MCX_MASTER_DICT = new_dict
        LAST_MCX_FETCH_DATE = today
    except Exception as e:
        print(f"❌ Failed to build MCX Dictionary: {e}")

def fetch_custom_mcx_chain(base_name, expiry_str, headers):
    """Bypasses native API: Uses Quotes API to manually construct an MCX option chain"""
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
        # Split into batches of 50 to avoid Upstox URL length limits
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
                    "call_options": {
                        "instrument_key": ce_key, # 🟢 RAW KEY FOR WEBSOCKET
                        "market_data": {
                            "ltp": cq.get("last_price", 0), "oi": cq.get("open_interest", 0), 
                            "volume": cq.get("volume", 0), "prev_oi": 0
                        }
                    },
                    "put_options": {
                        "instrument_key": pe_key, # 🟢 RAW KEY FOR WEBSOCKET
                        "market_data": {
                            "ltp": pq.get("last_price", 0), "oi": pq.get("open_interest", 0), 
                            "volume": pq.get("volume", 0), "prev_oi": 0
                        }
                    }
                })
    except Exception as e:
        print(f"MCX custom fetch error: {e}")
        
    return raw_list, spot

# ══════════════════════════════════════════════════════════
#  🟢 FIREBASE & MONGODB SETUP
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
    sys_col, users_col, history_col = db["system_config"], db["users"], db["history"]
    history_col.create_index("createdAt", expireAfterSeconds=3456000)
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
        if not header or not header.startswith("Bearer "): return jsonify({"error": "Unauthorized Access"}), 401
        token = header.split(" ")[1]
        try:
            decoded_token = auth.verify_id_token(token)
            request.user = decoded_token
        except Exception as e: return jsonify({"error": "Invalid or Expired Token", "details": str(e)}), 401
        return f(*args, **kwargs)
    return decorated_function

def auth_headers(): 
    return {"Authorization": f"Bearer {_access_token}", "Accept": "application/json"}

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
    if not vols_dict: return {"strike": 0, "state": "Strong", "target_strike": 0, "pct": 0, "val": 0, "msg": ""}
    sorted_vols = sorted(vols_dict.items(), key=lambda x: x[1], reverse=True)
    max_strike, max_vol = sorted_vols[0]
    sec_strike = sorted_vols[1][0] if len(sorted_vols) > 1 else 0
    sec_vol = sorted_vols[1][1] if len(sorted_vols) > 1 else 0
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

    # 🟢 SPLIT BRAIN: Route MCX vs Indices
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

@app.route("/options-chain", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def options_chain():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    cfg = SYMBOL_MAP.get(symbol)

    # 🟢 SPLIT BRAIN: Custom MCX Builder vs Native API
    if cfg.get("is_mcx"):
        raw_list, spot = fetch_custom_mcx_chain(cfg["base_name"], expiry, auth_headers())
        if not raw_list: return jsonify({"error": "No MCX options found for this expiry"}), 502
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
            # 🟢 RAW KEY EXPOSED FOR WEBSOCKET
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
        total_pe_oi += pe["oi"]
        chain_rows.append({"strike": strike, "atm": atm is not None and abs(strike - atm) < cfg["step"], "ce": ce, "pe": pe})

    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    coa_data = calculate_coa(chain_rows, symbol, expiry)

    return jsonify({
        "symbol": symbol, "expiry": expiry, "spot": spot, "pcr": round(total_pe_oi / max(total_ce_oi, 1), 2),
        "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi, "lot_size": cfg["lot"],
        "chain": chain_rows, "fetched_at": datetime.now().strftime("%H:%M:%S"), "coa": coa_data
    })

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
        history_col.update_one(
            {"sym": symbol, "exp": expiry, "date": date_str, "time_key": time_key},
            {"$set": {"sym": symbol, "exp": expiry, "date": date_str, "time_key": time_key, "createdAt": datetime.utcnow(), "spot": spot, "pcr": pcr, "chain": compressed_chain}},
            upsert=True
        )
        print(f"💾 SAVED TO MONGO: {symbol} at {time_key}")
    except Exception as e:
        print(f"❌ MongoDB Record Error: {e}")

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
    is_market_open = (
        (now.hour == 9 and now.minute >= 15) or 
        (now.hour > 9 and now.hour < 23) or 
        (now.hour == 23 and now.minute <= 30)
    )
    
    if not _access_token:
        return jsonify({"status": "blocked", "reason": "no_token"}), 403

    if is_weekday and is_market_open:
        try:
            for sym in SYMBOL_MAP.keys():
                fetch_and_record(sym)
            return jsonify({"status": "success", "message": f"Recorded all indices at {now.strftime('%H:%M:%S')} IST"})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)}), 500
    
    return jsonify({"status": "sleeping", "message": "Market Closed"}), 200

# ══════════════════════════════════════════════════════════
#  🟢 SERVER AUTH & USER MANAGEMENT
# ══════════════════════════════════════════════════════════
def load_saved_token():
    global _access_token
    try:
        token_doc = sys_col.find_one({"_id": "upstox_auth"})
        if token_doc: 
            _access_token = token_doc.get("access_token", "")
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
    return f'❌ Failed: {resp.text}'

@app.route("/user-profile", methods=['GET', 'OPTIONS'])
@require_firebase_auth
def user_profile():
    return jsonify({
        "tier": "pro", 
        "email": request.user.get('email', ''),
        "wallet_balance": 0.00  # Add this line
    })

if __name__ == "__main__":
    load_saved_token()
    print("\n Server Running\n" + "-" * 45)
    app.run(port=5001, debug=False)
