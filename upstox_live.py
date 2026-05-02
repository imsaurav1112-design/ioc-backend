"""
InsiderOwl — Upstox Live Market Backend (Cloud Edition)
====================================================================
Streamlined Core Indices Version.
IST Timezone enforced.
Includes Paper Trading Database & 3:30 PM Auto-Emailer.
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
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

# ══════════════════════════════════════════════════════════
#  🔑 CONFIGURATION & CLOUD SETUP
# ══════════════════════════════════════════════════════════
API_KEY      = "3e51765a-3794-41ab-b3c9-4a88e0d55e30"
API_SECRET   = "1ky9l299rf"
REDIRECT_URI = "https://ioc-backend-kq9x.onrender.com/callback"
BASE_URL     = "https://api.upstox.com/v2"
_access_token = None

def get_ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

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
    try:
        for i in range(0, len(keys_to_fetch), 50):
            batch = keys_to_fetch[i:i+50]
            r = requests.get(f"{BASE_URL}/market-quote/quotes", params={"instrument_key": ",".join(batch)}, headers=headers)
            if r.status_code == 200: all_quotes.update(r.json().get("data", {}))
    except Exception as e: pass
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
try:
    cred = credentials.Certificate(os.path.join(os.getcwd(), 'firebase-admin.json'))
    firebase_admin.initialize_app(cred)
except Exception as e: pass

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
    
    # 🟢 NEW: Added Collection for Paper Trades
    paper_trades_col = db["paper_trades"]
    
    history_col.create_index("createdAt", expireAfterSeconds=3456000)
except Exception as e: pass

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
#  🟢 TERMINAL DATA ROUTES
# ══════════════════════════════════════════════════════════
@app.route("/health")
def health(): 
    is_auth = _access_token is not None
    if not is_auth: is_auth = load_saved_token()
    return jsonify({"status": "ok", "authenticated": is_auth})

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
        total_pe_oi += pe["oi"]
        chain_rows.append({"strike": strike, "atm": atm is not None and abs(strike - atm) < cfg["step"], "ce": ce, "pe": pe})

    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    coa_data = calculate_coa(chain_rows, symbol, expiry)

    return jsonify({
        "symbol": symbol, "expiry": expiry, "spot": spot, "pcr": round(total_pe_oi / max(total_ce_oi, 1), 2),
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
    
    # ⚠️ EDIT THIS SECTION: Put your actual email and app password here ⚠️
    SENDER_EMAIL = "greekcalculator@gmail.com"
    SENDER_APP_PASSWORD = "llax olgz jdlh nybl"
    
    try:
        all_trades = list(paper_trades_col.find({"date": today_str}))
        if not all_trades:
            return jsonify({"status": "sleeping", "message": "No trades to email today."}), 200
            
        users = set(t.get("user_email") for t in all_trades if t.get("user_email"))
        
        for email in users:
            user_trades = [t for t in all_trades if t.get("user_email") == email]
            
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

        # Wipe today's trades after emailing
        paper_trades_col.delete_many({}) 
        
        return jsonify({"status": "success", "message": f"Emailed {len(users)} users and wiped today's trades."})
        
    except Exception as e:
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
        if cfg.get("is_mcx"):
            ensure_mcx_master()
            base = cfg["base_name"]
            valid_exps = []
            for e, data in MCX_MASTER_DICT.get(base, {}).items():
                if data['date'] >= get_ist_now().date():
                    valid_exps.append(e)
            if not valid_exps: return
            valid_exps.sort(key=lambda x: MCX_MASTER_DICT[base][x]['date'])
            exp = valid_exps[0]
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
    now = get_ist_now()
    
    global _access_token
    if not _access_token: load_saved_token()
    
    is_weekday = now.weekday() < 5
    is_market_open = (
        (now.hour == 9 and now.minute >= 15) or 
        (now.hour > 9 and now.hour < 15) or 
        (now.hour == 15 and now.minute <= 30)
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
    
    return jsonify({"status": "sleeping", "message": f"Market Closed (Server Time: {now.strftime('%H:%M:%S')} IST)"}), 200

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
        sys_col.update_one({"_id": "upstox_auth"}, {"$set": {"access_token": token, "date": get_ist_now().strftime("%Y-%m-%d")}}, upsert=True)
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

from datetime import timedelta

@app.route("/pay-with-wallet", methods=['POST', 'OPTIONS'])
@require_firebase_auth
def pay_with_wallet():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
        
    try:
        uid = request.user.get('uid')
        data = request.json
        plan_id = data.get("plan")
        cost = float(data.get("cost", 0))
        
        # 1. Validate the plan costs strictly on the backend so hackers can't send fake prices
        plan_prices = {"1_month": 249, "3_months": 599, "6_months": 999}
        plan_days = {"1_month": 30, "3_months": 90, "6_months": 180}
        
        if plan_id not in plan_prices or cost != plan_prices[plan_id]:
            return jsonify({"error": "Invalid plan or price manipulation detected."}), 400
            
        user_doc = users_col.find_one({"_id": uid})
        if not user_doc: 
            return jsonify({"error": "User not found in database."}), 404
        
        current_balance = float(user_doc.get("wallet_balance", 0))
        
        # 2. Check if they actually have enough money
        if current_balance < cost:
            return jsonify({"error": "Insufficient wallet balance."}), 400
            
        # 3. Deduct the cost
        new_balance = current_balance - cost
        
        # 4. Calculate the new Expiry Date safely
        now = get_ist_now()
        current_expiry = user_doc.get("expiry")
        
        # If they are already Pro, extend their current expiry date. Otherwise, start from today.
        if user_doc.get("tier") == "pro" and current_expiry and current_expiry > now:
            new_expiry = current_expiry + timedelta(days=plan_days[plan_id])
        else:
            new_expiry = now + timedelta(days=plan_days[plan_id])
            
        # 5. Save the updated Wallet Balance, Tier, and Expiry directly to MongoDB
        users_col.update_one(
            {"_id": uid},
            {"$set": {
                "wallet_balance": new_balance,
                "tier": "pro",
                "expiry": new_expiry
            }}
        )
        
        return jsonify({
            "status": "success", 
            "new_balance": new_balance, 
            "new_expiry": new_expiry.strftime("%Y-%m-%d")
        }), 200
        
    except Exception as e:
        print("Wallet Payment Error:", e)
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════
#  🟢 RAZORPAY PAYMENT ROUTES
# ══════════════════════════════════════════════════════════
from datetime import timedelta

@app.route("/create-order", methods=['POST', 'OPTIONS'])
@require_firebase_auth
def create_order():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
        
    try:
        data = request.json
        plan_id = data.get("plan")
        
        # 1. Server-side price validation
        plan_prices = {"1_month": 249, "3_months": 599, "6_months": 999}
        if plan_id not in plan_prices:
            return jsonify({"error": "Invalid Plan Selected"}), 400
            
        cost_inr = plan_prices[plan_id]
        
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
@require_firebase_auth
def verify_payment():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
        
    try:
        uid = request.user.get('uid')
        data = request.json
        
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
#  🟢 LIVE FOOTPRINT ENGINE & ROUTE
# ══════════════════════════════════════════════════════════

import upstox_client

# This now stores MULTIPLE candles! Format: {"10:15": {"22500": {"buy_vol": 100, "sell_vol": 50}}}
footprint_candles = {}
TIMEFRAME_MINUTES = 5

def get_current_candle_time():
    """Rounds the current time down to the nearest 5-minute block"""
    now = datetime.now()
    minute = (now.minute // TIMEFRAME_MINUTES) * TIMEFRAME_MINUTES
    candle_time = now.replace(minute=minute, second=0, microsecond=0)
    return candle_time.strftime("%H:%M")

def start_footprint_streamer():
    global _access_token, footprint_candles
    if not _access_token:
        print("Waiting for access token to start Footprint engine...")
        return

    try:
        configuration = upstox_client.Configuration()
        configuration.access_token = _access_token
        api_client = upstox_client.ApiClient(configuration)
        
        instrument = "NSE_INDEX|Nifty 50"
        streamer = upstox_client.MarketDataStreamerV3(api_client, [instrument], "full")

        def on_message(message):
            try:
                if "feeds" in message and instrument in message["feeds"]:
                    feed = message["feeds"][instrument]
                    
                    if "ff" in feed and "marketFF" in feed["ff"]:
                        market_data = feed["ff"]["marketFF"]
                        
                        ltp = float(market_data["ltpc"]["ltp"])
                        volume = int(market_data["ltpc"].get("ltq", 0))
                        
                        if volume > 0 and "marketLevel" in market_data:
                            bids_asks = market_data["marketLevel"].get("bidAskQuote", [])
                            if len(bids_asks) > 0:
                                top_bid = float(bids_asks[0]["bp"])
                                top_ask = float(bids_asks[0]["ap"])
                                rounded_price = round(ltp)
                                
                                # 1. Get the current 5-minute time block
                                candle_key = get_current_candle_time()
                                
                                # 2. If it's a new 5-minute candle, create it!
                                if candle_key not in footprint_candles:
                                    footprint_candles[candle_key] = {}
                                    
                                    # Optional: Prevent memory crashes by keeping only the last 12 candles (1 hour)
                                    if len(footprint_candles) > 12:
                                        oldest_candle = list(footprint_candles.keys())[0]
                                        del footprint_candles[oldest_candle]

                                current_matrix = footprint_candles[candle_key]
                                
                                if str(rounded_price) not in current_matrix:
                                    current_matrix[str(rounded_price)] = {"buy_vol": 0, "sell_vol": 0}
                                    
                                # 3. Add the volume to the CURRENT 5-minute candle
                                if ltp >= top_ask:
                                    current_matrix[str(rounded_price)]["buy_vol"] += volume
                                elif ltp <= top_bid:
                                    current_matrix[str(rounded_price)]["sell_vol"] += volume
                                    
            except Exception as e:
                pass 

        streamer.on("message", on_message)
        print(f"⚡ Footprint Streamer Connecting ({TIMEFRAME_MINUTES}-Min Candles)...")
        streamer.connect()
        
    except Exception as e:
        print("Footprint Streamer Crash:", e)

import time
import random
from datetime import datetime

# Temporary Simulator Memory
simulated_candles = {}

def get_current_candle_time():
    now = datetime.now()
    minute = (now.minute // 5) * 5
    candle_time = now.replace(minute=minute, second=0, microsecond=0)
    return candle_time.strftime("%H:%M")

@app.route("/api/footprint", methods=['GET', 'OPTIONS'])
def get_footprint():
    if request.method == 'OPTIONS': 
        return jsonify({"status": "ok"}), 200
    
    # --- WEEKEND SIMULATOR INJECTION ---
    global simulated_candles
    
    # 1. Get current 5-min block
    candle_key = get_current_candle_time()
    
    # 2. Start a new candle if needed
    if candle_key not in simulated_candles:
        simulated_candles[candle_key] = {}
        # Pre-fill with a base price to simulate an orderbook
        for i in range(-5, 6):
            price = str(22500 + (i * 5))
            simulated_candles[candle_key][price] = {"buy_vol": 0, "sell_vol": 0}

    # 3. Simulate a random massive trade every time the frontend asks for data (every 2 seconds)
    random_strike = str(22500 + (random.randint(-2, 2) * 5))
    vol = random.randint(10, 150)
    
    # Create a giant imbalance 20% of the time
    if random.random() > 0.8:
        vol = random.randint(400, 1000)
        
    if random.random() > 0.5:
        simulated_candles[candle_key][random_strike]["buy_vol"] += vol
    else:
        simulated_candles[candle_key][random_strike]["sell_vol"] += vol
    # -----------------------------------

    return jsonify({
        "status": "success",
        "instrument": "NIFTY",
        "data": simulated_candles 
    })

# ══════════════════════════════════════════════════════════
#  🟢 V-FOOTPRINT TEST CODE 
# ══════════════════════════════════════════════════════════
import time
import random
from datetime import datetime
import threading

simulated_candles = {}

def get_current_candle_time():
    now = datetime.now()
    minute = (now.minute // 5) * 5
    candle_time = now.replace(minute=minute, second=0, microsecond=0)
    return candle_time.strftime("%H:%M")

def realistic_market_simulator():
    """Generates realistic market trends (higher-highs, lower-lows)"""
    global simulated_candles
    print("🟢 REALISTIC MARKET SIMULATOR BOOTING UP...")
    
    current_price = 22500
    market_trend = 1 # 1 = Bullish, -1 = Bearish, 0 = Ranging
    trend_duration = 0
    
    while True:
        time.sleep(1) # Fake trade speed
        candle_key = get_current_candle_time()
        
        if candle_key not in simulated_candles:
            simulated_candles[candle_key] = {}
            
        # State Machine: Change the market trend every 30-60 seconds
        trend_duration += 1
        if trend_duration > random.randint(30, 60):
            market_trend = random.choice([1, 1, 0, -1, -1]) # Slightly favors trending
            trend_duration = 0
            
        # Move price according to trend
        if market_trend == 1:
            step = random.choice([0, 5, 5, 10]) # Higher Highs
        elif market_trend == -1:
            step = random.choice([0, -5, -5, -10]) # Lower Lows
        else:
            step = random.choice([-5, 0, 5]) # Ranging
            
        current_price += step
        
        # Round to nearest Nifty tick size (5)
        rounded_price = str(round(current_price / 5) * 5)
        
        if rounded_price not in simulated_candles[candle_key]:
            simulated_candles[candle_key][rounded_price] = {"buy_vol": 0, "sell_vol": 0}
            
        # Simulate Institutional Volume based on trend
        vol = random.randint(10, 150)
        
        # Inject Imbalances during strong moves
        if random.random() > 0.8:
            vol = random.randint(400, 1500)
            
        # In an uptrend, aggressive buyers (Ask) dominate. In downtrend, sellers (Bid) dominate.
        if market_trend == 1:
            simulated_candles[candle_key][rounded_price]["buy_vol"] += int(vol * random.uniform(1.0, 1.5))
            simulated_candles[candle_key][rounded_price]["sell_vol"] += int(vol * random.uniform(0.1, 0.5))
        elif market_trend == -1:
            simulated_candles[candle_key][rounded_price]["sell_vol"] += int(vol * random.uniform(1.0, 1.5))
            simulated_candles[candle_key][rounded_price]["buy_vol"] += int(vol * random.uniform(0.1, 0.5))
        else:
            if random.random() > 0.5:
                simulated_candles[candle_key][rounded_price]["buy_vol"] += vol
            else:
                simulated_candles[candle_key][rounded_price]["sell_vol"] += vol

threading.Thread(target=realistic_market_simulator, daemon=True).start()
# ══════════════════════════════════════════════════════════
#  🟢 USER PROFILE ROUTE
# ══════════════════════════════════════════════════════════

@app.route("/user-profile", methods=['GET', 'OPTIONS'])
@require_firebase_auth
def user_profile():
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200
        
    try:
        # 1. Get the Firebase User ID
        uid = request.user.get('uid')
        
        # 2. Fetch the real profile from your MongoDB database
        user_doc = users_col.find_one({"_id": uid})
        
        if user_doc:
            # Format the expiry date safely if it exists
            expiry_val = user_doc.get("expiry")
            expiry_str = expiry_val.strftime("%Y-%m-%d") if hasattr(expiry_val, 'strftime') else str(expiry_val) if expiry_val else None

            # 3. Return the real database values!
            return jsonify({
                "email": user_doc.get("email", request.user.get("email", "")),
                "tier": user_doc.get("tier", "free"),
                "name": user_doc.get("name", ""),
                "referral_code": user_doc.get("referral_code", ""),
                "wallet_balance": float(user_doc.get("wallet_balance", 0.00)),
                "expiry_date": expiry_str
            })
        else:
            # Fallback if a brand new user hasn't been added to the database yet
            return jsonify({
                "email": request.user.get("email", ""),
                "tier": "free",
                "name": "",
                "referral_code": "",
                "wallet_balance": 0.00
            })
            
    except Exception as e:
        print("Profile Fetch Error:", e)
        return jsonify({"error": str(e)}), 500

# KEEP THIS EXACTLY AS IT WAS:
if __name__ == "__main__":
    load_saved_token()
    print("\n Server Running\n" + "-" * 45)
    app.run(port=5001, debug=False)
    
