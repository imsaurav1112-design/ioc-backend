"""
InsiderOwl — Upstox Live Market Backend (Cloud Edition)
====================================================================
Includes 9:15-3:30 Automated Background Recorder (MongoDB Atlas).
Prioritizes Native Upstox IV/Greeks for Indices to prevent discrepancies.
Calculates EOR, EOS using the dual-rate Black-Scholes Engine.
Stores Upstox Auth Token securely in the cloud to survive Render restarts.
Razorpay Webhook Integrated for Drop-Off Prevention.
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

# 🟢 SCHEDULER IMPORTS
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import atexit

# 🟢 CLOUD IMPORTS
from pymongo import MongoClient
import firebase_admin
from firebase_admin import credentials, auth
import razorpay

app = Flask(__name__)
CORS(app)

# ══════════════════════════════════════════════════════════
#  🔑 CONFIGURATION & CLOUD SETUP
# ══════════════════════════════════════════════════════════
API_KEY      = "3e51765a-3794-41ab-b3c9-4a88e0d55e30"
API_SECRET   = "1ky9l299rf"
REDIRECT_URI = "https://ioc-backend-kq9x.onrender.com/callback"

BASE_URL   = "https://api.upstox.com/v2"
_access_token = None

SYMBOL_MAP = {
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 30,  "step": 100},
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    "MIDCPNIFTY": {"instrument_key": "NSE_INDEX|Nifty Midcap Select", "lot": 50,  "step": 25},
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100}, 
}

# 🟢 FIREBASE ADMIN SETUP
try:
    cred = credentials.Certificate(os.path.join(os.getcwd(), 'firebase-admin.json'))
    firebase_admin.initialize_app(cred)
except Exception as e:
    print(f"⚠️ Firebase Admin Init Error (Auth will fail): {e}")

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

    # TTL INDEX: Automatically delete records older than 40 days (3,456,000 seconds)
    history_col.create_index("createdAt", expireAfterSeconds=3456000)
    print("✅ Connected to MongoDB Atlas & TTL Index Verified")
except Exception as e:
    print(f"❌ MongoDB Connection Error: {e}")

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
#  🟢 ADVANCED STATE-MACHINE & COA LOGIC
# ══════════════════════════════════════════════════════════
COA_MEMORY = {}

def evaluate_side(vols_dict, mem_side, side_name):
    """State Machine that remembers shifting history to prevent false signals"""
    if not vols_dict: 
        return {"strike": 0, "state": "Strong", "target_strike": 0, "pct": 0, "val": 0, "msg": ""}
        
    sorted_vols = sorted(vols_dict.items(), key=lambda x: x[1], reverse=True)
    max_strike, max_vol = sorted_vols[0]
    sec_strike, sec_vol = 0, 0
    pct = 0.0
    
    if len(sorted_vols) > 1:
        sec_strike, sec_vol = sorted_vols[1]
        pct = round((sec_vol / max_vol * 100), 2) if max_vol > 0 else 0
        
    # 1. INITIALIZATION (First run of the day)
    if mem_side["base"] == 0:
        mem_side["base"] = max_strike
        return {"strike": max_strike, "state": "Strong", "target_strike": 0, "pct": pct, "val": max_vol, "msg": f"{side_name} established at {max_strike}."}

    msg = ""
    current_state = "Strong"
    target = 0

    # 2. BASE CROSSOVER CHECK (A new strike hit 100%)
    if max_strike != mem_side["base"]:
        mem_side["old_base"] = mem_side["base"]
        mem_side["base"] = max_strike
        mem_side["is_shifting"] = True
        mem_side["lowest_pct"] = pct
        msg = f"Shift in Progress: {side_name} base moved to {max_strike}, clearing residual volume at {mem_side['old_base']}."
        
    # 3. INTELLIGENT STATE EVALUATION
    if mem_side["is_shifting"]:
        # Scenario A: We are looking at the old decaying base
        if sec_strike == mem_side["old_base"]:
            mem_side["lowest_pct"] = min(mem_side["lowest_pct"], pct)
            
            if pct < 75.0:
                # The umbilical cord is cut. Shift is fully complete.
                mem_side["is_shifting"] = False
                mem_side["old_base"] = 0
                msg = f"Shift Complete: {side_name} has successfully consolidated at {max_strike}."
                current_state = "Strong"
            else:
                if mem_side["lowest_pct"] < 75.0:
                    # The Threat Returns! (True STB/STT)
                    current_state = "STT" if sec_strike > max_strike else "STB"
                    target = sec_strike
                    direction = "bullish" if current_state == "STT" else "bearish"
                    msg = f"Renewed Pressure: Old {side_name} at {sec_strike} is fighting back, creating {direction} pressure."
                else:
                    # Shift is just incomplete. Residual noise is ignored.
                    current_state = "Strong"
                    
        # Scenario B: THE ACTIVE CHALLENGER OVERRIDE
        else:
            if pct >= 75.0:
                current_state = "STT" if sec_strike > max_strike else "STB"
                target = sec_strike
                direction = "bullish" if current_state == "STT" else "bearish"
                msg = f"Active Challenger: A new {direction} pressure emerged at {sec_strike}, overriding the old shift."
            else:
                current_state = "Strong"
                
    # 4. NORMAL STABLE EVALUATION (No shift in progress)
    else:
        if pct >= 75.0:
            current_state = "STT" if sec_strike > max_strike else "STB"
            target = sec_strike
            
    # Generate simple pressure messages if state changed normally without a specific override message
    if msg == "" and current_state != mem_side["state"]:
        if current_state == "STT": msg = f"Bullish Pressure: {side_name} is Sliding Towards Top ({target})."
        elif current_state == "STB": msg = f"Bearish Pressure: {side_name} is Sliding Towards Bottom ({target})."
        elif current_state == "Strong": msg = f"{side_name} has become Strong at {max_strike}."
        
    # Save the current state to memory for the next minute
    mem_side["state"] = current_state
    mem_side["target"] = target
    
    return {"strike": max_strike, "state": current_state, "target_strike": target, "pct": pct, "val": max_vol, "msg": msg}

def generate_plain_english_status(res_state, sup_state):
    """Creates the beautiful plain-text readout for the UI Header"""
    res_desc = "in a Strong position"
    if res_state == "STT": res_desc = "experiencing bullish pressure (Sliding Towards Top)"
    elif res_state == "STB": res_desc = "experiencing bearish pressure (Sliding Towards Bottom)"
    
    sup_desc = "in a Strong position"
    if sup_state == "STT": sup_desc = "experiencing bullish pressure (Sliding Towards Top)"
    elif sup_state == "STB": sup_desc = "experiencing bearish pressure (Sliding Towards Bottom)"
    
    return f"Currently, Resistance is {res_desc} and Support is {sup_desc}."

def calculate_coa(chain_rows, symbol, expiry):
    global COA_MEMORY
    mem_key = f"{symbol}_{expiry}"
    
    # Initialize the complex memory block for this specific expiry
    if mem_key not in COA_MEMORY: 
        COA_MEMORY[mem_key] = {
            "sup_mem": {"base": 0, "old_base": 0, "is_shifting": False, "lowest_pct": 100.0, "state": "Strong", "target": 0},
            "res_mem": {"base": 0, "old_base": 0, "is_shifting": False, "lowest_pct": 100.0, "state": "Strong", "target": 0},
            "logs": []
        }
    mem = COA_MEMORY[mem_key]
    
    ce_vols = {r['strike']: r['ce'].get('volume', 0) for r in chain_rows if r['ce'].get('volume')}
    pe_vols = {r['strike']: r['pe'].get('volume', 0) for r in chain_rows if r['pe'].get('volume')}
    
    # Evaluate both sides using our new State Machine
    res = evaluate_side(ce_vols, mem['res_mem'], "Resistance")
    sup = evaluate_side(pe_vols, mem['sup_mem'], "Support")
    
    # Add new timeline events to the historical log array
    current_time = datetime.now().strftime("%I:%M %p")
    new_logs = []
    if res['msg']: new_logs.append(f"{current_time} - {res['msg']}")
    if sup['msg']: new_logs.append(f"{current_time} - {sup['msg']}")
    
    if new_logs:
        mem['logs'] = (new_logs + mem['logs'])[:100] # Keeps the last 100 events

    plain_english_status = generate_plain_english_status(res['state'], sup['state'])
    
    # 🟢 R1/S1 & R2/S2 CALCULATIONS
    step = SYMBOL_MAP.get(symbol, {}).get("step", 50)
    
    res_row = next((r for r in chain_rows if r['strike'] == res['strike']), None)
    sup_row = next((r for r in chain_rows if r['strike'] == sup['strike']), None)
    
    r1_val = res_row['ce_prz'] if res_row else res['strike']
    s1_val = sup_row['pe_prz'] if sup_row else sup['strike']
    
    # Calculate strikes for R2 (Up) and S2 (Down)
    r2_strike = res['strike'] + step if res['strike'] > 0 else 0
    s2_strike = sup['strike'] - step if sup['strike'] > 0 else 0
    
    r2_row = next((r for r in chain_rows if r['strike'] == r2_strike), None)
    s2_row = next((r for r in chain_rows if r['strike'] == s2_strike), None)
    
    r2_val = r2_row['ce_prz'] if r2_row else r2_strike
    s2_val = s2_row['pe_prz'] if s2_row else s2_strike

    return {
        "scenario_desc": plain_english_status, 
        "support": sup, 
        "resistance": res, 
        "s1": s1_val,  # Old EOS
        "r1": r1_val,  # Old EOR
        "s2": s2_val,  # New S-2 Boundary
        "r2": r2_val,  # New R-2 Boundary
        "logs": mem['logs']
    }
# ══════════════════════════════════════════════════════════
#  🟢 AUTOMATED 9:15 to 3:30 RECORDER (MONGODB UPDATE)
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
                r['strike'],
                r['ce'].get('oi', 0),
                r['ce'].get('volume', 0),
                float(r['ce'].get('ltp', 0)),
                r['pe'].get('oi', 0),
                r['pe'].get('volume', 0),
                float(r['pe'].get('ltp', 0))
            ])

    snapshot = {
        "sym": symbol,
        "exp": expiry,
        "date": date_str,
        "time_key": time_key,
        "createdAt": datetime.utcnow(), 
        "spot": spot,
        "pcr": pcr,
        "chain": compressed_chain
    }

    try:
        history_col.update_one(
            {"sym": symbol, "exp": expiry, "date": date_str, "time_key": time_key},
            {"$set": snapshot},
            upsert=True
        )
    except Exception as e:
        print(f"MongoDB Record Error: {e}")

def fetch_and_record(symbol):
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return
    
    resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
    data = resp.json().get("data") or []
    expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
    if not expiries: return
    closest_expiry = expiries[0]

    spot = None
    spot_resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
    if spot_resp.status_code == 200:
        safe_spot = spot_resp.json().get("data") or {}
        for v in safe_spot.values():
            spot = v.get("last_price")
            break
            
    chain_resp = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": closest_expiry}, headers=auth_headers())
    raw_list = chain_resp.json().get("data") or []
    if not raw_list: return
    
    if not spot: spot = float(raw_list[0].get("underlying_spot_price", 0))

    chain_rows, total_ce_oi, total_pe_oi = [], 0, 0
    for item in raw_list:
        strike = int(float(item.get("strike_price", 0)))
        def p_side(d):
            md, og = d.get("market_data") or {}, d.get("option_greeks") or {}
            raw_iv = float(og.get("iv") or 0)
            return {
                "ltp": md.get("ltp", 0), "oi": md.get("oi", 0), "volume": md.get("volume", 0),
                "iv": raw_iv * 100 if 0 < raw_iv < 1.0 else raw_iv, 
                "delta": og.get("delta", 0), "theta": og.get("theta", 0), "vega": og.get("vega", 0), "gamma": float(og.get("gamma") or 0.0005) * 10000
            }
        ce, pe = p_side(item.get("call_options") or {}), p_side(item.get("put_options") or {})
        total_ce_oi += ce["oi"]
        total_pe_oi += pe["oi"]
        chain_rows.append({"strike": strike, "ce": ce, "pe": pe})
        
    pcr = round(total_pe_oi / max(total_ce_oi, 1), 2)
    chain_rows = inject_prz(chain_rows, closest_expiry, cfg["step"], spot)
    compress_and_save(symbol, closest_expiry, spot, pcr, chain_rows)

def record_market_snapshot():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    
    is_weekday = now.weekday() < 5
    is_market_open = (
        (now.hour == 9 and now.minute >= 15) or 
        (now.hour > 9 and now.hour < 15) or 
        (now.hour == 15 and now.minute <= 30)
    )
    
    global _access_token
    if not _access_token:
        return

    if is_weekday and is_market_open:
        try:
            for sym in SYMBOL_MAP.keys():
                fetch_and_record(sym)
            print(f"📸 Snapshots recorded for ALL indices at {now.strftime('%H:%M:%S')} IST")
        except Exception as e:
            print(f"❌ Failed to record market snapshot: {str(e)}")


# ══════════════════════════════════════════════════════════
#  🟢 TERMINAL DATA ROUTES
# ══════════════════════════════════════════════════════════
def auth_headers(): return {"Authorization": f"Bearer {_access_token}", "Accept": "application/json"}

@app.route("/health")
def health(): return jsonify({"status": "ok", "authenticated": _access_token is not None})

@app.route("/expiry-dates", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def expiry_dates():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    if symbol not in SYMBOL_MAP: return jsonify({"error": "Invalid symbol"}), 400 
    
    is_backtest = request.headers.get("Referer", "").endswith("backtester.html")
    
    if is_backtest:
        try:
            saved_expiries = history_col.distinct("exp", {"sym": symbol})
            expiries = sorted(saved_expiries)
            return jsonify({"symbol": symbol, "expiries": expiries})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        cfg = SYMBOL_MAP.get(symbol)
        resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        data = resp.json().get("data") or []
        expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
        return jsonify({"symbol": symbol, "expiries": expiries})

@app.route("/api/available-dates", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def available_dates():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    
    if not symbol or not expiry: 
        return jsonify({"error": "Missing parameters"}), 400
        
    try:
        dates = history_col.distinct("date", {"sym": symbol, "exp": expiry})
        dates = sorted(dates, reverse=True)
        return jsonify({"dates": dates})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/intraday-history", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def intraday_history():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    if symbol not in SYMBOL_MAP: return jsonify({"error": "Invalid symbol"}), 400
    
    expiry = request.args.get("expiry", "").strip()
    target_date = request.args.get("date", (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")).strip()
    
    try:
        cursor = history_col.find({"sym": symbol, "exp": expiry, "date": target_date}).sort("createdAt", 1)
        records = list(cursor)

        if not records:
            return jsonify([])

        history_map = {}
        base_oi = {}

        for doc in records:
            time_key = doc.get("time_key")
            chain_arrays = doc.get("chain", [])

            if time_key not in history_map:
                history_map[time_key] = []

            for row in chain_arrays:
                if len(row) < 7: continue
                strike, ce_oi, ce_vol, ce_ltp, pe_oi, pe_vol, pe_ltp = row

                if strike not in base_oi:
                    base_oi[strike] = {"ce": ce_oi, "pe": pe_oi}

                history_map[time_key].append({
                    "strike": strike,
                    "ceVol": ce_vol,
                    "peVol": pe_vol,
                    "ceOI": ce_oi,
                    "peOI": pe_oi,
                    "ceOIChg": ce_oi - base_oi[strike]["ce"],
                    "peOIChg": pe_oi - base_oi[strike]["pe"],
                    "ceLTP": ce_ltp,
                    "peLTP": pe_ltp
                })

        formatted_history = [{"time": k, "rows": v} for k, v in history_map.items()]
        return jsonify(formatted_history)

    except Exception as e:
        print(f"Error fetching history: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/options-chain", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def options_chain():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    if symbol not in SYMBOL_MAP: return jsonify({"error": "Invalid symbol"}), 400 
    
    expiry = request.args.get("expiry", "").strip()
    cfg = SYMBOL_MAP.get(symbol)

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
            if not d: return {"ltp": 0, "oi": 0, "change_oi": 0, "volume": 0, "iv": 0, "delta": 0, "theta": 0, "vega": 0, "gamma": 0}
            md, og = d.get("market_data") or {}, d.get("option_greeks") or {}
            raw_iv = float(og.get("iv") or 0)
            return {
                "ltp": md.get("ltp"), "oi": int(md.get("oi") or 0), 
                "change_oi": int(md.get("oi") or 0) - int(md.get("prev_oi") or 0),
                "volume": md.get("volume", 0), 
                "iv": round(raw_iv * 100, 2) if 0 < raw_iv < 1.0 else round(raw_iv, 2), 
                "delta": round(float(og.get("delta") or 0), 4),
                "theta": round(float(og.get("theta") or 0), 4), 
                "vega": round(float(og.get("vega") or 0), 4), 
                "gamma": round(float(og.get("gamma") or 0.0005) * 10000, 2)
            }
            
        ce = parse_side(item.get("call_options"))
        pe = parse_side(item.get("put_options"))
        total_ce_oi += ce["oi"] or 0
        total_pe_oi += pe["oi"] or 0
        chain_rows.append({"strike": strike, "atm": atm is not None and abs(strike - atm) < cfg["step"], "ce": ce, "pe": pe})

    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    coa_data = calculate_coa(chain_rows, symbol, expiry)

    return jsonify({
        "symbol": symbol, "expiry": expiry, "spot": spot, "pcr": round(total_pe_oi / max(total_ce_oi, 1), 2),
        "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi, "lot_size": cfg["lot"],
        "chain": chain_rows, "fetched_at": datetime.now().strftime("%H:%M:%S"), "coa": coa_data
    })

# ══════════════════════════════════════════════════════════
#  🟢 CLOUD AUTH FLOW (UPSTOX TO MONGODB)
# ══════════════════════════════════════════════════════════
def load_saved_token():
    global _access_token
    try:
        token_doc = sys_col.find_one({"_id": "upstox_auth"})
        if not token_doc: return False
        
        token = token_doc.get("access_token", "")
        if token:
            try:
                import base64
                payload = token.split(".")[1]
                payload += "=" * (4 - len(payload) % 4)
                decoded = json.loads(base64.b64decode(payload))
                if decoded.get("exp", 0) > datetime.now().timestamp():
                    _access_token = token
                    return True
            except: pass
            
        if token_doc.get("date") != datetime.now().strftime("%Y-%m-%d"): return False
        _access_token = token
        return True
    except: return False

def save_token(token):
    global _access_token
    _access_token = token
    try:
        sys_col.update_one(
            {"_id": "upstox_auth"},
            {"$set": {"access_token": token, "date": datetime.now().strftime("%Y-%m-%d")}},
            upsert=True
        )
    except: pass

@app.route("/login")
def login_route():
    params = {"response_type": "code", "client_id": API_KEY, "redirect_uri": REDIRECT_URI}
    login_url = f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(params)}"
    return f'<h2 style="font-family:sans-serif;">Upstox Server Auth</h2><a href="{login_url}" style="padding:10px 20px; background:#3b82f6; color:white; text-decoration:none; border-radius:5px; font-family:sans-serif;">Click here to Login</a>'

@app.route("/callback")
def callback_route():
    code = request.args.get("code")
    if not code: return "Error: No auth code.", 400
    resp = requests.post("https://api.upstox.com/v2/login/authorization/token",
        data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"},
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    )
    if resp.status_code == 200:
        save_token(resp.json().get("access_token"))
        return '<h2 style="color:green; font-family:sans-serif;">✅ Login Successful! Token saved securely to MongoDB.</h2>'
    return f'<h2 style="color:red; font-family:sans-serif;">❌ Failed:</h2><p>{resp.text}</p>'

import random
import string

@app.route("/user-profile", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def user_profile():
    uid = request.user['uid']
    email = request.user.get('email', '')
    name = request.user.get('name', email.split('@')[0]) 
    
    user_doc = users_col.find_one({"_id": uid})
    
    if not user_doc:
        ref_code = "IOC-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        referred_by = request.args.get("ref", None)
        
        new_user = {
            "_id": uid,
            "email": email,
            "name": name,
            "tier": "free",
            "expiry": None,
            "referral_code": ref_code,
            "referred_by": referred_by,
            "wallet_balance": 0.00,
            "processed_payments": []
        }
        users_col.insert_one(new_user)
        user_doc = new_user
    else:
        updates = {}
        if "referral_code" not in user_doc:
            user_doc["referral_code"] = "IOC-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
            updates["referral_code"] = user_doc["referral_code"]
        if "wallet_balance" not in user_doc:
            user_doc["wallet_balance"] = 0.00
            updates["wallet_balance"] = 0.00
        if "name" not in user_doc:
            user_doc["name"] = name
            updates["name"] = name
            
        if updates:
            users_col.update_one({"_id": uid}, {"$set": updates})

    # 🟢 BULLETPROOF EXPIRY CHECK 
    formatted_expiry = None
    if user_doc.get("tier") == "pro":
        raw_expiry = user_doc.get("expiry")
        
        if raw_expiry:
            if isinstance(raw_expiry, str):
                try:
                    raw_expiry = datetime.strptime(raw_expiry[:10], "%Y-%m-%d")
                except:
                    pass 
            
            if isinstance(raw_expiry, datetime):
                if datetime.now() > raw_expiry:
                    users_col.update_one({"_id": uid}, {"$set": {"tier": "free"}})
                    user_doc["tier"] = "free"
                else:
                    formatted_expiry = raw_expiry.strftime("%d %b %Y")
            else:
                formatted_expiry = str(raw_expiry)
            
    return jsonify({
        "tier": user_doc.get("tier", "free"),
        "email": user_doc.get("email", email),
        "name": user_doc.get("name", name),
        "referral_code": user_doc.get("referral_code", ""),
        "wallet_balance": user_doc.get("wallet_balance", 0.00),
        "expiry_date": formatted_expiry
    })

# ══════════════════════════════════════════════════════════
#  🟢 BILLING & RAZORPAY WEBHOOK ENGINE
# ══════════════════════════════════════════════════════════
@app.route("/create-order", methods=['POST', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def create_order():
    data = request.json
    plan = data.get('plan') 
    
    prices = {"1_month": 24900, "3_months": 59900, "6_months": 99900}
    amount = prices.get(plan)
    
    if not amount: return jsonify({"error": "Invalid plan"}), 400

    short_receipt = f"r_{int(time.time())}_{request.user['uid'][:5]}"

    order_data = {
        "amount": amount,
        "currency": "INR",
        "receipt": short_receipt,
        "notes": { 
            "uid": request.user['uid'],
            "plan": plan
        }
    }
    
    try:
        order = rzp_client.order.create(data=order_data)
        return jsonify({"order_id": order['id'], "amount": amount, "key": RZP_KEY_ID})
    except Exception as e:
        print(f"RAZORPAY ERROR: {str(e)}") 
        return jsonify({"error": str(e)}), 500

def process_upgrade_and_commission(uid, plan, payment_id):
    user_check = users_col.find_one({"_id": uid})
    if not user_check or payment_id in user_check.get("processed_payments", []):
        print(f"🔒 Payment {payment_id} already processed. Skipping to prevent double-billing.")
        return False

    days_to_add = 30 if plan == "1_month" else 90 if plan == "3_months" else 180
    new_expiry = datetime.now() + timedelta(days=days_to_add)

    from pymongo import ReturnDocument
    user_doc = users_col.find_one_and_update(
        {"_id": uid}, 
        {
            "$set": {"tier": "pro", "expiry": new_expiry},
            "$push": {"processed_payments": payment_id} 
        },
        return_document=ReturnDocument.AFTER
    )

    if user_doc and user_doc.get("referred_by"):
        referrer_code = str(user_doc.get("referred_by")).strip().upper()
        prices_inr = {"1_month": 249, "3_months": 599, "6_months": 999}
        amount_paid = prices_inr.get(plan, 0)
        commission = round(amount_paid * 0.20, 2)
        
        if commission > 0:
            users_col.update_one(
                {"referral_code": referrer_code}, 
                {"$inc": {"wallet_balance": commission}}
            )
            print(f"💰 PAID OUT: ₹{commission} to {referrer_code} for payment {payment_id}")
            
    return True

@app.route("/verify-payment", methods=['POST', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def verify_payment():
    data = request.json
    try:
        rzp_client.utility.verify_payment_signature({
            'razorpay_order_id': data['razorpay_order_id'],
            'razorpay_payment_id': data['razorpay_payment_id'],
            'razorpay_signature': data['razorpay_signature']
        })
        
        uid = request.user['uid']
        plan = data.get('plan')
        payment_id = data['razorpay_payment_id']
        
        process_upgrade_and_commission(uid, plan, payment_id)
        return jsonify({"status": "success"})
        
    except razorpay.errors.SignatureVerificationError:
        return jsonify({"error": "Payment verification failed"}), 400
    except Exception as e:
        print(f"❌ SERVER ERROR IN PAYMENT: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

@app.route("/razorpay-webhook", methods=['POST'], strict_slashes=False)
def razorpay_webhook():
    webhook_body = request.get_data(as_text=True)
    webhook_signature = request.headers.get('X-Razorpay-Signature')
    
    try:
        rzp_client.utility.verify_webhook_signature(webhook_body, webhook_signature, RZP_WEBHOOK_SECRET)
    except Exception as e:
        print(f"🛑 SECURE WEBHOOK BLOCKED: Invalid Signature. {e}")
        return jsonify({"error": "Invalid signature"}), 400

    data = request.json
    
    if data.get('event') == 'payment.captured':
        payment_entity = data['payload']['payment']['entity']
        payment_id = payment_entity.get('id')
        
        notes = payment_entity.get('notes', {})
        uid = notes.get('uid')
        plan = notes.get('plan')
        
        if uid and plan:
            print(f"📡 WEBHOOK FIRED for UID {uid[:5]}...")
            process_upgrade_and_commission(uid, plan, payment_id)
            
    return jsonify({"status": "ok"}), 200

# ══════════════════════════════════════════════════════════
#  🛡️ ADMIN COMMAND CENTER ROUTES
# ══════════════════════════════════════════════════════════
import io
import csv
from flask import send_file

@app.route("/api/admin/download-archive", methods=['GET', 'OPTIONS'], strict_slashes=False)
@require_firebase_auth
def download_archive():
    ADMIN_UID = "VEbfwlnqrDgy6QoFnN6Bf6qWdr72" 
    
    if request.user['uid'] != ADMIN_UID:
        return jsonify({"error": "Unauthorized. Admin access only."}), 403

    try:
        cursor = history_col.find().sort("createdAt", 1)
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Date", "Time", "Symbol", "Expiry", "Spot", "PCR", "Strike", "CE_OI", "CE_Vol", "CE_LTP", "PE_OI", "PE_Vol", "PE_LTP"])
        
        for doc in cursor:
            base_info = [
                doc.get("date"), doc.get("time_key"), doc.get("sym"), 
                doc.get("exp"), doc.get("spot"), doc.get("pcr")
            ]
            for chain_row in doc.get("chain", []):
                if len(chain_row) >= 7:
                    writer.writerow(base_info + chain_row)
                    
        output.seek(0)
        mem_file = io.BytesIO()
        mem_file.write(output.getvalue().encode('utf-8'))
        mem_file.seek(0)
        
        filename = f"Options_Archive_{datetime.now().strftime('%Y_%m_%d')}.csv"
        return send_file(mem_file, mimetype='text/csv', as_attachment=True, download_name=filename)
        
    except Exception as e:
        print(f"Archive Download Error: {e}")
        return jsonify({"error": "Failed to generate archive"}), 500
        
# ══════════════════════════════════════════════════════════
#  SERVER START
# ══════════════════════════════════════════════════════════

# 🟢 Initialize the Background Scheduler
scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Kolkata'))
scheduler.add_job(func=record_market_snapshot, trigger="interval", minutes=1)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == "__main__":
    if API_KEY == "your_api_key_here":
        print("WARNING: Add keys to upstox_live.py")
        sys.exit(1)
        
    load_saved_token()
    print("\n Server: http://127.0.0.1:5001\n" + "-" * 45)
    app.run(port=5001, debug=False)
