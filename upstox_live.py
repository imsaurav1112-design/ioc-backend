"""
InsiderOwl — Upstox Live Market Backend (AOC Gamma Extension Engine)
====================================================================
Bypasses the broken Upstox Options Contract API for MCX commodities.
Bulletproofed against 'NoneType' API crashes and rate limits.
Includes Automated Market Intelligence (LTP Calculator Scenarios).
Calculates EOR, EOS using the dual-rate Black-Scholes Engine.
FORCES Custom Greeks and IV using Dynamic Interest Rates for ALL symbols.
"""

import os, sys, time, json, webbrowser, requests, gzip, io, csv
from datetime import datetime
from urllib.parse import urlencode
from flask import Flask, jsonify, request
from flask_cors import CORS
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq, fsolve

app = Flask(__name__)
CORS(app)

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
    """Calculates all 4 standard Option Greeks mathematically"""
    if T <= 0 or sigma <= 0:
        return {'delta': 0, 'gamma': 0.0005, 'theta': 0, 'vega': 0}
    
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
        
    return {
        'delta': round(delta, 4),
        'gamma': round(gamma * 10000, 2), 
        'theta': round(theta, 4),
        'vega': round(vega, 4)
    }
    
def calculate_custom_iv(market_ltp, S, K, T, r, opt_type='ce'):
    """Reverse-engineers IV. Includes Intrinsic Value safety net."""
    intrinsic = max(0.0, S - K) if opt_type == 'ce' else max(0.0, K - S)
    safe_ltp = max(market_ltp, intrinsic + 0.01)
        
    def objective(sigma):
        if opt_type == 'ce':
            return bs_call(S, K, T, r, sigma) - safe_ltp
        else:
            return bs_put(S, K, T, r, sigma) - safe_ltp
            
    try:
        return brentq(objective, 1e-4, 10.0)
    except:
        return 0.15 

def calc_prz(K_call, iv_call, K_put, iv_put, days_to_expiry, r=0.10):
    """Bulletproof PRZ Calculator using both Brentq and Fsolve fallbacks"""
    try:
        if iv_call <= 0 or iv_put <= 0: return 0.0
        T = max(0.001, days_to_expiry / 365.0)
        
        def objective(S):
            return bs_call(S, K_call, T, r, iv_call) - bs_put(S, K_put, T, r, iv_put)
            
        gap = abs(K_put - K_call)
        
        try:
            return round(brentq(objective, K_call - (gap*10), K_put + (gap*10)), 2)
        except:
            guess = (K_call + K_put) / 2.0
            ans, _, ier, _ = fsolve(objective, guess, full_output=True)
            if ier == 1: 
                return round(ans[0], 2)
            return 0.0
    except:
        return 0.0

def calibrate_live_interest_rate(spot_price, atm_strike, days_to_expiry, atm_ce_ltp, atm_pe_ltp):
    """Reverse-engineers the exact Interest Rate (Cost of Carry) from live premiums."""
    T = max(0.001, days_to_expiry / 365.0)
    if atm_ce_ltp <= 0 or atm_pe_ltp <= 0 or spot_price <= 0:
        return 0.10 

    try:
        parity_ratio = (spot_price - atm_ce_ltp + atm_pe_ltp) / atm_strike
        if parity_ratio <= 0: return 0.0
        implied_r = -np.log(parity_ratio) / T
        return max(0.0, min(0.15, implied_r))
    except:
        return 0.10

def inject_prz(chain_rows, expiry_date_str, step, spot_price):
    try:
        exp_date = datetime.strptime(f"{expiry_date_str} 15:30:00", "%Y-%m-%d %H:%M:%S")
        days_to_expiry = max(0.001, (exp_date - datetime.now()).total_seconds() / 86400.0)
    except:
        days_to_expiry = 5.0

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
                r["ce"]["iv"] = 0
                r["ce"]["delta"] = 0
                r["ce"]["theta"] = 0
                r["ce"]["vega"] = 0
                r["ce"]["gamma"] = 0
                
            if pe_ltp > 0:
                raw_pe_iv = calculate_custom_iv(pe_ltp, spot_price, strike, T, live_rate, 'pe')
                pe_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_pe_iv, 'pe')
                
                r["pe"]["iv"] = round(raw_pe_iv * 100, 2)
                r["pe"]["delta"] = pe_greeks["delta"]
                r["pe"]["theta"] = pe_greeks["theta"]
                r["pe"]["vega"] = pe_greeks["vega"]
                r["pe"]["gamma"] = pe_greeks["gamma"]
            else:
                r["pe"]["iv"] = 0
                r["pe"]["delta"] = 0
                r["pe"]["theta"] = 0
                r["pe"]["vega"] = 0
                r["pe"]["gamma"] = 0

        iv_map[strike] = {
            "ce_iv": raw_ce_iv if raw_ce_iv > 0 else 0.15,
            "pe_iv": raw_pe_iv if raw_pe_iv > 0 else 0.15
        }

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
#  🔑  FILL THESE IN
# ══════════════════════════════════════════════════════════
API_KEY      = "3e51765a-3794-41ab-b3c9-4a88e0d55e30"
API_SECRET   = "1ky9l299rf"
REDIRECT_URI = "https://ioc-backend-kq9x.onrender.com/callback"
# ══════════════════════════════════════════════════════════

TOKEN_FILE = os.path.join(os.getcwd(), "upstox_token.json")
MCX_TRACKER_FILE = os.path.join(os.getcwd(), "mcx_oi_tracker.json")
BASE_URL   = "https://api.upstox.com/v2"

_access_token = None

# ── SYMBOL MAP ────────────────────────────────────────────
SYMBOL_MAP = {
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 30,  "step": 100},
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    "MIDCPNIFTY": {"instrument_key": "NSE_INDEX|Nifty Midcap Select", "lot": 50,  "step": 25},
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100},
    
    "CRUDEOIL":   {"instrument_key": "", "lot": 100, "step": 10},
    "NATURALGAS": {"instrument_key": "", "lot": 1250, "step": 5}, 
}

# ── CACHE & SESSION TRACKER ───────────────────────────────
_cache = {}
CACHE_TTL = 3

MCX_SESSION_TRACKER = {
    "date": datetime.now().strftime("%Y-%m-%d"),
    "oi_start": {} 
}

def load_mcx_tracker():
    global MCX_SESSION_TRACKER
    current_date = datetime.now().strftime("%Y-%m-%d")
    try:
        if os.path.exists(MCX_TRACKER_FILE):
            with open(MCX_TRACKER_FILE, "r") as f:
                data = json.load(f)
                if data.get("date") == current_date:
                    MCX_SESSION_TRACKER["date"] = current_date
                    MCX_SESSION_TRACKER["oi_start"] = data.get("oi_start", {})
                    return
    except: pass
    MCX_SESSION_TRACKER["date"] = current_date
    MCX_SESSION_TRACKER["oi_start"] = {}

def save_mcx_tracker():
    try:
        with open(MCX_TRACKER_FILE, "w") as f:
            json.dump(MCX_SESSION_TRACKER, f)
    except: pass

load_mcx_tracker() # Initialize on Boot

def cache_get(key):
    e = _cache.get(key)
    return e["data"] if e and (time.time() - e["ts"]) < CACHE_TTL else None

def cache_set(key, data):
    _cache[key] = {"ts": time.time(), "data": data}

# ══════════════════════════════════════════════════════════
#  🟢 LTP CALCULATOR ENGINE (COA 1.0 & 2.0)
# ══════════════════════════════════════════════════════════
COA_MEMORY = {}

def evaluate_side(vols_dict):
    if not vols_dict: 
        return {"strike": 0, "state": "Strong", "target_strike": 0, "pct": 0, "val": 0}
        
    sorted_vols = sorted(vols_dict.items(), key=lambda x: x[1], reverse=True)
    max_strike, max_vol = sorted_vols[0]
    
    if len(sorted_vols) > 1:
        sec_strike, sec_vol = sorted_vols[1]
        pct = round((sec_vol / max_vol * 100), 2) if max_vol > 0 else 0
        
        if pct >= 75.0:
            if sec_strike > max_strike:
                # 🟢 Changed from WTT to STT
                return {"strike": max_strike, "state": "STT", "target_strike": sec_strike, "pct": pct, "val": max_vol}
            else:
                # 🟢 Changed from WTB to STB
                return {"strike": max_strike, "state": "STB", "target_strike": sec_strike, "pct": pct, "val": max_vol}
                
    return {"strike": max_strike, "state": "Strong", "target_strike": 0, "pct": 0, "val": max_vol}

def calculate_coa(chain_rows, symbol, expiry):
    global COA_MEMORY
    mem_key = f"{symbol}_{expiry}"
    if mem_key not in COA_MEMORY:
        COA_MEMORY[mem_key] = {"sup_strike": 0, "res_strike": 0, "sup_state": "", "res_state": "", "logs": []}
    
    mem = COA_MEMORY[mem_key]
    
    ce_vols = {r['strike']: r['ce'].get('volume', 0) for r in chain_rows if r['ce'].get('volume')}
    pe_vols = {r['strike']: r['pe'].get('volume', 0) for r in chain_rows if r['pe'].get('volume')}
    
    res = evaluate_side(ce_vols) 
    sup = evaluate_side(pe_vols) 
    
    scenario_id = 0
    scenario_desc = ""
    
    if sup['state'] == 'Strong' and res['state'] == 'Strong':
        scenario_id, scenario_desc = 1, "Consolidating. Rangebound between SPL and RPL."
    elif sup['state'] == 'Strong' and res['state'] == 'STB':
        scenario_id, scenario_desc = 2, "Mildly Bearish. Pressure is on the downside."
    elif sup['state'] == 'Strong' and res['state'] == 'STT':
        scenario_id, scenario_desc = 3, "Mildly Bullish. Pressure is on the upside."
    elif sup['state'] == 'STB' and res['state'] == 'Strong':
        scenario_id, scenario_desc = 4, "Mildly Bearish. Downward pressure from Support."
    elif sup['state'] == 'STT' and res['state'] == 'Strong':
        scenario_id, scenario_desc = 5, "Mildly Bullish. Upward pressure from Support."
    elif sup['state'] == 'STT' and res['state'] == 'STT':
        scenario_id, scenario_desc = 6, "Highly Bullish. Extreme upward pressure."
    elif sup['state'] == 'STB' and res['state'] == 'STB':
        scenario_id, scenario_desc = 7, "Highly Bearish. Extreme downward pressure."
    elif sup['state'] == 'STB' and res['state'] == 'STT':
        scenario_id, scenario_desc = 8, "Confusion (Diverging). Wide, wild moves possible."
    elif sup['state'] == 'STT' and res['state'] == 'STB':
        scenario_id, scenario_desc = 9, "Confusion (Converging). Highly unpredictable."

    current_time = datetime.now().strftime("%I:%M %p")
    new_logs = []
    
    if mem['sup_strike'] == 0:
        mem['sup_strike'], mem['res_strike'] = sup['strike'], res['strike']
        mem['sup_state'], mem['res_state'] = sup['state'], res['state']
        new_logs.append(f"{current_time} - Market Open: SPL Strong at {sup['strike']}, RPL Strong at {res['strike']}.")
    
    if res['strike'] > mem['res_strike'] and mem['res_strike'] != 0:
        new_logs.append(f"{current_time} - Scenario Change: Resistance Shifted B2T ({mem['res_strike']} ➔ {res['strike']}). Bullish indicator.")
        mem['res_strike'] = res['strike']
    elif res['strike'] < mem['res_strike'] and mem['res_strike'] != 0:
        new_logs.append(f"{current_time} - Scenario Change: Resistance Shifted T2B ({mem['res_strike']} ➔ {res['strike']}). Bearish indicator.")
        mem['res_strike'] = res['strike']
        
    if sup['strike'] > mem['sup_strike'] and mem['sup_strike'] != 0:
        new_logs.append(f"{current_time} - Scenario Change: Support Shifted B2T ({mem['sup_strike']} ➔ {sup['strike']}). Bullish indicator.")
        mem['sup_strike'] = sup['strike']
    elif sup['strike'] < mem['sup_strike'] and mem['sup_strike'] != 0:
        new_logs.append(f"{current_time} - Scenario Change: Support Shifted T2B ({mem['sup_strike']} ➔ {sup['strike']}). Bearish indicator.")
        mem['sup_strike'] = sup['strike']

    if res['state'] != mem['res_state']:
        if res['state'] in ['STT', 'STB']:
            new_logs.append(f"{current_time} - Resistance became {res['state']} ({res['pct']}% volume buildup at {res['target_strike']}).")
        elif res['state'] == 'Strong':
            new_logs.append(f"{current_time} - Resistance stabilized. Now Strong at {res['strike']}.")
        mem['res_state'] = res['state']

    if sup['state'] != mem['sup_state']:
        if sup['state'] in ['STT', 'STB']:
            new_logs.append(f"{current_time} - Support became {sup['state']} ({sup['pct']}% volume buildup at {sup['target_strike']}).")
        elif sup['state'] == 'Strong':
            new_logs.append(f"{current_time} - Support stabilized. Now Strong at {sup['strike']}.")
        mem['sup_state'] = sup['state']

    mem['logs'] = new_logs + mem['logs']
    mem['logs'] = mem['logs'][:50]
    
    res_row = next((r for r in chain_rows if r['strike'] == res['strike']), None)
    sup_row = next((r for r in chain_rows if r['strike'] == sup['strike']), None)
    
    eor_val = res_row['ce_prz'] if res_row else res['strike']
    eos_val = sup_row['pe_prz'] if sup_row else sup['strike']

    return {
        "scenario_id": scenario_id,
        "scenario_desc": scenario_desc,
        "support": sup,
        "resistance": res,
        "eos": eor_val,
        "eor": eos_val,
        "logs": mem['logs']
    }


# ══════════════════════════════════════════════════════════
#  MASTER CSV BYPASS
# ══════════════════════════════════════════════════════════
MCX_CACHE = {}

def load_mcx_options(symbol):
    global MCX_CACHE
    if symbol in MCX_CACHE: return MCX_CACHE[symbol]
    
    print(f"🔄 Downloading MCX Database to map {symbol} Options & Futures...")
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/MCX.csv.gz"
        resp = requests.get(url)
        
        db = {"options": {}, "futures": {}}
        
        with gzip.open(io.BytesIO(resp.content), 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                tradingsymbol = row.get('tradingsymbol', '').upper()
                inst_type = row.get('instrument_type', '').upper()
                
                if tradingsymbol.startswith(symbol):
                    raw_exp = row.get('expiry', '')
                    exp_fmt = raw_exp
                    for fmt_str in ('%d-%b-%Y', '%d-%b-%y', '%Y-%m-%d', '%d/%m/%Y'):
                        try: 
                            exp_fmt = datetime.strptime(raw_exp, fmt_str).strftime('%Y-%m-%d')
                            break
                        except ValueError: 
                            pass
                    
                    if inst_type == 'OPTFUT':
                        try:
                            strike_str = row.get('strike', '0')
                            strike = float(strike_str) if strike_str else 0.0
                        except ValueError:
                            continue 
                            
                        opt_type = row.get('option_type', '') 
                        
                        if exp_fmt not in db["options"]: db["options"][exp_fmt] = {}
                        if strike not in db["options"][exp_fmt]: db["options"][exp_fmt][strike] = {}
                        
                        db["options"][exp_fmt][strike][opt_type] = {
                            "key": row.get('instrument_key'), 
                            "tradingsymbol": row.get('tradingsymbol')
                        }
                    
                    elif 'FUT' in inst_type:
                        db["futures"][exp_fmt] = row.get('instrument_key')
                        
        MCX_CACHE[symbol] = db
        print(f"✅ Mapped {len(db['options'].keys())} Options and {len(db['futures'].keys())} Futures for {symbol}.")
        return db
    except Exception as e:
        print(f"❌ Failed to load MCX DB: {e}")
        return {"options": {}, "futures": {}}

def fmt(v, dec=2):
    try: return round(float(v), dec)
    except: return None

def _next_thursdays(n=4):
    from datetime import date, timedelta
    result, d = [], date.today()
    while len(result) < n:
        d += timedelta(days=1)
        if d.weekday() == 3: result.append(d.strftime("%Y-%m-%d"))
    return result

def build_mcx_chain(symbol, expiry, cfg):
    mcx_db = load_mcx_options(symbol)
    
    opts_db = mcx_db.get("options") or {}
    futs_db = mcx_db.get("futures") or {}

    if not opts_db or expiry not in opts_db:
        return {"error": f"No options mapped for {symbol} on {expiry}. Check expiry dates."}
        
    future_key = futs_db.get(expiry)
    if not future_key and futs_db:
        future_key = futs_db[sorted(futs_db.keys())[0]]

    spot = None
    if future_key:
        spot_resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": future_key}, headers=auth_headers())
        if spot_resp.status_code == 200:
            safe_data = spot_resp.json().get("data") or {}
            for v in safe_data.values():
                spot = fmt(v.get("last_price"))
                break

    atm = round(float(spot) / cfg["step"]) * cfg["step"] if spot else None

    ce_keys, pe_keys = {}, {}
    all_mapped_strikes = sorted(list(opts_db[expiry].keys()))
    
    if not atm and all_mapped_strikes:
        atm = all_mapped_strikes[len(all_mapped_strikes) // 2]
        spot = atm 

    for strike, opts in opts_db[expiry].items():
        if not opts: continue
        if atm and abs(strike - atm) > (30 * cfg["step"]): continue
        if 'CE' in opts: ce_keys[strike] = opts['CE']
        if 'PE' in opts: pe_keys[strike] = opts['PE']

    all_keys = [v["key"] for v in list(ce_keys.values()) + list(pe_keys.values())]
    quotes = {}
    
    print(f"\n📡 Ask Upstox for {len(all_keys)} {symbol} strikes...")
    
    for i in range(0, len(all_keys), 50):
        chunk = all_keys[i:i+50]
        q_resp = requests.get(f"{BASE_URL}/market-quote/quotes", params={"instrument_key": ",".join(chunk)}, headers=auth_headers())
        
        if q_resp.status_code == 200:
            safe_quotes = q_resp.json().get("data") or {}
            quotes.update(safe_quotes)
        else:
            print(f"❌ Error {q_resp.status_code}: {q_resp.text}")

    chain_rows, total_ce_oi, total_pe_oi = [], 0, 0
    all_strikes = sorted(list(set(ce_keys.keys()).union(pe_keys.keys())))

    global MCX_SESSION_TRACKER
    current_date = datetime.now().strftime("%Y-%m-%d")
    if MCX_SESSION_TRACKER.get("date") != current_date:
        MCX_SESSION_TRACKER["date"] = current_date
        MCX_SESSION_TRACKER["oi_start"] = {}
        save_mcx_tracker()

    tracker_updated = False

    for strike in all_strikes:
        ce_meta = ce_keys.get(strike)
        pe_meta = pe_keys.get(strike)
        
        c_key = ce_meta['key'] if ce_meta else ""
        c_trade = f"MCX_FO:{ce_meta['tradingsymbol']}" if ce_meta else ""
        c_quote = quotes.get(c_key) or quotes.get(c_trade) or {}
        
        p_key = pe_meta['key'] if pe_meta else ""
        p_trade = f"MCX_FO:{pe_meta['tradingsymbol']}" if pe_meta else ""
        p_quote = quotes.get(p_key) or quotes.get(p_trade) or {}
        
        c_oi = int(c_quote.get("oi") or c_quote.get("open_interest") or 0)
        p_oi = int(p_quote.get("oi") or p_quote.get("open_interest") or 0)

        c_key_track = f"{symbol}_{expiry}_{strike}_CE"
        p_key_track = f"{symbol}_{expiry}_{strike}_PE"
        
        if c_oi > 0 and c_key_track not in MCX_SESSION_TRACKER["oi_start"]:
            MCX_SESSION_TRACKER["oi_start"][c_key_track] = c_oi
            tracker_updated = True
        if p_oi > 0 and p_key_track not in MCX_SESSION_TRACKER["oi_start"]:
            MCX_SESSION_TRACKER["oi_start"][p_key_track] = p_oi
            tracker_updated = True

        c_change = c_oi - MCX_SESSION_TRACKER["oi_start"].get(c_key_track, c_oi)
        p_change = p_oi - MCX_SESSION_TRACKER["oi_start"].get(p_key_track, p_oi)

        ce_parsed = {
            "ltp": fmt(c_quote.get("last_price")), 
            "oi": c_oi,
            "change_oi": c_change, 
            "volume": fmt(c_quote.get("volume"), 0), 
            "iv": 0, "delta": 0, "theta": 0, "vega": 0, "gamma": 0
        }
        pe_parsed = {
            "ltp": fmt(p_quote.get("last_price")), 
            "oi": p_oi,
            "change_oi": p_change, 
            "volume": fmt(p_quote.get("volume"), 0), 
            "iv": 0, "delta": 0, "theta": 0, "vega": 0, "gamma": 0
        }
        
        total_ce_oi += ce_parsed["oi"] or 0
        total_pe_oi += pe_parsed["oi"] or 0
        chain_rows.append({"strike": strike, "atm": atm is not None and abs(strike - atm) < cfg["step"], "ce": ce_parsed, "pe": pe_parsed})

    if tracker_updated: save_mcx_tracker()

    chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
    coa_data = calculate_coa(chain_rows, symbol, expiry)

    return {
        "symbol": symbol, "expiry": expiry, "spot": spot, "pcr": round(total_pe_oi / max(total_ce_oi, 1), 2),
        "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi, "lot_size": cfg["lot"],
        "chain": chain_rows, "fetched_at": datetime.now().strftime("%H:%M:%S"),
        "coa": coa_data
    }

# ══════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════
@app.route("/health")
def health(): return jsonify({"status": "ok", "authenticated": _access_token is not None})

@app.route("/expiry-dates")
def expiry_dates():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return jsonify({"error": f"Unknown symbol: {symbol}"}), 400

    cache_key = f"expiry_{symbol}"
    cached = cache_get(cache_key)
    if cached: return jsonify(cached)

    try:
        if symbol in ["CRUDEOIL", "NATURALGAS"]:
            mcx_db = load_mcx_options(symbol)
            expiries = sorted(list((mcx_db.get("options") or {}).keys()))
            if not expiries: return jsonify({"error": f"No active {symbol} expiries found."}), 400
            result = {"symbol": symbol, "expiries": expiries}
            cache_set(cache_key, result)
            return jsonify(result)

        resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        data = resp.json().get("data") or []
        expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
        if not expiries: expiries = _next_thursdays(4)
        
        result = {"symbol": symbol, "expiries": expiries}
        cache_set(cache_key, result)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/spot-price")
def spot_price():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return jsonify({"error": f"Unknown symbol: {symbol}"}), 400
    cache_key = f"spot_{symbol}"
    cached = cache_get(cache_key)
    if cached: return jsonify(cached)

    try:
        inst_key = cfg.get("instrument_key")

        if symbol in ["CRUDEOIL", "NATURALGAS"]:
            mcx_db = load_mcx_options(symbol)
            futs_db = mcx_db.get("futures") or {}
            if futs_db:
                front_month = sorted(futs_db.keys())[0]
                inst_key = futs_db[front_month]

        if not inst_key: return jsonify({"error": "No instrument key available"}), 400

        resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": inst_key}, headers=auth_headers())
        safe_data = resp.json().get("data") or {}
        price = None
        for v in safe_data.values():
            price = fmt(v.get("last_price"))
            break
            
        result = {"symbol": symbol, "spot": price, "time": datetime.now().strftime("%H:%M:%S")}
        cache_set(cache_key, result)
        return jsonify(result)
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route("/options-chain")
def options_chain():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    expiry = request.args.get("expiry", "").strip()
    cfg = SYMBOL_MAP.get(symbol)

    if not cfg: return jsonify({"error": f"Unknown symbol: {symbol}"}), 400
    if not expiry: return jsonify({"error": "Expiry param required"}), 400

    cache_key = f"chain_{symbol}_{expiry}"
    cached = cache_get(cache_key)
    if cached: return jsonify(cached)

    try:
        if symbol in ["CRUDEOIL", "NATURALGAS"]:
            result = build_mcx_chain(symbol, expiry, cfg)
            if "error" in result: return jsonify(result), 400
            cache_set(cache_key, {**result, "cached": True})
            return jsonify(result)

        resp = requests.get(f"{BASE_URL}/option/chain", params={"instrument_key": cfg["instrument_key"], "expiry_date": expiry}, headers=auth_headers())
        raw_list = resp.json().get("data") or []
        if not raw_list: return jsonify({"error": "No NSE data returned."}), 502

        spot = None
        spot_resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
        if spot_resp.status_code == 200:
            safe_spot_data = spot_resp.json().get("data") or {}
            for v in safe_spot_data.values():
                spot = fmt(v.get("last_price"))
                break
                
        if not spot: spot = fmt(raw_list[0].get("underlying_spot_price")) if raw_list else None

        ce_vol_mult = float(request.args.get("ce_vol_mult", 1.0))
        pe_vol_mult = float(request.args.get("pe_vol_mult", 1.0))
        ce_oi_mult = float(request.args.get("ce_oi_mult", 1.0))
        pe_oi_mult = float(request.args.get("pe_oi_mult", 1.0))

        atm  = round(float(spot) / cfg["step"]) * cfg["step"] if spot else None

        chain_rows, total_ce_oi, total_pe_oi = [], 0, 0
        for item in raw_list:
            strike = int(float(item.get("strike_price", 0)))
            if atm and abs(strike - atm) > 1500: continue
            
            def parse_side_nse(d, v_mult, o_mult):
                if not d or not isinstance(d, dict): 
                    return {"ltp": 0, "oi": 0, "m_oi": 0, "change_oi": 0, "volume": 0, "m_vol": 0, "iv": 0, "delta": 0, "theta": 0, "vega": 0, "gamma": 0}
                md = d.get("market_data") or {}
                
                base_oi = int(md.get("oi") or 0)
                base_vol = fmt(md.get("volume"), 0) or 0
                
                return {
                    "ltp": fmt(md.get("ltp")), 
                    "oi": base_oi, 
                    "m_oi": int(base_oi * o_mult), 
                    "change_oi": int(md.get("oi") or 0) - int(md.get("prev_oi") or 0),
                    "volume": base_vol, 
                    "m_vol": int(base_vol * v_mult), 
                    "iv": 0, 
                    "delta": 0,
                    "theta": 0, 
                    "vega": 0, 
                    "gamma": 0 
                }
                
            ce = parse_side_nse(item.get("call_options") or {}, ce_vol_mult, ce_oi_mult)
            pe = parse_side_nse(item.get("put_options")  or {}, pe_vol_mult, pe_oi_mult)
            
            total_ce_oi += ce["oi"] or 0
            total_pe_oi += pe["oi"] or 0
            
            chain_rows.append({"strike": strike, "atm": atm is not None and abs(strike - atm) < cfg["step"], "ce": ce, "pe": pe})

        chain_rows = inject_prz(chain_rows, expiry, cfg["step"], spot)
        coa_data = calculate_coa(chain_rows, symbol, expiry)

        result = {
            "symbol": symbol, "expiry": expiry, "spot": spot, "pcr": round(total_pe_oi / max(total_ce_oi, 1), 2),
            "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi, "lot_size": cfg["lot"],
            "chain": chain_rows, "fetched_at": datetime.now().strftime("%H:%M:%S"), "cached": False,
            "coa": coa_data
        }
        cache_set(cache_key, {**result, "cached": True})
        return jsonify(result)

    except Exception as e: 
        return jsonify({"error": str(e)}), 500

@app.route("/record", methods=["POST"])
def record_csv():
    try:
        data = request.json
        if not data or "chain" not in data: 
            return jsonify({"error": "Invalid data"}), 400
        
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        record_dir = os.path.join(desktop, "InsiderQuant_Records")
        os.makedirs(record_dir, exist_ok=True)
        
        filename = os.path.join(record_dir, f"{data['symbol']}_{data['expiry']}.csv")
        file_exists = os.path.isfile(filename)
        
        with open(filename, 'a', newline='') as f:
            writer = csv.writer(f)
            
            if not file_exists:
                writer.writerow([
                    "Timestamp", "Symbol", "Expiry", "Spot", "PCR", "Strike", 
                    "CE_LTP", "CE_OI", "CE_Vol", "CE_IV", "CE_Delta", "CE_Theta", "CE_Vega", "CE_Gamma", 
                    "PE_LTP", "PE_OI", "PE_Vol", "PE_IV", "PE_Delta", "PE_Theta", "PE_Vega", "PE_Gamma"
                ])
            
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            safe_chain = data.get("chain") or []
            for r in safe_chain:
                writer.writerow([
                    ts, data['symbol'], data['expiry'], data['spot'], data['pcr'], r.get('strike'), 
                    r['ce'].get('ltp', 0), r['ce'].get('oi', 0), r['ce'].get('volume', 0), 
                    r['ce'].get('iv', 0), r['ce'].get('delta', 0), r['ce'].get('theta', 0), 
                    r['ce'].get('vega', 0), r['ce'].get('gamma', 0),
                    r['pe'].get('ltp', 0), r['pe'].get('oi', 0), r['pe'].get('volume', 0), 
                    r['pe'].get('iv', 0), r['pe'].get('delta', 0), r['pe'].get('theta', 0), 
                    r['pe'].get('vega', 0), r['pe'].get('gamma', 0)
                ])
                
        return jsonify({"status": "success", "file": filename})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ══════════════════════════════════════════════════════════
#  CLOUD AUTH FLOW (For PythonAnywhere)
# ══════════════════════════════════════════════════════════
def load_saved_token():
    global _access_token
    try:
        if not os.path.exists(TOKEN_FILE): return False
        with open(TOKEN_FILE) as f: data = json.load(f)
        token = data.get("access_token", "")
        if token:
            try:
                import base64
                payload = token.split(".")[1]
                payload += "=" * (4 - len(payload) % 4)
                decoded = json.loads(base64.b64decode(payload))
                if decoded.get("exp", 0) > datetime.now().timestamp():
                    _access_token = token
                    return True
            except Exception: pass
        if data.get("date") != datetime.now().strftime("%Y-%m-%d"): return False
        _access_token = token
        return True
    except Exception: return False

def save_token(token):
    global _access_token
    _access_token = token
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump({"access_token": token, "date": datetime.now().strftime("%Y-%m-%d")}, f)
    except Exception: pass

def auth_headers():
    return {"Authorization": f"Bearer {_access_token}", "Accept": "application/json"}

# 🟢 The "Secret URL" to generate the login link
@app.route("/login")
def login_route():
    params = {"response_type": "code", "client_id": API_KEY, "redirect_uri": REDIRECT_URI}
    login_url = f"https://api.upstox.com/v2/login/authorization/dialog?{urlencode(params)}"
    return f'''
        <h2 style="font-family:sans-serif;">Upstox Server Auth</h2>
        <a href="{login_url}" style="padding:10px 20px; background:#3b82f6; color:white; text-decoration:none; border-radius:5px; font-family:sans-serif;">Click here to Login</a>
    '''

# 🟢 The URL Upstox sends you back to with the daily code
@app.route("/callback")
def callback_route():
    code = request.args.get("code")
    if not code:
        return "Error: No auth code provided by Upstox.", 400

    resp = requests.post(
        "https://api.upstox.com/v2/login/authorization/token",
        data={"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "redirect_uri": REDIRECT_URI, "grant_type": "authorization_code"},
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    )
    
    if resp.status_code == 200:
        save_token(resp.json().get("access_token"))
        return '<h2 style="color:green; font-family:sans-serif;">✅ Login Successful! Token saved. You can close this tab and use the Terminal.</h2>'
    else:
        return f'<h2 style="color:red; font-family:sans-serif;">❌ Failed to get token:</h2><p>{resp.text}</p>'

if __name__ == "__main__":
    if API_KEY == "your_api_key_here":
        print("WARNING: Add keys to upstox_live.py")
        sys.exit(1)
        
    print("\n Server: http://127.0.0.1:5001\n" + "-" * 45)
    app.run(port=5001, debug=False)
