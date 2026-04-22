"""
InsiderOwl — Upstox Live Market Backend (Commercial Edition)
====================================================================
Includes 9:15-3:30 Automated Background Recorder for NIFTY, BANKNIFTY, SENSEX.
Prioritizes Native Upstox IV/Greeks for Indices to prevent discrepancies.
Calculates EOR, EOS using the dual-rate Black-Scholes Engine.
"""

import os, sys, time, json, webbrowser, requests, gzip, io, csv, threading
from datetime import datetime, timedelta
from urllib.parse import urlencode
from flask import Flask, jsonify, request, send_file
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
        
        # 🟢 COMMERCIAL FEATURE: Use Native Upstox IV & Greeks if available to avoid discrepancies.
        native_ce_iv = float(r["ce"].get("iv") or 0) / 100.0
        native_pe_iv = float(r["pe"].get("iv") or 0) / 100.0
        
        raw_ce_iv = native_ce_iv
        raw_pe_iv = native_pe_iv
        
        if spot_price and spot_price > 0:
            if native_ce_iv <= 0 and ce_ltp > 0:
                raw_ce_iv = calculate_custom_iv(ce_ltp, spot_price, strike, T, live_rate, 'ce')
                ce_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_ce_iv, 'ce')
                r["ce"]["iv"] = round(raw_ce_iv * 100, 2)
                r["ce"]["delta"] = ce_greeks["delta"]
                r["ce"]["theta"] = ce_greeks["theta"]
                r["ce"]["vega"] = ce_greeks["vega"]
                r["ce"]["gamma"] = ce_greeks["gamma"]
                
            if native_pe_iv <= 0 and pe_ltp > 0:
                raw_pe_iv = calculate_custom_iv(pe_ltp, spot_price, strike, T, live_rate, 'pe')
                pe_greeks = bs_greeks(spot_price, strike, T, live_rate, raw_pe_iv, 'pe')
                r["pe"]["iv"] = round(raw_pe_iv * 100, 2)
                r["pe"]["delta"] = pe_greeks["delta"]
                r["pe"]["theta"] = pe_greeks["theta"]
                r["pe"]["vega"] = pe_greeks["vega"]
                r["pe"]["gamma"] = pe_greeks["gamma"]

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
#  🔑  CONFIG
# ══════════════════════════════════════════════════════════
API_KEY      = "3e51765a-3794-41ab-b3c9-4a88e0d55e30"
API_SECRET   = "1ky9l299rf"
REDIRECT_URI = "https://ioc-backend-kq9x.onrender.com/callback"

TOKEN_FILE = os.path.join(os.getcwd(), "upstox_token.json")
MCX_TRACKER_FILE = os.path.join(os.getcwd(), "mcx_oi_tracker.json")
RECORDS_DIR = os.path.join(os.getcwd(), "InsiderQuant_Records")
os.makedirs(RECORDS_DIR, exist_ok=True)

BASE_URL   = "https://api.upstox.com/v2"
_access_token = None

SYMBOL_MAP = {
    "NIFTY":      {"instrument_key": "NSE_INDEX|Nifty 50",            "lot": 75,  "step": 50},
    "BANKNIFTY":  {"instrument_key": "NSE_INDEX|Nifty Bank",          "lot": 30,  "step": 100},
    "FINNIFTY":   {"instrument_key": "NSE_INDEX|Nifty Fin Service",   "lot": 40,  "step": 50},
    "MIDCPNIFTY": {"instrument_key": "NSE_INDEX|Nifty Midcap Select", "lot": 50,  "step": 25},
    "SENSEX":     {"instrument_key": "BSE_INDEX|SENSEX",              "lot": 20,  "step": 100},
    "CRUDEOIL":   {"instrument_key": "", "lot": 100, "step": 10},
    "NATURALGAS": {"instrument_key": "", "lot": 1250, "step": 5}, 
}

_cache = {}
CACHE_TTL = 3

# ══════════════════════════════════════════════════════════
#  🟢 LTP CALCULATOR ENGINE
# ══════════════════════════════════════════════════════════
COA_MEMORY = {}

def evaluate_side(vols_dict):
    if not vols_dict: return {"strike": 0, "state": "Strong", "target_strike": 0, "pct": 0, "val": 0}
    sorted_vols = sorted(vols_dict.items(), key=lambda x: x[1], reverse=True)
    max_strike, max_vol = sorted_vols[0]
    if len(sorted_vols) > 1:
        sec_strike, sec_vol = sorted_vols[1]
        pct = round((sec_vol / max_vol * 100), 2) if max_vol > 0 else 0
        if pct >= 75.0:
            if sec_strike > max_strike: return {"strike": max_strike, "state": "STT", "target_strike": sec_strike, "pct": pct, "val": max_vol}
            else: return {"strike": max_strike, "state": "STB", "target_strike": sec_strike, "pct": pct, "val": max_vol}
    return {"strike": max_strike, "state": "Strong", "target_strike": 0, "pct": 0, "val": max_vol}

def calculate_coa(chain_rows, symbol, expiry):
    global COA_MEMORY
    mem_key = f"{symbol}_{expiry}"
    if mem_key not in COA_MEMORY: COA_MEMORY[mem_key] = {"sup_strike": 0, "res_strike": 0, "sup_state": "", "res_state": "", "logs": []}
    mem = COA_MEMORY[mem_key]
    
    ce_vols = {r['strike']: r['ce'].get('volume', 0) for r in chain_rows if r['ce'].get('volume')}
    pe_vols = {r['strike']: r['pe'].get('volume', 0) for r in chain_rows if r['pe'].get('volume')}
    res, sup = evaluate_side(ce_vols), evaluate_side(pe_vols)
    
    scenario_id, scenario_desc = 0, ""
    if sup['state'] == 'Strong' and res['state'] == 'Strong': scenario_id, scenario_desc = 1, "Consolidating. Rangebound between SPL and RPL."
    elif sup['state'] == 'Strong' and res['state'] == 'STB': scenario_id, scenario_desc = 2, "Mildly Bearish. Pressure is on the downside."
    elif sup['state'] == 'Strong' and res['state'] == 'STT': scenario_id, scenario_desc = 3, "Mildly Bullish. Pressure is on the upside."
    elif sup['state'] == 'STB' and res['state'] == 'Strong': scenario_id, scenario_desc = 4, "Mildly Bearish. Downward pressure from Support."
    elif sup['state'] == 'STT' and res['state'] == 'Strong': scenario_id, scenario_desc = 5, "Mildly Bullish. Upward pressure from Support."
    elif sup['state'] == 'STT' and res['state'] == 'STT': scenario_id, scenario_desc = 6, "Highly Bullish. Extreme upward pressure."
    elif sup['state'] == 'STB' and res['state'] == 'STB': scenario_id, scenario_desc = 7, "Highly Bearish. Extreme downward pressure."
    elif sup['state'] == 'STB' and res['state'] == 'STT': scenario_id, scenario_desc = 8, "Confusion (Diverging). Wide, wild moves possible."
    elif sup['state'] == 'STT' and res['state'] == 'STB': scenario_id, scenario_desc = 9, "Confusion (Converging). Highly unpredictable."

    current_time = datetime.now().strftime("%I:%M %p")
    new_logs = []
    if mem['sup_strike'] == 0:
        mem['sup_strike'], mem['res_strike'], mem['sup_state'], mem['res_state'] = sup['strike'], res['strike'], sup['state'], res['state']
        new_logs.append(f"{current_time} - Market Open: SPL Strong at {sup['strike']}, RPL Strong at {res['strike']}.")
    
    if res['strike'] != mem['res_strike'] and mem['res_strike'] != 0:
        new_logs.append(f"{current_time} - Scenario Change: Resistance Shifted ({mem['res_strike']} ➔ {res['strike']}).")
        mem['res_strike'] = res['strike']
    if sup['strike'] != mem['sup_strike'] and mem['sup_strike'] != 0:
        new_logs.append(f"{current_time} - Scenario Change: Support Shifted ({mem['sup_strike']} ➔ {sup['strike']}).")
        mem['sup_strike'] = sup['strike']

    if res['state'] != mem['res_state']:
        new_logs.append(f"{current_time} - Resistance became {res['state']}.")
        mem['res_state'] = res['state']
    if sup['state'] != mem['sup_state']:
        new_logs.append(f"{current_time} - Support became {sup['state']}.")
        mem['sup_state'] = sup['state']

    mem['logs'] = (new_logs + mem['logs'])[:50]
    
    res_row = next((r for r in chain_rows if r['strike'] == res['strike']), None)
    sup_row = next((r for r in chain_rows if r['strike'] == sup['strike']), None)
    eor_val = res_row['ce_prz'] if res_row else res['strike']
    eos_val = sup_row['pe_prz'] if sup_row else sup['strike']

    return {"scenario_id": scenario_id, "scenario_desc": scenario_desc, "support": sup, "resistance": res, "eos": eor_val, "eor": eos_val, "logs": mem['logs']}

# ══════════════════════════════════════════════════════════
#  🟢 AUTOMATED 9:15 to 3:30 BACKGROUND RECORDER
# ══════════════════════════════════════════════════════════
def compress_and_save(symbol, expiry, spot, pcr, chain_rows):
    if not chain_rows or not spot: return
    
    filename = os.path.join(RECORDS_DIR, f"{symbol}_{expiry}_Recorded.csv")
    file_exists = os.path.isfile(filename)
    ts = datetime.utcnow() + timedelta(hours=5, minutes=30)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")

    # High Compression: Only keep ATM +/- 15 strikes
    step = SYMBOL_MAP[symbol]["step"]
    atm = round(spot / step) * step
    compressed_chain = [r for r in chain_rows if abs(r['strike'] - atm) <= (15 * step)]

    with open(filename, 'a', newline='') as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "Timestamp", "Symbol", "Expiry", "Spot", "PCR", "Strike", 
                "CE_LTP", "CE_OI", "CE_Vol", "CE_IV", "CE_Delta", "CE_Theta", "CE_Vega", "CE_Gamma", 
                "PE_LTP", "PE_OI", "PE_Vol", "PE_IV", "PE_Delta", "PE_Theta", "PE_Vega", "PE_Gamma"
            ])
        for r in compressed_chain:
            writer.writerow([
                ts_str, symbol, expiry, spot, pcr, r.get('strike'), 
                r['ce'].get('ltp', 0), r['ce'].get('oi', 0), r['ce'].get('volume', 0), 
                r['ce'].get('iv', 0), r['ce'].get('delta', 0), r['ce'].get('theta', 0), r['ce'].get('vega', 0), r['ce'].get('gamma', 0),
                r['pe'].get('ltp', 0), r['pe'].get('oi', 0), r['pe'].get('volume', 0), 
                r['pe'].get('iv', 0), r['pe'].get('delta', 0), r['pe'].get('theta', 0), r['pe'].get('vega', 0), r['pe'].get('gamma', 0)
            ])

def fetch_and_record(symbol):
    cfg = SYMBOL_MAP.get(symbol)
    if not cfg: return
    
    # 1. Fetch closest Expiry
    resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
    data = resp.json().get("data") or []
    expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
    if not expiries: return
    closest_expiry = expiries[0]

    # 2. Fetch Spot
    spot = None
    spot_resp = requests.get(f"{BASE_URL}/market-quote/ltp", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
    if spot_resp.status_code == 200:
        safe_spot = spot_resp.json().get("data") or {}
        for v in safe_spot.values():
            spot = v.get("last_price")
            break
            
    # 3. Fetch Chain
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

def background_market_recorder():
    while True:
        time.sleep(120) # Wake up every 2 minutes
        if not _access_token: continue
        
        # Determine IST time
        utcnow = datetime.utcnow()
        ist_now = utcnow + timedelta(hours=5, minutes=30)
        
        if ist_now.weekday() > 4: continue # Skip Weekends (Sat=5, Sun=6)
        
        time_int = ist_now.hour * 100 + ist_now.minute
        if 915 <= time_int <= 1530:
            for sym in ["NIFTY", "BANKNIFTY", "SENSEX"]:
                try: fetch_and_record(sym)
                except Exception as e: print(f"Recording Error for {sym}: {e}")

# Start the background recorder
threading.Thread(target=background_market_recorder, daemon=True).start()

# ══════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════
def load_mcx_options(symbol): return {"options": {}, "futures": {}} # Skeleton for space limits, add your MCX dict logic if needed

def auth_headers(): return {"Authorization": f"Bearer {_access_token}", "Accept": "application/json"}

@app.route("/health")
def health(): return jsonify({"status": "ok", "authenticated": _access_token is not None})

@app.route("/expiry-dates")
def expiry_dates():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
    cfg = SYMBOL_MAP.get(symbol)
    resp = requests.get(f"{BASE_URL}/option/contract", params={"instrument_key": cfg["instrument_key"]}, headers=auth_headers())
    data = resp.json().get("data") or []
    expiries = sorted({item["expiry"] for item in data if item.get("expiry")})
    return jsonify({"symbol": symbol, "expiries": expiries})

@app.route("/options-chain")
def options_chain():
    symbol = request.args.get("symbol", "NIFTY").upper().strip()
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
        if atm and abs(strike - atm) > 1500: continue
        
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

# 🟢 NEW: ENDPOINT TO VIEW AND DOWNLOAD YOUR RECORDED FILES
@app.route("/records")
def list_records():
    files = os.listdir(RECORDS_DIR) if os.path.exists(RECORDS_DIR) else []
    html = '<h2 style="font-family:sans-serif;">Recorded Backtest Data</h2><ul>'
    for f in sorted(files): html += f'<li><a href="/download/{f}">{f}</a></li>'
    return html + '</ul>'

@app.route("/download/<filename>")
def download_record(filename):
    file_path = os.path.join(RECORDS_DIR, filename)
    if os.path.exists(file_path): return send_file(file_path, as_attachment=True)
    return "File not found", 404

# ══════════════════════════════════════════════════════════
#  CLOUD AUTH FLOW 
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
            except: pass
        if data.get("date") != datetime.now().strftime("%Y-%m-%d"): return False
        _access_token = token
        return True
    except: return False

def save_token(token):
    global _access_token
    _access_token = token
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump({"access_token": token, "date": datetime.now().strftime("%Y-%m-%d")}, f)
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
        return '<h2 style="color:green; font-family:sans-serif;">✅ Login Successful! Token saved.</h2>'
    return f'<h2 style="color:red; font-family:sans-serif;">❌ Failed:</h2><p>{resp.text}</p>'

load_saved_token()
