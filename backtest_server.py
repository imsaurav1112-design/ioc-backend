"""
IOC Dedicated Backtest Server
=============================
Parses recorded CSV files, reconstructs historical options chains, 
calculates PRZ, and generates Market Intelligence (COA) logs sequentially.
Runs on Port 5002 to avoid clashing with the Live Server.
"""

import sys, json, csv
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq, fsolve

app = Flask(__name__)
CORS(app)

# ══════════════════════════════════════════════════════════
#  MATH ENGINE (PRZ)
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

def calc_prz(K_call, iv_call, K_put, iv_put, days_to_expiry, r=0.0675):
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

def inject_prz(chain_rows, expiry_date_str, step):
    try:
        exp_date = datetime.strptime(expiry_date_str, "%Y-%m-%d")
        days_to_expiry = max(0.001, (exp_date - datetime.now()).total_seconds() / 86400.0)
    except: days_to_expiry = 5.0

    iv_map = {}
    for r in chain_rows:
        strike = r["strike"]
        raw_ce_iv = float(r["ce"].get("iv") or 0) / 100.0
        raw_pe_iv = float(r["pe"].get("iv") or 0) / 100.0
        iv_map[strike] = {"ce_iv": raw_ce_iv if raw_ce_iv > 0 else 0.15, "pe_iv": raw_pe_iv if raw_pe_iv > 0 else 0.15}

    for r in chain_rows:
        strike = r["strike"]
        ce_iv, pe_iv = iv_map[strike]["ce_iv"], iv_map[strike]["pe_iv"]
        next_strike_up, next_strike_down = strike + step, strike - step
        pe_iv_up = iv_map.get(next_strike_up, {}).get("pe_iv", 0.15)
        ce_iv_down = iv_map.get(next_strike_down, {}).get("ce_iv", 0.15)
        
        r["ce_prz"] = calc_prz(strike, ce_iv, next_strike_up, pe_iv_up, days_to_expiry)
        r["pe_prz"] = calc_prz(next_strike_down, ce_iv_down, strike, pe_iv, days_to_expiry)
    return chain_rows

# ══════════════════════════════════════════════════════════
#  MARKET INTELLIGENCE (COA)
# ══════════════════════════════════════════════════════════
def evaluate_side(vols_dict):
    if not vols_dict: return {"strike": 0, "state": "Strong", "target_strike": 0, "pct": 0, "val": 0}
    sorted_vols = sorted(vols_dict.items(), key=lambda x: x[1], reverse=True)
    max_strike, max_vol = sorted_vols[0]
    
    if len(sorted_vols) > 1:
        sec_strike, sec_vol = sorted_vols[1]
        pct = round((sec_vol / max_vol * 100), 2) if max_vol > 0 else 0
        if pct >= 75.0:
            state = "WTT" if sec_strike > max_strike else "WTB"
            return {"strike": max_strike, "state": state, "target_strike": sec_strike, "pct": pct, "val": max_vol}
    return {"strike": max_strike, "state": "Strong", "target_strike": 0, "pct": 0, "val": max_vol}

def calculate_coa(chain_rows, timestamp, mem):
    ce_vols = {r['strike']: r['ce']['volume'] for r in chain_rows if r['ce'].get('volume')}
    pe_vols = {r['strike']: r['pe']['volume'] for r in chain_rows if r['pe'].get('volume')}
    res, sup = evaluate_side(ce_vols), evaluate_side(pe_vols)
    
    scenario_id, scenario_desc = 0, ""
    if sup['state'] == 'Strong' and res['state'] == 'Strong': scenario_id, scenario_desc = 1, "Consolidating. Rangebound."
    elif sup['state'] == 'Strong' and res['state'] == 'WTB': scenario_id, scenario_desc = 2, "Mildly Bearish. Downward pressure."
    elif sup['state'] == 'Strong' and res['state'] == 'WTT': scenario_id, scenario_desc = 3, "Mildly Bullish. Upward pressure."
    elif sup['state'] == 'WTB' and res['state'] == 'Strong': scenario_id, scenario_desc = 4, "Mildly Bearish. Support pushing down."
    elif sup['state'] == 'WTT' and res['state'] == 'Strong': scenario_id, scenario_desc = 5, "Mildly Bullish. Support pushing up."
    elif sup['state'] == 'WTT' and res['state'] == 'WTT': scenario_id, scenario_desc = 6, "Highly Bullish. Extreme upward pressure."
    elif sup['state'] == 'WTB' and res['state'] == 'WTB': scenario_id, scenario_desc = 7, "Highly Bearish. Extreme downward pressure."
    elif sup['state'] == 'WTB' and res['state'] == 'WTT': scenario_id, scenario_desc = 8, "Confusion (Diverging). Wild moves possible."
    elif sup['state'] == 'WTT' and res['state'] == 'WTB': scenario_id, scenario_desc = 9, "Confusion (Converging). Unpredictable."

    new_logs = []
    try: time_str = timestamp.split(" ")[1]
    except: time_str = timestamp

    if mem['sup_strike'] == 0:
        mem['sup_strike'], mem['res_strike'] = sup['strike'], res['strike']
        mem['sup_state'], mem['res_state'] = sup['state'], res['state']
        new_logs.append({"time": time_str, "eor": f"EOR@{res['strike']} Strong", "eos": f"EOS@{sup['strike']} Strong"})
    else:
        changed = False
        if res['strike'] != mem['res_strike'] or res['state'] != mem['res_state']:
            mem['res_strike'], mem['res_state'] = res['strike'], res['state']
            changed = True
        if sup['strike'] != mem['sup_strike'] or sup['state'] != mem['sup_state']:
            mem['sup_strike'], mem['sup_state'] = sup['strike'], sup['state']
            changed = True
            
        if changed:
            r_str = f"EOR@{res['strike']} {res['state']}" + (f"@{res['target_strike']}" if res['state'] != 'Strong' else "")
            s_str = f"EOS@{sup['strike']} {sup['state']}" + (f"@{sup['target_strike']}" if sup['state'] != 'Strong' else "")
            new_logs.append({"time": time_str, "eor": r_str, "eos": s_str})

    mem['logs'] = new_logs + mem['logs']
    mem['logs'] = mem['logs'][:50]
    
    res_row = next((r for r in chain_rows if r['strike'] == res['strike']), None)
    sup_row = next((r for r in chain_rows if r['strike'] == sup['strike']), None)
    eor_val = res_row['ce_prz'] if res_row else res['strike']
    eos_val = sup_row['pe_prz'] if sup_row else sup['strike']

    return {"scenario_id": scenario_id, "scenario_desc": scenario_desc, "support": sup, "resistance": res, "eos": eor_val, "eor": eos_val, "logs": mem['logs'].copy()}

# ══════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════
@app.route("/process_csv", methods=["POST"])
def process_csv():
    try:
        csv_text = request.data.decode('utf-8')
        lines = csv_text.strip().split('\n')
        if len(lines) <= 1: return jsonify({"error": "Empty or Invalid CSV"}), 400
        
        reader = list(csv.DictReader(lines))
        if not reader: return jsonify({"error": "Failed to parse CSV rows"}), 400

        # Infer step size dynamically from strikes
        strikes = sorted(list(set([float(row.get("Strike", 0)) for row in reader if row.get("Strike")])))
        step = strikes[1] - strikes[0] if len(strikes) > 1 else 50.0

        all_snapshots = {}
        for row in reader:
            ts = row.get("Timestamp")
            if not ts: continue
            
            if ts not in all_snapshots:
                all_snapshots[ts] = {
                    "symbol": row.get("Symbol", "UNKNOWN"),
                    "expiry": row.get("Expiry", ""),
                    "spot": float(row.get("Spot") or 0),
                    "pcr": float(row.get("PCR") or 0),
                    "chain": [],
                    "timestamp": ts
                }
            
            c_vol, p_vol = int(float(row.get("CE_Vol") or 0)), int(float(row.get("PE_Vol") or 0))
            c_oi, p_oi = int(float(row.get("CE_OI") or 0)), int(float(row.get("PE_OI") or 0))
            
            chain_row = {
                "strike": float(row.get("Strike", 0)),
                "ce": {
                    "ltp": float(row.get("CE_LTP") or 0), "oi": c_oi, "change_oi": 0, "volume": c_vol,
                    "iv": float(row.get("CE_IV") or 0), "delta": float(row.get("CE_Delta") or 0),
                    "theta": float(row.get("CE_Theta") or 0), "vega": float(row.get("CE_Vega") or 0), "gamma": float(row.get("CE_Gamma") or 0)
                },
                "pe": {
                    "ltp": float(row.get("PE_LTP") or 0), "oi": p_oi, "change_oi": 0, "volume": p_vol,
                    "iv": float(row.get("PE_IV") or 0), "delta": float(row.get("PE_Delta") or 0),
                    "theta": float(row.get("PE_Theta") or 0), "vega": float(row.get("PE_Vega") or 0), "gamma": float(row.get("PE_Gamma") or 0)
                }
            }
            all_snapshots[ts]["chain"].append(chain_row)
            
        sorted_timestamps = sorted(list(all_snapshots.keys()))
        snapshots_list = [all_snapshots[ts] for ts in sorted_timestamps]
        
        # Sequentially process PRZ and COA so logs build up historically
        coa_memory = {"sup_strike": 0, "res_strike": 0, "sup_state": "", "res_state": "", "logs": []}
        
        for snap in snapshots_list:
            snap["chain"] = inject_prz(snap["chain"], snap["expiry"], step)
            
            # Recalculate Total OI for safety
            snap["total_ce_oi"] = sum(r["ce"]["oi"] for r in snap["chain"])
            snap["total_pe_oi"] = sum(r["pe"]["oi"] for r in snap["chain"])
            
            snap["coa"] = calculate_coa(snap["chain"], snap["timestamp"], coa_memory)
            
        return jsonify({"status": "success", "snapshots": snapshots_list})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    print("\n" + "="*50)
    print(" 🦉 IOC DEDICATED BACKTEST SERVER STARTED")
    print(" Listening on: http://127.0.0.1:5002")
    print("="*50 + "\n")
    app.run(port=5002, debug=False)