import sqlite3
import pandas as pd
import MetaTrader5 as mt5
import json
import os
from datetime import datetime

# ==========================================
# ⚙️ LOAD CONFIG
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.exists(os.path.join(current_dir, 'system_config.json')):
    BASE_DIR = current_dir
else:
    BASE_DIR = os.path.join(current_dir, '..')

CONFIG_PATH = os.path.join(BASE_DIR, 'system_config.json')

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

DB_PATH = os.path.join(BASE_DIR, config['system']['db_path'])
TARGET_POINTS = config['strategies']['QT_Velocity']['trade_limits']['tp_points']
HORIZON_MINUTES = config['ml_pipeline']['labeling']['horizon_minutes']
SPREAD_ALLOWANCE = config['ml_pipeline']['labeling']['spread_allowance']
BROKER_OFFSET_HOURS = config['system'].get('broker_utc_offset_hours', 3)

SYMBOL = "US500"
# ==========================================

def run_diagnostic():
    print("🔌 Connecting to MetaTrader 5...")
    if not mt5.initialize():
        print(f"❌ MT5 initialization failed.")
        return

    conn = sqlite3.connect(DB_PATH)
    df_features = pd.read_sql("SELECT id, timestamp, symbol, features_json FROM ml_features ORDER BY timestamp ASC", conn)
    conn.close()

    print(f"\n🔍 Scanning for trades on March 25, 2026 using +{BROKER_OFFSET_HOURS} Hour Offset...")
    print("-" * 125)

    count = 0
    for index, row in df_features.iterrows():
        feature_id = row['id']
        
        # 1. Pure UTC Timestamp from C#
        utc_start_timestamp = int(row['timestamp'] / 1000)
        utc_dt = pd.to_datetime(utc_start_timestamp, unit='s')
        
        # Filter for March 25
        if utc_dt.year != 2026 or utc_dt.month != 3 or utc_dt.day != 25:
            continue
            
        count += 1
        
        # 2. Convert to MT5 Broker Time Integer
        broker_start_timestamp = utc_start_timestamp + (BROKER_OFFSET_HOURS * 3600)
        broker_end_timestamp = broker_start_timestamp + (HORIZON_MINUTES * 60)
        broker_time_str = datetime.utcfromtimestamp(broker_start_timestamp).strftime('%H:%M:%S')

        # 3. Strategy Logic (Mean Reversion)
        try:
            payload = json.loads(row['features_json'])
            speed = payload.get('trigger', {}).get('speed_delta', 0)
            action = "BUY" if speed < 0 else "SELL"
        except:
            continue

        # 4. FETCH THE EXACT MILLISECOND TICK (The Fix!)
        ticks = mt5.copy_ticks_range(SYMBOL, broker_start_timestamp, broker_start_timestamp + 60, mt5.COPY_TICKS_ALL)
        if ticks is None or len(ticks) == 0:
            print(f"ID: {feature_id:<4} | {action:<4} | ❌ NO MT5 DATA FOUND")
            continue
            
        exact_bid = ticks[0]['bid']
        exact_ask = ticks[0]['ask']

        # 5. Fetch 10-Minute Candle Horizon
        rates = mt5.copy_rates_range(SYMBOL, mt5.TIMEFRAME_M1, broker_start_timestamp, broker_end_timestamp)
        if rates is None or len(rates) == 0: continue
            
        df_rates = pd.DataFrame(rates)
        window_high = df_rates['high'].max()
        window_low = df_rates['low'].min()

        # 6. Evaluate Logic with Exact Tick Prices
        is_win = 0
        target_price = 0
        entry_price = 0
        
        if action == "BUY":
            entry_price = exact_ask
            target_price = entry_price + TARGET_POINTS
            if window_high >= target_price:
                is_win = 1
                
        elif action == "SELL":
            entry_price = exact_bid
            target_price = entry_price - TARGET_POINTS - SPREAD_ALLOWANCE
            if window_low <= target_price:
                is_win = 1

        # 7. Print the result
        result_str = "✅ 1 (WIN)" if is_win else "💀 0 (LOSS)"
        print(f"ID: {feature_id:<4} | TIME: {broker_time_str} | {action:<4} | ENTRY: {entry_price:<7.2f} | TARGET: {target_price:<7.2f} | HIGH: {window_high:<7.2f} | LOW: {window_low:<7.2f} | {result_str}")

    print("-" * 125)
    mt5.shutdown()

if __name__ == "__main__":
    run_diagnostic()