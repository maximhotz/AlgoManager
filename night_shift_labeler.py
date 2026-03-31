import sqlite3
import pandas as pd
import MetaTrader5 as mt5
import json
import os

# ==========================================
# ⚙️ LOAD GLOBAL CONFIGURATION
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.exists(os.path.join(current_dir, 'system_config.json')):
    BASE_DIR = current_dir
else:
    BASE_DIR = os.path.join(current_dir, '..')

CONFIG_PATH = os.path.join(BASE_DIR, 'system_config.json')

try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"❌ Could not find {CONFIG_PATH}.")
    exit(1)

DB_PATH = os.path.join(BASE_DIR, config['system']['db_path'])
TARGET_POINTS = config['strategies']['QT_Velocity']['trade_limits']['tp_points']
HORIZON_MINUTES = config['ml_pipeline']['labeling']['horizon_minutes']
SPREAD_ALLOWANCE = config['ml_pipeline']['labeling']['spread_allowance']
BROKER_OFFSET_HOURS = config['system'].get('broker_utc_offset_hours', 3)

SYMBOL_MAP = {
    "ES.M26": "US500" 
}
# ==========================================

def initialize_mt5():
    print("🔌 Connecting to MetaTrader 5...")
    if not mt5.initialize():
        print(f"❌ MT5 initialization failed. Error code: {mt5.last_error()}")
        return False
    return True

def run_night_shift():
    conn = sqlite3.connect(DB_PATH, timeout=15.0)
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE ml_features ADD COLUMN target_label INTEGER")
        conn.commit()
    except sqlite3.OperationalError:
        pass 

    try:
        df_features = pd.read_sql("SELECT id FROM ml_features", conn)
        df_unlabeled = pd.read_sql("SELECT id FROM ml_features WHERE target_label IS NULL", conn)
        
        print("\n" + "="*40)
        print("📊 DATABASE HEALTH CHECK")
        print("="*40)
        print(f"Total ML Snapshots Harvested: {len(df_features)}")
        print(f"Snapshots awaiting labels:    {len(df_unlabeled)}")
        print("="*40 + "\n")
        
    except Exception as e:
        print(f"Database read error: {e}")
        return

    if len(df_unlabeled) == 0:
        print("🎉 All data is already labeled! Sleeping until tomorrow.")
        return

    if not initialize_mt5(): return

    query = """
        SELECT id, timestamp, symbol, features_json 
        FROM ml_features 
        WHERE target_label IS NULL
    """
    
    try:
        unlabeled_rows = pd.read_sql(query, conn)
    except Exception as e:
        print(f"⚠️ Could not read ml_features: {e}")
        return

    print(f"🔍 Shadow-Labeling {len(unlabeled_rows)} signals using exact tick entries...\n")

    labeled_wins = 0
    labeled_losses = 0

    for index, row in unlabeled_rows.iterrows():
        feature_id = row['id']
        qt_symbol = row['symbol']
        symbol = SYMBOL_MAP.get(qt_symbol, qt_symbol) 
        
        # 1. Pure UTC Timestamp from C#
        timestamp_ms = row['timestamp']
        utc_start_timestamp = int(timestamp_ms / 1000)
        
        # 2. Convert to MT5 Broker Time Integer
        broker_start_timestamp = utc_start_timestamp + (BROKER_OFFSET_HOURS * 3600)
        broker_end_timestamp = broker_start_timestamp + (HORIZON_MINUTES * 60)
        
        # 3. Strategy Logic (Mean Reversion)
        try:
            payload = json.loads(row['features_json'])
            speed = payload.get('trigger', {}).get('speed_delta', 0)
            action = "BUY" if speed < 0 else "SELL"
        except Exception as e:
            print(f"⚠️ JSON parsing error for ID {feature_id}: {e}")
            continue

        if not mt5.symbol_select(symbol, True):
            print(f"⚠️ Could not select symbol {symbol} in MT5.")
            continue

        # 4. FETCH THE EXACT MILLISECOND TICK FOR PRECISE ENTRY
        ticks = mt5.copy_ticks_range(symbol, broker_start_timestamp, broker_start_timestamp + 60, mt5.COPY_TICKS_ALL)
        if ticks is None or len(ticks) == 0:
            continue
            
        exact_bid = ticks[0]['bid']
        exact_ask = ticks[0]['ask']

        # 5. Fetch 10-Minute Candle Horizon for the outcome
        rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M1, broker_start_timestamp, broker_end_timestamp)
        if rates is None or len(rates) == 0:
            continue

        df_rates = pd.DataFrame(rates)
        window_high = df_rates['high'].max()
        window_low = df_rates['low'].min()

        # 6. Evaluate Logic with Exact Tick Prices
        is_win = 0
        if action == "BUY":
            target_price = exact_ask + TARGET_POINTS
            if window_high >= target_price:
                is_win = 1
        elif action == "SELL":
            target_price = exact_bid - TARGET_POINTS - SPREAD_ALLOWANCE
            if window_low <= target_price:
                is_win = 1

        # 7. Commit to DB
        cursor.execute("UPDATE ml_features SET target_label = ? WHERE id = ?", (is_win, feature_id))
        
        if is_win == 1:
            labeled_wins += 1
        else:
            labeled_losses += 1

    conn.commit()
    conn.close()
    mt5.shutdown()

    print("✅ BATCH SHADOW-LABELING COMPLETE")
    
    total_labeled = labeled_wins + labeled_losses
    if total_labeled > 0:
        print(f"🏆 Wins Found:   {labeled_wins}")
        print(f"💀 Losses Found: {labeled_losses}")
        print(f"Win Rate: {(labeled_wins / total_labeled * 100):.2f}%")
    else:
        print("No valid MT5 candle data could be processed. (Market closed?)")

if __name__ == "__main__":
    run_night_shift()