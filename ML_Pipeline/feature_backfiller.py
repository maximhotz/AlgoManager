import sqlite3
import pandas as pd
import MetaTrader5 as mt5
import json
import os
import numpy as np
from datetime import datetime

# ==========================================
# ⚙️ LOAD CONFIGURATION
# ==========================================
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'system_config.json')
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', config['system']['db_path'])
JSON_COL = config['ml_pipeline']['alpha_filter']['json_column_name']

SYMBOL_MAP = {"ES.M26": "US500"}

def initialize_mt5():
    print("🔌 Connecting to MetaTrader 5 for Backfilling...")
    if not mt5.initialize():
        print(f"❌ MT5 initialization failed.")
        return False
    return True

def calculate_atr(high, low, close, period=14):
    tr = np.maximum(high - low, np.maximum(abs(high - close.shift(1)), abs(low - close.shift(1))))
    return tr.rolling(period).mean().iloc[-1]

def backfill_features():
    if not initialize_mt5(): return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # THE FIX: Join with 'trades' to get the exact Broker Server Time
    query = f"""
        SELECT f.id, f.{JSON_COL}, f.symbol, t.open_time 
        FROM ml_features f
        JOIN trades t ON f.id = t.ml_feature_id
    """
    df_features = pd.read_sql(query, conn)
    print(f"🔍 Found {len(df_features)} executed trades to check for backfilling...")

    updated_count = 0

    for index, row in df_features.iterrows():
        try:
            # 1. Parse existing JSON
            data = json.loads(row[JSON_COL])
            
            # Skip if we already backfilled this row!
            if 'sma_1h_dist_pct' in data['context']:
                continue

            qt_symbol = row['symbol']
            symbol = SYMBOL_MAP.get(qt_symbol, qt_symbol)
            
            # THE TIMEZONE FIX: Use the MT5 Broker Time string directly
            open_time_str = row['open_time']
            db_time = datetime.strptime(open_time_str, "%Y-%m-%d %H:%M:%S")
            mt5_timestamp = int(db_time.timestamp())

            # THE BASIS FIX: Get the exact MT5 CFD price for this exact minute
            m1_rates = mt5.copy_rates_from(symbol, mt5.TIMEFRAME_M1, mt5_timestamp, 1)
            if m1_rates is None or len(m1_rates) == 0: continue
            current_cfd_price = m1_rates[0]['close']

            # 2. PULL HISTORICAL H1 CANDLES (Broker Time)
            h1_rates = mt5.copy_rates_from(symbol, mt5.TIMEFRAME_H1, mt5_timestamp, 24)
            if h1_rates is None or len(h1_rates) < 20: continue
            
            df_h1 = pd.DataFrame(h1_rates)
            
            # Calculate 1H SMA and Distance using purely CFD pricing
            sma_1h = df_h1['close'].rolling(20).mean().iloc[-1]
            sma_1h_dist_pct = ((current_cfd_price - sma_1h) / sma_1h) * 100
            
            # Calculate 1H ATR (14-period)
            atr_1h = calculate_atr(df_h1['high'], df_h1['low'], df_h1['close'])

            # 3. PULL HISTORICAL D1 CANDLES (Broker Time)
            d1_rates = mt5.copy_rates_from(symbol, mt5.TIMEFRAME_D1, mt5_timestamp, 1)
            if d1_rates is None or len(d1_rates) == 0: continue
            
            daily_open = d1_rates[0]['open']
            daily_open_dist_pct = ((current_cfd_price - daily_open) / daily_open) * 100

            # 4. INJECT THE NEW FEATURES INTO THE JSON
            data['context']['sma_1h_dist_pct'] = round(sma_1h_dist_pct, 4)
            data['context']['daily_open_dist_pct'] = round(daily_open_dist_pct, 4)
            data['context']['atr_1h'] = round(atr_1h, 4)

            # 5. UPDATE THE DATABASE
            new_json_string = json.dumps(data)
            cursor.execute(f"UPDATE ml_features SET {JSON_COL} = ? WHERE id = ?", (new_json_string, row['id']))
            updated_count += 1

        except Exception as e:
            print(f"⚠️ Error processing row {row['id']}: {e}")
            continue

    conn.commit()
    conn.close()
    mt5.shutdown()

    print(f"✅ BATCH BACKFILL COMPLETE. Upgraded {updated_count} trades with Macro Features!")

if __name__ == "__main__":
    backfill_features()