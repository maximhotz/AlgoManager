import sqlite3
import pandas as pd
import numpy as np
import json
import os
import xgboost as xgb
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ LOAD GLOBAL CONFIGURATION
# ==========================================
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'system_config.json')
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

DB_PATH = os.path.join(os.path.dirname(__file__), '..', config['system']['db_path'])
ML_CONFIG = config['ml_pipeline']['alpha_filter']
JSON_COLUMN_NAME = ML_CONFIG['json_column_name']
MODEL_SAVE_PATH = os.path.join(os.path.dirname(__file__), '..', ML_CONFIG['model_save_path'])
XGB_PARAMS = ML_CONFIG['xgb_params']
# ==========================================

def load_data():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM ml_features WHERE target_label IS NOT NULL"
    df_raw = pd.read_sql(query, conn)
    conn.close()

    features_list = []
    for index, row in df_raw.iterrows():
        try:
            data = json.loads(row[JSON_COLUMN_NAME])
            feature_row = {**data['trigger'], **data['context']}
            feature_row['hour'] = data['temporal']['hour']
            feature_row['day_of_week'] = data['temporal']['day_of_week']
            
            # --- THE FIX: Safe Direction Extraction ---
            # Attempt to get direction from the database column first.
            # If it's missing (a blocked trade), safely calculate it from the JSON.
            if pd.notna(row['trade_action']) and row['trade_action'] != "":
                feature_row['is_buy'] = 1 if row['trade_action'] == 'BUY' else 0
            else:
                try:
                    speed = float(data.get('trigger', {}).get('speed_delta', 0))
                    feature_row['is_buy'] = 1 if speed < 0 else 0
                except (ValueError, TypeError):
                    feature_row['is_buy'] = 0 # Default fallback if speed is toxic

            bids, asks = data['dom']['bid_sizes'], data['dom']['ask_sizes']
            total_liq = max(sum(bids) + sum(asks), 1)
            for i, b in enumerate(bids): feature_row[f'bid_norm_{i}'] = b / total_liq
            for i, a in enumerate(asks): feature_row[f'ask_norm_{i}'] = a / total_liq
                
            feature_row['target'] = row['target_label']
            features_list.append(feature_row)
        except Exception as e:
            continue
            
    return pd.DataFrame(features_list)

def retrain_model():
    print("🔄 Starting Production Retraining Pipeline...")
    df = load_data()
    if df is None or len(df) == 0: 
        print("❌ No data available for retraining.")
        return

    cols_to_drop = ['target']
    if 'sma_1m' in df.columns: cols_to_drop.append('sma_1m')
    if 'sma_5m' in df.columns: cols_to_drop.append('sma_5m')
    
    X = df.drop(columns=cols_to_drop)
    y = df['target']

    # --- THE ULTIMATE BULLETPROOF SCRUBBER ---
    # 1. Force everything to numeric (turns random strings into NaN)
    X = X.apply(pd.to_numeric, errors='coerce')
    
    # 2. Downcast to float32 BEFORE replacing inf. 
    # This forces Python to trigger the exact same overflow XGBoost encounters in C++.
    X = X.astype('float32')
    
    # 3. Crush the newly exposed infinities into standard missing data (NaN)
    X.replace([np.inf, -np.inf], np.nan, inplace=True)

    print(f"📈 Training on 100% of data ({len(X)} trades)...")
    model = xgb.XGBClassifier(**XGB_PARAMS)
    model.fit(X, y)

    print("💾 Overwriting old AI brain...")
    model.save_model(MODEL_SAVE_PATH)
    print(f"✅ V1 Model Upgraded successfully at {MODEL_SAVE_PATH}")

if __name__ == "__main__":
    retrain_model()