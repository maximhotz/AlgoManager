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
    
    # --- FIXED: ID-Based Hard Join ---
    # We ignore 'target_label' and calculate the true win/loss from the actual trades table
    trades_query = "SELECT ml_feature_id, pnl FROM trades WHERE ml_feature_id IS NOT NULL"
    df_trades = pd.read_sql(trades_query, conn)
    
    ml_query = "SELECT id, features_json FROM ml_features"
    df_ml = pd.read_sql(ml_query, conn)
    conn.close()

    if df_trades.empty or df_ml.empty:
        return None

    # This Inner Join is 100% immune to Timezone bugs
    df_merged = pd.merge(df_trades, df_ml, left_on='ml_feature_id', right_on='id', how='inner')
    
    # Calculate truth target: 1 if trade made money, 0 if it lost
    df_merged['target'] = (df_merged['pnl'] > 0).astype(int)

    features_list = []
    for index, row in df_merged.iterrows():
        try:
            data = json.loads(row[JSON_COLUMN_NAME])
            feature_row = {**data.get('trigger', {}), **data.get('context', {})}
            feature_row['hour'] = data.get('temporal', {}).get('hour', 0)
            feature_row['day_of_week'] = data.get('temporal', {}).get('day_of_week', 0)
            
            try:
                speed = float(data.get('trigger', {}).get('speed_delta', 0))
                feature_row['is_buy'] = 1 if speed < 0 else 0
            except (ValueError, TypeError):
                feature_row['is_buy'] = 0 

            bids = data.get('dom', {}).get('bid_sizes', [])
            asks = data.get('dom', {}).get('ask_sizes', [])
            total_liq = max(sum(bids) + sum(asks), 1)
            
            for i, b in enumerate(bids): feature_row[f'bid_norm_{i}'] = b / total_liq
            for i, a in enumerate(asks): feature_row[f'ask_norm_{i}'] = a / total_liq
                
            feature_row['target'] = row['target']
            features_list.append(feature_row)
        except Exception as e:
            print(f"Row {row.get('id', 'Unknown')} failed parsing: {e}")
            continue
            
    return pd.DataFrame(features_list)

def retrain_model():
    print("🔄 Starting Production Retraining Pipeline...")
    df = load_data()
    if df is None or len(df) == 0: 
        print("❌ No fully mapped data available for retraining.")
        return

    cols_to_drop = ['target']
    if 'sma_1m' in df.columns: cols_to_drop.append('sma_1m')
    if 'sma_5m' in df.columns: cols_to_drop.append('sma_5m')
    
    # Ensure columns exist before dropping
    cols_to_drop = [c for c in cols_to_drop if c in df.columns]
    
    X = df.drop(columns=cols_to_drop)
    y = df['target']

    # --- THE ULTIMATE BULLETPROOF SCRUBBER ---
    X = X.apply(pd.to_numeric, errors='coerce')
    X = X.astype('float32')
    X.replace([np.inf, -np.inf], np.nan, inplace=True)

    print(f"📈 Training on {len(X)} perfectly mapped truth trades...")
    model = xgb.XGBClassifier(**XGB_PARAMS)
    model.fit(X, y)

    print("💾 Overwriting old AI brain...")
    model.save_model(MODEL_SAVE_PATH)
    print(f"✅ V2 Model Upgraded successfully at {MODEL_SAVE_PATH}")

if __name__ == "__main__":
    retrain_model()