import sqlite3
import pandas as pd
import numpy as np
import json
import os
import xgboost as xgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, classification_report
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
XGB_PARAMS = ML_CONFIG['xgb_params']
# ==========================================

def load_and_sort_data():
    print("📥 Loading features from Database...")
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
            
            # Extract timestamp to allow strict chronological sorting
            feature_row['timestamp'] = data.get('timestamp', 0)
            
            try:
                speed = float(data.get('trigger', {}).get('speed_delta', 0))
                feature_row['is_buy'] = 1 if speed < 0 else 0
            except (ValueError, TypeError):
                feature_row['is_buy'] = 0 

            bids, asks = data['dom']['bid_sizes'], data['dom']['ask_sizes']
            total_liq = max(sum(bids) + sum(asks), 1)
            for i, b in enumerate(bids): feature_row[f'bid_norm_{i}'] = b / total_liq
            for i, a in enumerate(asks): feature_row[f'ask_norm_{i}'] = a / total_liq
                
            feature_row['target'] = row['target_label']
            features_list.append(feature_row)
        except Exception as e:
            continue
            
    df = pd.DataFrame(features_list)
    
    # --- STRICT CHRONOLOGICAL SORTING ---
    print("🕒 Sorting timeline to prevent data leakage...")
    df.sort_values('timestamp', inplace=True)
    df.drop(columns=['timestamp'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    return df

def evaluate_model():
    df = load_and_sort_data()
    if df is None or len(df) == 0: 
        print("❌ No data available.")
        return

    cols_to_drop = ['target']
    if 'sma_1m' in df.columns: cols_to_drop.append('sma_1m')
    if 'sma_5m' in df.columns: cols_to_drop.append('sma_5m')
    
    X = df.drop(columns=cols_to_drop)
    y = df['target']

    # --- THE SCRUBBER ---
    X = X.apply(pd.to_numeric, errors='coerce')
    X = X.astype('float32')
    X.replace([np.inf, -np.inf], np.nan, inplace=True)

    # --- 3-WAY CHRONOLOGICAL SPLIT ---
    total_rows = len(df)
    split_1 = int(total_rows * 0.70)
    split_2 = int(total_rows * 0.85)

    # We add a small 10-row gap between splits to ensure the 10-minute forward-looking 
    # horizon of the last training trade doesn't bleed into the first validation trade.
    X_train, y_train = X.iloc[:split_1], y.iloc[:split_1]
    X_val, y_val = X.iloc[split_1 + 10 : split_2], y.iloc[split_1 + 10 : split_2]
    X_test, y_test = X.iloc[split_2 + 10 :], y.iloc[split_2 + 10 :]

    print(f"\n📊 Dataset Split:")
    print(f"   Train: {len(X_train)} trades")
    print(f"   Val:   {len(X_val)} trades")
    print(f"   Test:  {len(X_test)} trades")

    # --- TRAINING WITH EVALUATION SET ---
    print("\n🚀 Training XGBoost Model...")
    
    # We pass the Validation set directly into XGBoost so it can track its own out-of-sample error
    model = xgb.XGBClassifier(**XGB_PARAMS)
    model.fit(
        X_train, y_train,
        eval_set=[(X_train, y_train), (X_val, y_val)],
        verbose=False 
    )

    # --- THE FINAL TRUTH (TEST PHASE) ---
    print("\n==========================================")
    print("🛡️ BLIND TEST SET RESULTS (OUT-OF-SAMPLE)")
    print("==========================================")
    
    # The model predicts the Test set which it has NEVER seen
    preds = model.predict(X_test)
    
    acc = accuracy_score(y_test, preds)
    # Precision is crucial in trading: Out of all trades the AI said "Yes" to, how many were actually winners?
    prec = precision_score(y_test, preds)
    # Recall: Out of all the good trades that existed, how many did the AI catch?
    rec = recall_score(y_test, preds)

    print(f"Target Hit Rate (Accuracy):  {acc * 100:.2f}%")
    print(f"Sniper Accuracy (Precision): {prec * 100:.2f}%")
    print(f"Capture Rate (Recall):       {rec * 100:.2f}%\n")
    
    print("Detailed Classification Report:")
    print(classification_report(y_test, preds, target_names=["Loss/Skip (0)", "Winner (1)"]))

if __name__ == "__main__":
    evaluate_model()