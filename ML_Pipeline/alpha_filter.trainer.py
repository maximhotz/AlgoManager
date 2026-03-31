import sqlite3
import pandas as pd
import json
import os
import xgboost as xgb
import matplotlib.pyplot as plt
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# ⚙️ LOAD GLOBAL CONFIGURATION
# ==========================================
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'system_config.json')

try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"❌ Could not find {CONFIG_PATH}.")
    exit(1)

# Extract Config Variables
DB_PATH = os.path.join(os.path.dirname(__file__), '..', config['system']['db_path'])
ML_CONFIG = config['ml_pipeline']['alpha_filter']

JSON_COLUMN_NAME = ML_CONFIG['json_column_name']
MODEL_SAVE_PATH = os.path.join(os.path.dirname(__file__), '..', ML_CONFIG['model_save_path'])
TRAIN_TEST_SPLIT = ML_CONFIG['train_test_split']
XGB_PARAMS = ML_CONFIG['xgb_params']
# ==========================================

def load_and_preprocess_data():
    print(f"🔌 Connecting to Database ({config['system']['db_path']}) and unpacking JSON...")
    conn = sqlite3.connect(DB_PATH)
    
    # Fetch only trades that the Night Shift script has successfully labeled
    query = "SELECT * FROM ml_features WHERE target_label IS NOT NULL ORDER BY timestamp ASC"
    df_raw = pd.read_sql(query, conn)
    conn.close()

    if len(df_raw) == 0:
        print("❌ No labeled data found. Run night_shift_labeler.py first.")
        return None

    features_list = []
    
    for index, row in df_raw.iterrows():
        try:
            # 1. Parse the JSON
            data = json.loads(row[JSON_COLUMN_NAME])
            
            # 2. Extract Trigger & Context
            feature_row = {**data['trigger'], **data['context']}
            
            # 3. Extract Temporal
            feature_row['hour'] = data['temporal']['hour']
            feature_row['day_of_week'] = data['temporal']['day_of_week']
            
            # 4. Extract Trade Direction using the correct schema column!
            feature_row['is_buy'] = 1 if row['trade_action'] == 'BUY' else 0
            
            # 5. DOM NORMALIZATION
            bids = data['dom']['bid_sizes']
            asks = data['dom']['ask_sizes']
            total_liquidity = sum(bids) + sum(asks)
            
            if total_liquidity == 0: total_liquidity = 1 
            
            for i, b in enumerate(bids):
                feature_row[f'bid_norm_{i}'] = b / total_liquidity
            for i, a in enumerate(asks):
                feature_row[f'ask_norm_{i}'] = a / total_liquidity
                
            # 6. Attach the Target Label
            feature_row['target'] = row['target_label']
            
            features_list.append(feature_row)
            
        except Exception as e:
            print(f"⚠️ Error parsing row {row.get('id', 'Unknown')}: {e}")
            continue
            
    df_clean = pd.DataFrame(features_list)
    print(f"✅ Successfully processed {len(df_clean)} labeled trades.")
    return df_clean

def train_alpha_filter():
    df = load_and_preprocess_data()
    if df is None or len(df) == 0: 
        print("❌ DataFrame is empty. Cannot train.")
        return

    # 🚨 PREVENT DATA LEAKAGE: Drop Absolute Prices!
    cols_to_drop = ['target']
    if 'sma_1m' in df.columns: cols_to_drop.append('sma_1m')
    if 'sma_5m' in df.columns: cols_to_drop.append('sma_5m')
    
    X = df.drop(columns=cols_to_drop)
    y = df['target']

    # --- THE CHRONOLOGICAL SPLIT ---
    split_index = int(len(df) * TRAIN_TEST_SPLIT)
    
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
    
    print(f"\n📊 Splitting Data Chronologically ({TRAIN_TEST_SPLIT*100}% Train):")
    print(f"   Train Set: {len(X_train)} trades (Past)")
    print(f"   Test Set:  {len(X_test)} trades (Future)")

    # --- TRAIN THE AI ---
    print("\n🧠 Training XGBoost Alpha Filter...")
    model = xgb.XGBClassifier(**XGB_PARAMS)
    model.fit(X_train, y_train)

    # --- EVALUATE THE AI ---
    print("\n========================================")
    print("📈 FINDING THE GOLDILOCKS THRESHOLD")
    print("========================================")
    
    y_probabilities = model.predict_proba(X_test)[:, 1]
    
    print(f"{'Threshold':<10} | {'Wins Kept':<15} | {'Losses Blocked':<15}")
    print("-" * 45)
    
    for thresh in [0.50, 0.60, 0.65, 0.70, 0.75, 0.80, 0.82, 0.85, 0.90]:
        y_pred = [1 if prob >= thresh else 0 for prob in y_probabilities]
        
        # Calculate True Positives, False Positives, True Negatives, False Negatives
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
        
        # Prevent division by zero if threshold is too extreme
        win_recall = (tp / (tp + fn)) if (tp + fn) > 0 else 0
        loss_recall = (tn / (tn + fp)) if (tn + fp) > 0 else 0
        
        print(f"{thresh*100:>6.1f}%    | {win_recall*100:>12.1f}%  | {loss_recall*100:>13.1f}%")

    # --- TRANSPARENCY: FEATURE IMPORTANCE ---
    print("\n💾 Saving model brain to disk...")
    model.save_model(MODEL_SAVE_PATH)
    print(f"✅ Alpha Filter saved as: {MODEL_SAVE_PATH}")

    print("\n📊 Generating Feature Importance Chart...")
    fig, ax = plt.subplots(figsize=(10, 8)) # Fixed blank figure bug
    xgb.plot_importance(model, max_num_features=15, height=0.5, ax=ax, title="Top 15 Alpha Features")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    train_alpha_filter()