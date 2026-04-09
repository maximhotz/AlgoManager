import os
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import pickle

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR) if "Regime_Filter" in BASE_DIR else BASE_DIR
CONFIG_PATH = os.path.join(ROOT_DIR, "system_config.json")

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

rf_cfg = config['ml_pipeline']['rf_classifier']
WINDOW = config['ml_pipeline']['hmm_regime']['features_window']
MODEL_SAVE_PATH = os.path.join(ROOT_DIR, rf_cfg['model_save_path'])

def engineer_soldier_features(df):
    of_cols = ['Volume', 'Delta', 'Average buy size', 'Average sell size']
    for col in of_cols:
        if df[col].dtype == object:
            df[col] = df[col].str.replace(',', '.').astype(float)
        else:
            df[col] = df[col].astype(float)

    df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
    df['Variance'] = df['Log_Return'].rolling(window=WINDOW).std()
    df['CumDelta'] = df['Delta'].cumsum()
    df['Delta_Slope'] = (df['CumDelta'] - df['CumDelta'].shift(WINDOW)) / WINDOW
    
    MACRO_WINDOW = 240
    df['Macro_SMA'] = df['Close'].rolling(window=MACRO_WINDOW).mean()
    df['Macro_Distance'] = (df['Close'] - df['Macro_SMA']) / df['Macro_SMA']
    
    hours = df.index.hour
    df['Hour_Sin'] = np.sin(2 * np.pi * hours / 24)
    df['Hour_Cos'] = np.cos(2 * np.pi * hours / 24)
    
    df['RVOL'] = df['Volume'] / df['Volume'].rolling(window=WINDOW).mean()
    df['Size_Imbalance'] = df['Average buy size'] - df['Average sell size']
    df['Delta_Percent'] = np.where(df['Volume'] > 0, df['Delta'] / df['Volume'], 0)
    
    features = rf_cfg['features']
    df = df.dropna(subset=features + ['Action_State'])
    
    return df

def execute_production_soldier():
    print("Loading 100% full dataset...")
    df = pd.read_csv(os.path.join(BASE_DIR, "labels_full.csv"), index_col='DateTime', parse_dates=True)
    df = engineer_soldier_features(df)

    features = rf_cfg['features']
    X_full, y_full = df[features].values, df['Action_State'].values

    print("Training Production Random Forest Classifier...")
    model = RandomForestClassifier(
        n_estimators=rf_cfg['n_estimators'],
        max_depth=rf_cfg['max_depth'],
        random_state=rf_cfg['random_state'],
        n_jobs=-1,
        class_weight='balanced'
    )
    
    model.fit(X_full, y_full)
    
    os.makedirs(os.path.dirname(MODEL_SAVE_PATH), exist_ok=True)
    with open(MODEL_SAVE_PATH, "wb") as file:
        pickle.dump(model, file)

    print(f"✅ Production Soldier Complete. Model locked and saved to: {MODEL_SAVE_PATH}")

if __name__ == "__main__":
    execute_production_soldier()