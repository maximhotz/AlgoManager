import os
import sys
import json
import zmq
import pandas as pd
import numpy as np
import pickle
from datetime import datetime

# --- CONFIG ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR) if "Regime_Filter" in BASE_DIR else BASE_DIR
CONFIG_FILE = os.path.join(ROOT_DIR, "system_config.json")

# Ensure Python can find the 'components' folder at the root
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from components.database import Database

with open(CONFIG_FILE, "r") as f:
    config = json.load(f)

REGIME_PORT = config['system'].get('zmq_regime_port', 5557)
RF_CFG = config['ml_pipeline']['rf_classifier']
WINDOW = config['ml_pipeline']['hmm_regime']['features_window']
MODEL_PATH = os.path.join(ROOT_DIR, RF_CFG['model_save_path'])
MACRO_WINDOW = 240

# Build dynamic inverse mapping for the console logging
regime_map = config['ml_pipeline']['regime_mapping']
INVERSE_MAP = {
    regime_map['bull_longs_only']: "LONG_ONLY (Bull Trend)",
    regime_map['chop_bidirectional']: "CHOP_BIDIRECTIONAL",
    regime_map['bear_shorts_only']: "SHORT_ONLY (Bear Trend)"
}

def engineer_live_features(df):
    for col in ['Close', 'Volume', 'Delta', 'Average buy size', 'Average sell size']:
        df[col] = df[col].astype(float)
        
    df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
    df['Variance'] = df['Log_Return'].rolling(window=WINDOW).std()
    df['CumDelta'] = df['Delta'].cumsum()
    df['Delta_Slope'] = (df['CumDelta'] - df['CumDelta'].shift(WINDOW)) / WINDOW
    
    df['Macro_SMA'] = df['Close'].rolling(window=MACRO_WINDOW).mean()
    df['Macro_Distance'] = (df['Close'] - df['Macro_SMA']) / df['Macro_SMA']
    
    hours = df.index.hour
    df['Hour_Sin'] = np.sin(2 * np.pi * hours / 24)
    df['Hour_Cos'] = np.cos(2 * np.pi * hours / 24)
    
    df['RVOL'] = df['Volume'] / df['Volume'].rolling(window=WINDOW).mean()
    df['Size_Imbalance'] = df['Average buy size'] - df['Average sell size']
    df['Delta_Percent'] = np.where(df['Volume'] > 0, df['Delta'] / df['Volume'], 0)
    
    return df

def run_rf_watchtower():
    print("🔭 Starting ML Context Watchtower...")
    
    if not os.path.exists(MODEL_PATH):
        print(f"❌ Error: Model not found at {MODEL_PATH}")
        return

    with open(MODEL_PATH, "rb") as file:
        model = pickle.load(file)
    print(f"✅ Random Forest Loaded from {MODEL_PATH}.")
    print(f"📡 Listening for Quantower on TCP Port {REGIME_PORT}...")

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{REGIME_PORT}")

    features_list = RF_CFG['features']
    prediction_history = []

    try:
        while True:
            message = socket.recv_json() 
            
            try:
                df = pd.DataFrame(message['data'])
                df['DateTime'] = pd.to_datetime(df['DateTime'])
                df.set_index('DateTime', inplace=True)
                
                if len(df) < MACRO_WINDOW:
                    socket.send_json({"status": "error", "message": f"Need {MACRO_WINDOW} bars, got {len(df)}."})
                    continue

                df = engineer_live_features(df)
                live_row = df.iloc[-1:]
                
                if live_row[features_list].isnull().values.any():
                    socket.send_json({"status": "error", "message": "NaNs in live features. Check data payload length."})
                    continue

                X_live = live_row[features_list].values
                raw_pred = int(model.predict(X_live)[0])
                
                prediction_history.append(raw_pred)
                if len(prediction_history) > 3:
                    prediction_history.pop(0)
                    
                smoothed_pred = int(pd.Series(prediction_history).mode()[0])
                regime_name = INVERSE_MAP.get(smoothed_pred, "UNKNOWN_REGIME")

                print(f"[{datetime.now().strftime('%H:%M:%S')}] 👁️ RF Saw: {raw_pred} | Broadcast: {smoothed_pred} ({regime_name})")
                
                socket.send_json({
                    "signal": smoothed_pred, 
                    "raw_prediction": raw_pred,
                    "regime": regime_name, 
                    "status": "success"
                })

                # --- NEW: Log Regime to Database natively ---
                try:
                    db = Database()
                    db.log_regime(datetime.now().timestamp(), smoothed_pred, regime_name)
                except Exception as e:
                    print(f"Failed to log regime to DB: {e}")

            except Exception as e:
                socket.send_json({"status": "error", "message": str(e)})

    except KeyboardInterrupt:
        print("\nShutting down Watchtower.")
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    run_rf_watchtower()