import zmq
import json
import sys
import os
import time
import pandas as pd
import xgboost as xgb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from components.database import Database

# --- CONFIG & AI MODEL ---
CONFIG_FILE = os.path.join(BASE_DIR, "system_config.json")
config = {}
ai_model = None

def load_config():
    global config, ai_model
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)
        
    model_path = os.path.join(BASE_DIR, config.get('ml_pipeline', {}).get('alpha_filter', {}).get('model_save_path', ''))
    if os.path.exists(model_path):
        ai_model = xgb.XGBClassifier()
        ai_model.load_model(model_path)
        print(f"🧠 AI Alpha Filter Loaded from {model_path}")
    else:
        print("⚠️ Warning: AI Model not found. Filtering will be disabled.")

def calculate_dynamic_size(confidence):
    ml_settings = config['ml_pipeline']['alpha_filter']['dynamic_sizing']
    if not ml_settings['enabled']:
        return None 
        
    min_conf = ml_settings['min_confidence_threshold']
    min_vol = ml_settings['min_volume']
    max_vol = ml_settings['max_volume']
    power = ml_settings.get('curve_power', 2.0)
    
    if confidence < min_conf:
        return 0.0  
        
    linear_scale = (confidence - min_conf) / (1.0 - min_conf)
    top_heavy_scale = linear_scale ** power
    volume = min_vol + (top_heavy_scale * (max_vol - min_vol))
    return float(round(volume, 2))

def process_qt_velocity(payload):
    trigger = payload.get('trigger', {})
    
    if ai_model is None:
        speed = trigger.get('speed_delta', 0)
        action = "BUY" if speed < 0 else "SELL"
        return action, {"confidence": 0.0, "speed": speed}, None, False
        
    context_data = payload['context']
    temporal = payload['temporal']
    dom = payload['dom']
    
    action = "BUY" if trigger['speed_delta'] < 0 else "SELL"
    is_buy = 1 if action == "BUY" else 0
    
    bids = dom['bid_sizes']
    asks = dom['ask_sizes']
    total_liq = sum(bids) + sum(asks)
    if total_liq == 0: total_liq = 1
    
    dom_features = {}
    for i, b in enumerate(bids): dom_features[f'bid_norm_{i}'] = b / total_liq
    for i, a in enumerate(asks): dom_features[f'ask_norm_{i}'] = a / total_liq

    feature_dict = {
        **trigger, **context_data, 
        'hour': temporal['hour'], 'day_of_week': temporal['day_of_week'], 'is_buy': is_buy,
        **dom_features
    }
    
    feature_dict.pop('sma_1m', None)
    feature_dict.pop('sma_5m', None)
    
    df_live = pd.DataFrame([feature_dict])
    
    probabilities = ai_model.predict_proba(df_live)
    win_confidence = probabilities[0][1]
    
    volume = calculate_dynamic_size(win_confidence)
    blocked = volume == 0.0
    
    custom_metrics = {
        "confidence": float(win_confidence),
        "speed": trigger.get('speed_delta', 0),
        "absorption": trigger.get('absorption_ratio', 0)
    }
    
    return action, custom_metrics, volume, blocked

def process_qt_trend(payload):
    return "BUY", {}, None, False

def run_ml_brain():
    print("🧠 Starting ML Router Brain (HFT Optimized)...")
    load_config()
    db = Database()
    context = zmq.Context()
    
    receiver_socket = context.socket(zmq.REP)
    receiver_socket.bind("tcp://*:5556")
    
    manager_socket = context.socket(zmq.REQ)
    manager_socket.connect("tcp://localhost:5555")

    print("✅ Listening to Quantower | Connected to MT5 Manager")

    try:
        while True:
            message = receiver_socket.recv_string()
            receiver_socket.send_string("ACK") 
            
            payload = json.loads(message)
            symbol = payload.get('symbol', 'UNKNOWN')
            strategy_id = payload.get('strategy_id', 'UNKNOWN_STRATEGY')
            timestamp = payload.get('timestamp', 0)
            
            if strategy_id == "QT_Velocity":
                action, custom_metrics, volume, blocked = process_qt_velocity(payload)
            elif strategy_id == "QT_Trend":
                action, custom_metrics, volume, blocked = process_qt_trend(payload)
            else:
                continue

            volume = round(float(volume), 2) if volume is not None else 0.0

            # --- INJECT AI VERDICT INTO PAYLOAD FOR RECORD KEEPING ---
            payload['ai_decision'] = {
                "confidence": custom_metrics.get("confidence", 0),
                "blocked": blocked,
                "volume": volume
            }

            if blocked:
                print(f"🚫 BLOCKED by AI | Confidence: {payload['ai_decision']['confidence']*100:.1f}%")
                ml_id = int(time.time() * 1000000)
                db.insert_ml_snapshot(strategy_id, symbol, timestamp, payload, explicit_id=ml_id)
                continue 
                
            print(f"✅ APPROVED by AI | Confidence: {payload['ai_decision']['confidence']*100:.1f}% -> {volume} Lots")

            ml_id = int(time.time() * 1000000) 
            custom_metrics["ml_feature_id"] = ml_id
            
            trade_command = {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "action": action,
                "volume": volume, 
                "extra_metrics": custom_metrics
            }

            manager_socket.send_json(trade_command)
            db.insert_ml_snapshot(strategy_id, symbol, timestamp, payload, explicit_id=ml_id)
            mt5_reply = manager_socket.recv_string()
            print(f"MT5 Reply: {mt5_reply}")

    except KeyboardInterrupt:
        print("\nShutting down ML Brain.")
    finally:
        receiver_socket.close()
        manager_socket.close()
        context.term()

if __name__ == "__main__":
    run_ml_brain()