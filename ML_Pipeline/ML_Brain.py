import zmq
import json
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from components.database import Database

def process_qt_velocity(payload):
    """Specific logic for the QT_Velocity mean-reversion strategy"""
    speed = payload.get('trigger', {}).get('speed_delta', 0)
    absorption = payload.get('trigger', {}).get('absorption_ratio', 0)
    
    # Logic: Mean Reversion (Fade the spike)
    action = "BUY" if speed < 0 else "SELL"
    
    return action, {"speed": speed, "absorption": absorption}

def process_qt_trend(payload):
    """Example of a future strategy you might build"""
    # Logic: Breakout Continuation
    action = "BUY" # ... insert logic here
    return action, {}

def run_ml_brain():
    print("🧠 Starting ML Router Brain...")
    db = Database()
    context = zmq.Context()
    
    receiver_socket = context.socket(zmq.REP)
    receiver_socket.bind("tcp://*:5556")
    
    manager_socket = context.socket(zmq.REQ)
    manager_socket.connect("tcp://localhost:5555")

    print("✅ Listening to Quantower on port 5556 | Connected to MT5 Manager on port 5555")

    try:
        while True:
            message = receiver_socket.recv_string()
            payload = json.loads(message)
            
            # 1. Identify the Source Strategy
            symbol = payload.get('symbol', 'UNKNOWN')
            strategy_id = payload.get('strategy_id', 'UNKNOWN_STRATEGY')
            timestamp = payload.get('timestamp', 0)
            
            print(f"\n📡 SIGNAL RECEIVED: [{strategy_id}] on {symbol}")

            # 2. Save generic payload to DB
            ml_id = db.insert_ml_snapshot(strategy_id, symbol, timestamp, payload)

            # Unlock Quantower immediately
            receiver_socket.send_string("ACK")

            # 3. Route to the correct Strategy Logic
            if strategy_id == "QT_Velocity":
                action, custom_metrics = process_qt_velocity(payload)
            elif strategy_id == "QT_Trend":
                action, custom_metrics = process_qt_trend(payload)
            else:
                print(f"⚠️ Warning: No logic found for {strategy_id}. Ignoring.")
                continue

            # 4. Build and Fire Trade Command
            custom_metrics["ml_feature_id"] = ml_id # Attach DB ID for the loop-closer
            
            trade_command = {
                "strategy_id": strategy_id,
                "symbol": symbol,
                "action": action,
                "extra_metrics": custom_metrics
            }

            print(f"⚡ Firing {action} command to MT5 Manager...")
            manager_socket.send_json(trade_command)
            print(f"MT5 Reply: {manager_socket.recv_string()}")

    except KeyboardInterrupt:
        print("\nShutting down ML Brain.")
    finally:
        receiver_socket.close()
        manager_socket.close()
        context.term()

if __name__ == "__main__":
    run_ml_brain()