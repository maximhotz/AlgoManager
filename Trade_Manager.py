import zmq
import MetaTrader5 as mt5
import json
import os
import time
import sys
import traceback
from datetime import datetime

# --- DATABASE INTEGRATION ---
from components.database import Database

# --- CONFIG ---
CONFIG_FILE = "system_config.json"
last_config_mtime = 0
config = {} 
last_snapshot_time = 0
SNAPSHOT_INTERVAL = 60

# --- INTERNAL STATE ---
tracked_tickets = {} # Maps Ticket -> Strategy ID
trade_metadata = {}  # Maps Ticket -> {Speed, RSI, etc.}
system_locked = False 
basket_start_equity = None # The Equity Watermark Anchor

# Initialize Database
db = Database()

def get_file_mtime(filepath):
    if os.path.exists(filepath):
        return os.path.getmtime(filepath)
    return 0

def load_config():
    global last_config_mtime, config
    
    if not os.path.exists(CONFIG_FILE):
        print("Manager: Config file missing!")
        return False
        
    current_mtime = get_file_mtime(CONFIG_FILE)
    
    if current_mtime > last_config_mtime:
        try:
            with open(CONFIG_FILE, "r") as f:
                new_config = json.load(f)
                if 'system' in new_config and 'strategies' in new_config:
                    config = new_config
                    last_config_mtime = current_mtime
                    print("Manager: Configuration Loaded.")
                    return True
                else:
                    print("Manager: Config file is incomplete.")
        except Exception as e:
            print(f"Manager: Config Read Error: {e}")
            return False
            
    return bool(config)

def connect_mt5():
    if not config:
        if not load_config(): return False

    sys_conf = config.get('system', {})
    path = sys_conf.get('mt5_terminal_path')
    expected_account = sys_conf.get('authorized_account_number')
    
    if path and os.path.exists(path):
        if not mt5.initialize(path=path):
            print(f"Manager: MT5 Init Failed: {mt5.last_error()}")
            return False
    else:
        if not mt5.initialize():
            print(f"Manager: MT5 Default Init Failed: {mt5.last_error()}")
            return False

    current_info = mt5.account_info()
    if current_info is None:
        print("Manager: Failed to get Account Info")
        return False
        
    if expected_account and current_info.login != expected_account:
        print(f"!!! DANGER: Connected to Wrong Account ({current_info.login}) !!!")
        mt5.shutdown()
        return False

    print(f"Manager: Connected to Account {current_info.login}")
    return True

def sync_positions_on_startup():
    if not config: load_config()
    strategies = config.get('strategies', {})
    magic_map = {v['magic_number']: k for k, v in strategies.items()}
    
    positions = mt5.positions_get()
    count = 0
    if positions:
        for pos in positions:
            if pos.magic in magic_map:
                strat_id = magic_map[pos.magic]
                tracked_tickets[pos.ticket] = strat_id
                count += 1
    
    print(f"Manager: Synced {count} existing positions.")

def record_equity_snapshot():
    global last_snapshot_time
    if time.time() - last_snapshot_time < SNAPSHOT_INTERVAL:
        return

    acc = mt5.account_info()
    if not acc: return
    
    positions = mt5.positions_get()
    count = len(positions) if positions else 0
    
    # --- CALCULATE STRATEGY BREAKDOWN ---
    strategies = config.get('strategies', {})
    magic_map = {v['magic_number']: k for k, v in strategies.items()}
    
    strat_pl = {k: 0.0 for k in strategies.keys()} # Default to 0.0 for all known strategies
    
    if positions:
        for pos in positions:
            # Map Magic Number -> Strategy ID
            s_id = magic_map.get(pos.magic, "Manual/Other")
            
            # Accumulate Floating P/L (Profit + Swap)
            current_val = strat_pl.get(s_id, 0.0)
            strat_pl[s_id] = current_val + pos.profit + pos.swap
            
    # Save to DB (Passing strat_pl dict)
    db.log_equity_snapshot(acc.balance, acc.equity, count, strat_pl)
    
    last_snapshot_time = time.time()

def check_closed_trades():
    live_positions = mt5.positions_get()
    if live_positions is None: return 
    live_ticket_ids = {p.ticket for p in live_positions}
    
    missing_tickets = [t for t in tracked_tickets.keys() if t not in live_ticket_ids]
    
    for ticket in missing_tickets:
        strat_id = tracked_tickets[ticket]
        
        deals = mt5.history_deals_get(position=ticket)
        
        if deals is None or len(deals) == 0:
            continue
            
        entry_deal = next((d for d in deals if d.entry == mt5.DEAL_ENTRY_IN), None)
        exit_deal = next((d for d in deals if d.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]), None)
        
        if exit_deal:
            meta = trade_metadata.get(ticket, {})
            
            net_pl = exit_deal.profit + exit_deal.swap + exit_deal.commission
            duration = 0
            open_price = 0
            open_time = datetime.fromtimestamp(exit_deal.time)
            
            if entry_deal:
                duration = exit_deal.time - entry_deal.time
                open_price = entry_deal.price
                open_time = datetime.fromtimestamp(entry_deal.time)

            reason = "Unknown"
            if exit_deal.reason == mt5.DEAL_REASON_CLIENT: reason = "Manual Close"
            elif exit_deal.reason == mt5.DEAL_REASON_SL: reason = "Stop Loss"
            elif exit_deal.reason == mt5.DEAL_REASON_TP: reason = "Take Profit"

            print(f"💰 Closed: {strat_id} | ${net_pl:.2f} | {reason}")
            
            trade_record = {
                "ticket": ticket,
                "strategy_id": strat_id,
                "symbol": exit_deal.symbol,
                "action": "BUY" if entry_deal and entry_deal.type == mt5.DEAL_TYPE_BUY else "SELL",
                "open_time": open_time,
                "close_time": datetime.fromtimestamp(exit_deal.time),
                "duration": duration,
                "open_price": open_price,
                "close_price": exit_deal.price,
                "net_pnl": round(net_pl, 2),
                "commission": exit_deal.commission,
                "swap": exit_deal.swap,
                "reason": reason,
                "extra_metrics": meta 
            }
            
            db.log_trade(trade_record)
            
            del tracked_tickets[ticket]
            if ticket in trade_metadata: del trade_metadata[ticket]

def close_all_positions(reason="Global Basket Trigger"):
    positions = mt5.positions_get()
    if positions is None or len(positions) == 0:
        return

    print(f"\n--- CLOSING ALL ({reason}) ---")
    for pos in positions:
        tick = mt5.symbol_info_tick(pos.symbol)
        if not tick: continue
        
        type_close = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        price = tick.bid if type_close == mt5.ORDER_TYPE_SELL else tick.ask
        
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "position": pos.ticket,
            "symbol": pos.symbol,
            "volume": pos.volume,
            "type": type_close,
            "price": price,
            "magic": pos.magic,
            "comment": "Basket Close",
        }
        mt5.order_send(request)

def check_basket_logic():
    global system_locked, basket_start_equity
    
    if system_locked: return

    load_config() 
    risk = config.get('risk_management', {})
    
    if not risk.get('basket_enabled', False):
        basket_start_equity = None # Reset anchor if user turns off basket mode mid-flight
        return

    acc = mt5.account_info()
    if acc is None: return

    # --- THE FLAT ACCOUNT RESET ---
    positions = mt5.positions_get()
    if positions is None or len(positions) == 0:
        if basket_start_equity is not None:
            print("Manager: Account is flat. Wiping old Basket Anchor...")
            basket_start_equity = None
        return # Stop checking basket math, there are no open trades!

    current_equity = acc.equity

    # Set the Anchor Point (Watermark) ONLY when the first trade of a new basket opens
    if basket_start_equity is None:
        basket_start_equity = current_equity
        print(f"Manager: 🎯 New Basket Started. Anchor Equity: ${basket_start_equity:.2f}")

    # Grab target from config
    tp_limit = risk.get('basket_take_profit_usd')

    # Check Take Profit
    if tp_limit and tp_limit > 0:
        target_amount = basket_start_equity + tp_limit
        if current_equity >= target_amount:
            print(f"\n!!! BASKET TP HIT (Equity: ${current_equity:.2f} >= Target: ${target_amount:.2f}) !!!")
            close_all_positions(reason="Equity Target Reached")
            
            # Reset the anchor to None. 
            basket_start_equity = None
            print("🔄 BASKET RESET: Waiting for trades to clear before setting new anchor...\n")

def execute_trade(signal_data):
    if system_locked: return "Manager: REJECTED (System Locked)"

    load_config()
    strategies = config.get('strategies', {})
    
    strat_id = signal_data['strategy_id']
    if strat_id not in strategies: return "Manager: Unknown Strategy"
    
    settings = strategies[strat_id]
    if not settings['enabled']: return "Manager: Strategy Disabled"
    
    symbol = signal_data['symbol']
    action = signal_data['action']
    magic = settings['magic_number']
    
    if 'volume' in signal_data and float(signal_data['volume']) > 0:
        volume = float(signal_data['volume'])
    else:
        volume = settings['volume']
    
    limits = settings.get('trade_limits', {})
    sl_points = limits.get('sl_points', 0)
    
    if 'dynamic_tp' in signal_data:
        tp_points = float(signal_data['dynamic_tp'])
    else:
        tp_points = limits.get('tp_points', 1.0)
    
    tick = mt5.symbol_info_tick(symbol)
    if not tick: return "Manager: No Data"
    
    order_type = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL
    price = tick.ask if action == "BUY" else tick.bid
    
    sl_price, tp_price = 0.0, 0.0
    
    if sl_points > 0:
        sl_price = price - sl_points if action == "BUY" else price + sl_points
            
    if tp_points > 0:
        tp_price = price + tp_points if action == "BUY" else price - tp_points
            
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": price,
        "sl": sl_price,
        "tp": tp_price,
        "magic": magic,
        "comment": strat_id,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        return f"Manager: Failed ({result.comment})"
    
    tracked_tickets[result.order] = strat_id
    
    if 'extra_metrics' in signal_data:
        trade_metadata[result.order] = signal_data['extra_metrics']
        
    return f"Manager: OPENED {action} (Ticket: {result.order}) | Vol: {volume}"

def run_manager():
    if not load_config(): return

    sys_conf = config.get('system', {})
    zmq_port = sys_conf.get('zmq_port', 5555)

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    
    try:
        socket.bind(f"tcp://*:{zmq_port}")
    except zmq.ZMQError as e:
        print(f"CRITICAL: Could not bind to port {zmq_port}. Is Manager already running?")
        print(f"Error: {e}")
        return

    socket.setsockopt(zmq.RCVTIMEO, 100) 
    
    if not connect_mt5(): return

    print(f"--- Manager Listening on Port {zmq_port} ---")
    
    db.initialize()
    sync_positions_on_startup()

    while True:
        try:
            try:
                msg = socket.recv_json(flags=zmq.NOBLOCK)
                print(f"Manager: Signal -> {msg['strategy_id']} {msg['action']} (Vol: {msg.get('volume', 'Default')})")
                resp = execute_trade(msg)
                socket.send_string(resp)
            except zmq.Again:
                pass
            except Exception as e:
                print(f"Manager Loop Error: {e}")
                try: socket.send_string("Manager: Internal Error")
                except: pass

            check_closed_trades()
            check_basket_logic()
            record_equity_snapshot()
            time.sleep(0.01)

        except KeyboardInterrupt:
            print("Manager: Shutting down...")
            break
        except Exception as e:
            traceback.print_exc()

if __name__ == "__main__":
    try:
        run_manager()
    except Exception as e:
        print("\n\n" + "="*40)
        print("CRITICAL MANAGER CRASH")
        print("="*40)
        traceback.print_exc() 
        print("="*40)
        input("Press Enter to close this window...")