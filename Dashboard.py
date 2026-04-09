import streamlit as st
import time
import MetaTrader5 as mt5
import pandas as pd 
import json 
from datetime import datetime, timedelta

# Import Components
from components.utils import load_config, init_mt5
from components.live_monitor import render_live_panel
from components.strategy_lab import render_strategy_lab
from components.history import render_history_tab
from components.journal import render_journal_tab 
from components.analytics import render_analytics_tab
from components.database import Database

st.set_page_config(page_title="Algo Command", layout="wide")

# --- DYNAMIC CONFIGURATION ---
_cfg_init = load_config()
LOCAL_OFFSET = _cfg_init.get('system', {}).get('local_utc_offset_hours', 1) if _cfg_init else 1

def update_supervisor_mode(new_state):
    config_path = 'system_config.json' 
    try:
        with open(config_path, 'r') as f:
            cfg = json.load(f)
            
        sizing_ref = cfg['ml_pipeline']['alpha_filter']['dynamic_sizing']
        
        if 'supervisor_present' not in sizing_ref:
            sizing_ref['max_volume_supervised'] = 0.5
            sizing_ref['max_volume_unsupervised'] = 0.1
            
        sizing_ref['supervisor_present'] = new_state
        
        with open(config_path, 'w') as f:
            json.dump(cfg, f, indent=2)
            
        return True
    except Exception as e:
        st.sidebar.error(f"Failed to update config: {e}")
        return False

def on_supervisor_toggle():
    new_state = st.session_state.supervisor_switch
    if update_supervisor_mode(new_state):
        st.toast(f"Supervisor Mode {'ACTIVATED (0.5L)' if new_state else 'DEACTIVATED (0.1L)'}!")

# --- NEW: SYSTEM LOCK & HEDGE LOGIC ---
def toggle_system_lock_and_hedge(new_state):
    config_path = 'system_config.json'
    try:
        with open(config_path, 'r') as f:
            cfg = json.load(f)
            
        # Ensure MT5 is connected because Streamlit callbacks run before main()
        path = cfg.get('system', {}).get('mt5_terminal_path')
        mt5.initialize(path=path)

        symbol = cfg.get('strategies', {}).get('QT_Velocity', {}).get('symbol', 'US500')
        positions = mt5.positions_get(symbol=symbol)
        
        if 'saved_sl_tp' not in cfg['risk_management']['emergency_protocols']:
            cfg['risk_management']['emergency_protocols']['saved_sl_tp'] = {}
            
        saved_stops = cfg['risk_management']['emergency_protocols']['saved_sl_tp']

        if new_state: # --- WE ARE LOCKING THE SYSTEM ---
            if positions:
                for pos in positions:
                    if pos.tp != 0.0 or pos.sl != 0.0:
                        saved_stops[str(pos.ticket)] = {"sl": pos.sl, "tp": pos.tp}
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": pos.ticket,
                            "symbol": pos.symbol,
                            "sl": 0.0,
                            "tp": 0.0
                        }
                        mt5.order_send(request)

            cfg['risk_management']['emergency_protocols']['saved_sl_tp'] = saved_stops

            net_volume = 0.0
            if positions:
                for pos in positions:
                    if pos.type == mt5.POSITION_TYPE_BUY:
                        net_volume += pos.volume
                    elif pos.type == mt5.POSITION_TYPE_SELL:
                        net_volume -= pos.volume
            
            net_volume = round(net_volume, 2)
            
            if net_volume != 0:
                action = mt5.ORDER_TYPE_SELL if net_volume > 0 else mt5.ORDER_TYPE_BUY
                hedge_vol = abs(net_volume)
                tick = mt5.symbol_info_tick(symbol)
                price = tick.bid if action == mt5.ORDER_TYPE_SELL else tick.ask
                
                request = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": symbol,
                    "volume": hedge_vol,
                    "type": action,
                    "price": price,
                    "magic": 999999,
                    "comment": "MANUAL UI HEDGE",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }
                mt5.order_send(request)

        else: # --- WE ARE UNLOCKING THE SYSTEM ---
            if positions and saved_stops:
                for pos in positions:
                    ticket_str = str(pos.ticket)
                    if ticket_str in saved_stops:
                        sl = saved_stops[ticket_str]['sl']
                        tp = saved_stops[ticket_str]['tp']
                        request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": pos.ticket,
                            "symbol": pos.symbol,
                            "sl": sl,
                            "tp": tp
                        }
                        mt5.order_send(request)
            
            cfg['risk_management']['emergency_protocols']['saved_sl_tp'] = {}

        cfg['risk_management']['emergency_protocols']['system_locked'] = new_state
        with open(config_path, 'w') as f:
            json.dump(cfg, f, indent=2)
            
        return True
    except Exception as e:
        st.sidebar.error(f"Failed to execute lock/hedge protocol: {e}")
        return False

def on_system_lock_toggle():
    new_state = st.session_state.system_lock_switch
    if toggle_system_lock_and_hedge(new_state):
        if new_state:
            st.toast("🚨 SYSTEM LOCKED! Hedges placed and TPs removed.")
        else:
            st.toast("✅ SYSTEM UNLOCKED! Original TPs restored.")

# --- 1. DATABASE STATE RESTORATION ---
if 'data_restored' not in st.session_state:
    try:
        db = Database()
        
        recent_trades = db.fetch_trades(limit=200)
        d_pnl = 0.0
        d_trades = 0
        
        now_local = datetime.now() + timedelta(hours=LOCAL_OFFSET)
        midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if recent_trades:
            for t in recent_trades:
                try:
                    close_dt = pd.to_datetime(t['close_time']).to_pydatetime() + timedelta(hours=LOCAL_OFFSET)
                    if close_dt >= midnight_local:
                        d_pnl += float(t.get('pnl', 0.0))
                        d_trades += 1
                except Exception:
                    continue
                    
        st.session_state['daily_pnl'] = d_pnl
        st.session_state['daily_trades'] = d_trades
        
        equity_raw = db.fetch_equity_history(limit=2880) 
        equity_clean = []
        midnight_timestamp = midnight_local.timestamp()
        
        for row in equity_raw:
            d = dict(row)
            clean_row = {
                'Balance': d['balance'],
                'Equity': d['equity'],
            }
            
            if 'strategy_performance' in d and d['strategy_performance']:
                try:
                    strat_data = json.loads(d['strategy_performance'])
                    for k, v in strat_data.items():
                        clean_row[f"PL_{k}"] = v
                except:
                    pass 
            
            if 'timestamp' in d:
                try:
                    ts_pandas = pd.to_datetime(d['timestamp'])
                    ts_python = ts_pandas.to_pydatetime() + timedelta(hours=LOCAL_OFFSET)
                    t_unix = ts_python.timestamp()
                    
                    if t_unix < midnight_timestamp:
                        continue 
                    
                    clean_row['time_unix'] = t_unix
                    clean_row['time'] = ts_python.strftime('%H:%M:%S')
                except Exception:
                    continue 
            
            equity_clean.append(clean_row)
        
        equity_clean.reverse()
        st.session_state['session_full_history'] = equity_clean.copy()
        st.session_state['history_data'] = equity_clean[-200:] if len(equity_clean) > 200 else equity_clean.copy()
        
        st.session_state['data_restored'] = True
        print(f"Dashboard: Restored {len(equity_clean)} Snapshots")
        
    except Exception as e:
        print(f"Dashboard Load Error: {e}")
        st.session_state['daily_pnl'] = 0.0
        st.session_state['daily_trades'] = 0
        st.session_state['history_data'] = []
        st.session_state['session_full_history'] = []

# --- 2. SESSION STATE INITIALIZATION ---
if 'history_data' not in st.session_state:
    st.session_state.history_data = []  
if 'session_full_history' not in st.session_state:
    st.session_state.session_full_history = [] 

# --- 3. MT5 TICKET FILTER ---
if 'reset_ticket_threshold' not in st.session_state:
    if _cfg_init:
        path = _cfg_init['system'].get('mt5_terminal_path')
        if init_mt5(path):
            now_local = datetime.now() + timedelta(hours=LOCAL_OFFSET)
            midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            history_before = mt5.history_deals_get(midnight_local - timedelta(days=7), midnight_local)
            
            if history_before and len(history_before) > 0:
                st.session_state.reset_ticket_threshold = history_before[-1].ticket
            else:
                st.session_state.reset_ticket_threshold = 0
        else:
            st.session_state.reset_ticket_threshold = 0
    else:
        st.session_state.reset_ticket_threshold = 0

def main():
    st.title("⚡ Algo Command")
    
    config = load_config()
    if not config: return

    path = config['system'].get('mt5_terminal_path')
    if not init_mt5(path):
        st.error(f"Failed to connect to MT5 at {path}")
        return

    # --- SIDEBAR: CONTROLS ---
    with st.sidebar:
        st.header("Risk Management")
        
        risk_cfg = config.get('risk_management', {}).get('emergency_protocols', {})
        live_lock_state = risk_cfg.get('system_locked', False)
        
        if 'system_lock_switch' not in st.session_state:
            st.session_state.system_lock_switch = live_lock_state
        elif st.session_state.system_lock_switch != live_lock_state:
            st.session_state.system_lock_switch = live_lock_state
            
        st.toggle(
            "🛑 Emergency Lock", 
            key="system_lock_switch", 
            on_change=on_system_lock_toggle, 
            help="ON: Trading Suspended. OFF: Trading Active. Watcher can auto-trigger this."
        )

        sizing_cfg = config.get('ml_pipeline', {}).get('alpha_filter', {}).get('dynamic_sizing', {})
        current_supervisor_state = sizing_cfg.get('supervisor_present', False)
        
        if 'supervisor_switch' not in st.session_state:
            st.session_state.supervisor_switch = current_supervisor_state
            
        st.toggle(
            "👁️ Supervisor Mode", 
            key="supervisor_switch", 
            on_change=on_supervisor_toggle, 
            help="ON: Allows up to 0.5 Lots. OFF: Throttles AI to 0.1 Lots."
        )
        
        st.divider()

        st.header("Session Controls")
        if st.button("🔄 Reset Tracking Today", type="primary"):
            st.session_state.history_data = []
            st.session_state.session_full_history = []
            st.session_state['daily_pnl'] = 0.0
            st.session_state['daily_trades'] = 0
            
            now_local = datetime.now() + timedelta(hours=LOCAL_OFFSET)
            deals = mt5.history_deals_get(now_local - timedelta(days=7), now_local + timedelta(days=1))
            if deals and len(deals) > 0:
                st.session_state.reset_ticket_threshold = deals[-1].ticket
            
            st.success("Session View Reset!")
            time.sleep(0.5)
            st.rerun()

    # --- CALCULATE LIVE METRICS ---
    acc = mt5.account_info()
    strategies = config.get('strategies', {})
    positions = mt5.positions_get()
    
    global_net_lots = 0.0
    global_net_count = 0
    global_total_open = 0
    
    if positions:
        for pos in positions:
            global_total_open += 1
            if pos.type == mt5.POSITION_TYPE_BUY:
                global_net_lots += pos.volume
                global_net_count += 1
            elif pos.type == mt5.POSITION_TYPE_SELL:
                global_net_lots -= pos.volume
                global_net_count -= 1

    if global_net_lots > 0:
        exposure_val = f"{global_net_lots:+.2f} Lots"
        exposure_tag = "LONG 🐂"
        delta_color = "normal" 
    elif global_net_lots < 0:
        exposure_val = f"{global_net_lots:+.2f} Lots"
        exposure_tag = "-SHORT 🐻"
        delta_color = "normal" 
    else:
        exposure_val = "0.00 Lots"
        exposure_tag = "FLAT ⚪"
        delta_color = "off"

    if acc:
        kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
        
        kpi1.metric("Balance", f"${acc.balance:,.2f}")
        kpi2.metric("Equity", f"${acc.equity:,.2f}", delta=f"{acc.equity - acc.balance:.2f}")
        
        daily_pnl = st.session_state.get('daily_pnl', 0.0)
        daily_trades = st.session_state.get('daily_trades', 0)
        kpi3.metric("Daily PnL", f"${daily_pnl:,.2f}", f"{daily_trades} Trades")
        kpi4.metric("Open Positions", f"{global_total_open}")
        kpi5.metric("Net Exposure", exposure_val, exposure_tag, delta_color=delta_color)
        kpi6.metric("Position Delta", f"{global_net_count:+}", help="Positive = More Buys, Negative = More Sells")

    # --- TABS ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Live Pulse", "⚙️ Strategy Lab", "📜 History", "🗄️ Journal", "📊 Analytics"])

    with tab1:
        render_live_panel(strategies, config)
    with tab2:
        render_strategy_lab(strategies, config)
    with tab3:
        render_history_tab(strategies)
    with tab4:
        render_journal_tab()
    with tab5:
        render_analytics_tab()

    time.sleep(1)
    st.rerun()

if __name__ == "__main__":
    main()