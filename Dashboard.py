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

# --- CONFIGURATION ---
# FIX: Offset hours to match Frankfurt (CET) if Server is UTC
TIME_OFFSET = 1 

# --- 1. DATABASE STATE RESTORATION ---
if 'data_restored' not in st.session_state:
    try:
        db = Database()
        
        # A. Restore Daily Stats (Dynamically Computed)
        recent_trades = db.fetch_trades(limit=200)
        d_pnl = 0.0
        d_trades = 0
        
        # Calculate Midnight relative to LOCAL time
        now_local = datetime.now() + timedelta(hours=TIME_OFFSET)
        midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if recent_trades:
            for t in recent_trades:
                try:
                    # Convert trade close time and apply the Frankfurt offset
                    close_dt = pd.to_datetime(t['close_time']).to_pydatetime() + timedelta(hours=TIME_OFFSET)
                    if close_dt >= midnight_local:
                        d_pnl += float(t.get('pnl', 0.0))
                        d_trades += 1
                except Exception:
                    continue
                    
        st.session_state['daily_pnl'] = d_pnl
        st.session_state['daily_trades'] = d_trades
        
        # B. Restore Equity Curve (With Timezone Fix)
        equity_raw = db.fetch_equity_history(limit=2880) 
        equity_clean = []
        
        midnight_timestamp = midnight_local.timestamp()
        
        for row in equity_raw:
            d = dict(row)
            
            clean_row = {
                'Balance': d['balance'],
                'Equity': d['equity'],
            }
            
            # Unpack Strategy Performance
            if 'strategy_performance' in d and d['strategy_performance']:
                try:
                    strat_data = json.loads(d['strategy_performance'])
                    for k, v in strat_data.items():
                        clean_row[f"PL_{k}"] = v
                except:
                    pass 
            
            # Timezone Logic
            if 'timestamp' in d:
                try:
                    ts_pandas = pd.to_datetime(d['timestamp'])
                    # FIX: Shift DB timestamp to Local Time
                    ts_python = ts_pandas.to_pydatetime() + timedelta(hours=TIME_OFFSET)
                    t_unix = ts_python.timestamp()
                    
                    # Filter: Only show data from Local Midnight onwards
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
        print(f"Dashboard: Restored {len(equity_clean)} Snapshots (Time Corrected)")
        
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

# --- 3. MT5 TICKET FILTER (Midnight Sync) ---
if 'reset_ticket_threshold' not in st.session_state:
    config = load_config()
    if config:
        path = config['system'].get('mt5_terminal_path')
        if init_mt5(path):
            # FIX: Calculate midnight based on Local Time
            now_local = datetime.now() + timedelta(hours=TIME_OFFSET)
            midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Find the last ticket BEFORE local midnight
            # Note: We pass this datetime to MT5. If Broker time != Local Time, this might need further tuning.
            # Usually looking back from Local Midnight is safe.
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
    st.title("⚡ Algo Command (Frankfurt Time)")
    
    config = load_config()
    if not config: return

    path = config['system'].get('mt5_terminal_path')
    if not init_mt5(path):
        st.error(f"Failed to connect to MT5 at {path}")
        return

    # --- SIDEBAR: RESET BUTTON ---
    with st.sidebar:
        st.header("Session Controls")
        if st.button("🔄 Reset Tracking Today", type="primary"):
            st.session_state.history_data = []
            st.session_state.session_full_history = []
            
            st.session_state['daily_pnl'] = 0.0
            st.session_state['daily_trades'] = 0
            
            # Hard Reset to NOW (Local)
            now_local = datetime.now() + timedelta(hours=TIME_OFFSET)
            deals = mt5.history_deals_get(now_local - timedelta(days=7), now_local + timedelta(days=1))
            if deals and len(deals) > 0:
                st.session_state.reset_ticket_threshold = deals[-1].ticket
            
            st.success("Session View Reset!")
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

    # --- NEW: EXPOSURE DISPLAY LOGIC ---
    if global_net_lots > 0:
        exposure_val = f"{global_net_lots:+.2f} Lots"
        exposure_tag = "LONG 🐂" # Green tag automatically
        delta_color = "normal" 
    elif global_net_lots < 0:
        exposure_val = f"{global_net_lots:+.2f} Lots"
        exposure_tag = "-SHORT 🐻" # Minus forces Streamlit to make it Red
        delta_color = "normal" 
    else:
        exposure_val = "0.00 Lots"
        exposure_tag = "FLAT ⚪"
        delta_color = "off"

    # --- TOP METRICS ROW ---
    if acc:
        kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
        
        kpi1.metric("Balance", f"${acc.balance:,.2f}")
        kpi2.metric("Equity", f"${acc.equity:,.2f}", delta=f"{acc.equity - acc.balance:.2f}")
        
        daily_pnl = st.session_state.get('daily_pnl', 0.0)
        daily_trades = st.session_state.get('daily_trades', 0)
        kpi3.metric("Daily PnL", f"${daily_pnl:,.2f}", f"{daily_trades} Trades")

        kpi4.metric("Open Positions", f"{global_total_open}")
        
        # Swapped the label and tag here
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