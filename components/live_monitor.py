import streamlit as st
import pandas as pd
import MetaTrader5 as mt5
import json
import sqlite3
from datetime import datetime, timedelta
from components.charts import render_equity_chart, render_drawdown_chart
from components.utils import get_strategy_name

MAX_DATA_POINTS = 200
TIME_OFFSET = 1 # FIX: +1 Hour for Frankfurt

def render_live_panel(strategies, config):
    # --- DATA COLLECTION ---
    acc = mt5.account_info()
    if not acc: return

    positions = mt5.positions_get()

    strat_live_data = {
        name: {
            'floating': 0.0, 
            'open_lots': 0.0, 
            'open_count': 0,
            'net_lots': 0.0, 
            'net_count': 0
        } for name in strategies.keys()
    }
    
    if positions:
        for pos in positions:
            strat_name = get_strategy_name(pos.magic, strategies)
            
            if strat_name in strat_live_data:
                strat_live_data[strat_name]['floating'] += (pos.profit + pos.swap)
                strat_live_data[strat_name]['open_lots'] += pos.volume
                strat_live_data[strat_name]['open_count'] += 1
                
                if pos.type == mt5.POSITION_TYPE_BUY:
                    strat_live_data[strat_name]['net_lots'] += pos.volume
                    strat_live_data[strat_name]['net_count'] += 1
                elif pos.type == mt5.POSITION_TYPE_SELL:
                    strat_live_data[strat_name]['net_lots'] -= pos.volume
                    strat_live_data[strat_name]['net_count'] -= 1

    # --- SAVE SNAPSHOT (With Timezone Fix) ---
    now = datetime.now() + timedelta(hours=TIME_OFFSET)
    timestamp_str = now.strftime('%H:%M:%S')
    timestamp_unix = now.timestamp()
    
    snapshot = {
        'time': timestamp_str,
        'time_unix': timestamp_unix, 
        'Balance': acc.balance,
        'Equity': acc.equity,
    }
    for name, data in strat_live_data.items():
        snapshot[f"PL_{name}"] = data['floating']

    st.session_state.history_data.append(snapshot)
    if len(st.session_state.history_data) > MAX_DATA_POINTS:
        st.session_state.history_data.pop(0)

    st.session_state.session_full_history.append(snapshot)

    # --- LIVE AI FEED & STRATEGY DRAWDOWN ---
    df_live = pd.DataFrame(st.session_state.history_data)
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("🧠 Live AI Decision Feed")
        feed_container = st.container(height=300) 
        
        db_path = config['system'].get('db_path', 'trading_system.db')
        try:
            conn = sqlite3.connect(db_path)
            df_signals = pd.read_sql("SELECT timestamp, symbol, features_json FROM ml_features ORDER BY timestamp DESC LIMIT 20", conn)
            conn.close()
            
            if not df_signals.empty:
                valid_signals = 0
                for _, row in df_signals.iterrows():
                    try:
                        data = json.loads(row['features_json'])
                        ai_decision = data.get('ai_decision', {})
                        
                        if not ai_decision: continue
                        
                        conf = ai_decision.get('confidence', 0) * 100
                        blocked = ai_decision.get('blocked', False)
                        vol = ai_decision.get('volume', 0)
                        
                        ts_dt = datetime.fromtimestamp(row['timestamp'] / 1000) + timedelta(hours=TIME_OFFSET)
                        time_str = ts_dt.strftime('%H:%M:%S')
                        
                        if blocked:
                            border_color = "#ff4b4b" 
                            bg_color = "rgba(255, 75, 75, 0.1)"
                            msg = f"🚫 <b>BLOCKED</b> &nbsp;|&nbsp; {row['symbol']} &nbsp;|&nbsp; Conf: {conf:.1f}%"
                        else:
                            border_color = "#2bd67b" 
                            bg_color = "rgba(43, 214, 123, 0.1)"
                            msg = f"✅ <b>APPROVED</b> &nbsp;|&nbsp; {row['symbol']} &nbsp;|&nbsp; Conf: {conf:.1f}% &nbsp;|&nbsp; Size: {vol}L"
                        
                        html_string = f"""
                        <div style="
                            border-left: 4px solid {border_color}; 
                            background-color: {bg_color}; 
                            padding: 8px 12px; 
                            margin-bottom: 8px; 
                            border-radius: 4px;
                            font-family: monospace;
                            font-size: 0.9rem;
                        ">
                            <span style="color: #888;">{time_str}</span> &nbsp;|&nbsp; {msg}
                        </div>
                        """
                        feed_container.markdown(html_string, unsafe_allow_html=True)
                        valid_signals += 1
                        
                    except Exception:
                        continue
                        
                if valid_signals == 0:
                    feed_container.info("Waiting for AI signals... (No recent AI data found)")
            else:
                feed_container.info("Waiting for AI signals... (Database is empty)")
        except Exception as e:
            feed_container.error(f"Could not load AI feed: {e}")
            
    with c2:
        st.subheader("Strategy Drawdown (Attribution)")
        render_drawdown_chart(df_live, key="chart_drawdown_short")

    st.subheader("Full Session Performance")
    df_full = pd.DataFrame(st.session_state.session_full_history)
    if not df_full.empty:
        render_equity_chart(df_full, key="chart_live_long")
    else:
        st.info("Session data will build up here...")

    st.markdown("---")

    # --- SCORECARD TABLE ---
    st.subheader("Strategy Scorecard (Session)")
    
    from_date = datetime.now() - timedelta(days=3)
    to_date = datetime.now() + timedelta(days=1)
    history = mt5.history_deals_get(from_date, to_date)
    
    position_magic_map = {}
    if history:
        for d in history:
            if d.entry == 0: 
                position_magic_map[d.position_id] = d.magic

    scorecard_data = []
    
    for name, data in strategies.items():
        target_magic = data['magic_number']
        
        realized_pl = 0.0
        trades_count = 0
        wins = 0
        
        if history:
            deals = []
            for d in history:
                if d.entry in [1, 2] and d.ticket > st.session_state.reset_ticket_threshold:
                    original_magic = position_magic_map.get(d.position_id, d.magic)
                    if original_magic == target_magic:
                        deals.append(d)

            trades_count = len(deals)
            realized_pl = sum(d.profit + d.swap + d.commission for d in deals)
            wins = sum(1 for d in deals if d.profit > 0)
        
        win_rate = (wins / trades_count * 100) if trades_count > 0 else 0
        
        default_stats = {'floating': 0.0, 'open_lots': 0.0, 'open_count': 0, 'net_lots': 0.0, 'net_count': 0}
        live_stats = strat_live_data.get(name, default_stats)
        
        net_money = realized_pl + live_stats['floating']
        
        def fmt_pct(val, balance):
            if balance == 0: return "0%"
            pct = (val / balance) * 100
            return f"({pct:+.2f}%)"

        scorecard_data.append({
            "Strategy": name,
            "Status": "🟢 ON" if data['enabled'] else "🔴 OFF",
            "Net Money": f"${net_money:,.2f} {fmt_pct(net_money, acc.balance)}",
            "Floating P/L": f"${live_stats['floating']:,.2f} {fmt_pct(live_stats['floating'], acc.balance)}",
            "Banked (Session)": f"${realized_pl:,.2f} {fmt_pct(realized_pl, acc.balance)}",
            "Open Pos": f"{live_stats['open_count']} ({live_stats['open_lots']:.2f} lots)",
            "Net Exposure": f"{live_stats['net_count']:+} ({live_stats['net_lots']:+.2f} lots)",
            "Closed Pos": f"{trades_count}",
            "Win Rate": f"{win_rate:.0f}%"
        })
        
    df_score = pd.DataFrame(scorecard_data)
    
    def color_pnl(val):
        if isinstance(val, str):
            if "$-" in val: return 'color: #ff4b4b'
            elif "$0.00" in val: return 'color: white'
            else: return 'color: #2bd67b'
        return ''

    styled_df = df_score.style.map(color_pnl, subset=["Net Money", "Floating P/L", "Banked (Session)"])
    st.dataframe(styled_df, use_container_width=True, hide_index=True)