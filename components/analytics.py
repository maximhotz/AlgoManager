import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
from components.database import Database

def render_analytics_tab():
    st.header("📊 Deep Performance Analytics")
    
    db = Database()
    trades = db.fetch_trades(limit=5000) # Get lots of history
    
    if not trades:
        st.info("No data available.")
        return

    df = pd.DataFrame(trades)
    
    # --- 1. DATA PREPARATION ---
    # Convert string timestamps to datetime objects
    df['close_time'] = pd.to_datetime(df['close_time'])
    df['open_time'] = pd.to_datetime(df['open_time'])
    
    # Expand JSON Data (Unpack the meta metrics so we can analyze confidence)
    if 'meta_json' in df.columns:
        meta_list = df['meta_json'].tolist()
        meta_df = pd.json_normalize(meta_list)
        df = pd.concat([df.drop('meta_json', axis=1), meta_df], axis=1)
    
    # Extract "Features" for Filtering
    df['Day'] = df['close_time'].dt.date
    df['Weekday'] = df['close_time'].dt.day_name()
    df['Hour'] = df['close_time'].dt.hour
    
    # --- 2. AGGREGATE STATS (Daily/Weekly) ---
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("📆 Daily Performance")
        daily_stats = df.groupby('Day')['pnl'].sum().reset_index()
        daily_stats['Cumulative'] = daily_stats['pnl'].cumsum()
        
        fig_daily = px.bar(daily_stats, x='Day', y='pnl', 
                           color='pnl', color_continuous_scale=['red', 'green'])
        st.plotly_chart(fig_daily, use_container_width=True)

    with c2:
        st.subheader("📈 Equity Curve (Closed Trades)")
        fig_cum = px.line(daily_stats, x='Day', y='Cumulative', markers=True)
        st.plotly_chart(fig_cum, use_container_width=True)

    st.divider()

    # --- 3. ADVANCED HEATMAPS (Time of Day / Weekday) ---
    st.subheader("🕰️ Time & Day Analysis")
    st.caption("When are your strategies actually making money?")
    
    col_a, col_b = st.columns(2)
    
    with col_a:
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        day_stats = df.groupby('Weekday')['pnl'].sum().reindex(day_order).reset_index()
        
        fig_day = px.bar(day_stats, x='Weekday', y='pnl', title="PnL by Weekday",
                         color='pnl', color_continuous_scale='RdYlGn')
        st.plotly_chart(fig_day, use_container_width=True)
        
    with col_b:
        hour_stats = df.groupby('Hour')['pnl'].sum().reset_index()
        fig_hour = px.bar(hour_stats, x='Hour', y='pnl', title="PnL by Hour of Day",
                          color='pnl', color_continuous_scale='RdYlGn')
        st.plotly_chart(fig_hour, use_container_width=True)

    # --- 4. STRATEGY COMPARISON ---
    st.divider()
    st.subheader("🤖 Strategy Comparison")
    
    strat_perf = df.groupby('strategy_id').agg({
        'pnl': 'sum',
        'ticket': 'count',
        'duration_sec': 'mean'
    }).reset_index()
    
    strat_perf['Avg Profit per Trade'] = strat_perf['pnl'] / strat_perf['ticket']
    st.dataframe(strat_perf.style.background_gradient(subset=['pnl'], cmap='RdYlGn'), use_container_width=True)

    # --- 5. ORACLE REGIME MATRIX ---
    st.divider()
    st.subheader("🔭 Oracle Regime Edge Matrix")
    st.caption("Cross-referencing trades with the active HMM macro regime to prove the Unsupervised AI's edge.")

    regimes = db.fetch_regimes(limit=50000)
    if regimes:
        df_reg = pd.DataFrame(regimes)
        local_offset = 1
        config_path = "system_config.json"
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    cfg = json.load(f)
                    local_offset = cfg.get('system', {}).get('local_utc_offset_hours', 1)
            except Exception: pass

        df_reg['time'] = pd.to_datetime(df_reg['timestamp'], unit='s') + pd.Timedelta(hours=local_offset)
        df = df.sort_values('open_time')
        df_reg = df_reg.sort_values('time')

        df_merged = pd.merge_asof(
            df, df_reg[['time', 'regime', 'name']],
            left_on='open_time', right_on='time',
            direction='backward', tolerance=pd.Timedelta('24h')
        )
        df_merged['name'] = df_merged['name'].fillna("Unknown/No Data")

        regime_perf = df_merged.groupby('name').apply(
            lambda x: pd.Series({
                'Trades': x['ticket'].count(),
                'Wins': (x['pnl'] > 0).sum(),
                'Win Rate': f"{((x['pnl'] > 0).sum() / x['ticket'].count() * 100):.1f}%" if x['ticket'].count() > 0 else "0%",
                'Total PnL': x['pnl'].sum(),
                'Avg PnL': x['pnl'].sum() / x['ticket'].count() if x['ticket'].count() > 0 else 0
            })
        ).reset_index().sort_values('Total PnL', ascending=False)

        c_matrix, c_chart = st.columns([1.5, 1])
        with c_matrix:
            st.dataframe(regime_perf.style.background_gradient(subset=['Total PnL', 'Avg PnL'], cmap='RdYlGn'), use_container_width=True, hide_index=True)

        with c_chart:
            chart_data = regime_perf[regime_perf['name'] != "Unknown/No Data"]
            if not chart_data.empty:
                fig_regime = px.pie(
                    chart_data, values='Trades', names='name', title="Trade Distribution by Regime", hole=0.4,
                    color='name', color_discrete_map={"Bull": "#2bd67b", "Bear": "#ff4b4b", "Chop": "#888888"}
                )
                fig_regime.update_layout(margin=dict(t=30, b=0, l=0, r=0))
                st.plotly_chart(fig_regime, use_container_width=True)
            else: st.info("Not enough aligned data yet.")
    else: st.info("No regime data found in database. Let the Oracle run to build history.")

# --- 6. AI CONFIDENCE SIZING MATRIX ---
    st.divider()
    st.subheader("🎯 AI Confidence Sizing Matrix")
    st.caption("Does higher AI confidence actually translate to a higher win rate? Use this to tune your dynamic sizing.")

    # 1. Catch silent failures (If the column doesn't exist)
    if 'confidence' in df.columns:
        
        # 2. Filter out pre-AI historical trades
        conf_df = df[df['confidence'] > 0].copy()

        if not conf_df.empty:
            # Create discrete Tiers for the confidence scores
            bins = [0, 50, 60, 70, 80, 90, 100]
            labels = ['<50%', '50-60%', '60-70%', '70-80%', '80-90%', '90-100%']
            conf_df['Conf_Tier'] = pd.cut(conf_df['confidence'], bins=bins, labels=labels)

            # Calculate Win Rate and PnL per tier
            conf_stats = conf_df.groupby('Conf_Tier').apply(
                lambda x: pd.Series({
                    'Trades': x['ticket'].count(),
                    'Wins': (x['pnl'] > 0).sum(),
                    'Win Rate %': ((x['pnl'] > 0).sum() / x['ticket'].count() * 100) if x['ticket'].count() > 0 else 0,
                    'Total PnL': x['pnl'].sum()
                })
            ).reset_index()

            # Clean up empty bins
            conf_stats = conf_stats[conf_stats['Trades'] > 0]

            c_conf_mat, c_conf_chart = st.columns([1, 1.5])

            with c_conf_mat:
                st.dataframe(
                    conf_stats.style.background_gradient(subset=['Win Rate %', 'Total PnL'], cmap='RdYlGn')
                                    .format({'Win Rate %': '{:.1f}%', 'Total PnL': '${:.2f}'}),
                    use_container_width=True, hide_index=True
                )

            with c_conf_chart:
                fig_conf = px.bar(
                    conf_stats, x='Conf_Tier', y='Win Rate %',
                    color='Win Rate %', color_continuous_scale='RdYlGn',
                    text_auto='.1f',
                    title="Win Rate by Confidence Tier"
                )
                fig_conf.update_layout(showlegend=False, xaxis_title="AI Confidence Tier", yaxis_title="Win Rate (%)")
                st.plotly_chart(fig_conf, use_container_width=True)
        else:
            st.info(f"Waiting for AI execution data. (Analyzed {len(df)} trades, but none had a Confidence Score > 0).")
    else:
        st.error("🚨 Critical Error: The 'confidence' column is missing. The SQL Database JOIN likely failed.")