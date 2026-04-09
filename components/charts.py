import pandas as pd
import streamlit as st
import numpy as np
import plotly.graph_objects as go
from streamlit_lightweight_charts import renderLightweightCharts

def safe_float(val):
    """Ensures value is a valid float for JSON (no NaN or Inf)"""
    try:
        f_val = float(val)
        if pd.isna(f_val) or np.isinf(f_val):
            return 0.0
        return f_val
    except:
        return 0.0

def render_equity_chart(df_live, key=None):
    """
    Renders a professional TradingView-style area chart.
    Wraps options and series into the single list structure expected by the library.
    """
    if df_live.empty:
        st.info("Waiting for data...")
        return

    # 1. Format Data for the Library
    df_live = df_live.sort_values('time_unix')
    
    data_equity = []
    data_balance = []
    
    for _, row in df_live.iterrows():
        if pd.isna(row['time_unix']): continue
        
        t = int(row['time_unix']) 
        
        # FIX: Use safe_float helper
        eq_val = safe_float(row.get('Equity'))
        bal_val = safe_float(row.get('Balance'))
        
        data_equity.append({"time": t, "value": eq_val})
        data_balance.append({"time": t, "value": bal_val})

    # 2. Define Chart Options (Styling)
    chartOptions = {
        "layout": {
            "textColor": "#d1d4dc",
            "background": {"type": 'solid', "color": 'transparent'},
        },
        "grid": {
            "vertLines": {"color": "rgba(42, 46, 57, 0)"}, # Hidden grid
            "horzLines": {"color": "rgba(42, 46, 57, 0.6)"},
        },
        "rightPriceScale": {
            "borderColor": "rgba(197, 203, 206, 0.8)",
        },
        "timeScale": {
            "borderColor": "rgba(197, 203, 206, 0.8)",
            "timeVisible": True,
            "secondsVisible": True,
        },
        "height": 300
    }

    # 3. Define Series (The actual lines)
    series = [
        {
            "type": "Area",
            "data": data_equity,
            "options": {
                "topColor": "rgba(33, 150, 243, 0.56)",
                "bottomColor": "rgba(33, 150, 243, 0.04)",
                "lineColor": "rgba(33, 150, 243, 1)",
                "lineWidth": 2,
                "title": "Equity"
            },
        },
        {
            "type": "Line",
            "data": data_balance,
            "options": {
                "color": "#ff9800", # Orange for Balance
                "lineWidth": 2,
                "lineStyle": 2, # Dashed
                "title": "Balance"
            },
        }
    ]

    # 4. Render
    renderLightweightCharts([
        {
            "chart": chartOptions,
            "series": series
        }
    ], key=key)

def render_drawdown_chart(df_live, key=None):
    """
    Renders a line chart for individual strategy floating P/L.
    """
    if df_live.empty:
        return

    df_live = df_live.sort_values('time_unix')
    
    # Identify PL columns dynamically
    pl_cols = [c for c in df_live.columns if c.startswith("PL_")]
    
    series_list = []
    # Pick distinct colors for strategies
    colors = ['#2962FF', '#E91E63', '#00E676', '#FFD600', '#AB47BC']
    
    for i, col in enumerate(pl_cols):
        data_series = []
        for _, row in df_live.iterrows():
            if pd.isna(row['time_unix']): continue
            
            # FIX: Use safe_float helper to catch NaNs and Infs
            val = safe_float(row.get(col))
                
            data_series.append({"time": int(row['time_unix']), "value": val})
        
        strat_name = col.replace("PL_", "")
        color = colors[i % len(colors)]
        
        series_list.append({
            "type": "Line",
            "data": data_series,
            "options": {
                "color": color,
                "lineWidth": 2,
                "title": strat_name
            }
        })

    chartOptions = {
        "layout": { "textColor": "#d1d4dc", "background": { "type": 'solid', "color": 'transparent' } },
        "grid": { "vertLines": {"visible": False}, "horzLines": {"color": "rgba(42, 46, 57, 0.5)"} },
        "height": 300
    }

    renderLightweightCharts([
        {
            "chart": chartOptions,
            "series": series_list
        }
    ], key=key)

def render_regime_chart(df):
    """
    Renders a Plotly candlestick chart with background colors for the AI Regime.
    """
    if df.empty:
        st.info("Waiting for price data...")
        return

    # 1. Base Candlestick Chart
    fig = go.Figure(data=[go.Candlestick(
        x=df['time'],
        open=df['open'], high=df['high'],
        low=df['low'], close=df['close'],
        increasing_line_color='#2bd67b', decreasing_line_color='#ff4b4b'
    )])

    # 2. Color Map matching your legacy C# parameters (0 = Bull, 1 = Chop, 2 = Bear)
    color_map = {
        0: "rgba(43, 214, 123, 0.15)",  # Green (Bull)
        1: "rgba(255, 255, 255, 0.05)", # Gray  (Chop)
        2: "rgba(255, 75, 75, 0.15)"    # Red   (Bear)
    }

    # 3. Paint Background Regimes
    if 'regime' in df.columns and not df['regime'].isna().all():
        # Create blocks of continuous regimes
        df['block'] = (df['regime'] != df['regime'].shift(1)).cumsum()
        
        for _, group in df.groupby('block'):
            regime_val = group['regime'].iloc[0]
            if pd.isna(regime_val): continue
            
            start_time = group['time'].iloc[0]
            end_time = group['time'].iloc[-1]
            
            fig.add_vrect(
                x0=start_time, x1=end_time,
                fillcolor=color_map.get(regime_val, "rgba(0,0,0,0)"),
                opacity=1,
                layer="below", line_width=0,
            )

    # 4. Styling to match Dashboard Theme
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=0, r=0, t=30, b=0),
        height=300,
        xaxis_rangeslider_visible=False,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )

    st.plotly_chart(fig, use_container_width=True)