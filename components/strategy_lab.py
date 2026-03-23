import streamlit as st
from components.utils import save_config

def render_strategy_lab(strategies, config):
    st.subheader("Manage Strategies")
    
    for strat_id, settings in strategies.items():
        with st.expander(f"{strat_id} Settings", expanded=True):
            with st.form(f"edit_{strat_id}"):
                c1, c2 = st.columns(2)
                
                with c1:
                    st.write("Core Settings")
                    new_enabled = st.checkbox("Enabled", value=settings.get('enabled', True))
                    new_vol = st.number_input("Lot Size", value=settings.get('volume', 0.05), step=0.01)
                
                with c2:
                    st.write("Fixed Trade Limits (Points)")
                    limits = settings.get('trade_limits', {})
                    # FORCE floats to prevent Streamlit type-crashes
                    new_tp_fixed = st.number_input("Take Profit", value=float(limits.get('tp_points', 1.0)), step=0.25)
                    new_sl_fixed = st.number_input("Stop Loss (0 = None)", value=float(limits.get('sl_points', 0.0)), step=0.25)

                if st.form_submit_button("💾 Save to Engine"):
                    # Update config dictionary
                    config['strategies'][strat_id]['enabled'] = new_enabled
                    config['strategies'][strat_id]['volume'] = new_vol
                    config['strategies'][strat_id]['trade_limits']['tp_points'] = new_tp_fixed
                    config['strategies'][strat_id]['trade_limits']['sl_points'] = new_sl_fixed
                    
                    # Write to disk
                    save_config(config)