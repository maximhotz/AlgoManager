import MetaTrader5 as mt5
import pandas as pd
import random
import json
import os
import numpy as np

# ==========================================
# ⚙️ LOAD CONFIGURATION
# ==========================================
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'system_config.json')
try:
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print(f"❌ Could not find {CONFIG_PATH}.")
    exit(1)

SYMBOL = config['strategies']['QT_Velocity']['symbol']
TP_POINTS = config['strategies']['QT_Velocity']['trade_limits']['tp_points']
HORIZON_MINUTES = config['ml_pipeline']['labeling']['horizon_minutes']
SPREAD_ALLOWANCE = config['ml_pipeline']['labeling']['spread_allowance']

# --- CONVERGENCE UPGRADE ---
NUM_ITERATIONS = 100
TRADES_PER_ITERATION = 1000

def run_monte_carlo():
    print(f"🔌 Connecting to MetaTrader 5...")
    if not mt5.initialize():
        print("❌ MT5 initialization failed")
        return

    print(f"📈 Running Monte Carlo Convergence Test on {SYMBOL}...")
    print(f"   Parameters: TP={TP_POINTS}pts, Horizon={HORIZON_MINUTES}m, Spread={SPREAD_ALLOWANCE}pts")
    print(f"   Simulating {NUM_ITERATIONS} batches of {TRADES_PER_ITERATION} trades (Total: {NUM_ITERATIONS * TRADES_PER_ITERATION})...\n")

    # Pull the last ~2 weeks of 1-minute candles
    rates = mt5.copy_rates_from_pos(SYMBOL, mt5.TIMEFRAME_M1, 0, 15000)
    if rates is None or len(rates) == 0:
        print("❌ Failed to get historical data from MT5.")
        return

    df_rates = pd.DataFrame(rates)
    valid_start_indices = range(len(df_rates) - HORIZON_MINUTES)
    
    batch_win_rates = []

    for i in range(NUM_ITERATIONS):
        random_indices = random.choices(valid_start_indices, k=TRADES_PER_ITERATION)
        wins = 0
        
        for idx in random_indices:
            action = random.choice(["BUY", "SELL"])
            open_price = df_rates.iloc[idx]['close'] 
            horizon_df = df_rates.iloc[idx + 1 : idx + 1 + HORIZON_MINUTES]
            
            is_win = False
            if action == "BUY":
                if (horizon_df['high'] >= open_price + TP_POINTS + SPREAD_ALLOWANCE).any():
                    is_win = True
            elif action == "SELL":
                if (horizon_df['low'] <= open_price - TP_POINTS - SPREAD_ALLOWANCE).any():
                    is_win = True
            
            if is_win: wins += 1

        win_rate = (wins / TRADES_PER_ITERATION) * 100
        batch_win_rates.append(win_rate)
        print(f"   Batch {i+1}/{NUM_ITERATIONS}: {win_rate:.2f}% Win Rate")

    final_average = np.mean(batch_win_rates)
    
    print("\n========================================")
    print("🎯 FINAL CONVERGED BASELINE RESULTS")
    print("========================================")
    print(f"Total Simulated Trades: {NUM_ITERATIONS * TRADES_PER_ITERATION}")
    print(f"Converged True Baseline: {final_average:.2f}%")
    
    # Calculate edge based on your exact 78.76% C# bot win rate
    c_sharp_win_rate = 78.76
    edge = c_sharp_win_rate - final_average
    
    print(f"Your C# Bot Win Rate:    {c_sharp_win_rate:.2f}%")
    print(f"True Mathematical Edge:  +{edge:.2f}%")
    print("========================================\n")

    mt5.shutdown()

if __name__ == "__main__":
    run_monte_carlo()