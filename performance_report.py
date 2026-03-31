import sqlite3
import pandas as pd
import os
import json

# ==========================================
# ⚙️ LOAD CONFIGURATION
# ==========================================
current_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(current_dir, 'system_config.json')

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

DB_PATH = os.path.join(current_dir, config['system']['db_path'])

def generate_report():
    conn = sqlite3.connect(DB_PATH)
    
    query = """
    SELECT 
        f.id, 
        f.target_label,
        f.features_json,
        CASE WHEN t.ticket IS NOT NULL THEN 1 ELSE 0 END as executed
    FROM ml_features f
    LEFT JOIN trades t ON f.id = t.ml_feature_id
    WHERE f.target_label IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        print("❌ No labeled data found.")
        return

    # Process AI Decisions from JSON
    def process_row(row):
        data = json.loads(row['features_json'])
        ai_meta = data.get('ai_decision', {})
        
        # Determine the Category
        if row['executed'] == 1:
            return "EXECUTED"
        elif ai_meta.get('blocked') == True:
            return "AI_BLOCKED"
        else:
            return "TECH_FAIL"

    df['category'] = df.apply(process_row, axis=1)

    executed_trades = df[df['category'] == "EXECUTED"]
    blocked_trades = df[df['category'] == "AI_BLOCKED"]
    tech_fails = df[df['category'] == "TECH_FAIL"]

    # Metrics Calculations
    total_signals = len(df)
    raw_win_rate = (len(df[df['target_label'] == 1]) / total_signals * 100) if total_signals > 0 else 0
    mt5_win_rate = (len(executed_trades[executed_trades['target_label'] == 1]) / len(executed_trades) * 100) if not executed_trades.empty else 0
    blocking_precision = (len(blocked_trades[blocked_trades['target_label'] == 0]) / len(blocked_trades) * 100) if not blocked_trades.empty else 0

    print("\n" + "="*50)
    print("🤖 AI ALPHA FILTER PERFORMANCE REPORT")
    print("="*50)
    
    print(f"📊 VOLUME ANALYSIS")
    print(f"Total Signals Generated:   {total_signals}")
    print(f"Executed on MT5:           {len(executed_trades)}")
    print(f"Blocked by AI:             {len(blocked_trades)}")
    print(f"Technical Failures:        {len(tech_fails)} (Slippage/Errors)")
    
    print(f"\n🏆 WIN RATE COMPARISON")
    print(f"Raw Strategy (No AI):      {raw_win_rate:.2f}%")
    print(f"Filtered (Real MT5):       {mt5_win_rate:.2f}%")
    
    alpha_boost = mt5_win_rate - raw_win_rate
    print(f"{'✅' if alpha_boost > 0 else '⚠️'} AI Win-Rate Boost:       {alpha_boost:+.2f}%")

    print(f"\n🛡️ AI BLOCKING QUALITY")
    print(f"Correctly Blocked Losses:  {len(blocked_trades[blocked_trades['target_label'] == 0])}")
    print(f"Missed Wins (Scared AI):   {len(blocked_trades[blocked_trades['target_label'] == 1])}")
    print(f"Blocking Precision:        {blocking_precision:.2f}%")
    
    print("="*50 + "\n")

if __name__ == "__main__":
    generate_report()