import os
import json
import numpy as np
import pandas as pd
from hmmlearn.hmm import GaussianHMM
from sklearn.preprocessing import RobustScaler
import pickle

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR) if "Regime_Filter" in BASE_DIR else BASE_DIR
CONFIG_PATH = os.path.join(ROOT_DIR, "system_config.json")

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

hmm_cfg = config['ml_pipeline']['hmm_regime']
regime_map = config['ml_pipeline']['regime_mapping']

WINDOW = hmm_cfg.get('features_window', 15)
N_STATES = hmm_cfg.get('n_components', 5)
CSV_FILE = os.path.join(BASE_DIR, "ESM26_OF.csv")
MODEL_SAVE_PATH = os.path.join(ROOT_DIR, hmm_cfg['model_save_path'])

# Dynamic mapping pulled straight from JSON
LABEL_BULL = regime_map['bull_longs_only']
LABEL_CHOP = regime_map['chop_bidirectional']
LABEL_BEAR = regime_map['bear_shorts_only']

def execute_production_oracle():
    print("Executing Production HMM Labeling (100% Data)...")
    df = pd.read_csv(CSV_FILE, decimal=',', sep=',')
    df.columns = [col.strip() for col in df.columns]
    df['DateTime'] = pd.to_datetime(df['DateTime'], format="%d/%m/%Y %H:%M:%S")
    df.set_index('DateTime', inplace=True)
    df.sort_index(inplace=True)

    df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
    df['Variance'] = df['Log_Return'].rolling(window=WINDOW).std()
    df['CumDelta'] = df['Delta'].cumsum()
    df['Delta_Slope'] = (df['CumDelta'] - df['CumDelta'].shift(WINDOW)) / WINDOW
    
    df = df.dropna(subset=['Log_Return', 'Variance', 'Delta_Slope'])

    features = ['Log_Return', 'Variance', 'Delta_Slope']
    
    scaler = RobustScaler()
    X_full = scaler.fit_transform(df[features].values)

    model = GaussianHMM(n_components=N_STATES, covariance_type="full", n_iter=100, random_state=hmm_cfg['random_state'])
    model.fit(X_full)

    with open(MODEL_SAVE_PATH, "wb") as file:
        pickle.dump(model, file)

    df['Raw_State'] = model.predict(X_full)

    state_stats = df.groupby('Raw_State').agg(
        Mean_Var=('Variance', 'mean'),
        Mean_Ret=('Log_Return', 'mean')
    )

    chop_states = state_stats.sort_values(by='Mean_Var').index[:2].tolist()
    trend_states = state_stats.drop(chop_states)
    bull_states = trend_states[trend_states['Mean_Ret'] > 0].index.tolist()
    bear_states = trend_states[trend_states['Mean_Ret'] < 0].index.tolist()

    # Apply the global JSON labels dynamically
    df['Action_State'] = LABEL_CHOP 
    df.loc[df['Raw_State'].isin(bull_states), 'Action_State'] = LABEL_BULL
    df.loc[df['Raw_State'].isin(bear_states), 'Action_State'] = LABEL_BEAR
        
    export_cols = ['Close', 'Variance', 'Delta', 'Volume', 'Average buy size', 'Average sell size', 'Action_State']
    
    df[export_cols].to_csv(os.path.join(BASE_DIR, "labels_full.csv"))
    
    print("\n✅ Production Oracle Complete.")
    print(f"Total Rows Labeled: {len(df)}")
    print(f"Chop States (Mapped to {LABEL_CHOP}): {chop_states}")
    print(f"Bull States (Mapped to {LABEL_BULL}): {bull_states}")
    print(f"Bear States (Mapped to {LABEL_BEAR}): {bear_states}")

if __name__ == "__main__":
    execute_production_oracle()