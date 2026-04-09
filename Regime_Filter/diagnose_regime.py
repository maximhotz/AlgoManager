import os
import json
import pandas as pd
import pickle
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR) if "Regime_Filter" in BASE_DIR else BASE_DIR

CONFIG_PATH = os.path.join(ROOT_DIR, "system_config.json")
with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

RF_CFG = config['ml_pipeline']['rf_classifier']
MODEL_PATH = os.path.join(ROOT_DIR, RF_CFG['model_save_path'])
CSV_PATH = os.path.join(BASE_DIR, "labels_full.csv")

def run_diagnostics():
    print("=== 1. DATA DISTRIBUTION CHECK ===")
    df = pd.read_csv(CSV_PATH)
    counts = df['Action_State'].value_counts()
    total = len(df)
    
    print(f"Total Trading Minutes: {total}")
    for state, count in counts.items():
        if state == 0: name = "LONG_ONLY (0)"
        elif state == 1: name = "CHOP (1)"
        elif state == 2: name = "SHORT_ONLY (2)"
        else: name = f"UNKNOWN ({state})"
        print(f"{name:<18}: {count} minutes ({round((count/total)*100, 1)}%)")

    print("\n=== 2. RANDOM FOREST BRAIN CHECK ===")
    if not os.path.exists(MODEL_PATH):
        print("Model file not found!")
        return

    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)

    features = RF_CFG['features']
    importances = model.feature_importances_
    
    # Sort features by importance
    indices = np.argsort(importances)[::-1]
    
    print("Top Decision Factors (What the AI is looking at):")
    for i in range(len(features)):
        print(f"{i+1}. {features[indices[i]]:<15}: {round(importances[indices[i]] * 100, 2)}%")

if __name__ == "__main__":
    run_diagnostics()