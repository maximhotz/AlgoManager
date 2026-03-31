CREATE TABLE IF NOT EXISTS trades (
    ticket INTEGER PRIMARY KEY,
    ml_feature_id INTEGER,  
    strategy_id TEXT,
    symbol TEXT,
    action TEXT,
    open_time TIMESTAMP,
    close_time TIMESTAMP,
    duration_sec REAL,
    open_price REAL,
    close_price REAL,
    sl REAL,
    tp REAL,
    pnl REAL,              
    pnl_points REAL,       
    commission REAL,
    swap REAL,
    close_reason TEXT,
    mfe REAL,              
    mae REAL                
);

CREATE TABLE IF NOT EXISTS equity_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP,
    balance REAL,
    equity REAL,
    open_positions INTEGER,
    strategy_performance TEXT
);

-- CLEANED UP: Only the core ID, raw JSON payload, and the AI's Target Label
CREATE TABLE IF NOT EXISTS ml_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp INTEGER,
    symbol TEXT,
    strategy_id TEXT,
    features_json TEXT,     
    target_label INTEGER    
);