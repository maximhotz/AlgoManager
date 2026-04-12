import sqlite3
import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_FILE = os.path.join(BASE_DIR, "trading_system.db")
SCHEMA_FILE = os.path.join(BASE_DIR, "components", "schema.sql")

class Database:
    def __init__(self):
        self.conn = None
        self.initialize()

    def get_connection(self):
        # --- FIX: Added 15s timeout and WAL mode for simultaneous read/write ---
        conn = sqlite3.connect(DB_FILE, timeout=15.0, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        if not os.path.exists(SCHEMA_FILE): return
        with open(SCHEMA_FILE, 'r') as f: schema_script = f.read()
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.executescript(schema_script)
            try: c.execute("SELECT strategy_performance FROM equity_history LIMIT 1")
            except sqlite3.OperationalError: c.execute("ALTER TABLE equity_history ADD COLUMN strategy_performance TEXT")
            conn.commit()
        except Exception as e: print(f"DB Init Error: {e}")
        finally: conn.close()

    # --- HFT UPGRADE: Added explicit_id support ---
    def insert_ml_snapshot(self, strategy_id, symbol, timestamp_ms, payload, explicit_id=None):
        try:
            conn = self.get_connection()
            c = conn.cursor()
            json_str = json.dumps(payload)
            
            if explicit_id:
                c.execute("""
                    INSERT INTO ml_features (id, strategy_id, symbol, timestamp, features_json)
                    VALUES (?, ?, ?, ?, ?)
                """, (explicit_id, strategy_id, symbol, timestamp_ms, json_str))
                inserted_id = explicit_id
            else:
                c.execute("""
                    INSERT INTO ml_features (strategy_id, symbol, timestamp, features_json)
                    VALUES (?, ?, ?, ?)
                """, (strategy_id, symbol, timestamp_ms, json_str))
                inserted_id = c.lastrowid
                
            conn.commit()
            conn.close()
            return inserted_id
        except Exception as e:
            print(f"Database Error (Insert ML): {e}")
            return None

    def log_trade(self, trade_data):
        try:
            conn = self.get_connection()
            c = conn.cursor()
            c.execute("""
                INSERT INTO trades (
                    ticket, ml_feature_id, strategy_id, symbol, action, 
                    open_time, close_time, duration_sec, open_price, close_price, 
                    sl, tp, pnl, pnl_points, commission, swap, close_reason, mfe, mae
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_data['ticket'], trade_data.get('ml_feature_id'), trade_data['strategy_id'],
                trade_data['symbol'], trade_data['action'], trade_data['open_time'],
                trade_data['close_time'], trade_data['duration'], trade_data['open_price'],
                trade_data['close_price'], trade_data['sl'], trade_data['tp'], trade_data['net_pnl'],
                trade_data['pnl_points'], trade_data['commission'], trade_data['swap'],
                trade_data['reason'], trade_data.get('mfe', 0.0), trade_data.get('mae', 0.0)
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Database Error (Log Trade): {e}")

    def log_equity_snapshot(self, balance, equity, open_positions, strategy_pl_dict):
        try:
            conn = self.get_connection()
            c = conn.cursor()
            strat_json = json.dumps(strategy_pl_dict)
            c.execute("""
                INSERT INTO equity_history (timestamp, balance, equity, open_positions, strategy_performance)
                VALUES (?, ?, ?, ?, ?)
            """, (datetime.now(), balance, equity, open_positions, strat_json))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Database Error (Log Equity): {e}")

    def fetch_recent_trades_with_features(self, limit=50, strategy_id=None):
        conn = self.get_connection()
        c = conn.cursor()
        
        query = '''
            SELECT t.*, m.features_json 
            FROM trades t
            LEFT JOIN ml_features m ON t.ml_feature_id = m.id
        '''
        params = []
        if strategy_id:
            query += " WHERE t.strategy_id = ?"
            params.append(strategy_id)
        query += " ORDER BY t.close_time DESC LIMIT ?"
        params.append(limit)
        
        try:
            c.execute(query, params)
            rows = c.fetchall()
            results = []
            
            for row in rows:
                d = dict(row)
                pseudo_meta = {}
                
                if d.get('features_json'):
                    feat = json.loads(d['features_json'])
                    trigger = feat.get('trigger', {})
                    context = feat.get('context', {})
                    
                    pseudo_meta['speed'] = trigger.get('speed_delta')
                    pseudo_meta['absorption'] = trigger.get('absorption_ratio')
                    pseudo_meta['vwap_dist'] = context.get('vwap_dist_pct')
                
                d['meta_json'] = pseudo_meta
                if 'features_json' in d: del d['features_json']
                results.append(d)
                
            return results
        except Exception as e: 
            print(f"Fetch Error: {e}")
            return []
        finally:
            conn.close()

    def fetch_trades(self, limit=1000):
        return self.fetch_recent_trades_with_features(limit=limit)

    def fetch_equity_history(self, limit=2880):
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute("SELECT * FROM equity_history ORDER BY timestamp DESC LIMIT ?", (limit,))
            return c.fetchall()
        except Exception as e:
            print(f"DB Error (fetch_equity_history): {e}")
            return []
        finally:
            conn.close()

    def log_regime(self, timestamp, regime, name):
        try:
            conn = self.get_connection()
            c = conn.cursor()
            c.execute("INSERT INTO regime_history VALUES (?, ?, ?)", (timestamp, regime, name))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Database Error (Log Regime): {e}")

    def fetch_regimes(self, limit=300):
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute("SELECT * FROM regime_history ORDER BY timestamp DESC LIMIT ?", (limit,))
            rows = c.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            print(f"DB Error (fetch_regimes): {e}")
            return []
        finally:
            conn.close()