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
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
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

    def insert_ml_snapshot(self, strategy_id, symbol, timestamp, payload):
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute('''
                INSERT INTO ml_features (timestamp, symbol, strategy_id, features_json) 
                VALUES (?, ?, ?, ?)
            ''', (timestamp, symbol, strategy_id, json.dumps(payload)))
            conn.commit()
            return c.lastrowid 
        except Exception as e: return None
        finally: conn.close()

    def log_trade(self, trade_dict):
        conn = self.get_connection()
        c = conn.cursor()
        
        try:
            o_time = trade_dict['open_time']
            c_time = trade_dict['close_time']
            if isinstance(o_time, datetime): o_time = o_time.strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(c_time, datetime): c_time = c_time.strftime('%Y-%m-%d %H:%M:%S')

            ml_id = trade_dict.get('ml_feature_id')
            mfe = trade_dict.get('mfe', 0.0)
            mae = trade_dict.get('mae', 0.0)
            reason = trade_dict['reason']
            
            # Extract PnL points early so both tables can use it
            pnl_in_points = trade_dict.get('pnl_points', 0.0)

            # --- INSERT EXACT HARD METRICS (NOW INCLUDING pnl_points) ---
            c.execute('''
                INSERT OR REPLACE INTO trades (
                    ticket, ml_feature_id, strategy_id, symbol, action, open_time, close_time, 
                    duration_sec, open_price, close_price, sl, tp, 
                    pnl, pnl_points, commission, swap, close_reason, mfe, mae
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                trade_dict['ticket'], ml_id, trade_dict['strategy_id'], trade_dict['symbol'], trade_dict['action'],
                o_time, c_time, trade_dict['duration'], trade_dict['open_price'], trade_dict['close_price'],
                trade_dict.get('sl', 0), trade_dict.get('tp', 0), trade_dict['net_pnl'], pnl_in_points,
                trade_dict['commission'], trade_dict['swap'], reason, mfe, mae
            ))

            # --- THE ML LOOP CLOSER ---
            if ml_id:
                c.execute('''
                    UPDATE ml_features 
                    SET trade_action = ?, trade_pnl = ?, trade_close_reason = ?, mfe = ?, mae = ?
                    WHERE id = ?
                ''', (trade_dict['action'], pnl_in_points, reason, mfe, mae, ml_id))
                print(f"🧠 ML DB: Row {ml_id} updated -> Point PnL: {pnl_in_points} pts")

            conn.commit()
        except Exception as e: print(f"Database Error: {e}")
        finally: conn.close()

    def get_todays_stats(self):
        conn = self.get_connection()
        c = conn.cursor()
        try:
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            today_str = today_start.strftime('%Y-%m-%d %H:%M:%S')
            c.execute("SELECT COUNT(*), SUM(pnl) FROM trades WHERE close_time >= ?", (today_str,))
            row = c.fetchone()
            return {"daily_pnl": row[1] if row[1] else 0.0, "trade_count": row[0] if row[0] else 0}
        except Exception: return {"daily_pnl": 0.0, "trade_count": 0}
        finally: conn.close()

    def log_equity_snapshot(self, balance, equity, open_positions, strategy_data=None):
        conn = self.get_connection()
        c = conn.cursor()
        strat_json = json.dumps(strategy_data) if strategy_data else "{}"
        try:
            c.execute('''
                INSERT INTO equity_history (timestamp, balance, equity, open_positions, strategy_performance)
                VALUES (?, ?, ?, ?, ?)
            ''', (datetime.now(), balance, equity, open_positions, strat_json))
            conn.commit()
        except Exception: pass
        finally: conn.close()

    def fetch_equity_history(self, limit=1000):
        if not os.path.exists(DB_FILE): return []
        conn = self.get_connection()
        c = conn.cursor()
        try:
            c.execute("SELECT * FROM equity_history ORDER BY timestamp DESC LIMIT ?", (limit,))
            return c.fetchall()
        finally: conn.close()

    def fetch_trades(self, strategy_id=None, limit=100):
        if not os.path.exists(DB_FILE): return []
        conn = self.get_connection()
        c = conn.cursor()
        
        # --- THE MAGIC JOIN ---
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
                
                if 'features_json' in d:
                    del d['features_json']
                    
                results.append(d)
                
            return results
        except Exception as e: 
            print(f"Fetch Error: {e}")
            return []
        finally: conn.close()