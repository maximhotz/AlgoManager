import MetaTrader5 as mt5
import json
import time
import os
import collections
import requests
import pytz
from datetime import datetime, timezone

CONFIG_FILE = "system_config.json"

def load_config():
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

def get_daily_adr(symbol, days=14):
    rates = mt5.copy_rates_from_pos(symbol, mt5.TIMEFRAME_D1, 1, days)
    if rates is None or len(rates) == 0:
        return 0
    total_range = sum([r['high'] - r['low'] for r in rates])
    return total_range / len(rates)

def fetch_tier1_news():
    print("🌐 Fetching latest Economic Calendar from Forex Factory...")
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    
    tier1_times = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code == 200:
            events = resp.json()
            
            for ev in events:
                # Target only High Impact USD events
                if ev.get('country') == 'USD' and ev.get('impact') == 'High':
                    date_str = ev.get('date') # Format: 2026-04-02T08:30:00-04:00
                    
                    if not date_str:
                        continue 
                        
                    try:
                        # Python natively parses the ISO string and calculates the timezone offset
                        dt_aware = datetime.fromisoformat(date_str)
                        event_timestamp = dt_aware.timestamp()
                        
                        # Only track it if the news hasn't happened yet
                        if event_timestamp > time.time():
                            tier1_times.append(event_timestamp)
                            local_time_str = datetime.fromtimestamp(event_timestamp).strftime('%m-%d at %H:%M:%S')
                            print(f"   📅 HIGH IMPACT NEWS LOGGED: {ev.get('title')} at {local_time_str} Local Time")
                    except ValueError as e:
                        print(f"   [ERROR] Failed to parse date: '{date_str}' - {e}")
                        continue
        else:
            print(f"❌ Forex Factory API Error: HTTP {resp.status_code}")
    except Exception as e:
        print(f"❌ News API Connection Error: {e}")
        
    
    print(f"   ✅ Calendar parsed successfully! Tracking {len(tier1_times)} upcoming Tier-1 events.")
    
    return tier1_times

def execute_hedge_and_lock(symbol, reason):
    print(f"\n🚨 EMERGENCY TRIGGERED: {reason} 🚨")
    
    positions = mt5.positions_get(symbol=symbol)
    
    config = load_config()
    if 'saved_sl_tp' not in config['risk_management']['emergency_protocols']:
        config['risk_management']['emergency_protocols']['saved_sl_tp'] = {}
    saved_stops = config['risk_management']['emergency_protocols']['saved_sl_tp']

    if not positions:
        print("No open positions to hedge. Locking system anyway.")
        config['risk_management']['emergency_protocols']['system_locked'] = True
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f, indent=2)
        return

    # 1. Wipe and save all TPs to prevent the hedge from breaking
    for pos in positions:
        if pos.tp != 0.0 or pos.sl != 0.0:
            saved_stops[str(pos.ticket)] = {"sl": pos.sl, "tp": pos.tp}
            request = {
                "action": mt5.TRADE_ACTION_SLTP,
                "position": pos.ticket,
                "symbol": pos.symbol,
                "sl": 0.0,
                "tp": 0.0
            }
            mt5.order_send(request)
            
    config['risk_management']['emergency_protocols']['saved_sl_tp'] = saved_stops

    # 2. Calculate net exposure
    net_volume = 0.0
    for pos in positions:
        if pos.type == mt5.POSITION_TYPE_BUY:
            net_volume += pos.volume
        elif pos.type == mt5.POSITION_TYPE_SELL:
            net_volume -= pos.volume

    net_volume = round(net_volume, 2)
    
    # 3. Fire the offsetting hedge order
    if net_volume != 0:
        action = mt5.ORDER_TYPE_SELL if net_volume > 0 else mt5.ORDER_TYPE_BUY
        hedge_vol = abs(net_volume)
        tick = mt5.symbol_info_tick(symbol)
        price = tick.bid if action == mt5.ORDER_TYPE_SELL else tick.ask
        
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": hedge_vol,
            "type": action,
            "price": price,
            "magic": 999999, 
            "comment": "EMERGENCY HEDGE",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        
        result = mt5.order_send(request)
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            print(f"🛡️ HEDGE SUCCESSFUL: Opened {hedge_vol} Lots to neutralize exposure.")
        else:
            print(f"❌ HEDGE FAILED: {result.comment}")

    # 4. Lock the global config
    config['risk_management']['emergency_protocols']['system_locked'] = True
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)
    print("🔒 CONFIG UPDATED: System LOCKED and TPs stripped.")

def run_safety_watcher():
    config = load_config()
    path = config['system'].get('mt5_terminal_path')
    if not mt5.initialize(path=path):
        print("Watcher failed to connect to MT5")
        return

    # 1. Flash Crash Parameters
    symbol = config['strategies']['QT_Velocity']['symbol']
    fc_params = config['risk_management']['emergency_protocols']['flash_crash_watcher']
    window_sec = fc_params['evaluation_window_seconds']
    adr_fraction = fc_params['adr_fraction_threshold']
    
    # 2. News Parameters
    news_params = config['risk_management']['emergency_protocols']['news_watcher']
    news_enabled = news_params['enabled']
    flatten_minutes = news_params['flatten_minutes_before_tier1']

    print(f"👁️ Safety Watcher Online. Monitoring {symbol} for anomalies...")

    # Initialize ADR Baseline
    adr = get_daily_adr(symbol, fc_params['adr_days'])
    panic_point_threshold = adr * adr_fraction
    print(f"📊 {fc_params['adr_days']}-Day ADR: {adr:.2f} pts. | Panic Threshold: {panic_point_threshold:.2f} pts in {window_sec}s.")

    # State variables
    price_history = collections.deque()
    last_news_fetch_time = 0  # Changed from daily tracking to timestamp tracking
    NEWS_FETCH_INTERVAL_SEC = 4 * 3600  # 4 hours in seconds
    tier1_timestamps = []

    while True:
        try:
            now_ts = time.time()
            now_utc = datetime.now(timezone.utc)

            # Check if system is already locked by user or previous emergency
            live_config = load_config()
            if live_config['risk_management']['emergency_protocols'].get('system_locked', False):
                time.sleep(5) 
                continue

            # --- NEWS API INTRADAY REFRESH ---
            if news_enabled and (now_ts - last_news_fetch_time) >= NEWS_FETCH_INTERVAL_SEC:
                tier1_timestamps = fetch_tier1_news()
                last_news_fetch_time = now_ts

            # --- NEWS BLACKOUT CHECK ---
            if news_enabled:
                for news_ts in tier1_timestamps[:]: 
                    if news_ts - (flatten_minutes * 60) <= now_ts < news_ts:
                        event_time_str = datetime.fromtimestamp(news_ts).strftime('%H:%M:%S')
                        execute_hedge_and_lock(symbol, f"Tier-1 News Blackout Approaching (Event at {event_time_str})")
                        tier1_timestamps.remove(news_ts) 
                        break 

            # --- FLASH CRASH CHECK ---
            tick = mt5.symbol_info_tick(symbol)
            if not tick:
                time.sleep(0.1)
                continue

            current_price = (tick.bid + tick.ask) / 2.0
            price_history.append((now_ts, current_price))

            while price_history and now_ts - price_history[0][0] > window_sec:
                price_history.popleft()

            if len(price_history) > 1:
                oldest_price = price_history[0][1]
                price_delta = abs(current_price - oldest_price)

                if price_delta >= panic_point_threshold:
                    execute_hedge_and_lock(symbol, f"Flash Crash Detected! Moved {price_delta:.2f} pts in <= {window_sec}s")
                    price_history.clear() 

            time.sleep(0.1) 

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Watcher Error: {e}")
            time.sleep(1)

    mt5.shutdown()

if __name__ == "__main__":
    run_safety_watcher()