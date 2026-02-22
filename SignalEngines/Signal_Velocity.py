import zmq
import MetaTrader5 as mt5
import json
import time
import pandas as pd
import numpy as np
import os
import sys
import gc 
from datetime import datetime, timedelta

# --- IDENTITY ---
MY_STRATEGY_ID = "SPEED_US500_01"

# --- PATH FINDER ---
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path_1 = os.path.join(script_dir, "..", "system_config.json")
config_path_2 = os.path.join(script_dir, "system_config.json")

if os.path.exists(config_path_1): CONFIG_FILE = config_path_1
elif os.path.exists(config_path_2): CONFIG_FILE = config_path_2
else:
    print("CRITICAL ERROR: Config file not found.")
    time.sleep(10)
    sys.exit(1)

def load_config():
    with open(CONFIG_FILE, "r") as f:
        data = json.load(f)
        return data['strategies'][MY_STRATEGY_ID], data['system']

def calculate_rsi_series(prices, period=14):
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_volatility_tp(ticks_array, limits):
    if not limits.get('use_volatility_based_tp', False): return limits.get('tp_points', 1.0)
    lookback_ms = limits.get('volatility_lookback_sec', 60) * 1000
    current_msc = ticks_array[-1]['time_msc']
    cutoff_msc = current_msc - lookback_ms
    
    start_idx = np.searchsorted(ticks_array['time_msc'], cutoff_msc)
    if start_idx >= len(ticks_array): return limits.get('tp_points', 1.0)
    
    recent_vol = ticks_array[start_idx:]
    if len(recent_vol) == 0: return limits.get('tp_points', 1.0)
    
    high = np.max(recent_vol['ask'])
    low = np.min(recent_vol['ask'])
    multiplier = limits.get('tp_volatility_multiplier', 0.5)
    return float(max(limits.get('min_tp_points', 0.5), min((high - low) * multiplier, limits.get('max_tp_points', 5.0))))

def calibrate_time_specific_threshold(symbol, time_window_sec, lookback_days, percentile, time_slice_minutes):
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        return None
        
    server_now = datetime.fromtimestamp(tick.time)
    deltas = []
    
    for i in range(lookback_days):
        target_time = server_now - timedelta(days=i)
        slice_start = target_time - timedelta(minutes=time_slice_minutes)
        slice_end = target_time + timedelta(minutes=time_slice_minutes)
        ticks = mt5.copy_ticks_range(symbol, slice_start, slice_end, mt5.COPY_TICKS_ALL)
        if ticks is not None and len(ticks) > 10:
            df = pd.DataFrame(ticks)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            window_str = f"{time_window_sec}s"
            df['window'] = df['time'].dt.floor(window_str)
            for window, group in df.groupby('window'):
                if len(group) > 1:
                    delta = abs(group['ask'].iloc[-1] - group['ask'].iloc[0])
                    deltas.append(delta)
    if not deltas: return None
    deltas_series = pd.Series(deltas)
    return float(deltas_series.quantile(percentile))

def get_inventory_skew(symbol, magic, lookback_minutes):
    positions = mt5.positions_get(symbol=symbol)
    if positions is None or len(positions) == 0:
        return 0, 0, 0
        
    tick = mt5.symbol_info_tick(symbol)
    if tick is None: 
        return 0, 0, 0
        
    current_server_time = tick.time
    cutoff_timestamp = current_server_time - (lookback_minutes * 60)
    
    recent_longs = 0
    recent_shorts = 0
    
    for pos in positions:
        if pos.magic == magic and pos.time >= cutoff_timestamp:
            if pos.type == mt5.ORDER_TYPE_BUY:
                recent_longs += 1
            elif pos.type == mt5.ORDER_TYPE_SELL:
                recent_shorts += 1
                
    skew = recent_longs - recent_shorts
    return skew, recent_longs, recent_shorts

def run_speed_engine():
    try: my_conf, sys_conf = load_config()
    except: return

    SYMBOL = my_conf['symbol']
    MAGIC = my_conf['magic_number']
    PARAMS = my_conf['parameters']
    
    TIME_WINDOW_SEC = PARAMS['time_window_sec']
    COOLDOWN_SEC = PARAMS['cooldown_sec']
    FALLBACK_THRESHOLD = PARAMS['fallback_threshold']
    USE_DYNAMIC_THRESHOLD = PARAMS.get('use_dynamic_threshold', False)
    
    CALIB_CONF = PARAMS.get('calibration', {})
    CALIB_DAYS = CALIB_CONF.get('lookback_days', 10)
    CALIB_PERCENTILE = CALIB_CONF.get('percentile', 0.95)
    CALIB_INTERVAL_MIN = CALIB_CONF.get('recalibrate_minutes', 2) 
    CALIB_SLICE_MIN = CALIB_CONF.get('time_slice_minutes', 30)
    
    USE_RSI = PARAMS.get('use_rsi_filter', False)
    RSI_PERIOD = PARAMS.get('rsi_period', 14)
    RSI_UPPER = PARAMS.get('rsi_upper', 65) 
    RSI_LOWER = PARAMS.get('rsi_lower', 35) 
    RSI_ANCHOR_MINUTES = PARAMS.get('rsi_rolling_window_minutes', 60)
    SELECTED_TF = mt5.TIMEFRAME_M1
    
    DYN_CONF = PARAMS.get('dynamic_sizing', {})
    USE_DYN_SIZE = DYN_CONF.get('enabled', False)
    SKEW_LOOKBACK = DYN_CONF.get('lookback_minutes', 60)
    BASE_VOL = DYN_CONF.get('base_volume', my_conf['volume'])
    
    GRIND_THR = DYN_CONF.get('grind_zone', {}).get('skew_threshold', 5)
    GRIND_WITH_TREND = DYN_CONF.get('grind_zone', {}).get('with_trend_volume', 0.08)
    GRIND_FADE_TREND = DYN_CONF.get('grind_zone', {}).get('fade_trend_volume', 0.02)
    
    RUNAWAY_THR = DYN_CONF.get('runaway_zone', {}).get('skew_threshold', 10)
    RUNAWAY_WITH_TREND = DYN_CONF.get('runaway_zone', {}).get('with_trend_volume', 0.12)
    RUNAWAY_FADE_TREND = DYN_CONF.get('runaway_zone', {}).get('fade_trend_volume', 0.0)

    # --- PROGRESSIVE COOLDOWN CONFIG ---
    PROG_COOLDOWN_CONF = PARAMS.get('progressive_cooldown', {})

    TRADE_LIMITS = my_conf['trade_limits']
    USE_VOL_TP = TRADE_LIMITS.get('use_volatility_based_tp', False)
    TP_POINTS = TRADE_LIMITS.get('tp_points', 1.0)

    if not mt5.initialize(): sys.exit(1)
    if not mt5.symbol_select(SYMBOL, True): sys.exit(1)

    if USE_DYNAMIC_THRESHOLD:
        print(f"init Sniper Calibration (Days:{CALIB_DAYS}, Slice:+/-{CALIB_SLICE_MIN}m)...", end="", flush=True)
        calib_success = False
        for attempt in range(10):
            calibrated = calibrate_time_specific_threshold(SYMBOL, TIME_WINDOW_SEC, CALIB_DAYS, CALIB_PERCENTILE, CALIB_SLICE_MIN)
            if calibrated: 
                FALLBACK_THRESHOLD = calibrated
                print(f" Done. Thr: {FALLBACK_THRESHOLD:.5f}")
                calib_success = True
                break
            else:
                print(".", end="", flush=True)
                time.sleep(3)
        if not calib_success: print(" Failed (Timeout). Using Fallback.")

    zmq_host = sys_conf['zmq_host']
    zmq_port = sys_conf['zmq_port']
    context = zmq.Context()
    
    def connect_zmq():
        s = context.socket(zmq.REQ)
        s.connect(f"tcp://{zmq_host}:{zmq_port}")
        s.setsockopt(zmq.RCVTIMEO, 2000) 
        s.setsockopt(zmq.LINGER, 0)
        return s

    socket = connect_zmq()
    print(f"✓ ZMQ connected. Strategy: {MY_STRATEGY_ID}")
    print(f"✓ SPEED: HFT Millisecond Core | DYNAMIC SIZING: {'ON' if USE_DYN_SIZE else 'OFF'}")

    last_processed_tick_msc = 0
    last_slow_check = 0
    last_calibration_time = time.time()
    RECALIBRATE_INTERVAL_SEC = CALIB_INTERVAL_MIN * 60

    current_rsi = 50.0
    anchor_rsi = 50.0 
    skew = 0
    recent_longs = 0
    recent_shorts = 0
    buy_vol = BASE_VOL
    sell_vol = BASE_VOL
    skew_state = "CHOP"
    
    signal_count = 0

    try:
        while True:
            if USE_DYNAMIC_THRESHOLD and (time.time() - last_calibration_time > RECALIBRATE_INTERVAL_SEC):
                new_threshold = calibrate_time_specific_threshold(SYMBOL, TIME_WINDOW_SEC, CALIB_DAYS, CALIB_PERCENTILE, CALIB_SLICE_MIN)
                if new_threshold: FALLBACK_THRESHOLD = new_threshold
                last_calibration_time = time.time()
                gc.collect() 

            server_tick = mt5.symbol_info_tick(SYMBOL)
            if server_tick is None:
                time.sleep(1)
                continue
            
            server_now = datetime.fromtimestamp(server_tick.time)
            from_time = server_now - timedelta(seconds=15) 
            to_time = server_now + timedelta(seconds=10)
            
            ticks = mt5.copy_ticks_range(SYMBOL, from_time, to_time, mt5.COPY_TICKS_ALL)
            
            if ticks is not None and len(ticks) > 1:
                
                new_ticks_mask = ticks['time_msc'] > last_processed_tick_msc
                new_ticks = ticks[new_ticks_mask]
                
                if len(new_ticks) == 0:
                    time.sleep(0.005)
                    continue

                for i in range(len(new_ticks)):
                    current_tick = new_ticks[i]
                    current_msc = current_tick['time_msc']
                    last_processed_tick_msc = current_msc

                    # --- SLOW CHECKS ---
                    if i == len(new_ticks) - 1:
                        if time.time() - last_slow_check > 2:
                            if USE_RSI:
                                fetch_count = max(100, RSI_ANCHOR_MINUTES + 10)
                                rates = mt5.copy_rates_from_pos(SYMBOL, SELECTED_TF, 0, fetch_count)
                                if rates is not None and len(rates) > RSI_PERIOD:
                                    df_rates = pd.DataFrame(rates)
                                    rsi_series = calculate_rsi_series(df_rates['close'], RSI_PERIOD)
                                    current_rsi = rsi_series.iloc[-1]
                                    anchor_rsi = rsi_series.iloc[-min(len(rsi_series)-1, RSI_ANCHOR_MINUTES)]
                            
                            if USE_DYN_SIZE:
                                skew, recent_longs, recent_shorts = get_inventory_skew(SYMBOL, MAGIC, SKEW_LOOKBACK)
                                buy_vol = BASE_VOL
                                sell_vol = BASE_VOL
                                skew_state = "CHOP (Balanced)"
                                
                                if skew <= -RUNAWAY_THR:
                                    skew_state = "RUNAWAY UP"
                                    buy_vol = RUNAWAY_WITH_TREND
                                    sell_vol = RUNAWAY_FADE_TREND
                                elif skew <= -GRIND_THR:
                                    skew_state = "GRIND UP"
                                    buy_vol = GRIND_WITH_TREND
                                    sell_vol = GRIND_FADE_TREND
                                elif skew >= RUNAWAY_THR:
                                    skew_state = "RUNAWAY DOWN"
                                    buy_vol = RUNAWAY_FADE_TREND
                                    sell_vol = RUNAWAY_WITH_TREND
                                elif skew >= GRIND_THR:
                                    skew_state = "GRIND DOWN"
                                    buy_vol = GRIND_FADE_TREND
                                    sell_vol = GRIND_WITH_TREND

                            last_slow_check = time.time()
                            gc.collect() 

                    # --- FAST CHECKS ---
                    cutoff_msc = current_msc - (TIME_WINDOW_SEC * 1000)
                    start_idx = np.searchsorted(ticks['time_msc'], cutoff_msc)
                    
                    if start_idx < len(ticks):
                        tick_start = ticks[start_idx]
                        tick_end = current_tick
                        
                        if tick_start['time_msc'] > current_msc:
                            continue

                        delta = tick_end['ask'] - tick_start['ask']
                        
                        if USE_RSI:
                            bias = "NEUTRAL (Block)"
                            if anchor_rsi > RSI_UPPER: bias = "HOT (Allow Sell)"
                            elif anchor_rsi < RSI_LOWER: bias = "COLD (Allow Buy)"
                            rsi_txt = f"Curr:{current_rsi:.1f} | Past(-{RSI_ANCHOR_MINUTES}m):{anchor_rsi:.1f} [{bias}]"
                        else:
                            rsi_txt = "OFF"
                            
                        skew_txt = f"Skew:{skew:+d} (L:{recent_longs} S:{recent_shorts}) [{skew_state}]" if USE_DYN_SIZE else "Size: STATIC"
                        
                        if i == len(new_ticks) - 1:
                            print(f"Thr:{FALLBACK_THRESHOLD:.3f} | Speed:{delta:+.3f} | {rsi_txt} | {skew_txt}       ", end='\r', flush=True)

                        if abs(delta) > FALLBACK_THRESHOLD:
                            action = "SELL" if delta > 0 else "BUY"
                            is_valid = True 
                            
                            if USE_RSI:
                                if action == "SELL" and anchor_rsi <= RSI_UPPER: is_valid = False
                                elif action == "BUY" and anchor_rsi >= RSI_LOWER: is_valid = False

                            target_vol = buy_vol if action == "BUY" else sell_vol
                            
                            if target_vol <= 0.0:
                                is_valid = False
                                ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                                print(f"\n[{ts}] 🛑 SIGNAL BLOCKED: {action} blocked by {skew_state} protective filter.", flush=True)

                            if is_valid:
                                # --- PROGRESSIVE COOLDOWN LOGIC (Independent Window) ---
                                effective_cooldown = COOLDOWN_SEC
                                same_dir_count = 0
                                
                                if PROG_COOLDOWN_CONF.get('enabled', False):
                                    cooldown_lookback = PROG_COOLDOWN_CONF.get('lookback_minutes', 60)
                                    # Convert current millisecond tick to Server seconds
                                    cutoff_time = (current_msc / 1000.0) - (cooldown_lookback * 60)
                                    
                                    open_positions = mt5.positions_get(symbol=SYMBOL)
                                    if open_positions:
                                        target_type = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL
                                        for p in open_positions:
                                            # Ensure we match strategy, direction, AND the isolated lookback window
                                            if p.magic == MAGIC and p.type == target_type and p.time >= cutoff_time:
                                                same_dir_count += 1
                                                
                                    tiers = sorted(PROG_COOLDOWN_CONF.get('tiers', []), key=lambda x: x['open_trades'], reverse=True)
                                    for tier in tiers:
                                        if same_dir_count >= tier['open_trades']:
                                            effective_cooldown = tier['cooldown_sec']
                                            break

                                tp = calculate_volatility_tp(ticks, TRADE_LIMITS) if USE_VOL_TP else TP_POINTS
                                signal_count += 1
                                
                                t_start_str = pd.to_datetime(tick_start['time_msc'], unit='ms').strftime('%H:%M:%S.%f')[:-3]
                                t_end_str = pd.to_datetime(tick_end['time_msc'], unit='ms').strftime('%H:%M:%S.%f')[:-3]
                                
                                print("\n\n" + "="*55)
                                print("🚨 SIGNAL AUDIT RECEIPT 🚨")
                                print(f"Action:      {action} (Signal #{signal_count})")
                                print(f"Time Window: {TIME_WINDOW_SEC} Seconds")
                                print(f"Start Price: {tick_start['ask']:.3f} (at {t_start_str})")
                                print(f"End Price:   {tick_end['ask']:.3f} (at {t_end_str})")
                                print(f"Math:        {tick_end['ask']:.3f} - {tick_start['ask']:.3f} = {delta:+.3f}")
                                print(f"Threshold:   {FALLBACK_THRESHOLD:.3f} (Valid: {abs(delta):.3f} > {FALLBACK_THRESHOLD:.3f})")
                                if USE_RSI:
                                    print(f"Anchor RSI:  {anchor_rsi:.1f} (Allowed: {'<' if action=='BUY' else '>'} {RSI_LOWER if action=='BUY' else RSI_UPPER})")
                                if USE_DYN_SIZE:
                                    print(f"Skew State:  {skew_state}")
                                    print(f"Recent Skew: {skew:+d} (Stranded L: {recent_longs} | S: {recent_shorts})")
                                print(f"Volume:      {target_vol:.2f} lots")
                                print(f"Dynamic TP:  {tp:.2f} points")
                                
                                # Dynamic Output for Progressive Cooldown
                                if PROG_COOLDOWN_CONF.get('enabled', False):
                                    print(f"Cooldown:    {effective_cooldown}s (Open in last {PROG_COOLDOWN_CONF.get('lookback_minutes', 60)}m: {same_dir_count})")
                                else:
                                    print(f"Cooldown:    {effective_cooldown}s (Static)")
                                
                                print("="*55 + "\n", flush=True)
                                
                                payload = {
                                    "strategy_id": MY_STRATEGY_ID, "symbol": SYMBOL, "action": action, 
                                    "dynamic_tp": tp, "volume": target_vol,
                                    "extra_metrics": {"rsi": current_rsi, "speed": delta, "magic": MAGIC, "skew": skew}
                                }
                                
                                try:
                                    socket.send_json(payload)
                                    print(f"        Manager: {socket.recv_string()}", flush=True)
                                except (zmq.Again, zmq.ZMQError):
                                    print(f"        ⚠️ Comms Error. Resetting...", flush=True)
                                    socket.close()
                                    socket = connect_zmq()
                                
                                time.sleep(effective_cooldown)
                                break 

            time.sleep(0.005)

    except KeyboardInterrupt: print("\nStopped.")
    finally: mt5.shutdown(); socket.close(); context.term()

if __name__ == "__main__":
    run_speed_engine()