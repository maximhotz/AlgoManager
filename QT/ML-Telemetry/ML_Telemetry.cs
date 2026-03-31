// Copyright QUANTOWER LLC. © 2017-2023. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TradingPlatform.BusinessLayer;
using NetMQ;
using NetMQ.Sockets;
using Newtonsoft.Json;

namespace ML_Telemetry
{
    public class ML_Telemetry : Strategy
    {
        // ==========================================
        // UI SETTINGS
        // ==========================================
        [InputParameter("Symbol", 10)]
        public Symbol symbol;

        [InputParameter("Vol Multiplier (% of 5m Range)", 20)]
        public double VolMultiplier = 0.30;

        [InputParameter("Minimum Spike Floor (Pts)", 21)]
        public double MinSpikeFloor = 1.0;

        [InputParameter("Time Window (ms)", 30)]
        public int TimeWindowMs = 2000;

        [InputParameter("Cooldown (ms)", 32)]
        public int CooldownMs = 5000;

        [InputParameter("VWAP Reset Hour (UTC)", 35)]
        public int VwapResetHourUtc = 22;

        [InputParameter("ZMQ Address", 40)]
        public string ZmqAddress = "tcp://localhost:5556";
        // ==========================================

        private RequestSocket _zmqSocket;

        // --- MEMORY BANKS ---
        private Queue<Last> _tickHistory;
        private Queue<double> _tickImbalanceHistory;
        private Queue<Last> _oneMinHistory;
        private Queue<Last> _fiveMinHistory;

        private bool _isZmqConnected = false;
        private object _lockObject = new object();

        // VWAP & Trigger Variables
        private double _cumulativePriceVolume = 0;
        private double _cumulativeVolume = 0;
        private DateTime _lastVwapReset = DateTime.MinValue;
        private DateTime _lastTriggerTime = DateTime.MinValue;

        // Dynamic Threshold tracking
        private int _lastCalcSecond = -1;
        private double _dynamicSpeedThreshold = 2.0;

        // --- MACRO FEATURE TRACKERS ---
        private double _sma1h = 0;
        private double _atr1h = 0;
        private double _dailyOpen = 0;
        private bool _runMacroUpdater = true;

        public ML_Telemetry() : base()
        {
            this.Name = "ML_Telemetry";
            this.Description = "L2 Bot with AI Macro Anchors";
        }

        protected override void OnCreated()
        {
            _tickHistory = new Queue<Last>();
            _tickImbalanceHistory = new Queue<double>();
            _oneMinHistory = new Queue<Last>();
            _fiveMinHistory = new Queue<Last>();
        }

        protected override void OnRun()
        {
            if (this.symbol == null) return;
            this.symbol = Core.GetSymbol(this.symbol.CreateInfo());
            if (this.symbol == null) return;

            Log($"✓ Symbol {this.symbol.Name} found and locked.", StrategyLoggingLevel.Trading);

            PrimeVwapData();

            // Start the 60-second background updater
            _runMacroUpdater = true;
            Task.Run(MacroUpdaterLoop);

            this.symbol.NewLast += SymbolOnNewLast;
            this.symbol.NewLevel2 += SymbolOnNewLevel2;

            Task.Run(() =>
            {
                try
                {
                    _zmqSocket = new RequestSocket();
                    _zmqSocket.Connect(ZmqAddress);
                    _isZmqConnected = true;
                    Log($"✓ ZMQ Connected to Python ML Brain on {ZmqAddress}", StrategyLoggingLevel.Trading);
                }
                catch (Exception ex)
                {
                    Log($"ZMQ Init Error: {ex.Message}", StrategyLoggingLevel.Error);
                }
            });
        }

        protected override void OnStop()
        {
            if (this.symbol != null)
            {
                this.symbol.NewLast -= SymbolOnNewLast;
                this.symbol.NewLevel2 -= SymbolOnNewLevel2;
            }

            _runMacroUpdater = false;
            _isZmqConnected = false;
            _zmqSocket?.Dispose();
            NetMQConfig.Cleanup(false);
            Log("ZMQ Disconnected.", StrategyLoggingLevel.Trading);
        }

        protected override void OnRemove()
        {
            this.symbol = null;
        }

        // ==========================================
        // 🔄 BACKGROUND MACRO UPDATER 
        // ==========================================
        private async Task MacroUpdaterLoop()
        {
            while (_runMacroUpdater && this.symbol != null)
            {
                try
                {
                    UpdateMacroFeatures();
                }
                catch (Exception ex)
                {
                    Log($"Macro Updater Error: {ex.Message}", StrategyLoggingLevel.Error);
                }
                await Task.Delay(60000);
            }
        }

        private void UpdateMacroFeatures()
        {
            DateTime now = Core.Instance.TimeUtils.DateTimeUtcNow;

            // 1. Get Daily Open
            using (var d1History = this.symbol.GetHistory(Period.DAY1, now.AddDays(-3), now))
            {
                if (d1History != null && d1History.Count > 0)
                {
                    var candles = new List<IHistoryItem>();
                    foreach (IHistoryItem item in d1History) candles.Add(item);

                    var latestDaily = candles.OrderByDescending(x => x.TimeLeft).FirstOrDefault();
                    if (latestDaily != null) _dailyOpen = latestDaily[PriceType.Open];
                }
            }

            // 2. Get 1H History (Pull 10 days to ensure enough candles for smooth ATR)
            using (var h1History = this.symbol.GetHistory(Period.HOUR1, now.AddDays(-10), now))
            {
                if (h1History != null && h1History.Count >= 20)
                {
                    var candles = new List<IHistoryItem>();
                    foreach (IHistoryItem item in h1History) candles.Add(item);

                    // Force sorting: Newest candles at index 0, 1, 2...
                    var newestCandles = candles.OrderByDescending(x => x.TimeLeft).ToList();

                    // --- Calculate 1H SMA (20-period) ---
                    double sumClose = 0;
                    int smaCount = Math.Min(20, newestCandles.Count);
                    for (int i = 0; i < smaCount; i++)
                    {
                        sumClose += newestCandles[i][PriceType.Close];
                    }
                    _sma1h = sumClose / smaCount;

                    // --- Calculate 1H ATR (14-period Wilder's Smoothing) ---
                    int atrCount = Math.Min(100, newestCandles.Count);
                    var atrCandles = newestCandles.Take(atrCount).ToList();

                    // Reverse to chronological order (Oldest first) for correct smoothing math
                    atrCandles.Reverse();

                    double currentAtr = 0;
                    for (int i = 1; i < atrCandles.Count; i++)
                    {
                        double high = atrCandles[i][PriceType.High];
                        double low = atrCandles[i][PriceType.Low];
                        double prevClose = atrCandles[i - 1][PriceType.Close];

                        double tr1 = high - low;
                        double tr2 = Math.Abs(high - prevClose);
                        double tr3 = Math.Abs(low - prevClose);
                        double tr = Math.Max(tr1, Math.Max(tr2, tr3));

                        if (i == 1)
                        {
                            currentAtr = tr;
                        }
                        else
                        {
                            currentAtr = ((currentAtr * 13) + tr) / 14.0; // Wilder's Smoothing
                        }
                    }
                    if (currentAtr > 0) _atr1h = currentAtr;
                }
            }
        }

        // ==========================================

        private void PrimeVwapData()
        {
            try
            {
                DateTime now = Core.Instance.TimeUtils.DateTimeUtcNow;
                DateTime startTime = new DateTime(now.Year, now.Month, now.Day, VwapResetHourUtc, 0, 0, DateTimeKind.Utc);

                if (startTime > now) startTime = startTime.AddDays(-1);

                using (var history = this.symbol.GetHistory(Period.MIN1, startTime, now))
                {
                    _cumulativePriceVolume = 0;
                    _cumulativeVolume = 0;

                    if (history != null)
                    {
                        foreach (IHistoryItem item in history)
                        {
                            double close = item[PriceType.Close];
                            double volume = item[PriceType.Volume];

                            _cumulativeVolume += volume;
                            _cumulativePriceVolume += (close * volume);
                        }
                    }
                }

                _lastVwapReset = startTime;
                Log($"✅ VWAP Primed! Started tracking from {startTime:HH:mm} UTC.", StrategyLoggingLevel.Trading);
            }
            catch (Exception ex)
            {
                Log($"Warning: Could not prime historical VWAP: {ex.Message}", StrategyLoggingLevel.Error);
            }
        }

        private void SymbolOnNewLast(Symbol currentSymbol, Last last)
        {
            lock (_lockObject)
            {
                // 1. VWAP CALCULATION
                if (last.Time.Hour == VwapResetHourUtc && (last.Time - _lastVwapReset).TotalHours > 12)
                {
                    _cumulativePriceVolume = 0;
                    _cumulativeVolume = 0;
                    _lastVwapReset = last.Time;
                }

                _cumulativeVolume += last.Size;
                _cumulativePriceVolume += (last.Price * last.Size);
                double currentVwap = _cumulativeVolume > 0 ? (_cumulativePriceVolume / _cumulativeVolume) : last.Price;

                // 2. 1-MINUTE OHLC QUEUE
                _oneMinHistory.Enqueue(last);
                DateTime oneMinCutoff = last.Time.AddMinutes(-1);
                while (_oneMinHistory.Count > 0 && _oneMinHistory.Peek().Time < oneMinCutoff)
                    _oneMinHistory.Dequeue();

                // 3. 5-MINUTE OHLC QUEUE
                _fiveMinHistory.Enqueue(last);
                DateTime fiveMinCutoff = last.Time.AddMinutes(-5);
                while (_fiveMinHistory.Count > 0 && _fiveMinHistory.Peek().Time < fiveMinCutoff)
                    _fiveMinHistory.Dequeue();

                // 4. GET CURRENT IMBALANCE SNAPSHOT
                double currentImbalance = 0;
                var dom = this.symbol.DepthOfMarket.GetDepthOfMarketAggregatedCollections();
                if (dom != null && dom.Bids != null && dom.Asks != null)
                {
                    double totalBid = dom.Bids.Take(10).Sum(b => b.Size);
                    double totalAsk = dom.Asks.Take(10).Sum(a => a.Size);
                    if (totalBid + totalAsk > 0)
                    {
                        currentImbalance = Math.Round(((totalBid - totalAsk) / (totalBid + totalAsk)) * 100, 2);
                    }
                }

                // 5. 2-SECOND SPEED & IMBALANCE QUEUES
                _tickHistory.Enqueue(last);
                _tickImbalanceHistory.Enqueue(currentImbalance);
                DateTime speedCutoff = last.Time.AddMilliseconds(-TimeWindowMs);

                while (_tickHistory.Count > 0 && _tickHistory.Peek().Time < speedCutoff)
                {
                    _tickHistory.Dequeue();
                    _tickImbalanceHistory.Dequeue();
                }

                if (_tickHistory.Count > 1)
                {
                    var oldestSpeedTick = _tickHistory.Peek();
                    double speedDelta = Math.Round(last.Price - oldestSpeedTick.Price, 2);

                    double actualTimeMs = (last.Time - oldestSpeedTick.Time).TotalMilliseconds;
                    if (actualTimeMs == 0) actualTimeMs = 1;
                    double pointsPerSecond = Math.Round(speedDelta / (actualTimeMs / 1000.0), 2);

                    double minutesStored = _fiveMinHistory.Count > 0 ? (last.Time - _fiveMinHistory.Peek().Time).TotalMinutes : 0;

                    // DYNAMIC THRESHOLD MATH
                    if (minutesStored >= 4.9 && last.Time.Second != _lastCalcSecond)
                    {
                        double pa5mHigh = _fiveMinHistory.Max(t => t.Price);
                        double pa5mLow = _fiveMinHistory.Min(t => t.Price);
                        double range5m = pa5mHigh - pa5mLow;

                        _dynamicSpeedThreshold = Math.Max(MinSpikeFloor, Math.Round(range5m * VolMultiplier, 2));
                        _lastCalcSecond = last.Time.Second;
                    }

                    bool cooldownOver = (DateTime.Now - _lastTriggerTime).TotalMilliseconds > CooldownMs;

                    // --- ADAPTIVE TRIGGER ---
                    if (Math.Abs(speedDelta) >= _dynamicSpeedThreshold && minutesStored >= 4.9 && cooldownOver)
                    {
                        int tickCount = _tickHistory.Count;
                        double volumeInWindow = _tickHistory.Sum(t => t.Size);
                        double absorptionRatio = speedDelta != 0 ? Math.Round(volumeInWindow / Math.Abs(speedDelta), 2) : 0;

                        double vwapDistPct = currentVwap > 0 ? Math.Round(((last.Price - currentVwap) / currentVwap) * 100, 4) : 0;

                        double pa5mOpenNorm = 0, pa5mHighNorm = 0, pa5mLowNorm = 0, pa5mRange = 0;
                        if (_fiveMinHistory.Count > 0)
                        {
                            double pa5mOpen = _fiveMinHistory.Peek().Price;
                            double pa5mHigh = _fiveMinHistory.Max(t => t.Price);
                            double pa5mLow = _fiveMinHistory.Min(t => t.Price);

                            pa5mRange = Math.Round(pa5mHigh - pa5mLow, 2);

                            if (pa5mRange > 0)
                            {
                                pa5mOpenNorm = Math.Round((pa5mOpen - last.Price) / pa5mRange, 3);
                                pa5mHighNorm = Math.Round((pa5mHigh - last.Price) / pa5mRange, 3);
                                pa5mLowNorm = Math.Round((pa5mLow - last.Price) / pa5mRange, 3);
                            }
                        }

                        double sma1m = _oneMinHistory.Count > 0 ? Math.Round(_oneMinHistory.Average(t => t.Price), 2) : last.Price;
                        double sma5m = _fiveMinHistory.Count > 0 ? Math.Round(_fiveMinHistory.Average(t => t.Price), 2) : last.Price;
                        double trendDist = Math.Round(sma1m - sma5m, 2);
                        int activityTicks = _fiveMinHistory.Count;

                        double imbalanceAgo = _tickImbalanceHistory.Peek();
                        double imbalanceShift = Math.Round(currentImbalance - imbalanceAgo, 2);

                        // --- CALCULATE THE MACRO DISTANCES FOR THE JSON ---
                        double sma1hDistPct = _sma1h > 0 ? Math.Round(((last.Price - _sma1h) / _sma1h) * 100, 4) : 0;
                        double dailyOpenDistPct = _dailyOpen > 0 ? Math.Round(((last.Price - _dailyOpen) / _dailyOpen) * 100, 4) : 0;
                        double atr1hRound = Math.Round(_atr1h, 4);

                        Log($"🚨 SPIKE: {speedDelta:F2} pts | 1H SMA Dist: {sma1hDistPct}% | ATR: {atr1hRound}", StrategyLoggingLevel.Trading);

                        TriggerMLSnapshot(speedDelta, pointsPerSecond, actualTimeMs, tickCount, volumeInWindow, absorptionRatio, vwapDistPct, pa5mOpenNorm, pa5mHighNorm, pa5mLowNorm, pa5mRange, sma1m, sma5m, trendDist, activityTicks, currentImbalance, imbalanceAgo, imbalanceShift, sma1hDistPct, dailyOpenDistPct, atr1hRound, dom);

                        _lastTriggerTime = DateTime.Now;
                    }
                }
            }
        }

        private void TriggerMLSnapshot(double speedDelta, double pointsPerSec, double actualTimeMs, int tickCount, double volWindow, double absorption, double vwapDistPct, double openNorm, double highNorm, double lowNorm, double range, double sma1m, double sma5m, double trendDist, int activityTicks, double currentImb, double agoImb, double shiftImb, double sma1hDistPct, double dailyOpenDistPct, double atr1hRound, DepthOfMarketAggregatedCollections domData)
        {
            if (!_isZmqConnected) return;

            try
            {
                var bids = domData.Bids.Take(10).Select(b => b.Size).ToArray();
                var asks = domData.Asks.Take(10).Select(a => a.Size).ToArray();
                double spreadTicks = Math.Round((this.symbol.Ask - this.symbol.Bid) / this.symbol.TickSize, 1);

                var payload = new
                {
                    timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    symbol = this.symbol.Name,
                    strategy_id = "QT_Velocity",
                    trigger = new
                    {
                        speed_delta = speedDelta,
                        points_per_second = pointsPerSec,
                        actual_time_ms = actualTimeMs,
                        time_window_ms = TimeWindowMs,
                        tick_count = tickCount,
                        total_volume = volWindow,
                        absorption_ratio = absorption,
                        spread_ticks = spreadTicks
                    },
                    context = new
                    {
                        vwap_dist_pct = vwapDistPct,
                        pa_5m_open_norm = openNorm,
                        pa_5m_high_norm = highNorm,
                        pa_5m_low_norm = lowNorm,
                        pa_5m_range = range,

                        sma_1m = sma1m,
                        sma_5m = sma5m,
                        trend_dist = trendDist,
                        activity_5m_ticks = activityTicks,

                        imbalance_current = currentImb,
                        imbalance_ago = agoImb,
                        imbalance_shift = shiftImb,

                        sma_1h_dist_pct = sma1hDistPct,
                        daily_open_dist_pct = dailyOpenDistPct,
                        atr_1h = atr1hRound
                    },
                    temporal = new
                    {
                        hour = DateTime.UtcNow.Hour,
                        minute = DateTime.UtcNow.Minute,
                        day_of_week = (int)DateTime.UtcNow.DayOfWeek
                    },
                    dom = new { bid_sizes = bids, ask_sizes = asks }
                };

                string json = JsonConvert.SerializeObject(payload);
                _zmqSocket.SendFrame(json);

                string mlDecision;
                bool gotReply = _zmqSocket.TryReceiveFrameString(TimeSpan.FromMilliseconds(500), out mlDecision);

                if (!gotReply)
                {
                    Log("⚠️ ZMQ Timeout: No ACK from Python. Resetting socket...", StrategyLoggingLevel.Error);
                    _zmqSocket.Dispose();
                    _zmqSocket = new RequestSocket();
                    _zmqSocket.Connect(ZmqAddress);
                }
            }
            catch (Exception ex)
            {
                Log($"Snapshot Error: {ex.Message}", StrategyLoggingLevel.Error);
            }
        }

        private void SymbolOnNewLevel2(Symbol currentSymbol, Level2Quote level2, DOMQuote dom) { }
    }
}