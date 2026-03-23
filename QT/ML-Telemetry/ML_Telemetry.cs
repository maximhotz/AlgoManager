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

        [InputParameter("Speed Threshold (Points)", 20)]
        public double SpeedThreshold = 2.0;

        [InputParameter("Time Window (ms)", 30)]
        public int TimeWindowMs = 2000;

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

        // VWAP Variables
        private double _cumulativePriceVolume = 0;
        private double _cumulativeVolume = 0;
        private DateTime _lastVwapReset = DateTime.MinValue;

        public ML_Telemetry() : base()
        {
            this.Name = "ML_Telemetry";
            this.Description = "L2 Bot with Regime & Order Book Imbalance Shift";
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

            this.symbol.NewLast += SymbolOnNewLast;

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
            }

            _isZmqConnected = false;
            _zmqSocket?.Dispose();
            NetMQConfig.Cleanup(false);
            Log("ZMQ Disconnected.", StrategyLoggingLevel.Trading);
        }

        protected override void OnRemove()
        {
            this.symbol = null;
        }

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
                double initialVwap = _cumulativeVolume > 0 ? (_cumulativePriceVolume / _cumulativeVolume) : this.symbol.Last;
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

                    double minutesStored = _fiveMinHistory.Count > 0
                        ? (last.Time - _fiveMinHistory.Peek().Time).TotalMinutes
                        : 0;

                    // WARMUP LOCK
                    if (Math.Abs(speedDelta) >= SpeedThreshold && minutesStored >= 4.9)
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

                        // REGIME MATH
                        double sma1m = _oneMinHistory.Count > 0 ? Math.Round(_oneMinHistory.Average(t => t.Price), 2) : last.Price;
                        double sma5m = _fiveMinHistory.Count > 0 ? Math.Round(_fiveMinHistory.Average(t => t.Price), 2) : last.Price;
                        double trendDist = Math.Round(sma1m - sma5m, 2);
                        int activityTicks = _fiveMinHistory.Count;

                        // IMBALANCE SHIFT MATH
                        double imbalanceAgo = _tickImbalanceHistory.Peek();
                        double imbalanceShift = Math.Round(currentImbalance - imbalanceAgo, 2);

                        Log($"🚨 SPIKE: {speedDelta:F2} pts! Shift: {imbalanceShift}% | Trend: {trendDist} pts", StrategyLoggingLevel.Trading);

                        TriggerMLSnapshot(speedDelta, tickCount, volumeInWindow, absorptionRatio, vwapDistPct, pa5mOpenNorm, pa5mHighNorm, pa5mLowNorm, pa5mRange, sma1m, sma5m, trendDist, activityTicks, currentImbalance, imbalanceAgo, imbalanceShift, dom);

                        _tickHistory.Clear();
                        _tickImbalanceHistory.Clear();
                    }
                }
            }
        }

        // FIX: Replaced non-existent Interface with concrete class 'DepthOfMarketAggregatedCollections'
        private void TriggerMLSnapshot(double speedDelta, int tickCount, double volWindow, double absorption, double vwapDistPct, double openNorm, double highNorm, double lowNorm, double range, double sma1m, double sma5m, double trendDist, int activityTicks, double currentImb, double agoImb, double shiftImb, DepthOfMarketAggregatedCollections domData)
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
                        imbalance_shift = shiftImb
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
    }
}