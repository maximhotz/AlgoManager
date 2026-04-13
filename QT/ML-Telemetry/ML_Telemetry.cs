// Copyright QUANTOWER LLC. © 2017-2023. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TradingPlatform.BusinessLayer;
using NetMQ;
using NetMQ.Sockets;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

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

        [InputParameter("ZMQ Brain Address", 40)]
        public string ZmqAddress = "tcp://localhost:5556";

        [InputParameter("ZMQ Regime Address", 41)]
        public string ZmqRegimeAddress = "tcp://localhost:5557";
        // ==========================================

        private RequestSocket _zmqSocket;
        private RequestSocket _zmqRegimeSocket;

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

        // --- SIGLIP AI REGIME STATE ---
        // 0 = Longs Only, 1 = Scalp Both (Chop), 2 = Shorts Only
        private int _currentMacroRegime = 1;

        public ML_Telemetry() : base()
        {
            this.Name = "ML_Telemetry";
            this.Description = "L2 Bot with AI Macro Anchors & SigLIP Watchtower";
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

            _runMacroUpdater = true;
            Task.Run(MacroUpdaterLoop);
            Task.Run(RegimeUpdaterLoop);

            this.symbol.NewLast += SymbolOnNewLast;
            this.symbol.NewLevel2 += SymbolOnNewLevel2;

            Task.Run(() =>
            {
                try
                {
                    _zmqSocket = new RequestSocket();
                    _zmqSocket.Connect(ZmqAddress);

                    _zmqRegimeSocket = new RequestSocket();
                    _zmqRegimeSocket.Connect(ZmqRegimeAddress);

                    _isZmqConnected = true;
                    Log($"✓ ZMQ Connected: Brain ({ZmqAddress}) | Watchtower ({ZmqRegimeAddress})", StrategyLoggingLevel.Trading);
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
            _zmqRegimeSocket?.Dispose();

            NetMQConfig.Cleanup(false);
            Log("ZMQ Disconnected.", StrategyLoggingLevel.Trading);
        }

        protected override void OnRemove()
        {
            this.symbol = null;
        }

        // ==========================================
        // 🔭 1-MINUTE RANDOM FOREST WATCHTOWER LOOP
        // ==========================================
        private async Task RegimeUpdaterLoop()
        {
            while (_runMacroUpdater && this.symbol != null)
            {
                try
                {
                    if (_isZmqConnected && _zmqRegimeSocket != null)
                    {
                        DateTime now = Core.Instance.TimeUtils.DateTimeUtcNow;
                        int requiredBars = 240;

                        // --- THE FIX 1: Look back 4 days to bridge the weekend gap ---
                        using (var history = this.symbol.GetHistory(Period.MIN1, now.AddDays(-4), now))
                        {
                            if (history != null && history.Count >= requiredBars)
                            {
                                var lastBars = history.OrderByDescending(x => x.TimeLeft).Take(requiredBars).Reverse().ToList();

                                var payloadData = new List<object>();

                                foreach (var bar in lastBars)
                                {
                                    var volAnalysis = bar.VolumeAnalysisData;
                                    var totalVol = volAnalysis != null ? volAnalysis.Total : null;

                                    double delta = totalVol != null ? totalVol.Delta : 0;
                                    double avgBuySize = totalVol != null && totalVol.BuyTrades > 0 ? totalVol.BuyVolume / totalVol.BuyTrades : 0;
                                    double avgSellSize = totalVol != null && totalVol.SellTrades > 0 ? totalVol.SellVolume / totalVol.SellTrades : 0;

                                    payloadData.Add(new
                                    {
                                        DateTime = bar.TimeLeft.ToString("yyyy-MM-dd HH:mm:ss"),
                                        Close = bar[PriceType.Close],
                                        Volume = bar[PriceType.Volume],
                                        Delta = delta,
                                        Property1 = new Newtonsoft.Json.JsonPropertyAttribute("Average buy size"),
                                        AvgBuy = avgBuySize,
                                        Property2 = new Newtonsoft.Json.JsonPropertyAttribute("Average sell size"),
                                        AvgSell = avgSellSize
                                    });
                                }

                                var finalPayloadList = payloadData.Select(p => {
                                    dynamic d = p;
                                    return new Dictionary<string, object>
                                    {
                                        { "DateTime", d.DateTime },
                                        { "Close", d.Close },
                                        { "Volume", d.Volume },
                                        { "Delta", d.Delta },
                                        { "Average buy size", d.AvgBuy },
                                        { "Average sell size", d.AvgSell }
                                    };
                                }).ToList();

                                var payload = new { data = finalPayloadList };
                                string jsonPayload = JsonConvert.SerializeObject(payload);

                                _zmqRegimeSocket.SendFrame(jsonPayload);

                                string reply;
                                if (_zmqRegimeSocket.TryReceiveFrameString(TimeSpan.FromSeconds(5), out reply))
                                {
                                    var jsonResponse = JObject.Parse(reply);
                                    if (jsonResponse["status"]?.ToString() == "success")
                                    {
                                        int newRegime = (int)jsonResponse["signal"];
                                        string regimeName = jsonResponse["regime"]?.ToString();

                                        if (_currentMacroRegime != newRegime)
                                        {
                                            Log($"🔭 WATCHTOWER: Regime Shift to {regimeName.ToUpper()} ({newRegime})", StrategyLoggingLevel.Trading);
                                            _currentMacroRegime = newRegime;
                                        }
                                    }
                                    else
                                    {
                                        Log($"⚠️ Watchtower Python Error: {jsonResponse["message"]}", StrategyLoggingLevel.Error);
                                    }
                                }
                                else
                                {
                                    Log("⚠️ Watchtower Timeout. Reconnecting...", StrategyLoggingLevel.Error);

                                    // --- THE FIX 2: Kill the zombie message before disposing ---
                                    if (_zmqRegimeSocket != null)
                                    {
                                        _zmqRegimeSocket.Options.Linger = TimeSpan.Zero;
                                        _zmqRegimeSocket.Dispose();
                                    }

                                    _zmqRegimeSocket = new RequestSocket();
                                    _zmqRegimeSocket.Connect(ZmqRegimeAddress);
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log($"Regime Loop Error: {ex.Message}", StrategyLoggingLevel.Error);

                    // --- THE FIX 3: Mid-Flight Socket Rebuild to prevent XSend lock ---
                    try
                    {
                        if (_zmqRegimeSocket != null)
                        {
                            _zmqRegimeSocket.Options.Linger = TimeSpan.Zero;
                            _zmqRegimeSocket.Dispose();
                        }

                        _zmqRegimeSocket = new RequestSocket();
                        _zmqRegimeSocket.Connect(ZmqRegimeAddress);
                        Log("🔧 Watchtower Socket auto-repaired after runtime crash.", StrategyLoggingLevel.Trading);
                    }
                    catch (Exception rebuildEx)
                    {
                        Log($"Socket Rebuild Failed: {rebuildEx.Message}", StrategyLoggingLevel.Error);
                    }
                }

                await Task.Delay(60000); // Polling every 60 seconds
            }
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

            using (var d1History = this.symbol.GetHistory(Period.DAY1, now.AddDays(-3), now))
            {
                if (d1History != null && d1History.Count > 0)
                {
                    var latestDaily = d1History.OrderByDescending(x => x.TimeLeft).FirstOrDefault();
                    if (latestDaily != null)
                    {
                        _dailyOpen = latestDaily[PriceType.Open];
                    }
                }
            }

            using (var h1History = this.symbol.GetHistory(Period.HOUR1, now.AddDays(-10), now))
            {
                if (h1History != null && h1History.Count >= 20)
                {
                    var newestCandles = h1History.OrderByDescending(x => x.TimeLeft).ToList();

                    double sumClose = 0;
                    int smaCount = Math.Min(20, newestCandles.Count);
                    for (int i = 0; i < smaCount; i++)
                    {
                        sumClose += newestCandles[i][PriceType.Close];
                    }
                    _sma1h = sumClose / smaCount;

                    int atrCount = Math.Min(20, newestCandles.Count);
                    double sumTr = 0;

                    for (int i = 0; i < atrCount; i++)
                    {
                        double high = newestCandles[i][PriceType.High];
                        double low = newestCandles[i][PriceType.Low];

                        double prevClose = (i + 1 < newestCandles.Count) ? newestCandles[i + 1][PriceType.Close] : newestCandles[i][PriceType.Open];

                        double tr1 = high - low;
                        double tr2 = Math.Abs(high - prevClose);
                        double tr3 = Math.Abs(low - prevClose);

                        sumTr += Math.Max(tr1, Math.Max(tr2, tr3));
                    }

                    if (atrCount > 0)
                    {
                        _atr1h = sumTr / atrCount;
                    }
                }
            }
        }

        private void PrimeVwapData()
        {
            try
            {
                DateTime now = Core.Instance.TimeUtils.DateTimeUtcNow;
                DateTime startTime = new DateTime(now.Year, now.Month, now.Day, VwapResetHourUtc, 0, 0, DateTimeKind.Utc);

                if (startTime > now)
                {
                    startTime = startTime.AddDays(-1);
                }

                using (var history = this.symbol.GetHistory(Period.MIN1, startTime, now))
                {
                    _cumulativePriceVolume = 0;
                    _cumulativeVolume = 0;
                    if (history != null)
                    {
                        foreach (IHistoryItem item in history)
                        {
                            _cumulativeVolume += item[PriceType.Volume];
                            _cumulativePriceVolume += (item[PriceType.Close] * item[PriceType.Volume]);
                        }
                    }
                }
                _lastVwapReset = startTime;
            }
            catch (Exception ex)
            {
                Log($"VWAP Prime Error: {ex.Message}", StrategyLoggingLevel.Error);
            }
        }

        private void SymbolOnNewLast(Symbol currentSymbol, Last last)
        {
            lock (_lockObject)
            {
                if (last.Time.Hour == VwapResetHourUtc && (last.Time - _lastVwapReset).TotalHours > 12)
                {
                    _cumulativePriceVolume = 0;
                    _cumulativeVolume = 0;
                    _lastVwapReset = last.Time;
                }

                _cumulativeVolume += last.Size;
                _cumulativePriceVolume += (last.Price * last.Size);
                double currentVwap = _cumulativeVolume > 0 ? (_cumulativePriceVolume / _cumulativeVolume) : last.Price;

                _oneMinHistory.Enqueue(last);
                while (_oneMinHistory.Count > 0 && _oneMinHistory.Peek().Time < last.Time.AddMinutes(-1))
                {
                    _oneMinHistory.Dequeue();
                }

                _fiveMinHistory.Enqueue(last);
                while (_fiveMinHistory.Count > 0 && _fiveMinHistory.Peek().Time < last.Time.AddMinutes(-5))
                {
                    _fiveMinHistory.Dequeue();
                }

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

                _tickHistory.Enqueue(last);
                _tickImbalanceHistory.Enqueue(currentImbalance);

                while (_tickHistory.Count > 0 && _tickHistory.Peek().Time < last.Time.AddMilliseconds(-TimeWindowMs))
                {
                    _tickHistory.Dequeue();
                    _tickImbalanceHistory.Dequeue();
                }

                if (_tickHistory.Count > 1)
                {
                    var oldestSpeedTick = _tickHistory.Peek();
                    double speedDelta = Math.Round(last.Price - oldestSpeedTick.Price, 2);
                    double actualTimeMs = Math.Max(1, (last.Time - oldestSpeedTick.Time).TotalMilliseconds);
                    double pointsPerSecond = Math.Round(speedDelta / (actualTimeMs / 1000.0), 2);

                    if (_fiveMinHistory.Count > 0 && last.Time.Second != _lastCalcSecond)
                    {
                        double range5m = _fiveMinHistory.Max(t => t.Price) - _fiveMinHistory.Min(t => t.Price);
                        _dynamicSpeedThreshold = Math.Max(MinSpikeFloor, Math.Round(range5m * VolMultiplier, 2));
                        _lastCalcSecond = last.Time.Second;
                    }

                    if (Math.Abs(speedDelta) >= _dynamicSpeedThreshold && (DateTime.Now - _lastTriggerTime).TotalMilliseconds > CooldownMs)
                    {
                        double vwapDistPct = currentVwap > 0 ? Math.Round(((last.Price - currentVwap) / currentVwap) * 100, 4) : 0;
                        double pa5mRange = Math.Max(0.01, Math.Round(_fiveMinHistory.Max(t => t.Price) - _fiveMinHistory.Min(t => t.Price), 2));

                        double volumeInWindow = _tickHistory.Sum(t => t.Size);
                        double absorptionRatio = speedDelta != 0 ? Math.Round(volumeInWindow / Math.Abs(speedDelta), 2) : 0;

                        double pa5mOpenNorm = Math.Round((_fiveMinHistory.Peek().Price - last.Price) / pa5mRange, 3);
                        double pa5mHighNorm = Math.Round((_fiveMinHistory.Max(t => t.Price) - last.Price) / pa5mRange, 3);
                        double pa5mLowNorm = Math.Round((_fiveMinHistory.Min(t => t.Price) - last.Price) / pa5mRange, 3);

                        double sma1m = Math.Round(_oneMinHistory.Average(t => t.Price), 2);
                        double sma5m = Math.Round(_fiveMinHistory.Average(t => t.Price), 2);
                        double trendDist = Math.Round(sma1m - sma5m, 2);

                        double imbalanceAgo = _tickImbalanceHistory.Peek();
                        double imbalanceShift = Math.Round(currentImbalance - imbalanceAgo, 2);

                        double sma1hDistPct = _sma1h > 0 ? Math.Round(((last.Price - _sma1h) / _sma1h) * 100, 4) : 0;
                        double dailyOpenDistPct = _dailyOpen > 0 ? Math.Round(((last.Price - _dailyOpen) / _dailyOpen) * 100, 4) : 0;
                        double atr1hRound = Math.Round(_atr1h, 4);

                        Log($"🚨 SPIKE: {speedDelta:F2} pts | 1H SMA Dist: {sma1hDistPct}% | ATR: {atr1hRound}", StrategyLoggingLevel.Trading);

                        TriggerMLSnapshot(
                            speedDelta,
                            pointsPerSecond,
                            actualTimeMs,
                            _tickHistory.Count,
                            volumeInWindow,
                            absorptionRatio,
                            vwapDistPct,
                            pa5mOpenNorm,
                            pa5mHighNorm,
                            pa5mLowNorm,
                            pa5mRange,
                            sma1m,
                            sma5m,
                            trendDist,
                            _fiveMinHistory.Count,
                            currentImbalance,
                            imbalanceAgo,
                            imbalanceShift,
                            sma1hDistPct,
                            dailyOpenDistPct,
                            atr1hRound,
                            dom
                        );

                        _lastTriggerTime = DateTime.Now;
                    }
                }
            }
        }

        private void TriggerMLSnapshot(
            double speedDelta, double pointsPerSec, double actualTimeMs, int tickCount, double volWindow,
            double absorption, double vwapDistPct, double openNorm, double highNorm, double lowNorm,
            double range, double sma1m, double sma5m, double trendDist, int activityTicks,
            double currentImb, double agoImb, double shiftImb, double sma1hDistPct,
            double dailyOpenDistPct, double atr1hRound, DepthOfMarketAggregatedCollections domData)
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
                        atr_1h = atr1hRound,

                        macro_regime_state = _currentMacroRegime
                    },
                    temporal = new
                    {
                        hour = DateTime.UtcNow.Hour,
                        minute = DateTime.UtcNow.Minute,
                        day_of_week = (int)DateTime.UtcNow.DayOfWeek
                    },
                    dom = new
                    {
                        bid_sizes = bids,
                        ask_sizes = asks
                    }
                };

                string json = JsonConvert.SerializeObject(payload);
                _zmqSocket.SendFrame(json);

                string reply;
                if (!_zmqSocket.TryReceiveFrameString(TimeSpan.FromMilliseconds(500), out reply))
                {
                    Log("⚠️ ZMQ Timeout: No ACK from Python. Resetting socket...", StrategyLoggingLevel.Error);

                    // --- THE FIX: Kill the HFT zombie message before disposing ---
                    if (_zmqSocket != null)
                    {
                        _zmqSocket.Options.Linger = TimeSpan.Zero;
                        _zmqSocket.Dispose();
                    }
                    _zmqSocket = new RequestSocket();
                    _zmqSocket.Connect(ZmqAddress);
                }
            }
            catch (Exception ex)
            {
                Log($"Snapshot Error: {ex.Message}", StrategyLoggingLevel.Error);

                // --- THE FIX: Mid-Flight HFT Socket Rebuild ---
                try
                {
                    if (_zmqSocket != null)
                    {
                        _zmqSocket.Options.Linger = TimeSpan.Zero;
                        _zmqSocket.Dispose();
                    }
                    _zmqSocket = new RequestSocket();
                    _zmqSocket.Connect(ZmqAddress);
                }
                catch { } // Fail silently if rebuild fails so we don't crash the main tick loop
            }
        }

        private void SymbolOnNewLevel2(Symbol currentSymbol, Level2Quote level2, DOMQuote dom) { }
    }
}