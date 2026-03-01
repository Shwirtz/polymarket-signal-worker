/**
 * Stateless Signal Worker for PolyTerminal
 * 
 * Deploy to Deno Deploy (dash.deno.com) with these environment variables:
 *   - COINGECKO_API_KEY: Your CoinGecko Analyst API key
 *   - SUPABASE_URL: Your Supabase project URL
 *   - SUPABASE_SERVICE_ROLE_KEY: Your Supabase service role key
 * 
 * Optional env vars for threshold tuning:
 *   - SPIKE_THRESHOLD (default: 0.02)
 *   - RSI_OVERBOUGHT (default: 75)
 *   - RSI_OVERSOLD (default: 25)
 *   - VOLUME_SURGE_RATIO (default: 3)
 * 
 * This worker is STATELESS — all price history is stored in the `price_ticks`
 * database table. Each invocation fetches prices, writes ticks, reads the
 * last 15 minutes from the DB, and computes signals. State survives restarts.
 * 
 * Endpoints:
 *   GET /poll   — Fetch prices, compute signals, write to DB
 *   GET /health — Status check with DB-backed metrics
 *   GET /       — Simple identity response
 * 
 * Security: This worker has NO access to private keys or trading capability.
 * It only reads price data and writes to two database tables.
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

// ---- Configuration (env-overridable) ----

// Ticket 1: Use 'matic-network' to match autopilot's COIN_PATTERNS mapping
const TRACKED_COINS = [
  'bitcoin', 'ethereum', 'solana', 'dogecoin', 'ripple',
  'cardano', 'avalanche-2', 'chainlink', 'polkadot', 'matic-network',
];

const SIGNAL_EXPIRY_MS = 5 * 60 * 1000;       // 5 minutes
const PRICE_WINDOW_MS = 15 * 60 * 1000;       // 15-minute rolling window
const SIGNAL_DEDUP_MS = 5 * 60 * 1000;        // 5-minute dedup window
const MIN_RSI_TICKS = 16;                     // Minimum ticks for RSI
const MIN_PRICE_VARIANCE = 0.0005;            // 0.05% minimum variance

// Ticket 6: Named cleanup constants (previously magic numbers)
const SIGNAL_CLEANUP_MS = 10 * 60 * 1000;     // 10 minutes (intentionally > SIGNAL_EXPIRY_MS to allow late consumption)
const TICK_RETENTION_MS = 60 * 60 * 1000;     // 1 hour tick retention

// Ticket 5: Stale price threshold
const STALE_THRESHOLD_S = 120;                // Skip prices older than 2 minutes

// Env-overridable thresholds
const SPIKE_THRESHOLD = Number(Deno.env.get('SPIKE_THRESHOLD') || '0.02');
const RSI_OVERBOUGHT = Number(Deno.env.get('RSI_OVERBOUGHT') || '75');
const RSI_OVERSOLD = Number(Deno.env.get('RSI_OVERSOLD') || '25');
const VOLUME_SURGE_RATIO = Number(Deno.env.get('VOLUME_SURGE_RATIO') || '3');

// ---- Supabase Client ----

const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const cgKey = Deno.env.get('COINGECKO_API_KEY')!;

if (!supabaseUrl || !supabaseKey || !cgKey) {
  console.error('Missing required environment variables');
  Deno.exit(1);
}

const db = createClient(supabaseUrl, supabaseKey);

// ---- Ticket 4: Fetch with backoff, accepts headers ----

async function fetchWithBackoff(url: string, headers: Record<string, string> = {}, maxRetries = 3): Promise<Response> {
  const delays = [2000, 4000, 8000];
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    const res = await fetch(url, { 
      headers,
      signal: AbortSignal.timeout(12000) 
    });
    if (res.status !== 429 || attempt === maxRetries) return res;
    const delay = delays[attempt] || 8000;
    console.warn(`[BACKOFF] CoinGecko 429, retrying in ${delay}ms (attempt ${attempt + 1}/${maxRetries})`);
    await new Promise(r => setTimeout(r, delay));
  }
  return await fetch(url, { headers });
}

// ---- Signal Detection (pure functions, no state) ----

function computeRSI(prices: number[], period = 14): number {
  if (prices.length < period + 1) return 50;
  let avgGain = 0, avgLoss = 0;
  for (let i = 1; i <= period; i++) {
    const delta = prices[i] - prices[i - 1];
    if (delta > 0) avgGain += delta;
    else avgLoss += Math.abs(delta);
  }
  avgGain /= period;
  avgLoss /= period;
  for (let i = period + 1; i < prices.length; i++) {
    const delta = prices[i] - prices[i - 1];
    avgGain = (avgGain * (period - 1) + (delta > 0 ? delta : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (delta < 0 ? Math.abs(delta) : 0)) / period;
  }
  if (avgLoss === 0) return 100;
  return 100 - (100 / (1 + avgGain / avgLoss));
}

interface PriceTick {
  price: number;
  volume: number;
  created_at: string;
}

async function detectSignals(coinId: string, ticks: PriceTick[]): Promise<Array<{ type: string; data: Record<string, unknown> }>> {
  const signals: Array<{ type: string; data: Record<string, unknown> }> = [];
  if (ticks.length < 2) return signals;

  // Filter out zero/negative prices to prevent NaN/infinity in calculations
  const validTicks = ticks.filter(t => t.price > 0);
  if (validTicks.length < 2) return signals;

  const current = validTicks[validTicks.length - 1];
  const now = Date.now();

  // 1. Price spike/crash: >2% move in last 5 minutes
  const fiveMinAgo = now - 5 * 60 * 1000;
  const recentTicks = validTicks.filter(t => new Date(t.created_at).getTime() >= fiveMinAgo);
  if (recentTicks.length >= 2) {
    const oldest = recentTicks[0];
    const changePct = (current.price - oldest.price) / oldest.price;
    if (Math.abs(changePct) >= SPIKE_THRESHOLD) {
      signals.push({
        type: changePct > 0 ? 'price_spike' : 'price_crash',
        data: {
          price: current.price,
          change_pct: changePct,
          direction: changePct > 0 ? 'up' : 'down',
          window_seconds: Math.round((new Date(current.created_at).getTime() - new Date(oldest.created_at).getTime()) / 1000),
          tick_count: recentTicks.length,
        },
      });
    }
  }

  // 2. RSI extremes (with strict variance guard)
  if (validTicks.length >= MIN_RSI_TICKS) {
    const prices = validTicks.map(t => t.price);
    const minP = Math.min(...prices);
    const maxP = Math.max(...prices);
    const priceRange = minP > 0 ? (maxP - minP) / minP : 0;
    if (priceRange > MIN_PRICE_VARIANCE) {
      const rsi = computeRSI(prices);
      if (rsi < 100 && (rsi >= RSI_OVERBOUGHT || rsi <= RSI_OVERSOLD)) {
        signals.push({
          type: 'rsi_extreme',
          data: {
            price: current.price,
            rsi: Math.round(rsi * 100) / 100,
            direction: rsi >= RSI_OVERBOUGHT ? 'overbought' : 'oversold',
            tick_count: validTicks.length,
            price_variance_pct: Math.round(priceRange * 10000) / 100,
          },
        });
      }
    }
  }

  // 3. Volume surge
  const volumes = validTicks.map(t => t.volume).filter(v => v > 0);
  if (volumes.length >= 10) {
    const avgVol = volumes.slice(0, -3).reduce((a, b) => a + b, 0) / (volumes.length - 3);
    const recentVol = volumes.slice(-3).reduce((a, b) => a + b, 0) / 3;
    if (avgVol > 0 && recentVol / avgVol >= VOLUME_SURGE_RATIO) {
      signals.push({
        type: 'volume_surge',
        data: {
          price: current.price,
          volume_ratio: Math.round((recentVol / avgVol) * 100) / 100,
        },
      });
    }
  }

  // 4. Bollinger Squeeze: bandwidth < 2% of SMA
  if (validTicks.length >= 20) {
    const prices = validTicks.slice(-20).map(t => t.price);
    const sma = prices.reduce((a, b) => a + b, 0) / prices.length;
    if (sma > 0) {
      const variance = prices.reduce((sum, p) => sum + (p - sma) ** 2, 0) / prices.length;
      const stdDev = Math.sqrt(variance);
      const bandwidth = (2 * stdDev) / sma;
      if (bandwidth < 0.02) {
        signals.push({
          type: 'bollinger_squeeze',
          data: {
            price: current.price,
            sma: Math.round(sma * 100) / 100,
            bandwidth_pct: Math.round(bandwidth * 10000) / 100,
            stddev: Math.round(stdDev * 100) / 100,
            tick_count: 20,
          },
        });
      }
    }
  }

  return signals;
}

// ---- Core Poll Logic ----

async function poll(): Promise<{ prices: number; signals: number; ticks: number; errors: string[] }> {
  const errors: string[] = [];
  let priceCount = 0;
  let signalCount = 0;
  let tickCount = 0;

  // Step 1: Fetch prices from CoinGecko REST (Ticket 4: header auth, Ticket 5: include_last_updated_at)
  let priceData: Record<string, any> = {};
  try {
    const ids = TRACKED_COINS.join(',');
    const res = await fetchWithBackoff(
      `https://pro-api.coingecko.com/api/v3/simple/price?ids=${ids}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true`,
      { 'x-cg-pro-api-key': cgKey }
    );
    if (!res.ok) {
      errors.push(`CoinGecko API ${res.status}`);
      return { prices: 0, signals: 0, ticks: 0, errors };
    }
    priceData = await res.json();

    // Ticket 7: Log rate limit header for diagnostics
    const remaining = res.headers.get('x-ratelimit-remaining');
    if (remaining !== null) {
      console.log(`[CG] Status: ${res.status}, rate-limit-remaining: ${remaining}, coins returned: ${Object.keys(priceData).length}`);
    }
  } catch (e) {
    errors.push(`CoinGecko fetch failed: ${e}`);
    return { prices: 0, signals: 0, ticks: 0, errors };
  }

  // Step 2: Insert ticks into price_ticks table (Ticket 5: stale price filter)
  const tickRows: Array<{ coin_id: string; price: number; volume: number }> = [];
  for (const coinId of TRACKED_COINS) {
    const coin = priceData[coinId];
    if (!coin || !coin.usd || coin.usd <= 0) continue;

    // Ticket 5: Skip stale prices
    if (coin.last_updated_at) {
      const ageSeconds = Math.floor(Date.now() / 1000) - coin.last_updated_at;
      if (ageSeconds > STALE_THRESHOLD_S) {
        console.warn(`[STALE] ${coinId} price is ${ageSeconds}s old, skipping`);
        continue;
      }
    }

    tickRows.push({
      coin_id: coinId,
      price: coin.usd,
      volume: coin.usd_24h_vol || 0,
    });
    priceCount++;
  }

  if (tickRows.length > 0) {
    const { error } = await db.from('price_ticks').insert(tickRows);
    if (error) {
      errors.push(`Tick insert failed: ${error.message}`);
    } else {
      tickCount = tickRows.length;
    }
  }

  // Step 3: Read last 15 minutes of ticks from DB for each coin
  const windowStart = new Date(Date.now() - PRICE_WINDOW_MS).toISOString();
  const { data: allTicks } = await db.from('price_ticks')
    .select('coin_id, price, volume, created_at')
    .gte('created_at', windowStart)
    .order('created_at', { ascending: true });

  if (!allTicks) {
    errors.push('Failed to read price_ticks');
    return { prices: priceCount, signals: 0, ticks: tickCount, errors };
  }

  // Group ticks by coin
  const ticksByCoin = new Map<string, PriceTick[]>();
  for (const tick of allTicks) {
    const arr = ticksByCoin.get(tick.coin_id) || [];
    arr.push({ price: Number(tick.price), volume: Number(tick.volume), created_at: tick.created_at });
    ticksByCoin.set(tick.coin_id, arr);
  }

  // Step 4: Detect signals for each coin (Ticket 3: batch dedup)
  const allDetected: Array<{ coinId: string; signal: { type: string; data: Record<string, unknown> } }> = [];

  for (const [coinId, ticks] of ticksByCoin) {
    const signals = await detectSignals(coinId, ticks);
    for (const signal of signals) {
      allDetected.push({ coinId, signal });
    }
  }

  if (allDetected.length > 0) {
    // Single batch query: fetch ALL recent signals for dedup
    const dedupCutoff = new Date(Date.now() - SIGNAL_DEDUP_MS).toISOString();
    const { data: recentSignals } = await db.from('bot_signals')
      .select('coin_id, signal_type')
      .gte('created_at', dedupCutoff);

    // Build a Set for O(1) lookup: "coinId::signalType"
    const existingKeys = new Set(
      (recentSignals || []).map((s: any) => `${s.coin_id}::${s.signal_type}`)
    );

    // Write only non-duplicate signals
    for (const { coinId, signal } of allDetected) {
      const key = `${coinId}::${signal.type}`;
      if (existingKeys.has(key)) continue;

      const { error } = await db.from('bot_signals').insert({
        signal_type: signal.type,
        coin_id: coinId,
        data: signal.data,
        expires_at: new Date(Date.now() + SIGNAL_EXPIRY_MS).toISOString(),
        consumed: false,
      });

      if (!error) {
        signalCount++;
        existingKeys.add(key);
        console.log(`[SIGNAL] ${signal.type} for ${coinId}:`, JSON.stringify(signal.data));
      }
    }
  }

  // Step 5: Cleanup old ticks to prevent table bloat (Ticket 6: named constant)
  await db.from('price_ticks').delete().lt('created_at', new Date(Date.now() - TICK_RETENTION_MS).toISOString());

  // Step 6: Cleanup expired signals (buffer beyond SIGNAL_EXPIRY_MS for late consumers)
  await db.from('bot_signals').delete().lt('created_at', new Date(Date.now() - SIGNAL_CLEANUP_MS).toISOString());

  return { prices: priceCount, signals: signalCount, ticks: tickCount, errors };
}

// ---- HTTP Server ----

Deno.serve({ port: 8000 }, async (req) => {
  const url = new URL(req.url);

  if (url.pathname === '/poll') {
    const start = performance.now();
    const result = await poll();
    const elapsed = Math.round(performance.now() - start);
    console.log(`[POLL] ${result.prices} prices, ${result.signals} signals, ${result.ticks} ticks written in ${elapsed}ms`);
    if (result.errors.length > 0) {
      console.warn(`[POLL] Errors:`, result.errors);
    }
    return new Response(JSON.stringify({ ...result, elapsed_ms: elapsed }, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  if (url.pathname === '/health') {
    // 1b. Enhanced health endpoint with richer metrics
    const now = Date.now();
    const windowStart = new Date(now - PRICE_WINDOW_MS).toISOString();
    const oneHourAgo = new Date(now - 60 * 60 * 1000).toISOString();
    const fiveMinAgo = new Date(now - 5 * 60 * 1000).toISOString();

    const [tickResult, signalsHourResult, recentSignalResult, recentTickResult] = await Promise.all([
      db.from('price_ticks').select('coin_id').gte('created_at', windowStart),
      db.from('bot_signals').select('id').gte('created_at', oneHourAgo),
      db.from('bot_signals')
        .select('signal_type, coin_id, created_at')
        .gte('created_at', new Date(now - 10 * 60 * 1000).toISOString())
        .neq('signal_type', 'heartbeat')
        .order('created_at', { ascending: false })
        .limit(20),
      db.from('price_ticks').select('coin_id').gte('created_at', fiveMinAgo),
    ]);

    const coinTicks = new Map<string, number>();
    if (tickResult.data) {
      for (const t of tickResult.data) {
        coinTicks.set(t.coin_id, (coinTicks.get(t.coin_id) || 0) + 1);
      }
    }

    // Coins with data in last 5 min
    const coinsWithRecentData = new Set(recentTickResult.data?.map((t: any) => t.coin_id) || []);

    const status = {
      mode: 'stateless-db-backed',
      coins: TRACKED_COINS.length,
      ticksInWindow: Object.fromEntries(coinTicks),
      totalTicksInWindow: tickResult.data?.length || 0,
      signals_generated_last_hour: signalsHourResult.data?.length || 0,
      coins_with_recent_data: [...coinsWithRecentData],
      recentSignals: recentSignalResult.data?.length || 0,
      signals: recentSignalResult.data || [],
      thresholds: { SPIKE_THRESHOLD, RSI_OVERBOUGHT, RSI_OVERSOLD, VOLUME_SURGE_RATIO },
    };
    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response('PolyTerminal Signal Worker (stateless)', { status: 200 });
});

// ---- Ticket 2: Two named crons for reliable ~30s polling ----
Deno.cron('poll-prices-a', '* * * * *', async () => {
  await poll();
});

Deno.cron('poll-prices-b', '* * * * *', async () => {
  await new Promise(r => setTimeout(r, 30_000));
  await poll();
});
