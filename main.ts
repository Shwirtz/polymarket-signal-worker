/**
 * Stateless Signal Worker for PolyTerminal
 * 
 * Deploy to Deno Deploy (dash.deno.com) with these environment variables:
 *   - COINGECKO_API_KEY: Your CoinGecko Analyst API key
 *   - SUPABASE_URL: Your Supabase project URL
 *   - SUPABASE_SERVICE_ROLE_KEY: Your Supabase service role key
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

// ---- Configuration ----

const TRACKED_COINS = [
  'bitcoin', 'ethereum', 'solana', 'dogecoin', 'ripple',
  'cardano', 'avalanche-2', 'chainlink', 'polkadot', 'polygon-ecosystem-token',
];

const SIGNAL_EXPIRY_MS = 5 * 60 * 1000;       // 5 minutes
const PRICE_WINDOW_MS = 15 * 60 * 1000;       // 15-minute rolling window
const SPIKE_THRESHOLD = 0.02;                   // 2% move in 5 min
const RSI_OVERBOUGHT = 75;
const RSI_OVERSOLD = 25;
const VOLUME_SURGE_RATIO = 3;                   // 3x average
const MIN_RSI_TICKS = 16;                       // Minimum ticks for RSI
const MIN_PRICE_VARIANCE = 0.0005;              // 0.05% minimum variance
const SIGNAL_DEDUP_MS = 2 * 60 * 1000;          // 2-minute dedup window

// ---- Supabase Client ----

const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const cgKey = Deno.env.get('COINGECKO_API_KEY')!;

if (!supabaseUrl || !supabaseKey || !cgKey) {
  console.error('Missing required environment variables');
  Deno.exit(1);
}

const db = createClient(supabaseUrl, supabaseKey);

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

  const current = ticks[ticks.length - 1];
  const now = Date.now();

  // 1. Price spike/crash: >2% move in last 5 minutes
  const fiveMinAgo = now - 5 * 60 * 1000;
  const recentTicks = ticks.filter(t => new Date(t.created_at).getTime() >= fiveMinAgo);
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
  if (ticks.length >= MIN_RSI_TICKS) {
    const prices = ticks.map(t => t.price);
    const minP = Math.min(...prices);
    const maxP = Math.max(...prices);
    const priceRange = minP > 0 ? (maxP - minP) / minP : 0;
    if (priceRange > MIN_PRICE_VARIANCE) {
      const rsi = computeRSI(prices);
      // Never emit RSI=100 — it's always an artifact
      if (rsi < 100 && (rsi >= RSI_OVERBOUGHT || rsi <= RSI_OVERSOLD)) {
        signals.push({
          type: 'rsi_extreme',
          data: {
            price: current.price,
            rsi: Math.round(rsi * 100) / 100,
            direction: rsi >= RSI_OVERBOUGHT ? 'overbought' : 'oversold',
            tick_count: ticks.length,
            price_variance_pct: Math.round(priceRange * 10000) / 100,
          },
        });
      }
    }
  }

  // 3. Volume surge
  const volumes = ticks.map(t => t.volume).filter(v => v > 0);
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

  return signals;
}

// ---- Core Poll Logic ----

async function poll(): Promise<{ prices: number; signals: number; ticks: number; errors: string[] }> {
  const errors: string[] = [];
  let priceCount = 0;
  let signalCount = 0;
  let tickCount = 0;

  // Step 1: Fetch prices from CoinGecko REST
  let priceData: Record<string, any> = {};
  try {
    const ids = TRACKED_COINS.join(',');
    const res = await fetch(
      `https://pro-api.coingecko.com/api/v3/simple/price?ids=${ids}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true&x_cg_pro_api_key=${cgKey}`
    );
    if (!res.ok) {
      errors.push(`CoinGecko API ${res.status}`);
      return { prices: 0, signals: 0, ticks: 0, errors };
    }
    priceData = await res.json();
  } catch (e) {
    errors.push(`CoinGecko fetch failed: ${e}`);
    return { prices: 0, signals: 0, ticks: 0, errors };
  }

  // Step 2: Insert ticks into price_ticks table
  const tickRows: Array<{ coin_id: string; price: number; volume: number }> = [];
  for (const coinId of TRACKED_COINS) {
    const coin = priceData[coinId];
    if (!coin || !coin.usd || coin.usd <= 0) continue;
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

  // Step 4: Detect signals for each coin
  for (const [coinId, ticks] of ticksByCoin) {
    const signals = await detectSignals(coinId, ticks);
    for (const signal of signals) {
      // Dedup: check if same signal type exists for this coin in last 2 minutes
      const dedupCutoff = new Date(Date.now() - SIGNAL_DEDUP_MS).toISOString();
      const { data: existing } = await db.from('bot_signals')
        .select('id')
        .eq('coin_id', coinId)
        .eq('signal_type', signal.type)
        .gte('created_at', dedupCutoff)
        .limit(1);

      if (existing && existing.length > 0) continue;

      // Write signal
      const { error } = await db.from('bot_signals').insert({
        signal_type: signal.type,
        coin_id: coinId,
        data: signal.data,
        expires_at: new Date(Date.now() + SIGNAL_EXPIRY_MS).toISOString(),
        consumed: false,
      });

      if (!error) {
        signalCount++;
        console.log(`[SIGNAL] ${signal.type} for ${coinId}:`, JSON.stringify(signal.data));
      }
    }
  }

  // Step 5: Cleanup old ticks (>1 hour) to prevent table bloat
  const cleanupCutoff = new Date(Date.now() - 60 * 60 * 1000).toISOString();
  await db.from('price_ticks').delete().lt('created_at', cleanupCutoff);

  // Step 6: Cleanup expired signals (>10 min old)
  await db.from('bot_signals').delete().lt('created_at', new Date(Date.now() - 10 * 60 * 1000).toISOString());

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
    // DB-backed health check — no in-memory state needed
    const windowStart = new Date(Date.now() - PRICE_WINDOW_MS).toISOString();
    const { data: tickCounts } = await db.from('price_ticks')
      .select('coin_id')
      .gte('created_at', windowStart);

    const coinTicks = new Map<string, number>();
    if (tickCounts) {
      for (const t of tickCounts) {
        coinTicks.set(t.coin_id, (coinTicks.get(t.coin_id) || 0) + 1);
      }
    }

    const tenMinAgo = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    const { data: recentSignals } = await db.from('bot_signals')
      .select('signal_type, coin_id, created_at')
      .gte('created_at', tenMinAgo)
      .neq('signal_type', 'heartbeat')
      .order('created_at', { ascending: false })
      .limit(20);

    const status = {
      mode: 'stateless-db-backed',
      coins: TRACKED_COINS.length,
      ticksInWindow: Object.fromEntries(coinTicks),
      totalTicksInWindow: tickCounts?.length || 0,
      recentSignals: recentSignals?.length || 0,
      signals: recentSignals || [],
    };
    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response('PolyTerminal Signal Worker (stateless)', { status: 200 });
});

// ---- Deno Deploy Cron: poll every 30 seconds for high-frequency signal detection ----
Deno.cron('poll-prices', '* * * * *', async () => {
  // Cron runs every minute; we do 2 polls (0s and 30s) for ~30s frequency
  await poll();
  await new Promise(r => setTimeout(r, 30_000));
  await poll();
});
