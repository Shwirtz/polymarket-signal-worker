/**
 * Persistent Signal Worker for PolyTerminal
 * 
 * Deploy to Deno Deploy (dash.deno.com) with these environment variables:
 *   - COINGECKO_API_KEY: Your CoinGecko Analyst API key
 *   - SUPABASE_URL: Your Supabase project URL
 *   - SUPABASE_SERVICE_ROLE_KEY: Your Supabase service role key
 * 
 * This worker maintains a 24/7 WebSocket connection to CoinGecko,
 * detects crypto price signals, and writes them to the bot_signals table.
 * The main live-trade-autopilot reads these signals each cycle.
 * 
 * Security: This worker has NO access to private keys or trading capability.
 * It only reads price data and writes to one database table.
 */

import { createClient } from "https://esm.sh/@supabase/supabase-js@2.49.1";

// ---- Configuration ----

const TRACKED_COINS = [
  'bitcoin', 'ethereum', 'solana', 'dogecoin', 'ripple',
  'cardano', 'avalanche-2', 'chainlink', 'polkadot', 'matic-network',
];

const SIGNAL_EXPIRY_MS = 5 * 60 * 1000;       // 5 minutes
const PRICE_WINDOW_MS = 15 * 60 * 1000;       // 15-minute rolling window
const SPIKE_THRESHOLD = 0.02;                   // 2% move in 5 min
const RSI_OVERBOUGHT = 75;
const RSI_OVERSOLD = 25;
const VOLUME_SURGE_RATIO = 3;                   // 3x average
const HEARTBEAT_INTERVAL_MS = 30_000;           // 30 seconds
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 60_000;
const REST_POLL_INTERVAL_MS = 15_000;           // Poll REST every 15s (Analyst tier: 500 req/min)

// ---- State ----

interface PriceTick {
  price: number;
  volume: number;
  timestamp: number;
}

const priceWindows = new Map<string, PriceTick[]>();
let reconnectAttempts = 0;
let ws: WebSocket | null = null;

// ---- Supabase Client ----

const supabaseUrl = Deno.env.get('SUPABASE_URL')!;
const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!;
const cgKey = Deno.env.get('COINGECKO_API_KEY')!;

if (!supabaseUrl || !supabaseKey || !cgKey) {
  console.error('Missing required environment variables: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, COINGECKO_API_KEY');
  Deno.exit(1);
}

const db = createClient(supabaseUrl, supabaseKey);

// ---- Signal Detection ----

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

function addTick(coinId: string, price: number, volume = 0) {
  const now = Date.now();
  if (!priceWindows.has(coinId)) priceWindows.set(coinId, []);
  const window = priceWindows.get(coinId)!;
  window.push({ price, volume, timestamp: now });

  // Trim to 15-minute window
  const cutoff = now - PRICE_WINDOW_MS;
  while (window.length > 0 && window[0].timestamp < cutoff) {
    window.shift();
  }
}

async function checkSignals(coinId: string) {
  const window = priceWindows.get(coinId);
  if (!window || window.length < 2) return;

  const now = Date.now();
  const current = window[window.length - 1];

  // 1. Price spike/crash: >2% move in last 5 minutes
  const fiveMinAgo = now - 5 * 60 * 1000;
  const recentPrices = window.filter(t => t.timestamp >= fiveMinAgo);
  if (recentPrices.length >= 2) {
    const oldest = recentPrices[0];
    const changePct = (current.price - oldest.price) / oldest.price;

    if (Math.abs(changePct) >= SPIKE_THRESHOLD) {
      const signalType = changePct > 0 ? 'price_spike' : 'price_crash';
      await writeSignal(coinId, signalType, {
        price: current.price,
        change_pct: changePct,
        direction: changePct > 0 ? 'up' : 'down',
        window_seconds: Math.round((current.timestamp - oldest.timestamp) / 1000),
      });
    }
  }

  // 2. RSI extremes
  if (window.length >= 16) {
    const prices = window.map(t => t.price);
    const rsi = computeRSI(prices);
    if (rsi >= RSI_OVERBOUGHT || rsi <= RSI_OVERSOLD) {
      await writeSignal(coinId, 'rsi_extreme', {
        price: current.price,
        rsi,
        direction: rsi >= RSI_OVERBOUGHT ? 'overbought' : 'oversold',
      });
    }
  }

  // 3. Volume surge
  const volumes = window.map(t => t.volume).filter(v => v > 0);
  if (volumes.length >= 10) {
    const avgVol = volumes.slice(0, -3).reduce((a, b) => a + b, 0) / (volumes.length - 3);
    const recentVol = volumes.slice(-3).reduce((a, b) => a + b, 0) / 3;
    if (avgVol > 0 && recentVol / avgVol >= VOLUME_SURGE_RATIO) {
      await writeSignal(coinId, 'volume_surge', {
        price: current.price,
        volume_ratio: recentVol / avgVol,
      });
    }
  }
}

// Dedup: don't write same signal type for same coin within 2 minutes
const lastSignalTime = new Map<string, number>();

async function writeSignal(coinId: string, signalType: string, data: Record<string, unknown>) {
  const key = `${coinId}:${signalType}`;
  const now = Date.now();
  if (lastSignalTime.has(key) && now - lastSignalTime.get(key)! < 120_000) return;
  lastSignalTime.set(key, now);

  try {
    const expiresAt = new Date(now + SIGNAL_EXPIRY_MS).toISOString();
    await db.from('bot_signals').insert({
      signal_type: signalType,
      coin_id: coinId,
      data,
      expires_at: expiresAt,
      consumed: false,
    });
    console.log(`[SIGNAL] ${signalType} for ${coinId}:`, JSON.stringify(data));
  } catch (e) {
    console.error(`[SIGNAL] Failed to write signal for ${coinId}:`, e);
  }
}

// ---- REST Polling (Primary method - works on all CoinGecko tiers) ----

async function pollPricesREST() {
  try {
    const ids = TRACKED_COINS.join(',');
    const res = await fetch(
      `https://pro-api.coingecko.com/api/v3/simple/price?ids=${ids}&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true&x_cg_pro_api_key=${cgKey}`
    );
    if (!res.ok) {
      console.warn(`[REST] CoinGecko API ${res.status}`);
      return;
    }
    const data = await res.json();

    for (const coinId of TRACKED_COINS) {
      const coin = data[coinId];
      if (!coin) continue;
      const price = coin.usd;
      const volume = coin.usd_24h_vol || 0;
      if (price > 0) {
        addTick(coinId, price, volume);
        await checkSignals(coinId);
      }
    }
  } catch (e) {
    console.error('[REST] Poll failed:', e);
  }
}

// ---- WebSocket Connection (Analyst tier bonus) ----

function connectWebSocket() {
  try {
    const wsUrl = `wss://ws.coingecko.com/api/v3/ws?x_cg_pro_api_key=${cgKey}`;
    console.log('[WS] Connecting to CoinGecko WebSocket...');
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      console.log('[WS] Connected');
      reconnectAttempts = 0;

      // Subscribe to price channels
      const subscribeMsg = {
        action: 'subscribe',
        channels: TRACKED_COINS.map(id => `prices:${id}:usd`),
      };
      ws!.send(JSON.stringify(subscribeMsg));
      console.log(`[WS] Subscribed to ${TRACKED_COINS.length} coin channels`);

      // Start heartbeat
      const heartbeat = setInterval(() => {
        if (ws?.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ action: 'ping' }));
        } else {
          clearInterval(heartbeat);
        }
      }, HEARTBEAT_INTERVAL_MS);
    };

    ws.onmessage = async (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'pong' || msg.type === 'welcome') return;

        // Extract price data from WebSocket message
        const coinId = msg.coin_id || msg.id;
        const price = msg.price || msg.usd;
        if (coinId && price && TRACKED_COINS.includes(coinId)) {
          addTick(coinId, price, msg.volume || 0);
          await checkSignals(coinId);
        }
      } catch {
        // Ignore parse errors from non-JSON frames
      }
    };

    ws.onclose = (event) => {
      console.warn(`[WS] Closed: code=${event.code} reason=${event.reason}`);
      scheduleReconnect();
    };

    ws.onerror = (error) => {
      console.error('[WS] Error:', error);
      // WebSocket will be available on Analyst tier — fall back to REST if unavailable
    };
  } catch (e) {
    console.warn('[WS] Connection failed (may need Analyst tier):', e);
    scheduleReconnect();
  }
}

function scheduleReconnect() {
  const delay = Math.min(RECONNECT_BASE_MS * Math.pow(2, reconnectAttempts), RECONNECT_MAX_MS);
  reconnectAttempts++;
  console.log(`[WS] Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
  setTimeout(connectWebSocket, delay);
}

// ---- Main Loop ----

console.log('=== PolyTerminal Signal Worker Starting ===');
console.log(`Tracking ${TRACKED_COINS.length} coins`);
console.log(`Signal expiry: ${SIGNAL_EXPIRY_MS / 1000}s`);
console.log(`REST poll interval: ${REST_POLL_INTERVAL_MS / 1000}s`);

// Start REST polling (always works, primary data source)
await pollPricesREST();
setInterval(pollPricesREST, REST_POLL_INTERVAL_MS);

// Attempt WebSocket connection (Analyst tier bonus — enhances with sub-second data)
connectWebSocket();

// Keep the process alive (Deno Deploy)
// For Deno Deploy, we serve a simple health endpoint
Deno.serve({ port: 8000 }, (req) => {
  const url = new URL(req.url);
  if (url.pathname === '/health') {
    const status = {
      uptime: Math.round(performance.now() / 1000),
      coins: TRACKED_COINS.length,
      windowSizes: Object.fromEntries(
        [...priceWindows.entries()].map(([k, v]) => [k, v.length])
      ),
      wsConnected: ws?.readyState === WebSocket.OPEN,
      lastSignals: Object.fromEntries(lastSignalTime),
    };
    return new Response(JSON.stringify(status, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  }
  return new Response('PolyTerminal Signal Worker', { status: 200 });
});
