// MassLaunch backend — sequentially launches pump.fun tokens via plain RPC.
// No Jito bundle, no dev buy, no metadata bells & whistles. Just create the token.
//
// Wire protocol (client → server):
//   { type: "launch", devSecret, tokens: [{ name, symbol, fileBase64, description?, twitter?, telegram?, website? }],
//     delayMs?, cuLimit?, cuPrice?, cashback? }
//   { type: "stop" }
//
// Server → client events:
//   { type: "log", message, level }
//   { type: "progress", index, total, status, mint?, signature?, error? }
//   { type: "done", launched, failed }

import { WebSocketServer } from 'ws';
import {
  Connection,
  Keypair,
  VersionedTransaction,
  TransactionMessage,
  ComputeBudgetProgram,
} from '@solana/web3.js';
import bs58 from 'bs58';
import dotenv from 'dotenv';
import { PumpSdk, bondingCurvePda } from '@pump-fun/pump-sdk';
import { createHash } from 'crypto';

dotenv.config();

const PORT = parseInt(process.env.PORT) || 43904;
const RPC_URL = process.env.RPC_URL;
if (!RPC_URL) { console.error('Missing RPC_URL'); process.exit(1); }

// SECURITY: bind to loopback only by default — never expose to LAN/internet locally.
// PUBLIC_MODE (set ALLOW_PUBLIC=true on Railway) lifts the loopback/origin gate and
// binds all interfaces so the service is reachable over the internet. When the flag is
// unset (local default) it stays loopback-only + origin-checked, so it's never exposed
// unless you explicitly opt in.
const PUBLIC_MODE = process.env.ALLOW_PUBLIC === 'true';
const WS_HOST = process.env.WS_HOST || (PUBLIC_MODE ? '0.0.0.0' : '127.0.0.1');

const connection = new Connection(RPC_URL, 'confirmed');
const PUMP_SDK = new PumpSdk();

// ────────────────────────────────────────────────────────────────────────────
// Metadata upload — same endpoint pump.fun's UI uses. Accepts a base64 image
// from the frontend and posts it as multipart/form-data.
// ────────────────────────────────────────────────────────────────────────────
// In-memory metadata cache. Keyed by a SHA-256 of the input fields. Lets us
// upload once and reuse for every launch in a batch with identical metadata.
// Lives for the lifetime of the process — no need for TTL since IPFS URIs are immutable.
const _metadataCache = new Map();

async function uploadTokenMetadataCached(input, log) {
  const key = metadataKey(input);
  const cached = _metadataCache.get(key);
  if (cached) {
    log?.(`Using cached metadata URI: ${cached}`, 'info');
    return { metadataUri: cached };
  }
  const result = await uploadTokenMetadata(input);
  if (result?.metadataUri) _metadataCache.set(key, result.metadataUri);
  return result;
}

function metadataKey({ name, symbol, description, twitter, telegram, website, fileBase64 }) {
  const h = createHash('sha256');
  h.update(String(name || ''));
  h.update('|');
  h.update(String(symbol || ''));
  h.update('|');
  h.update(String(description || ''));
  h.update('|');
  h.update(String(twitter || ''));
  h.update('|');
  h.update(String(telegram || ''));
  h.update('|');
  h.update(String(website || ''));
  h.update('|');
  h.update(String(fileBase64 || ''));
  return h.digest('hex');
}

async function uploadTokenMetadata({ name, symbol, description, twitter, telegram, website, fileBase64 }) {
  if (!fileBase64) throw new Error('Missing image (fileBase64)');

  // base64 data URL → Blob
  const m = fileBase64.match(/^data:([^;]+);base64,(.*)$/);
  const mime = m ? m[1] : 'image/png';
  const b64 = m ? m[2] : fileBase64;
  const buf = Buffer.from(b64, 'base64');
  const blob = new Blob([buf], { type: mime });

  const formData = new FormData();
  formData.append('file', blob, mime.includes('jpeg') ? 'image.jpg' : 'image.png');
  formData.append('name', name);
  formData.append('symbol', symbol);
  formData.append('description', description || '');
  formData.append('twitter', twitter || '');
  formData.append('telegram', telegram || '');
  formData.append('website', website || '');
  formData.append('showName', 'true');

  const resp = await fetch('https://pump.fun/api/ipfs', { method: 'POST', body: formData });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Metadata upload failed (${resp.status}): ${text.slice(0, 200)}`);
  }
  return resp.json();
}

// ────────────────────────────────────────────────────────────────────────────
// Build + submit one create-only TX (no buy)
// ────────────────────────────────────────────────────────────────────────────
// Sends a single create-only TX and returns AS SOON AS the network accepts the
// signature — does NOT wait for on-chain confirmation. Confirmation is tracked
// in the background and progress events are streamed when each tx lands or fails.
async function launchOneToken({ devKp, mintKp, tokenInfo, cuLimit, cuPrice, cashback, log, send, index, total, onConfirm }) {
  const stepStart = (label) => {
    send('progress', { index, total, status: 'building', mint: mintKp.publicKey.toBase58(), step: label });
  };

  stepStart('Uploading metadata…');
  const meta = await uploadTokenMetadataCached(tokenInfo, (m, l) => log(`[${index + 1}/${total}] ${m}`, l));

  stepStart('Building tx…');
  const createIx = await PUMP_SDK.createV2Instruction({
    mint: mintKp.publicKey,
    name: tokenInfo.name,
    symbol: tokenInfo.symbol,
    uri: meta.metadataUri,
    creator: devKp.publicKey,
    user: devKp.publicKey,
    mayhemMode: false,
    cashback: !!cashback,
  });
  const extendIx = await PUMP_SDK.extendAccountInstruction({
    account: bondingCurvePda(mintKp.publicKey),
    user: devKp.publicKey,
  });

  const { blockhash, lastValidBlockHeight } = await connection.getLatestBlockhash('confirmed');
  const ixs = [
    ComputeBudgetProgram.setComputeUnitLimit({ units: cuLimit ?? 400_000 }),
    ComputeBudgetProgram.setComputeUnitPrice({ microLamports: cuPrice ?? 25_000 }),
    createIx,
    extendIx,
  ];
  const tx = new VersionedTransaction(
    new TransactionMessage({
      payerKey: devKp.publicKey,
      recentBlockhash: blockhash,
      instructions: ixs,
    }).compileToV0Message()
  );
  tx.sign([devKp, mintKp]);

  stepStart('Sending tx…');
  const sig = await connection.sendTransaction(tx, { skipPreflight: true, maxRetries: 3 });
  log(`[${index + 1}/${total}] Sent — ${sig.slice(0, 16)}…`, 'info');

  // Optimistic "sent" status — the loop continues, confirmation lands in the background
  send('progress', { index, total, status: 'sent', mint: mintKp.publicKey.toBase58(), signature: sig, step: 'Sent — confirming async…' });

  // Fire-and-forget confirmation tracker. It will emit success/failed via onConfirm later.
  connection.confirmTransaction({ signature: sig, blockhash, lastValidBlockHeight }, 'confirmed')
    .then((conf) => {
      const ok = !conf.value.err;
      onConfirm?.({ index, mint: mintKp.publicKey.toBase58(), signature: sig, success: ok, error: conf.value.err });
    })
    .catch((e) => {
      onConfirm?.({ index, mint: mintKp.publicKey.toBase58(), signature: sig, success: false, error: e.message });
    });

  return { signature: sig, mint: mintKp.publicKey.toBase58() };
}

// ────────────────────────────────────────────────────────────────────────────
// WS server (loopback-only + origin-checked, like the other services)
// ────────────────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({
  host: WS_HOST,
  port: PORT,
  verifyClient: (info, cb) => {
    if (PUBLIC_MODE) { cb(true); return; } // public/Railway deploy — no loopback/origin gate
    const remote = info.req.socket.remoteAddress || '';
    const isLoopback = remote === '127.0.0.1' || remote === '::1' || remote === '::ffff:127.0.0.1';
    if (!isLoopback) return cb(false, 403, 'Forbidden');
    const allowed = new Set(['http://localhost:43900', 'http://127.0.0.1:43900', 'http://localhost:5180', 'http://127.0.0.1:5180', '', 'null']);
    if (!allowed.has(info.origin || '')) return cb(false, 403, 'Forbidden');
    cb(true);
  },
});
console.log(`MassLaunch WS listening on ws://${WS_HOST}:${PORT}${PUBLIC_MODE ? ' (PUBLIC)' : ' (loopback-only, origin-checked)'}`);

wss.on('connection', (ws) => {
  console.log('Client connected');
  let stopRequested = false;

  const send = (type, data = {}) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ type, ...data })); };
  const log = (message, level = 'info') => { send('log', { message, level }); console.log(`[${level}] ${message}`); };

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { send('error', { message: 'Invalid JSON' }); return; }
    if (msg.type === 'stop') { stopRequested = true; log('Stop requested', 'warn'); return; }
    if (msg.type !== 'launch') { send('error', { message: `Unknown message type: ${msg.type}` }); return; }

    try { await runLaunch(msg); }
    catch (err) {
      log(`Fatal: ${err.message}`, 'error');
      console.error(err);
    }
  });

  ws.on('close', () => { console.log('Client disconnected'); stopRequested = true; });

  async function runLaunch({ devSecret, tokens, delayMs, cuLimit, cuPrice, cashback }) {
    if (!devSecret) return send('error', { message: 'Missing devSecret' });
    let devKp;
    try { devKp = Keypair.fromSecretKey(bs58.decode(devSecret)); }
    catch { return send('error', { message: 'Invalid devSecret' }); }

    if (!Array.isArray(tokens) || tokens.length === 0) return send('error', { message: 'tokens[] required' });

    log(`Launching ${tokens.length} token(s) from ${devKp.publicKey.toBase58()}`);
    let launched = 0;
    let failed = 0;
    stopRequested = false;

    // Pre-warm the metadata cache: dedupe distinct configs and upload each ONCE up front.
    // For the common case where all N tokens share one config, this collapses to a single upload.
    const distinctConfigs = new Map(); // key → first occurrence config
    for (const t of tokens) {
      if (!t || !t.name || !t.symbol || !t.fileBase64) continue;
      const k = metadataKey(t);
      if (!distinctConfigs.has(k)) distinctConfigs.set(k, t);
    }
    if (distinctConfigs.size > 0) {
      log(`Pre-uploading metadata for ${distinctConfigs.size} distinct config(s)…`);
      const t0 = Date.now();
      for (const t of distinctConfigs.values()) {
        try { await uploadTokenMetadataCached(t, (m, l) => log(`(prewarm) ${m}`, l)); }
        catch (e) { log(`Pre-upload failed: ${e.message}`, 'warn'); }
      }
      log(`Pre-upload done in ${Date.now() - t0}ms — every launch will reuse the cached URI`, 'success');
    }

    for (let i = 0; i < tokens.length; i++) {
      if (stopRequested) { log('Stopped by user', 'warn'); break; }
      const t = tokens[i];
      if (!t || !t.name || !t.symbol || !t.fileBase64) {
        send('progress', { index: i, total: tokens.length, status: 'failed', error: 'Missing name/symbol/image' });
        failed++;
        continue;
      }

      const mintKp = Keypair.generate();
      send('progress', { index: i, total: tokens.length, status: 'building', mint: mintKp.publicKey.toBase58() });
      log(`[${i + 1}/${tokens.length}] Launching "${t.name}" (${t.symbol}) → ${mintKp.publicKey.toBase58().slice(0, 8)}…`);

      try {
        await launchOneToken({
          devKp, mintKp, tokenInfo: t, cuLimit, cuPrice, cashback,
          log, send, index: i, total: tokens.length,
          onConfirm: ({ index, mint, signature, success, error }) => {
            // Background confirmation handler — fires after the loop has already moved on
            if (success) {
              launched++;
              send('progress', { index, total: tokens.length, status: 'success', mint, signature });
              log(`[${index + 1}/${tokens.length}] ✓ confirmed ${mint}`, 'success');
            } else {
              failed++;
              send('progress', { index, total: tokens.length, status: 'failed', mint, signature, error: typeof error === 'string' ? error : JSON.stringify(error) });
              log(`[${index + 1}/${tokens.length}] ✗ confirmation failed: ${typeof error === 'string' ? error : JSON.stringify(error)}`, 'error');
            }
          },
        });
        // launchOneToken returns as soon as the TX is SENT — counts as "fired", not "confirmed"
      } catch (e) {
        failed++;
        send('progress', { index: i, total: tokens.length, status: 'failed', mint: mintKp.publicKey.toBase58(), error: e.message });
        log(`[${i + 1}/${tokens.length}] ✗ ${e.message}`, 'error');
      }

      // Delay between launches (skip after last token)
      if (i < tokens.length - 1 && delayMs && delayMs > 0 && !stopRequested) {
        await new Promise((r) => setTimeout(r, delayMs));
      }
    }

    log(`All ${tokens.length} TXs sent — confirmations tracked in background`, 'info');
    send('done', { launched: tokens.length - failed, failed });
  }
});
