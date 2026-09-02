# Purpose: Python inference microservice for the betting_features demo.
#
# Shows the request-response feature serving pattern — equivalent to
# chalk_client.query() or a Feast online server call.
#
# Two modes:
#   1. REST endpoint   POST /score  — synchronous single-bet scoring
#   2. Kafka consumer  background   — streaming auto-scoring
#
# Feature lookup: O(1) PK lookup directly from fluss.betting.player_feature_store
# via the pyfluss async client. Both this service and the Flink temporal join in
# 04_risk_scoring_inference.sql read the same online store — no mirror, no lag.
#
# Scoring: ONNX model (train_model.py) or rule engine fallback.
#
# Usage:
#   python3 app.py
#   curl -X POST http://localhost:9051/score \
#        -H "Content-Type: application/json" \
#        -d '{"player_id":"BOT_001"}'

import asyncio
import collections
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone

import fluss
import numpy as np
from flask import Flask, jsonify, render_template_string, request
from kafka import KafkaConsumer, KafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "host.minikube.internal:9092")
FLUSS_BOOTSTRAP  = os.getenv("FLUSS_BOOTSTRAP",
    "coordinator-server-0.coordinator-server-hs.fluss.svc.cluster.local:9124")

ONNX_MODEL_PATH = os.path.join(os.path.dirname(__file__), "model", "risk_model.onnx")

app = Flask(__name__)

_kafka_events = collections.deque(maxlen=50)  # ring buffer of recent Kafka-scored bets

# ── Persistent Fluss connection ────────────────────────────────────────────────
# pyfluss is async; Flask is synchronous. Bridge: run a dedicated asyncio event
# loop in a daemon thread and submit each lookup via run_coroutine_threadsafe().
# One connection is created at startup and reused across all requests.

_loop = asyncio.new_event_loop()
threading.Thread(target=_loop.run_forever, daemon=True, name="fluss-loop").start()

_fluss_conn = None  # type: fluss.FlussConnection


async def _init_fluss() -> None:
    global _fluss_conn
    config = fluss.Config({"bootstrap.servers": FLUSS_BOOTSTRAP})
    _fluss_conn = await fluss.FlussConnection.create(config)
    print(f"[inference] Fluss connection established → {FLUSS_BOOTSTRAP}")


try:
    asyncio.run_coroutine_threadsafe(_init_fluss(), _loop).result(timeout=10)
except Exception as e:
    print(f"[inference] WARNING: Fluss init failed — feature lookups will return empty: {e}")


# ── ONNX model (optional) ──────────────────────────────────────────────────────
ort_session = None
try:
    import onnxruntime as rt
    if os.path.exists(ONNX_MODEL_PATH):
        ort_session = rt.InferenceSession(ONNX_MODEL_PATH)
        print(f"[inference] ONNX model loaded from {ONNX_MODEL_PATH}")
    else:
        print("[inference] ONNX model not found — falling back to rule engine")
except ImportError:
    print("[inference] onnxruntime not installed — using rule engine fallback")

RISK_LABELS = ["NORMAL", "RG_ALERT", "SHARP_BETTOR", "STAKE_LIMIT", "FRAUD_HIGH"]


# ── Feature lookup ─────────────────────────────────────────────────────────────
def lookup_features(player_id: str) -> dict:
    """
    O(1) PK lookup directly from fluss.betting.player_feature_store.
    Equivalent to chalk_client.query(output=[PlayerFeatures], input={player_id}).
    Returns an empty dict if the player is not in the store yet.
    """
    if _fluss_conn is None:
        return {}

    async def _lookup():
        table = await _fluss_conn.get_table(
            fluss.TablePath("betting", "player_feature_store")
        )
        lookuper = table.new_lookup().create_lookuper()
        return await lookuper.lookup({"player_id": player_id})

    try:
        result = asyncio.run_coroutine_threadsafe(_lookup(), _loop).result(timeout=5.0)
        features = result or {}
    except Exception as e:
        print(f"[inference] Fluss lookup failed for {player_id}: {e}")
        return {}

    # Staleness guard: decay current_loss_streak to 0 if the feature row hasn't
    # been updated in 60 min. Mirrors the CASE WHEN in 04_risk_scoring_inference.sql.
    last_updated = features.get("last_updated_at")
    if last_updated:
        try:
            ts = last_updated if hasattr(last_updated, "tzinfo") else last_updated
            if hasattr(ts, "replace"):
                age = datetime.now(timezone.utc) - ts.replace(tzinfo=timezone.utc)
            else:
                age = timedelta(0)
            if age > timedelta(minutes=60):
                features["current_loss_streak"] = 0
        except Exception:
            pass

    return features


# ── Scoring ────────────────────────────────────────────────────────────────────
def _f(features, key, default):
    """Return feature value, falling back to default when missing OR None."""
    v = features.get(key)
    return v if v is not None else default


def rule_engine(features: dict):
    """Rule-based fallback — identical logic to 04_risk_scoring_inference.sql."""
    reasons = []
    triggered_features = set()

    if _f(features, "bets_last_10min",      0)   > 8:
        reasons.append("BOT_VELOCITY");    triggered_features.add("bets_last_10min")
    if _f(features, "geo_mismatches_15min",  0)   > 1:
        reasons.append("GEO_MISMATCH");    triggered_features.add("geo_mismatches_15min")
    if _f(features, "current_loss_streak",   0)   >= 6:
        reasons.append("LOSS_STREAK");     triggered_features.add("current_loss_streak")
    if _f(features, "live_bet_ratio_15min",  0.0) > 0.5:
        reasons.append("HIGH_LIVE_RATIO"); triggered_features.add("live_bet_ratio_15min")
    if _f(features, "distinct_sports_30min", 1)   >= 4:
        reasons.append("CHASING_LOSSES");  triggered_features.add("distinct_sports_30min")
    if _f(features, "avg_odds_15min",        2.0) < 2 \
       and _f(features, "bets_last_10min",   0)   > 5:
        reasons.append("SHARP_ODDS_PATTERN")
        triggered_features.update(["avg_odds_15min", "bets_last_10min"])
    if _f(features, "total_stake_30min",     0.0) > 500:
        reasons.append("DAILY_LIMIT");     triggered_features.add("total_stake_30min")

    if "BOT_VELOCITY" in reasons or "GEO_MISMATCH" in reasons:
        return "FRAUD_HIGH", reasons, triggered_features
    if any(r in reasons for r in ["LOSS_STREAK", "HIGH_LIVE_RATIO", "CHASING_LOSSES"]):
        return "RG_ALERT", reasons, triggered_features
    if "SHARP_ODDS_PATTERN" in reasons:
        return "SHARP_BETTOR", reasons, triggered_features
    if "DAILY_LIMIT" in reasons:
        return "STAKE_LIMIT", reasons, triggered_features
    return "NORMAL", reasons, triggered_features


def score_from_features(features: dict):
    if ort_session is None:
        return rule_engine(features)

    feat_vec = np.array([[
        float(_f(features, "bets_last_10min",      0)),
        float(_f(features, "total_stake_30min",    0)),
        float(_f(features, "live_bet_ratio_15min", 0)),
        float(_f(features, "avg_odds_15min",       2.0)),
        float(_f(features, "current_loss_streak",  0)),
        float(_f(features, "geo_mismatches_15min", 0)),
        float(_f(features, "distinct_sports_30min",1)),
        float(_f(features, "net_pnl_30min",        0)),
    ]], dtype=np.float32)

    try:
        pred = ort_session.run(None, {"features": feat_vec})[0]
        risk_level = RISK_LABELS[int(pred[0])]
        _, reasons, triggered = rule_engine(features)
        return risk_level, reasons, triggered
    except Exception as e:
        print(f"[inference] ONNX inference failed: {e} — falling back to rules")
        return rule_engine(features)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _jsonify_features(features: dict) -> dict:
    """Convert datetime/Decimal values so Flask's jsonify can serialize them."""
    out = {}
    for k, v in features.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ── REST endpoint ──────────────────────────────────────────────────────────────
@app.route("/score", methods=["POST"])
def score_endpoint():
    t0 = time.monotonic()
    body = request.get_json(force=True)
    if not body or "player_id" not in body:
        return jsonify({"error": "player_id required"}), 400

    features = lookup_features(body["player_id"])
    risk_level, reasons, triggered = score_from_features(features)

    return jsonify({
        "player_id":           body["player_id"],
        "risk_level":          risk_level,
        "risk_reasons":        reasons,
        "triggered_features":  list(triggered),
        "features":            _jsonify_features(features),
        "response_time_ms":    round((time.monotonic() - t0) * 1000, 2),
    })


@app.route("/features/<player_id>", methods=["GET"])
def get_features(player_id):  # noqa: F811
    t0 = time.monotonic()
    features = lookup_features(player_id)
    return jsonify({
        "player_id":        player_id,
        "features":         _jsonify_features(features),
        "response_time_ms": round((time.monotonic() - t0) * 1000, 2),
    })


@app.route("/events")
def events():
    return jsonify(list(_kafka_events))


@app.route("/health")
def health():
    return jsonify({
        "status":      "ok",
        "onnx_loaded": ort_session is not None,
        "fluss_ready": _fluss_conn is not None,
    })


_UI = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Betting Risk Inference</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; padding: 2rem; }
  h1 { font-size: 1.4rem; font-weight: 700; color: #fff; margin-bottom: 1.5rem; }
  h1 span { color: #6366f1; }
  .card { background: #1e2030; border: 1px solid #2d3148; border-radius: 10px; padding: 1.25rem; max-width: 960px; }
  .card h2 { font-size: 0.85rem; font-weight: 600; text-transform: uppercase; letter-spacing: .05em; color: #818cf8; margin-bottom: 1rem; }
  label:not(.switch) { display: block; font-size: 0.8rem; color: #94a3b8; margin-bottom: 0.25rem; margin-top: 0.75rem; }
  input { width: 100%; background: #0f1117; border: 1px solid #2d3148; border-radius: 6px; color: #e2e8f0;
          padding: 0.5rem 0.75rem; font-size: 0.875rem; outline: none; }
  input:focus { border-color: #6366f1; }
  .hint { font-size: 0.72rem; color: #475569; margin-top: 0.25rem; }
  button { margin-top: 1rem; width: 100%; background: #6366f1; color: #fff; border: none; border-radius: 6px;
           padding: 0.6rem; font-size: 0.875rem; font-weight: 600; cursor: pointer; transition: background .15s; }
  button:hover { background: #4f46e5; }
  button:disabled { background: #374151; color: #6b7280; cursor: default; }
  .result { margin-top: 1rem; }
  .badge { display: inline-block; padding: 0.25rem 0.75rem; border-radius: 999px; font-size: 0.8rem; font-weight: 700; }
  .NORMAL       { background: #14532d; color: #4ade80; }
  .RG_ALERT     { background: #78350f; color: #fbbf24; }
  .SHARP_BETTOR { background: #1e3a5f; color: #60a5fa; }
  .STAKE_LIMIT  { background: #312e81; color: #a78bfa; }
  .FRAUD_HIGH   { background: #7f1d1d; color: #f87171; }
  .meta { font-size: 0.75rem; color: #64748b; margin-top: 0.4rem; }
  .reasons { display: flex; flex-wrap: wrap; gap: 0.35rem; margin-top: 0.5rem; }
  .reason { background: #1e293b; border: 1px solid #334155; border-radius: 4px; font-size: 0.72rem; padding: 0.2rem 0.5rem; }
  table { width: 100%; border-collapse: collapse; margin-top: 0.5rem; font-size: 0.8rem; }
  td { padding: 0.3rem 0.4rem; border-bottom: 1px solid #1e293b; }
  td:first-child { color: #94a3b8; width: 55%; }
  td:last-child { color: #e2e8f0; font-variant-numeric: tabular-nums; }
  tr.triggered td { background: rgba(239,68,68,.12); }
  tr.triggered td:first-child { color: #fca5a5; font-weight: 600; }
  tr.triggered td:last-child { color: #f87171; font-weight: 700; }
  .empty { color: #475569; font-size: 0.8rem; margin-top: 0.75rem; }
  .err { color: #f87171; font-size: 0.8rem; margin-top: 0.75rem; }
  .layout { display: grid; grid-template-columns: 1fr 1fr; gap: 1.5rem; max-width: 960px; align-items: start; }
  @media(max-width:700px){ .layout { grid-template-columns: 1fr; } }
  .feed-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 0.75rem; }
  .feed-header-left { display: flex; align-items: center; gap: 0.5rem; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: #22c55e; animation: pulse 1.5s infinite; }
  .dot.off { background: #374151; animation: none; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  .feed-row { display: flex; align-items: center; gap: 0.6rem; padding: 0.45rem 0;
              border-bottom: 1px solid #1e293b; font-size: 0.78rem; }
  .feed-row:last-child { border-bottom: none; }
  .feed-ts { color: #475569; min-width: 4.5rem; font-variant-numeric: tabular-nums; }
  .feed-pid { color: #94a3b8; min-width: 7rem; }
  .feed-reasons { color: #64748b; font-size: 0.72rem; flex: 1; }
  .feed-empty { color: #475569; font-size: 0.8rem; padding: 0.5rem 0; }
  .feed-scroll { max-height: 420px; overflow-y: auto; }
  /* toggle switch */
  .switch { position: relative; display: inline-block; width: 36px; height: 20px; flex-shrink: 0; }
  .switch input { opacity: 0; width: 0; height: 0; }
  .slider { position: absolute; cursor: pointer; inset: 0; background: #374151; border-radius: 20px; transition: .2s; }
  .slider:before { position: absolute; content: ""; height: 14px; width: 14px; left: 3px; bottom: 3px;
                   background: #fff; border-radius: 50%; transition: .2s; }
  input:checked + .slider { background: #6366f1; }
  input:checked + .slider:before { transform: translateX(16px); }
  /* player chips */
  .players { margin-bottom: 1rem; max-width: 960px; }
  .players p { font-size: 0.72rem; color: #475569; margin-bottom: 0.5rem; }
  .players-row { display: flex; flex-wrap: wrap; gap: 0.4rem; }
  .pchip { display: flex; flex-direction: column; align-items: flex-start; background: #1e2030;
           border: 1px solid #2d3148; border-radius: 8px; padding: 0.35rem 0.65rem;
           cursor: pointer; transition: border-color .15s, background .15s; }
  .pchip:hover { border-color: #6366f1; background: #252840; }
  .pchip.selected { border-color: #6366f1; background: #252840; }
  .pchip-id { font-size: 0.8rem; font-weight: 600; color: #e2e8f0; }
  .pchip-label { font-size: 0.68rem; color: #64748b; margin-top: 1px; }
  .auto-row { display: flex; align-items: center; gap: 0.5rem; margin-top: 0.75rem; }
  .auto-label { font-size: 0.78rem; color: #64748b; }
</style>
</head>
<body>
<h1>Betting Risk <span>Inference</span></h1>

<div class="players">
  <p>Quick select — click to load a known outlier player:</p>
  <div class="players-row" id="pchips"></div>
</div>

<div class="layout">

  <!-- Left: on-demand score -->
  <div class="card">
    <h2>Score a Player &nbsp;/score</h2>
    <label>Player ID</label>
    <input id="s-pid" value="GEO_001" placeholder="e.g. BOT_001 or any player ID">
    <div class="auto-row">
      <label class="switch" title="Auto-refresh result every 2 s">
        <input type="checkbox" id="auto-refresh" checked onchange="if(!this.checked) stopAutoRefresh()">
        <span class="slider"></span>
      </label>
      <span class="auto-label">Auto-refresh every 2 s</span>
      <div class="dot" id="auto-dot" style="visibility:hidden"></div>
    </div>
    <button id="s-btn" onclick="scoreBtn()">Score</button>
    <div id="s-result"></div>
  </div>

  <!-- Right: Kafka live feed -->
  <div class="card">
    <div class="feed-header">
      <div class="feed-header-left">
        <div class="dot" id="feed-dot"></div>
        <h2>Kafka Stream &nbsp;— live scored bets</h2>
      </div>
      <label class="switch" title="Pause / resume live feed">
        <input type="checkbox" id="feed-toggle" checked onchange="toggleFeed(this)">
        <span class="slider"></span>
      </label>
    </div>
    <div class="feed-scroll">
      <div id="feed-body"><p class="feed-empty">Waiting for bets…</p></div>
    </div>
  </div>

</div>

<script>
const BASE = '';

let _autoInterval = null;
let _scoring      = false;

const KNOWN_PLAYERS = [
  { id: 'BOT_001',    label: 'Bot velocity',    alert: 'FRAUD_HIGH'   },
  { id: 'GEO_001',    label: 'Geo mismatch',    alert: 'FRAUD_HIGH'   },
  { id: 'STREAK_001', label: 'Loss streak',     alert: 'RG_ALERT'     },
  { id: 'LIVE_001',   label: 'High live ratio', alert: 'RG_ALERT'     },
  { id: 'SHARP_001',  label: 'Sharp odds',      alert: 'SHARP_BETTOR' },
  { id: 'LIMIT_001',  label: 'Stake limit',     alert: 'STAKE_LIMIT'  },
];

function buildChips() {
  const container = document.getElementById('pchips');
  KNOWN_PLAYERS.forEach(p => {
    const chip = document.createElement('div');
    chip.className = 'pchip';
    chip.dataset.id = p.id;
    chip.innerHTML = `<span class="pchip-id">${p.id}</span><span class="pchip-label">${p.label}</span>`;
    chip.onclick = () => selectPlayer(p.id);
    container.appendChild(chip);
  });
}

function selectPlayer(pid) {
  document.getElementById('s-pid').value = pid;
  document.querySelectorAll('.pchip').forEach(c => c.classList.toggle('selected', c.dataset.id === pid));
  stopAutoRefresh();
}

document.getElementById('s-pid').addEventListener('input', e => {
  document.querySelectorAll('.pchip').forEach(c =>
    c.classList.toggle('selected', c.dataset.id === e.target.value));
  stopAutoRefresh();
});

buildChips();
selectPlayer('GEO_001');

function riskBadge(level) {
  return `<span class="badge ${level}">${level}</span>`;
}

function stopAutoRefresh() {
  if (_autoInterval) { clearInterval(_autoInterval); _autoInterval = null; }
  document.getElementById('auto-dot').style.visibility = 'hidden';
}

function startAutoRefresh() {
  stopAutoRefresh();
  if (!document.getElementById('auto-refresh').checked) return;
  document.getElementById('auto-dot').style.visibility = 'visible';
  _autoInterval = setInterval(doScore, 2000);
}

async function doScore() {
  if (_scoring) return;
  _scoring = true;
  const pid = document.getElementById('s-pid').value.trim();
  try {
    const r = await fetch(BASE + '/score', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({player_id: pid})
    });
    const d = await r.json();
    if (!r.ok) { document.getElementById('s-result').innerHTML = `<p class="err">${d.error||r.status}</p>`; return; }
    const reasons = (d.risk_reasons||[]).map(r => `<span class="reason">${r}</span>`).join('');
    document.getElementById('s-result').innerHTML = `
      <div class="result">
        ${riskBadge(d.risk_level)}
        <p class="meta">${d.response_time_ms} ms &bull; ${d.player_id}</p>
        <div class="reasons">${reasons || '<span class="reason">no triggers</span>'}</div>
        ${featureTable(d.features, d.triggered_features||[])}
      </div>`;
  } catch(e) {
    document.getElementById('s-result').innerHTML = `<p class="err">${e.message}</p>`;
  } finally {
    _scoring = false;
  }
}

async function scoreBtn() {
  const btn = document.getElementById('s-btn');
  btn.disabled = true; btn.textContent = 'Scoring…';
  stopAutoRefresh();
  await doScore();
  btn.disabled = false; btn.textContent = 'Score';
  startAutoRefresh();
}

let feedRunning = true;

function toggleFeed(cb) {
  feedRunning = cb.checked;
  document.getElementById('feed-dot').classList.toggle('off', !feedRunning);
}

async function pollFeed() {
  if (feedRunning) {
    try {
      const r = await fetch(BASE + '/events');
      const events = await r.json();
      const el = document.getElementById('feed-body');
      if (!events.length) {
        el.innerHTML = '<p class="feed-empty">Waiting for bets…</p>';
      } else {
        el.innerHTML = events.map(e => {
          const reasons = e.reasons.length ? e.reasons.join(', ') : 'no triggers';
          return `<div class="feed-row">
            <span class="feed-ts">${e.ts}</span>
            <span class="feed-pid">${e.player_id||'—'}</span>
            <span>${riskBadge(e.risk_level)}</span>
            <span class="feed-reasons">${reasons}</span>
          </div>`;
        }).join('');
      }
    } catch(_) {}
  }
  setTimeout(pollFeed, 2000);
}
pollFeed();

function featureTable(features, triggered) {
  if (!features || Object.keys(features).length === 0) return '';
  const hit = new Set(triggered || []);
  const ORDER = ['bets_last_10min','live_bet_ratio_15min','avg_odds_15min','geo_mismatches_15min',
                 'current_loss_streak','distinct_sports_30min','total_stake_30min','net_pnl_30min',
                 'kyc_tier','balance','lifetime_deposits','last_updated_at'];
  const keys = [...ORDER.filter(k => k in features), ...Object.keys(features).filter(k => !ORDER.includes(k))];
  const rows = keys.map(k => {
    let v = features[k];
    if (v === null || v === undefined) v = '—';
    else if (typeof v === 'number') v = Number.isInteger(v) ? v : v.toFixed(4);
    const cls = hit.has(k) ? ' class="triggered"' : '';
    return `<tr${cls}><td>${k}</td><td>${v}</td></tr>`;
  }).join('');
  return `<table>${rows}</table>`;
}
</script>
</body>
</html>"""


@app.route("/ui")
@app.route("/")
def ui():
    return render_template_string(_UI)


# ── Kafka streaming consumer ───────────────────────────────────────────────────
def kafka_consumer_thread():
    """
    Background thread: consumes 'bets' topic and emits risk scores to
    'risk_scores_flask' — the streaming equivalent of the Flink job in 04_.
    Run both together during demos to show the two serving modes side-by-side.
    """
    try:
        consumer = KafkaConsumer(
            "bets",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            group_id="inference-service",
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda m: json.dumps(m).encode(),
        )
        print("[inference] Kafka consumer started — listening on 'bets' topic")
        for msg in consumer:
            bet = json.loads(msg.value.decode())
            features = lookup_features(bet.get("player_id", ""))
            risk_level, reasons, triggered = score_from_features(features)
            producer.send("risk_scores_flask", {
                "bet_id":       bet.get("bet_id"),
                "player_id":    bet.get("player_id"),
                "risk_level":   risk_level,
                "risk_reasons": ",".join(reasons),
            })
            _kafka_events.appendleft({
                "ts":         time.strftime("%H:%M:%S"),
                "bet_id":     bet.get("bet_id"),
                "player_id":  bet.get("player_id"),
                "amount":     bet.get("amount"),
                "risk_level": risk_level,
                "reasons":    reasons,
                "triggered":  list(triggered),
            })
    except Exception as e:
        print(f"[inference] Kafka consumer error: {e}")


threading.Thread(target=kafka_consumer_thread, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9051, debug=False)
