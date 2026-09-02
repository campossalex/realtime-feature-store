# Purpose: Generates anomalous betting patterns for the betting_features demo.
#
# Exposes a self-service web UI on port 9050.
# Click a button to fire any archetype; live Kafka events stream in the log panel.
#
# Archetypes:
#   BOT     — BOT_001:    20 bets in 10 min, identical odds → BOT_VELOCITY
#   GEO     — GEO_001:    login from home country then foreign country → GEO_MISMATCH
#   STREAK  — STREAK_001: 8 consecutive losses → LOSS_STREAK + CHASING_LOSSES
#   LIVE    — LIVE_001:   90% live bets → HIGH_LIVE_RATIO
#   SHARP   — SHARP_001:  consistently odds < 2 → SHARP_ODDS_PATTERN
#   LIMIT   — LIMIT_001:  £7 000 staked in one session → DAILY_LIMIT

import configparser
import json
import queue
import random
import threading
import time
import uuid
from datetime import datetime, date

import psycopg2
from faker import Faker
from flask import Flask, Response, jsonify
from kafka import KafkaProducer

fake = Faker()
app  = Flask(__name__)

# ── Configuration ──────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read("configuration/configuration.ini")

BOOTSTRAP     = config["KAFKA"]["bootstrap_servers"]
T_BETS        = config["KAFKA"]["topic_bets"]
T_SETTLEMENTS = config["KAFKA"]["topic_settlements"]
T_LOGINS      = config["KAFKA"]["topic_logins"]
T_DEPOSITS    = config["KAFKA"]["topic_deposits"]

PG_HOST = config["POSTGRES"]["host"]
PG_PORT = int(config["POSTGRES"]["port"])
PG_DB   = config["POSTGRES"]["database"]
PG_USER = config["POSTGRES"]["user"]
PG_PASS = config["POSTGRES"]["password"]

# ── Shared state ───────────────────────────────────────────────────────────────
_log_listeners: list[queue.Queue] = []
_log_lock       = threading.Lock()
_running: dict[str, bool] = {k: False for k in ["BOT", "GEO", "STREAK", "LIVE", "SHARP", "LIMIT"]}

producer = None  # initialised in main()


def _broadcast(msg: str):
    """Push a log line to every active SSE listener."""
    with _log_lock:
        for q in _log_listeners:
            q.put(msg)


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _broadcast(line)


# ── Helpers ────────────────────────────────────────────────────────────────────
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def new_bet_id():
    return "BET-" + uuid.uuid4().hex[:10].upper()

def send_bet(player_id, amount, bet_type, sport, odds):
    bid = new_bet_id()
    producer.send(T_BETS, value={
        "bet_id":     bid,
        "player_id":  player_id,
        "amount":     amount,
        "bet_type":   bet_type,
        "sport":      sport,
        "odds":       odds,
        "event_time": now_str(),
    })
    log(f"  [BET]  {bid}  player={player_id}  {bet_type}  {sport}  £{amount}  @{odds}")
    return bid

def send_settlement(player_id, bid, stake, outcome):
    payout = round(stake * random.uniform(1.5, 3.0), 2) if outcome == "win" else 0.00
    producer.send(T_SETTLEMENTS, value={
        "settlement_id": "SET-" + uuid.uuid4().hex[:10].upper(),
        "bet_id":        bid,
        "player_id":     player_id,
        "outcome":       outcome,
        "stake":         stake,
        "payout_amount": payout,
        "event_time":    now_str(),
    })
    log(f"  [SET]  {bid} → {outcome}  payout=£{payout}")

def send_login(player_id, country):
    producer.send(T_LOGINS, value={
        "event_id":    "LOG-" + uuid.uuid4().hex[:10].upper(),
        "player_id":   player_id,
        "session_id":  "SES-" + uuid.uuid4().hex[:8].upper(),
        "device_type": "mobile",
        "ip_address":  fake.ipv4_public(),
        "country":     country,
        "event_time":  now_str(),
    })
    log(f"  [LOG]  player={player_id}  country={country}")


# ── PostgreSQL seeding ─────────────────────────────────────────────────────────
OUTLINER_PLAYERS = [
    {"player_id": "BOT_001",    "username": "bot_bettor_001",    "kyc_tier": "BASIC",    "country": "GB"},
    {"player_id": "GEO_001",    "username": "geo_hopper_001",    "kyc_tier": "STANDARD", "country": "GB"},
    {"player_id": "STREAK_001", "username": "streaky_player_001","kyc_tier": "STANDARD", "country": "DE"},
    {"player_id": "LIVE_001",   "username": "live_addict_001",   "kyc_tier": "BASIC",    "country": "ES"},
    {"player_id": "SHARP_001",  "username": "sharp_bettor_001",  "kyc_tier": "ENHANCED", "country": "GB"},
    {"player_id": "LIMIT_001",  "username": "high_roller_001",   "kyc_tier": "ENHANCED", "country": "AU"},
]

def seed_outliners():
    log("Seeding outliner players into PostgreSQL...")
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                            user=PG_USER, password=PG_PASS)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO public.players
                      (player_id, username, kyc_tier, registration_date, country)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (player_id) DO NOTHING
                    """,
                    [(p["player_id"], p["username"], p["kyc_tier"],
                      date(2023, 1, 1), p["country"]) for p in OUTLINER_PLAYERS],
                )
                cur.executemany(
                    """
                    INSERT INTO public.player_wallets
                      (player_id, balance, lifetime_deposits, lifetime_withdrawals)
                    VALUES (%s, %s, %s, 0.00)
                    ON CONFLICT (player_id) DO NOTHING
                    """,
                    [(p["player_id"], 10000.00, 50000.00) for p in OUTLINER_PLAYERS],
                )
        log("Outliner players seeded.")
    finally:
        conn.close()


# ── Archetype scenarios ────────────────────────────────────────────────────────

def run_bot():
    """BOT_001: 25 bets in ~2 min at fixed odds → BOT_VELOCITY within the first HOP slide."""
    log("=== BOT_001 — 25 bets in ~2 min ===")
    send_login("BOT_001", "GB")
    for i in range(25):
        send_bet("BOT_001", amount=50.00, bet_type="single", sport="football", odds=1.95)
        time.sleep(random.uniform(3, 5))
    log("=== BOT_001 done ===")


def run_geo():
    """GEO_001: login GB then Brazil within 15-min window → GEO_MISMATCH."""
    log("=== GEO_001 — UK then Brazil logins ===")
    send_login("GEO_001", "GB")
    bid = send_bet("GEO_001", amount=75.00, bet_type="single", sport="tennis", odds=2.10)
    time.sleep(5)
    send_settlement("GEO_001", bid, 75.00, "loss")
    time.sleep(30)
    send_login("GEO_001", "BR")
    send_bet("GEO_001", amount=75.00, bet_type="live", sport="football", odds=1.80)
    log("=== GEO_001 done — GEO_MISMATCH triggered ===")


_STREAK_SPORTS = ["football", "basketball", "tennis", "horse_racing",
                  "cricket", "golf", "rugby", "baseball"]

def run_streak():
    """STREAK_001: 8 consecutive losses rotating across sports → LOSS_STREAK + CHASING_LOSSES."""
    log("=== STREAK_001 — 8 consecutive losses across 8 sports ===")
    send_login("STREAK_001", "DE")
    for i in range(8):
        stake = round(20 + i * 10, 2)
        sport = _STREAK_SPORTS[i % len(_STREAK_SPORTS)]
        bid = send_bet("STREAK_001", amount=stake, bet_type="accumulator",
                       sport=sport, odds=random.uniform(3.0, 6.0))
        time.sleep(3)
        send_settlement("STREAK_001", bid, stake, "loss")
        time.sleep(5)
    # Terminate with a win so MATCH_RECOGNIZE emits the streak length
    bid = send_bet("STREAK_001", amount=10.00, bet_type="single",
                   sport="football", odds=2.5)
    time.sleep(3)
    send_settlement("STREAK_001", bid, 10.00, "win")
    log("=== STREAK_001 done — LOSS_STREAK + CHASING_LOSSES triggered ===")


def run_live():
    """LIVE_001: 10 out of 12 bets are live → HIGH_LIVE_RATIO within ~1 min.
    HOP slide is 10 s; bets arrive every 8–10 s so ~7 bets land before the
    first feature-store update that crosses the 0.5 ratio threshold.
    """
    log("=== LIVE_001 — 90% live bet ratio (12 bets, 8–10 s apart) ===")
    send_login("LIVE_001", "ES")
    for i in range(12):
        bet_type = "live" if i < 10 else "single"
        send_bet("LIVE_001",
                 amount=round(random.uniform(10, 80), 2),
                 bet_type=bet_type,
                 sport=random.choice(["football", "tennis", "basketball"]),
                 odds=random.uniform(1.4, 3.5))
        time.sleep(random.uniform(8, 10))
    log("=== LIVE_001 done ===")


def run_sharp():
    """SHARP_001: 8 bets at odds 1.05–1.95, 3–5 s apart → SHARP_BETTOR in ~40 s.
    No settlements needed — rule uses bets_last_10min and avg_odds_15min only.
    8 bets keeps bets_last_10min at 8 (≤ 8) so FRAUD_HIGH does not shadow this alert.
    """
    log("=== SHARP_001 — 8 low-odds bets, 3–5 s apart ===")
    send_login("SHARP_001", "GB")
    for i in range(8):
        send_bet("SHARP_001", amount=round(random.uniform(200, 500), 2),
                 bet_type="single", sport="football",
                 odds=round(random.uniform(1.05, 1.95), 2))
        time.sleep(random.uniform(3, 5))
    log("=== SHARP_001 done — SHARP_BETTOR expected within next HOP slide ===")


def run_limit():
    """LIMIT_001: £7 000 staked in one session → DAILY_LIMIT."""
    log("=== LIMIT_001 — £7000 staked ===")
    send_login("LIMIT_001", "AU")
    remaining = 7000.0
    while remaining > 0:
        stake = min(remaining, round(random.uniform(300, 1500), 2))
        remaining -= stake
        send_bet("LIMIT_001", amount=stake,
                 bet_type=random.choice(["single", "accumulator"]),
                 sport=random.choice(["horse_racing", "football"]),
                 odds=random.uniform(2.0, 5.0))
        time.sleep(random.uniform(5, 15))
    log("=== LIMIT_001 done — DAILY_LIMIT triggered ===")


ARCHETYPES = {
    "BOT":    run_bot,
    "GEO":    run_geo,
    "STREAK": run_streak,
    "LIVE":   run_live,
    "SHARP":  run_sharp,
    "LIMIT":  run_limit,
}

ARCHETYPE_META = {
    "BOT":    {"label": "Bot / Velocity",      "player": "BOT_001",    "rule": "BOT_VELOCITY",      "desc": "25 bets in ~2 min at identical odds — triggers FRAUD_HIGH on the first feature-store slide"},
    "GEO":    {"label": "Geo Mismatch",         "player": "GEO_001",    "rule": "GEO_MISMATCH",      "desc": "Login from GB then Brazil in the same 15-min window"},
    "STREAK": {"label": "Loss Streak",          "player": "STREAK_001", "rule": "LOSS_STREAK",       "desc": "8 consecutive losses rotating across all 8 sports → LOSS_STREAK + CHASING_LOSSES"},
    "LIVE":   {"label": "Live Bet Addict",      "player": "LIVE_001",   "rule": "HIGH_LIVE_RATIO",   "desc": "10 of 12 bets are live (83% ratio), one bet every 8–10 s — triggers RG_ALERT in ~1 min"},
    "SHARP":  {"label": "Sharp Bettor",         "player": "SHARP_001",  "rule": "SHARP_ODDS_PATTERN","desc": "8 bets at odds < 2 placed 3–5 s apart — triggers SHARP_BETTOR in ~40 s"},
    "LIMIT":  {"label": "Stake Limit Breach",   "player": "LIMIT_001",  "rule": "DAILY_LIMIT",       "desc": "£7 000 staked in a single session"},
}


# ── Flask routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return Response(HTML, mimetype="text/html")


@app.route("/run/<code>", methods=["POST"])
def trigger(code):
    code = code.upper()
    if code not in ARCHETYPES:
        return jsonify({"error": "unknown archetype"}), 400
    if _running[code]:
        return jsonify({"status": "already_running"}), 409

    def _run():
        _running[code] = True
        try:
            ARCHETYPES[code]()
        finally:
            _running[code] = False
            _broadcast(f"__STATUS__{code}__false")

    _running[code] = True
    _broadcast(f"__STATUS__{code}__true")
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/status")
def status():
    return jsonify(_running)


@app.route("/stream")
def stream():
    """Server-Sent Events: pushes log lines and status updates to the browser."""
    q: queue.Queue = queue.Queue(maxsize=500)
    with _log_lock:
        _log_listeners.append(q)

    def generate():
        try:
            while True:
                try:
                    msg = q.get(timeout=25)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            with _log_lock:
                _log_listeners.remove(q)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Embedded HTML UI ───────────────────────────────────────────────────────────

HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Outliner Generator — Betting Features Demo</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: #0f1117; color: #e2e8f0; min-height: 100vh; padding: 2rem; }
  h1   { font-size: 1.4rem; font-weight: 600; color: #f8fafc; margin-bottom: .25rem; }
  .subtitle { font-size: .85rem; color: #64748b; margin-bottom: 2rem; }
  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
  .card {
    background: #1e2130; border: 1px solid #2d3148; border-radius: 10px;
    padding: 1.25rem; display: flex; flex-direction: column; gap: .75rem;
  }
  .card-header { display: flex; align-items: flex-start; justify-content: space-between; gap: .5rem; }
  .card-title  { font-size: 1rem; font-weight: 600; color: #f1f5f9; }
  .badge {
    font-size: .7rem; font-weight: 700; padding: .2rem .55rem; border-radius: 99px;
    white-space: nowrap; background: #1a2744; color: #60a5fa; border: 1px solid #1e40af;
  }
  .player { font-size: .75rem; color: #64748b; }
  .desc   { font-size: .82rem; color: #94a3b8; line-height: 1.5; flex: 1; }
  .btn {
    margin-top: .25rem; padding: .55rem 1rem; border: none; border-radius: 6px;
    font-size: .85rem; font-weight: 600; cursor: pointer; transition: opacity .15s, background .15s;
    background: #2563eb; color: #fff;
  }
  .btn:hover:not(:disabled) { background: #1d4ed8; }
  .btn:disabled { opacity: .45; cursor: not-allowed; }
  .btn.running  { background: #b45309; }
  .log-panel {
    background: #0d1117; border: 1px solid #2d3148; border-radius: 10px;
    padding: 1rem; height: 320px; overflow-y: auto; font-family: monospace;
    font-size: .78rem; color: #94a3b8; scroll-behavior: smooth;
  }
  .log-panel p { line-height: 1.6; white-space: pre-wrap; word-break: break-all; }
  .log-panel p.bet    { color: #38bdf8; }
  .log-panel p.set    { color: #4ade80; }
  .log-panel p.login  { color: #a78bfa; }
  .log-panel p.status { color: #fbbf24; font-weight: 600; }
  .log-label { font-size: .8rem; color: #475569; margin-bottom: .4rem; }
</style>
</head>
<body>
<h1>Outliner Generator</h1>
<p class="subtitle">Trigger anomalous player archetypes to observe risk scoring in real time.</p>

<div class="grid" id="cards"></div>

<p class="log-label">Live Kafka event log</p>
<div class="log-panel" id="log"></div>

<script>
const ARCHETYPES = """ + json.dumps(ARCHETYPE_META) + """;

const cards = document.getElementById('cards');
const logEl  = document.getElementById('log');

// Build cards
Object.entries(ARCHETYPES).forEach(([code, meta]) => {
  cards.innerHTML += `
    <div class="card" id="card-${code}">
      <div class="card-header">
        <span class="card-title">${meta.label}</span>
        <span class="badge">${meta.rule}</span>
      </div>
      <span class="player">${meta.player}</span>
      <p class="desc">${meta.desc}</p>
      <button class="btn" id="btn-${code}" onclick="trigger('${code}')">Run</button>
    </div>`;
});

function trigger(code) {
  fetch('/run/' + code, {method: 'POST'})
    .then(r => r.json())
    .then(d => {
      if (d.status === 'already_running') appendLog('status', `[${code}] already running.`);
    });
}

function setRunning(code, running) {
  const btn = document.getElementById('btn-' + code);
  if (!btn) return;
  btn.disabled = running;
  btn.textContent = running ? 'Running…' : 'Run';
  btn.classList.toggle('running', running);
}

// Load initial status
fetch('/status').then(r => r.json()).then(s => {
  Object.entries(s).forEach(([k, v]) => setRunning(k, v));
});

// SSE log stream
function appendLog(type, text) {
  const p = document.createElement('p');
  p.className = type;
  p.textContent = text;
  logEl.appendChild(p);
  logEl.scrollTop = logEl.scrollHeight;
  // Keep log from growing unbounded
  while (logEl.children.length > 500) logEl.removeChild(logEl.firstChild);
}

const es = new EventSource('/stream');
es.onmessage = e => {
  const msg = e.data;
  if (msg.startsWith('__STATUS__')) {
    const [, code, state] = msg.split('__').filter(Boolean);
    setRunning(code, state === 'true');
    return;
  }
  let type = 'default';
  if (msg.includes('[BET]'))  type = 'bet';
  else if (msg.includes('[SET]'))  type = 'set';
  else if (msg.includes('[LOG]'))  type = 'login';
  else if (msg.includes('==='))    type = 'status';
  appendLog(type, msg);
};
es.onerror = () => appendLog('status', '[stream] Connection lost — refresh to reconnect.');
</script>
</body>
</html>"""


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    global producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )
    seed_outliners()
    print(f"Outliner UI ready at http://0.0.0.0:9050")
    app.run(host="0.0.0.0", port=9050, threaded=True)


if __name__ == "__main__":
    main()
