# Purpose: Generates realistic background betting traffic for the betting_features demo.
#
# On startup: seeds 500 synthetic players + wallets into PostgreSQL so the
# postgres-cdc → Fluss pipeline (Job A in 03_) captures them as CDC events.
#
# Then runs an infinite loop producing to four Kafka topics:
#   bets             — bet placements (single / accumulator / live)
#   bet_settlements  — win / loss outcomes (30–120 s after placement)
#   login_events     — session starts (1–2 per day per player)
#   deposits         — payment events (weekly rhythm)
#
# All behaviour is within normal thresholds so the feature store is populated
# with "safe" values before the outliner generator fires during the demo.

import configparser
import json
import random
import time
import uuid
from collections import deque
from datetime import datetime, date

import psycopg2
from faker import Faker
from kafka import KafkaProducer

fake = Faker()
random.seed(42)

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

NUM_PLAYERS     = int(config["GENERATOR"]["num_players"])
MIN_FREQ_MS     = int(config["GENERATOR"]["min_event_freq_ms"])
MAX_FREQ_MS     = int(config["GENERATOR"]["max_event_freq_ms"])

# ── Constants ──────────────────────────────────────────────────────────────────
SPORTS    = ["football", "basketball", "tennis", "horse_racing", "cricket",
             "golf", "rugby", "baseball"]
BET_TYPES = ["single", "accumulator", "live"]
KYC_TIERS = ["BASIC", "STANDARD", "ENHANCED"]
COUNTRIES = ["GB", "DE", "FR", "ES", "IT", "NL", "SE", "NO", "AU", "CA"]
DEVICES   = ["mobile", "desktop", "tablet"]
METHODS   = ["card", "bank_transfer", "ewallet"]

# ── Helpers ────────────────────────────────────────────────────────────────────
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def rand_odds():
    """Normal bettors favor 2.0–4.0; longshots exist but are rare."""
    return round(random.choices(
        [random.uniform(2.0, 3.5), random.uniform(3.5, 7.0), random.uniform(7.0, 15.0)],
        weights=[0.55, 0.30, 0.15]
    )[0], 2)

def rand_stake():
    """Casual bettors: mostly small stakes."""
    return round(random.choices(
        [random.uniform(5, 25), random.uniform(25, 80)],
        weights=[0.90, 0.10]
    )[0], 2)


# ── Player pool ────────────────────────────────────────────────────────────────
def build_players(n):
    players = []
    for _ in range(n):
        country = random.choice(COUNTRIES)
        players.append({
            "player_id":         "P-" + uuid.uuid4().hex[:8].upper(),
            "username":          fake.user_name(),
            "kyc_tier":          random.choices(KYC_TIERS, weights=[0.5, 0.35, 0.15])[0],
            "registration_date": fake.date_between(start_date="-5y", end_date="today"),
            "country":           country,
            "balance":           round(random.uniform(10, 2000), 2),
            "lifetime_deposits": round(random.uniform(100, 10000), 2),
        })
    return players


def seed_players(players):
    print(f"Seeding {len(players)} players into PostgreSQL...")
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
                      p["registration_date"], p["country"]) for p in players],
                )
                cur.executemany(
                    """
                    INSERT INTO public.player_wallets
                      (player_id, balance, lifetime_deposits, lifetime_withdrawals)
                    VALUES (%s, %s, %s, 0.00)
                    ON CONFLICT (player_id) DO NOTHING
                    """,
                    [(p["player_id"], p["balance"], p["lifetime_deposits"]) for p in players],
                )
        print(f"  Done. {len(players)} players seeded.")
    finally:
        conn.close()


# ── Event factories ────────────────────────────────────────────────────────────
def make_bet(player):
    stake = rand_stake()
    bet_id = "BET-" + uuid.uuid4().hex[:10].upper()
    return {
        "bet_id":     bet_id,
        "player_id":  player["player_id"],
        "amount":     stake,
        "bet_type":   random.choices(BET_TYPES, weights=[0.70, 0.20, 0.10])[0],
        "sport":      random.choice(SPORTS),
        "odds":       rand_odds(),
        "event_time": now_str(),
    }, bet_id, stake


def make_settlement(player_id, bet_id, stake):
    outcome = random.choices(["win", "loss", "void"], weights=[0.45, 0.50, 0.05])[0]
    payout  = round(stake * random.uniform(1.5, 4.0), 2) if outcome == "win" else 0.00
    return {
        "settlement_id": "SET-" + uuid.uuid4().hex[:10].upper(),
        "bet_id":        bet_id,
        "player_id":     player_id,
        "outcome":       outcome,
        "stake":         stake,
        "payout_amount": payout,
        "event_time":    now_str(),
    }


def make_login(player):
    return {
        "event_id":    "LOG-" + uuid.uuid4().hex[:10].upper(),
        "player_id":   player["player_id"],
        "session_id":  "SES-" + uuid.uuid4().hex[:8].upper(),
        "device_type": random.choice(DEVICES),
        "ip_address":  fake.ipv4_public(),
        "country":     player["country"],   # baseline: always home country
        "event_time":  now_str(),
    }


def make_deposit(player):
    return {
        "deposit_id": "DEP-" + uuid.uuid4().hex[:10].upper(),
        "player_id":  player["player_id"],
        "amount":     round(random.uniform(10, 200), 2),
        "method":     random.choice(METHODS),
        "event_time": now_str(),
    }


# ── Test player (fixed ID, random profile) ────────────────────────────────────
# Seeded into PostgreSQL so Job A (CDC → Fluss) creates a feature-store row.
# Use for ad-hoc queries:
#   SELECT * FROM fluss.betting.player_feature_store WHERE player_id = 'P-TEST'
TEST_PLAYER = {
    "player_id":         "P-TEST",
    "username":          fake.user_name(),
    "kyc_tier":          random.choices(KYC_TIERS, weights=[0.5, 0.35, 0.15])[0],
    "registration_date": fake.date_between(start_date="-3y", end_date="today"),
    "country":           random.choice(COUNTRIES),
    "balance":           round(random.uniform(100, 5000), 2),
    "lifetime_deposits": round(random.uniform(500, 20000), 2),
}


# ── Main loop ──────────────────────────────────────────────────────────────────
def main():
    players  = build_players(NUM_PLAYERS) + [TEST_PLAYER]
    seed_players(players)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    # Pending bets waiting for settlement (deque of (player_id, bet_id, stake, settle_after))
    pending = deque()
    iteration = 0

    print(f"baseline_generator started: {NUM_PLAYERS} players")
    print(f"  Kafka: {BOOTSTRAP}")

    while True:
        player = random.choice(players)

        # Settle pending bets whose delay has elapsed
        now_ts = time.time()
        while pending and pending[0][3] <= now_ts:
            pid, bid, stake, _ = pending.popleft()
            settlement = make_settlement(pid, bid, stake)
            producer.send(T_SETTLEMENTS, value=settlement)
            print(f"[SET] {bid} → {settlement['outcome']} payout={settlement['payout_amount']}")

        # Every ~50 iterations emit a login event
        if iteration % 50 == 0:
            producer.send(T_LOGINS, value=make_login(player))
            print(f"[LOG] player={player['player_id']} country={player['country']}")

        # Every ~200 iterations emit a deposit
        if iteration % 200 == 0:
            dep = make_deposit(player)
            producer.send(T_DEPOSITS, value=dep)
            print(f"[DEP] player={player['player_id']} amount={dep['amount']}")

        # Place a bet
        bet, bet_id, stake = make_bet(player)
        producer.send(T_BETS, value=bet)
        print(f"[BET] {bet_id} player={player['player_id']} "
              f"type={bet['bet_type']} sport={bet['sport']} "
              f"stake={stake} odds={bet['odds']}")

        # Schedule settlement 30–120 s later
        delay = random.uniform(30, 120)
        pending.append((player["player_id"], bet_id, stake, now_ts + delay))

        iteration += 1
        time.sleep(random.randint(MIN_FREQ_MS, MAX_FREQ_MS) / 1000)


if __name__ == "__main__":
    main()
