"""Web SQL editor for the betting feature store scenario.

A thin proxy in front of the Flink SQL Gateway. The browser cannot call the gateway
directly (it sends no CORS headers), and a gateway session starts as empty as a bare
`sql-client.sh`: the default catalog is in-memory, so every session needs the DDL.
This app does what `sql-client.sh --init 01_sources_and_catalogs.sql` does for the
terminal:

  - optionally (PRELOAD_DDL=true) runs 01_sources_and_catalogs.sql into each new session
    first. Off by default: a preloaded table makes a hand-typed CREATE TABLE fail as a
    duplicate. The editor has a checkbox for it
  - backs the catalog browser (static/catalog.js): SHOW / DESCRIBE / SHOW CREATE in the
    browser's own session, so it sees that session's in-memory tables too
  - backs the Pipeline Builder (static/pipeline.html, at /pipeline): the architecture
    diagram as a control surface, with what exists and what runs read from the gateway
    and the Flink JobManager

Unlike the coffee-shop editor this was ported from, the SQL files here carry no
${PLACEHOLDERS} (hosts and credentials are written in), so statements go to the gateway
exactly as typed.
"""
import json
import os
import re
import threading
import time

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

GATEWAY = os.environ.get("GATEWAY_URL", "http://sql-gateway:8083").rstrip("/")
FLINK = os.environ.get("FLINK_URL", "http://jobmanager:8081").rstrip("/")
SQL_DIR = os.environ.get("SQL_DIR", "/opt/sql")
BUILTIN_CATALOG = "default_catalog"
PRELOAD_DDL = os.environ.get("PRELOAD_DDL", "false").lower() in ("1", "true", "yes")

SOURCES_FILE = "01_sources_and_catalogs.sql"
STORE_FILE = "02_fluss_feature_store.sql"
JOB_FILES = ["03_cdc_dimensions.sql", "04_feature_calculations.sql", "05_risk_scoring_inference.sql"]
QUERIES_FILE = "06_analytics_and_monitoring.sql"

app = Flask(__name__, static_folder="static")


# ── SQL text handling ─────────────────────────────────────────────────────────

STATEMENT_SET = re.compile(r"\s*EXECUTE\s+STATEMENT\s+SET\s+BEGIN\b", re.I)


def split_statements(sql):
    """Split a script into statements on `;`, dropping comments.

    A character scanner rather than str.split: string literals can hold `;` or `--`,
    and 01_sources_and_catalogs.sql puts `--` comments inside WITH (...) clauses.
    An `EXECUTE STATEMENT SET BEGIN ... END;` stays one statement: the `;` after each
    INSERT inside it does not end it (04_feature_calculations.sql uses them).
    """
    out, buf, i, n = [], [], 0, len(sql)
    while i < n:
        c = sql[i]
        if c in ("'", "`", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote is an escape
                        j += 2
                        continue
                    break
                j += 1
            buf.append(sql[i:j + 1])
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*+", i):
            # A hint, not a comment: `/*+ OPTIONS('slot.name' = '...') */` is part of the
            # statement, and dropping it would silently run a different query.
            j = sql.find("*/", i + 3)
            j = n if j < 0 else j + 2
            buf.append(sql[i:j])
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif c == ";":
            stmt = "".join(buf)
            if STATEMENT_SET.match(stmt) and not re.search(r"\bEND\s*$", stmt, re.I):
                buf.append(c)
            else:
                out.append(stmt)
                buf = []
            i += 1
        else:
            buf.append(c)
            i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


def read_sql(rel):
    with open(os.path.join(SQL_DIR, rel)) as f:
        return f.read()


DDL = split_statements(read_sql(SOURCES_FILE))

# The files' section banners: `-- ── Title ─────`.
BANNER = re.compile(r"^-- ── (.+?)\s*─*\s*$")
PIPELINE_NAME = re.compile(r"SET\s+'pipeline\.name'\s*=\s*'([^']+)'", re.I)

# Appended to each job. The job's SET 'pipeline.name' is a session option, so without
# this every SELECT run afterwards would show up in the Flink UI under the job's name.
# The gateway runs without a checkpoint interval so interactive SELECTs stream at once
# (see docker-compose.yml), so each job the editor offers switches it on for itself,
# matching what sql-client's config.yaml gives the same job.
CHECKPOINTING = """-- Added by the editor: checkpoint this job like sql-client does (the gateway's default is off).
SET 'execution.checkpointing.interval' = '60s';
"""

RESET_NAME = """
-- Added by the editor: stop later statements in this session inheriting the job settings.
RESET 'pipeline.name';
RESET 'execution.checkpointing.interval';
"""


def load_sections(rel):
    """A SQL file cut at its `-- ── title` banners: [(title, body), ...]. Whatever comes
    before the first banner (the file's header box) is dropped."""
    sections, title, body = [], None, []
    for line in read_sql(rel).splitlines(keepends=True):
        m = BANNER.match(line)
        if m:
            if title:
                sections.append((title, "".join(body).strip() + "\n"))
            title, body = m.group(1), []
        elif title:
            body.append(line)
    if title:
        sections.append((title, "".join(body).strip() + "\n"))
    return sections


def section(rel, prefix):
    for title, body in load_sections(rel):
        if title.startswith(prefix):
            return f"-- {title}\n{body}"
    raise KeyError(f"no '{prefix}' section in {rel}")


def job_blocks():
    """Every job in the job files, in file order: [(pipeline.name, banner title, sql)].
    Each job sits in a banner section of its own holding exactly one SET 'pipeline.name'."""
    jobs = []
    for rel in JOB_FILES:
        for title, body in load_sections(rel):
            m = PIPELINE_NAME.search(body)
            if m:
                body = body[:m.start()] + CHECKPOINTING + body[m.start():]
                jobs.append((m.group(1), title, f"-- {title}\n{body}"))
    return jobs


def create_table(name):
    """One CREATE TABLE of 01_sources_and_catalogs.sql, with the comment lines just above
    it. 01 groups several tables under one banner, so a banner section is too coarse."""
    text = read_sql(SOURCES_FILE)
    m = re.search(rf"^CREATE TABLE {re.escape(name)} \(.*?^\);\n", text, re.S | re.M)
    if not m:
        raise KeyError(f"no CREATE TABLE {name} in {SOURCES_FILE}")
    lines = text[:m.start()].splitlines()
    comments = []
    while lines and lines[-1].startswith("--") and not BANNER.match(lines[-1]):
        comments.insert(0, lines.pop())
    return "\n".join(comments + [m.group(0)])


def query_chunks(rel):
    """06's queries one by one: each statement with the comment lines above it, titled
    by the first of them."""
    out = []
    for title, body in load_sections(rel):
        for chunk in re.split(r";[ \t]*\n", body):
            if not split_statements(chunk):
                continue   # only comments (the section's trailing notes)
            chunk = chunk.strip() + ";\n"
            first = next((l[2:].strip() for l in chunk.splitlines() if l.startswith("--")), title)
            if len(first) > 60:
                first = first[:57].rstrip() + "…"
            out.append({"group": "Queries", "title": f"{title}: {first}", "sql": chunk})
    return out


def load_examples():
    """In pipeline order: the DDL files, then every job (one entry per pipeline.name),
    then 06's analytics queries one at a time."""
    examples = [{"group": "DDL", "title": rel, "sql": read_sql(rel)} for rel in (SOURCES_FILE, STORE_FILE)]
    examples += [{"group": "Jobs", "title": f"{name} — {title}", "sql": sql.rstrip() + "\n" + RESET_NAME}
                 for name, title, sql in job_blocks()]
    examples += query_chunks(QUERIES_FILE)
    return examples


# ── Gateway access ────────────────────────────────────────────────────────────

class GatewayError(Exception):
    pass


def gw(method, path, body=None):
    r = requests.request(method, GATEWAY + path, json=body, timeout=60)
    if r.status_code >= 400:
        raise GatewayError(error_text(r.text))
    return r.json() if r.text else {}


def error_text(text):
    """The gateway returns {"errors": ["Internal server error.", "<stack trace>"]}."""
    try:
        errors = json.loads(text).get("errors") or [text]
    except ValueError:
        errors = [text]
    return "\n".join(e for e in errors if e != "Internal server error.") or text


def result_path(session, op, token=0):
    return f"/v2/sessions/{session}/operations/{op}/result/{token}?rowFormat=JSON"


def execute_and_wait(session, sql, timeout=60):
    op = gw("POST", f"/v1/sessions/{session}/statements", {"statement": sql})["operationHandle"]
    path, deadline = result_path(session, op), time.time() + timeout
    while path:
        res = gw("GET", path)
        if res.get("resultType") == "EOS":
            break
        if time.time() > deadline:
            raise GatewayError(f"timed out after {timeout}s: {sql[:80]}")
        if res.get("resultType") == "NOT_READY":
            time.sleep(0.2)
        path = res.get("nextResultUri")
    gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")


def json_error(e, status=502):
    return jsonify({"error": str(e)}), status


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/pipeline")
def pipeline_page():
    return send_from_directory(app.static_folder, "pipeline.html")


@app.get("/api/config")
def config():
    return jsonify({"preloadDdl": PRELOAD_DDL})


@app.post("/api/session")
def open_session():
    load_ddl = (request.get_json(silent=True) or {}).get("ddl", PRELOAD_DDL)
    try:
        session = gw("POST", "/v1/sessions", {"sessionName": "sql-editor"})["sessionHandle"]
        if load_ddl:
            for stmt in DDL:
                execute_and_wait(session, stmt)
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not open a session with the DDL loaded:\n{e}")
    return jsonify({"session": session, "ddl": len(DDL) if load_ddl else 0})


@app.post("/api/split")
def split():
    return jsonify({"statements": split_statements(request.get_json()["sql"])})


@app.post("/api/run/<session>")
def run(session):
    try:
        res = gw("POST", f"/v1/sessions/{session}/statements", {"statement": request.get_json()["sql"]})
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    return jsonify({"op": res["operationHandle"]})


@app.get("/api/result/<session>/<op>/<int:token>")
def result(session, op, token):
    try:
        res = gw("GET", result_path(session, op, token))
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    nxt = res.get("nextResultUri")
    return jsonify({
        "type": res.get("resultType"),
        "jobId": res.get("jobID"),
        "isQuery": res.get("isQueryResult"),
        "columns": [c["name"] for c in (res.get("results") or {}).get("columns", [])],
        "rows": (res.get("results") or {}).get("data", []),
        "next": int(nxt.split("/result/")[1].split("?")[0]) if nxt else None,
    })


# Close, not cancel: a streaming SELECT's operation is FINISHED as soon as its job is
# submitted, so /cancel refuses it ("Failed to convert the Operation Status from
# FINISHED to CANCELED"). Closing releases the result fetcher, which cancels the job.
@app.post("/api/cancel/<session>/<op>")
def cancel(session, op):
    try:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.post("/api/heartbeat/<session>")
def heartbeat(session):
    try:
        gw("POST", f"/v1/sessions/{session}/heartbeat")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 410)
    return jsonify({})


@app.post("/api/close/<session>")
def close(session):
    try:
        gw("DELETE", f"/v1/sessions/{session}")
    except (GatewayError, requests.RequestException):
        pass  # already gone — nothing to do
    return Response(status=204)


# ── Catalog browser ───────────────────────────────────────────────────────────
#
# Runs SHOW / DESCRIBE in the browser's own session, so the tree also shows that
# session's in-memory tables (bets, players_cdc, ...), which no other session can see.

def query_rows(session, sql, timeout=60):
    """Run one statement and return every row's fields. For bounded statements only."""
    op = gw("POST", f"/v1/sessions/{session}/statements", {"statement": sql})["operationHandle"]
    path, rows, columns, deadline = result_path(session, op), [], [], time.time() + timeout
    try:
        while path:
            res = gw("GET", path)
            if res.get("resultType") == "EOS":
                break
            if time.time() > deadline:
                raise GatewayError(f"timed out after {timeout}s: {sql}")
            results = res.get("results") or {}
            columns = [c["name"] for c in results.get("columns", [])] or columns
            rows += [r["fields"] for r in results.get("data", [])]
            if res.get("resultType") == "NOT_READY":
                time.sleep(0.2)
            path = res.get("nextResultUri")
    finally:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    return columns, rows


def ident(*parts):
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


@app.get("/api/catalog/<session>/current")
def catalog_current(session):
    """The session's current catalog and database: what an unqualified name in a DDL
    statement resolves against, so the browser knows which tree level it changed."""
    try:
        _, c = query_rows(session, "SHOW CURRENT CATALOG")
        _, d = query_rows(session, "SHOW CURRENT DATABASE")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"catalog": c[0][0], "database": d[0][0]})


@app.get("/api/catalog/<session>/structure")
def catalog_structure(session):
    """One table's columns (DESCRIBE) and its DDL (SHOW CREATE), for the structure
    dialog. SHOW CREATE TABLE refuses a view, so a view falls back to SHOW CREATE VIEW."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    if not (c and d and t):
        return json_error("catalog, database and table are required", 400)
    try:
        names, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        kind, ddl = "table", None
        try:
            _, out = query_rows(session, f"SHOW CREATE TABLE {ident(c, d, t)}")
        except GatewayError:
            kind = "view"
            _, out = query_rows(session, f"SHOW CREATE VIEW {ident(c, d, t)}")
        ddl = out[0][0] if out else None
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"kind": kind, "columns": [dict(zip(names, r)) for r in rows], "ddl": ddl})


@app.get("/api/catalog/<session>")
def catalog(session):
    """No arguments: the catalogs. ?catalog=: its databases. + &database=: its tables and
    views. + &table=: that table's columns, as DESCRIBE reports them."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    try:
        if not c:
            _, rows = query_rows(session, "SHOW CATALOGS")
            return jsonify({"items": [{"name": r[0]} for r in rows]})
        if not d:
            _, rows = query_rows(session, f"SHOW DATABASES IN {ident(c)}")
            return jsonify({"items": [{"name": r[0]} for r in rows]})
        if not t:
            # Views included: SHOW TABLES lists both. (USE would change the session's
            # current database.)
            _, rows = query_rows(session, f"SHOW TABLES IN {ident(c, d)}")
            return jsonify({"items": sorted(({"name": r[0]} for r in rows), key=lambda i: i["name"])})
        columns, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        return jsonify({"items": [dict(zip(columns, r)) for r in rows]})
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)


# ── Pipeline page (static/pipeline.html) ─────────────────────────────────────
#
# The architecture diagram as a control surface: each box is a table, each job box the
# Flink job that writes one. The page asks /api/pipeline/status what exists and what
# runs, and runs every action's SQL in the browser's own session, like the editor — the
# two share one session.

def flink(method, path, **kw):
    r = requests.request(method, FLINK + path, timeout=kw.pop("timeout", 30), **kw)
    if r.status_code >= 400:
        raise GatewayError(error_text(r.text))
    return r.json() if r.text else {}


ACTIVE = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING", "FAILING", "CANCELLING", "RECONCILING"}
DEFAULT_DB = (BUILTIN_CATALOG, "default_database")
FEATURE_STORE = ("fluss", "betting", "player_feature_store")

# Box -> its table. `kind` drives the state: a source is live as soon as it exists (the
# generators publish all along); a sink is probed for a first row while the job feeding it
# runs, unless `probe` is False (a Kafka topic: a probe would be an unbounded read).
NODES = {
    "players_cdc":        {"table": DEFAULT_DB + ("players_cdc",), "kind": "source"},
    "player_wallets_cdc": {"table": DEFAULT_DB + ("player_wallets_cdc",), "kind": "source"},
    "bets":               {"table": DEFAULT_DB + ("bets",), "kind": "source"},
    "bet_settlements":    {"table": DEFAULT_DB + ("bet_settlements",), "kind": "source"},
    "login_events":       {"table": DEFAULT_DB + ("login_events",), "kind": "source"},
    "feature_store":      {"table": FEATURE_STORE, "kind": "sink",
                           "feed": ["jobA", "jobB1", "jobB2", "jobB3", "jobC1", "jobC2", "jobD"]},
    "risk_scores":        {"table": DEFAULT_DB + ("risk_scores",), "kind": "sink", "feed": ["risk_scoring"],
                           "probe": False},
    "risk_score_log":     {"table": DEFAULT_DB + ("risk_score_log",), "kind": "sink", "feed": ["risk_log"]},
}
# Job boxes, matched to Flink jobs by pipeline.name: all seven feature jobs write the same
# Fluss table, so the sink in a job's plan cannot tell them apart.
EDGES = {
    "jobA":         {"from": ["players_cdc", "player_wallets_cdc"], "to": "feature_store", "name": "jobA-cdc-dimensions"},
    "jobB1":        {"from": ["bets"], "to": "feature_store", "name": "jobB1-bet-velocity"},
    "jobB2":        {"from": ["bets"], "to": "feature_store", "name": "jobB2-live-ratio-avg-odds"},
    "jobB3":        {"from": ["bets"], "to": "feature_store", "name": "jobB3-stake-sport-variety"},
    "jobC1":        {"from": ["bet_settlements"], "to": "feature_store", "name": "jobC1-net-pnl"},
    "jobC2":        {"from": ["bet_settlements"], "to": "feature_store", "name": "jobC2-loss-streak"},
    "jobD":         {"from": ["login_events"], "to": "feature_store", "name": "jobD-geo-mismatches"},
    "risk_scoring": {"from": ["bets", "feature_store"], "to": "risk_scores", "name": "risk-scoring-kafka"},
    "risk_log":     {"from": ["risk_scores"], "to": "risk_score_log", "name": "risk-score-log-postgres"},
}

# What every session needs before any box works: the idle-timeout option and the fluss
# catalog. Session-scoped, so run on page load whenever the session lacks the catalog.
BOOTSTRAP = [(SOURCES_FILE, "Session options"), (SOURCES_FILE, "Catalogs")]


def bootstrap_statements():
    out = []
    for rel, prefix in BOOTSTRAP:
        out += split_statements(section(rel, prefix))
    return out


@app.get("/api/pipeline/sql")
def pipeline_sql():
    """Every action's default SQL, cut from flink_sql/ so the files stay the one source."""
    stream = lambda t, note, hint="": f"-- {note}\nSELECT * FROM {t}{hint};\n"
    batch = lambda note, q: (f"-- {note}\nSET 'execution.runtime-mode' = 'batch';\n{q}\n"
                             "SET 'execution.runtime-mode' = 'streaming';\n")
    # A CDC source holds its replication slot while it reads, and PostgreSQL allows one
    # reader per slot, so a preview of a table Job A is reading needs a slot of its own.
    # Dropped when the preview stops, so no slot is left behind to pin WAL.
    cdc = lambda t, slot, note: stream(
        t, note, f" /*+ OPTIONS('slot.name' = '{slot}', 'debezium.slot.drop.on.stop' = 'true') */")
    jobs = {name: sql for name, _, sql in job_blocks()}
    return jsonify({
        "nodes": {
            "players_cdc": {"create": create_table("players_cdc"),
                            "view": cdc("players_cdc", "flink_view_players_slot",
                                        "Every player in PostgreSQL: a snapshot, then each change. It reads through a replication slot of its own, so it can run next to Job A.")},
            "player_wallets_cdc": {"create": create_table("player_wallets_cdc"),
                                   "view": cdc("player_wallets_cdc", "flink_view_wallets_slot",
                                               "Every wallet in PostgreSQL: a snapshot, then each balance change as settlements pay out.")},
            "bets": {"create": create_table("bets"),
                     "view": stream("bets", "Every bet on the bets topic. latest-offset: only bets placed from now on. A streaming read: it keeps going until you cancel or close.")},
            "bet_settlements": {"create": create_table("bet_settlements"),
                                "view": stream("bet_settlements", "Every settled bet on the bet_settlements topic: win, loss or void, stake and payout.")},
            "login_events": {"create": create_table("login_events"),
                             "view": stream("login_events", "Every login on the login_events topic, with the country it came from.")},
            "feature_store": {"create": without_banner(read_sql(STORE_FILE)),
                              "view": batch(
                "200 of the feature store's current rows, one per player. A bounded read of the Fluss primary-key table:\n"
                "-- in batch mode Fluss serves a LIMIT scan or a key lookup (WHERE player_id = 'BOT_001'), not an ORDER BY.",
                "SELECT * FROM fluss.betting.player_feature_store LIMIT 200;")},
            "risk_scores": {"create": create_table("risk_scores"),
                            "view": stream("risk_scores", "Every scored bet as it is written to the risk_scores topic, from now on.",
                                           " /*+ OPTIONS('scan.startup.mode' = 'latest-offset') */")},
            "risk_score_log": {"create": create_table("risk_score_log"),
                               "view": batch(
                "The alerts in PostgreSQL's risk_score_log (NORMAL is filtered out), newest first: what Grafana charts.",
                "SELECT * FROM risk_score_log ORDER BY score_time DESC LIMIT 200;")},
        },
        "edges": {e: {"sql": jobs[spec["name"]].rstrip() + "\n" + RESET_NAME} for e, spec in EDGES.items()},
    })


def without_banner(sql):
    """A file minus its `-- =====` header box, keeping the comments after it."""
    lines = sql.splitlines()
    if lines and lines[0].startswith("-- ="):
        end = next((i for i in range(1, len(lines)) if lines[i].startswith("-- =")), 0)
        lines = lines[end + 1:]
    return "\n".join(lines).strip() + "\n"


def qualified(stmt):
    """A CREATE TABLE in the default catalog, whatever the session's USE."""
    return re.sub(r"^CREATE\s+TABLE\s+", lambda m: f"CREATE TABLE {ident(*DEFAULT_DB)}.", stmt, count=1, flags=re.I)


@app.post("/api/pipeline/bootstrap/<session>")
def pipeline_bootstrap(session):
    """Register what every box needs (the fluss catalog), and the tables a running
    pipeline already uses."""
    try:
        if "fluss" not in names(session, "SHOW CATALOGS"):
            for stmt in bootstrap_statements():
                execute_and_wait(session, stmt)
        restored = restore_sources(session)
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(e)
    return jsonify({"ok": True, "sources": restored})


# Which tables a job writes, from its plan: shown in the catalog panel's Job Catalog.
SINK_FULL = re.compile(r"Sink\(table=\[([^\]]+)\]")
plan_sinks = {}


def job_sinks(jid):
    if jid not in plan_sinks:
        nodes = flink("GET", f"/jobs/{jid}/plan").get("plan", {}).get("nodes", [])
        text = " ".join(n.get("description", "") for n in nodes)
        plan_sinks[jid] = [s.replace("`", "") for s in SINK_FULL.findall(text)]
    return plan_sinks[jid]


def active_jobs():
    """Flink jobs that are up."""
    return [j for j in flink("GET", "/jobs/overview").get("jobs", [])
            if j["state"] in ACTIVE and j["state"] != "CANCELLING"]


def edge_jobs(active, edge):
    return [j for j in active if j["name"] == EDGES[edge]["name"]]


def names(session, sql):
    try:
        return {r[0] for r in query_rows(session, sql)[1]}
    except GatewayError:
        return set()   # e.g. the catalog or database does not exist yet


def restore_sources(session):
    """Create in this session the default-catalog tables a running job uses. A full
    ./start.sh creates them only in its own SQL Client sessions, so a browser session
    would otherwise show a running pipeline over boxes that do not exist."""
    active = active_jobs()
    running = {e for e in EDGES if any(j["state"] == "RUNNING" for j in edge_jobs(active, e))}
    listed = names(session, f"SHOW TABLES IN {ident(*DEFAULT_DB)}")
    restored = []
    for node, spec in NODES.items():
        if spec["table"][:2] != DEFAULT_DB or spec["table"][2] in listed:
            continue
        if any(node in (EDGES[e]["from"] + [EDGES[e]["to"]]) for e in running):
            for stmt in split_statements(create_table(spec["table"][2])):
                execute_and_wait(session, qualified(stmt))
            restored.append(node)
    return restored


# ── The data probe: does a table return a row? ──
# The one status check that costs a Flink job, so it runs in the background, one at a
# time, in a session of its own, and only for a box whose feeding job runs and whose data
# is not confirmed yet. Confirmed boxes are not probed again. The probe session runs in
# batch mode, so a probe is a bounded read that ends at once.
probe_lock = threading.Lock()
probe_wake = threading.Event()
probe_ok, probe_next, probe_wanted = set(), {}, {}
PROBE_TIMEOUT, PROBE_RETRY = 60, 15


def want_probe(node):
    with probe_lock:
        if node in probe_ok or node in probe_wanted or time.time() < probe_next.get(node, 0):
            return
        probe_wanted[node] = f"SELECT 1 FROM {ident(*NODES[node]['table'])} LIMIT 1"
    probe_wake.set()


def first_row(session, sql):
    op = gw("POST", f"/v1/sessions/{session}/statements", {"statement": sql})["operationHandle"]
    path, deadline = result_path(session, op), time.time() + PROBE_TIMEOUT
    try:
        while path and time.time() < deadline:
            res = gw("GET", path)
            if (res.get("results") or {}).get("data"):
                return True
            if res.get("resultType") == "EOS":
                return False
            time.sleep(0.5)
            path = res.get("nextResultUri")
        return False
    finally:
        try:
            gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
        except (GatewayError, requests.RequestException):
            pass


def probe_worker():
    session = None
    while True:
        probe_wake.wait()
        probe_wake.clear()
        while True:
            with probe_lock:
                if not probe_wanted:
                    break
                node, sql = next(iter(probe_wanted.items()))
            ok = False
            try:
                if session is None:
                    session = gw("POST", "/v1/sessions", {"sessionName": "pipeline-probe"})["sessionHandle"]
                    for stmt in bootstrap_statements() + [create_table("risk_score_log")]:
                        execute_and_wait(session, stmt)
                    execute_and_wait(session, "SET 'execution.runtime-mode' = 'batch'")
                ok = first_row(session, sql)
            except (GatewayError, requests.RequestException):
                session = None   # gone or broken: a fresh one next time
            with probe_lock:
                probe_wanted.pop(node, None)
                if ok:
                    probe_ok.add(node)
                else:
                    probe_next[node] = time.time() + PROBE_RETRY


threading.Thread(target=probe_worker, daemon=True, name="pipeline-probe").start()


@app.get("/api/pipeline/status/<session>")
def pipeline_status(session):
    try:
        catalogs = names(session, "SHOW CATALOGS")
        listed = {
            DEFAULT_DB: names(session, f"SHOW TABLES IN {ident(*DEFAULT_DB)}"),
            FEATURE_STORE[:2]: names(session, f"SHOW TABLES IN {ident(*FEATURE_STORE[:2])}") if "fluss" in catalogs else set(),
        }
    except requests.RequestException as e:
        return json_error(e)
    exists = {n: spec["table"][2] in listed.get(spec["table"][:2], set()) for n, spec in NODES.items()}

    try:
        active = active_jobs()
    except (GatewayError, requests.RequestException):
        active = []
    edges = {}
    for e, spec in EDGES.items():
        jobs = [{"jid": j["jid"], "name": j["name"], "state": j["state"]} for j in edge_jobs(active, e)]
        states = {j["state"] for j in jobs}
        edges[e] = {"state": "running" if "RUNNING" in states else
                             "failing" if states & {"RESTARTING", "FAILING"} else
                             "starting" if jobs else "idle",
                    "jobs": jobs, "ready": all(exists[n] for n in spec["from"] + [spec["to"]])}

    nodes = {}
    for n, spec in NODES.items():
        fed = any(edges[e]["state"] == "running" for e in spec.get("feed", []))
        if not exists[n]:
            with probe_lock:
                probe_ok.discard(n)   # dropped: whatever was confirmed is gone with it
            state = "inactive"
        elif spec["kind"] == "source":
            state = "live"            # the generators publish all along
        elif fed and spec.get("probe", True):
            want_probe(n)
            state = "live" if n in probe_ok else "starting"
        else:
            state = "live" if fed else "ready"
        nodes[n] = {"state": state, "exists": exists[n]}

    running = {e for e, s in edges.items() if s["state"] == "running"}
    missing = "fluss" not in catalogs or any(
        not exists[n] and NODES[n]["table"][:2] == DEFAULT_DB
        for e in running for n in EDGES[e]["from"] + [EDGES[e]["to"]])
    return jsonify({"nodes": nodes, "edges": edges, "bootstrap": missing})


# The data probe's jobs (want_probe): SELECT 1 FROM <table> LIMIT 1, as Flink names them.
PROBE_JOB = re.compile(r"\s*SELECT 1\s+FROM\b.*(FETCH NEXT 1 ROWS ONLY|LIMIT 1)\s*$", re.S | re.I)


@app.get("/api/jobs")
def jobs_list():
    """Every Flink job, newest first, for the catalog panel's Job Catalog: a pipeline job
    (with the table it writes, read from its plan) or an interactive query."""
    try:
        jobs = flink("GET", "/jobs/overview").get("jobs", [])
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not reach the Flink JobManager: {e}")
    out = []
    for j in sorted(jobs, key=lambda j: j.get("start-time", 0), reverse=True):
        name, state = j["name"], j["state"]
        if PROBE_JOB.match(name):
            continue   # the pipeline page's own "does this table return a row?" checks
        kind = "query" if name.lstrip().upper().startswith("SELECT") else "job"
        sinks = []
        if kind == "job" and state in ACTIVE:
            try:
                sinks = job_sinks(j["jid"])
            except (GatewayError, requests.RequestException):
                pass
        out.append({"jid": j["jid"], "name": " ".join(name.split()), "state": state, "kind": kind,
                    "sinks": sinks, "start": j.get("start-time"), "duration": j.get("duration"),
                    "active": state in ACTIVE})
    return jsonify({"jobs": out})


@app.post("/api/jobs/<jid>/cancel")
def job_cancel(jid):
    try:
        flink("PATCH", f"/jobs/{jid}?mode=cancel")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.get("/api/examples")
def examples():
    return jsonify(load_examples())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8084, threaded=True)
