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
import glob
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
# Complete mode (start.sh, via .env): Kafka is ingested into Fluss log tables that the
# feature jobs read, the lake, the tiering service and the new feature exist. Simple mode
# has none of them — its jobs read the Kafka topics — and the editor leaves them out.
LAKEHOUSE = os.environ.get("LAKEHOUSE", "1") == "1"

SOURCES_FILE = "01_sources_and_catalogs.sql"
STORE_FILE = "02_fluss_feature_store.sql"
INGEST_FILE = "03_ingest_to_fluss.sql"          # complete mode only, with the log tables
FEATURE_FILE = "08_add_feature_avg_stake.sql"   # the new feature, complete mode only
JOB_FILES = ([INGEST_FILE] if LAKEHOUSE else []) + [
    "04_cdc_dimensions.sql", "05_feature_calculations.sql", "06_risk_scoring_inference.sql"] + (
    [FEATURE_FILE] if LAKEHOUSE else [])
QUERIES_FILE = "07_analytics_and_monitoring.sql"
TIERING_JAR_DIR = os.environ.get("TIERING_JAR_DIR", "/opt/tiering")
TIERING_ARGS = os.environ.get("TIERING_ARGS", "/opt/tiering.args")

app = Flask(__name__, static_folder="static")


# ── SQL text handling ─────────────────────────────────────────────────────────

STATEMENT_SET = re.compile(r"\s*EXECUTE\s+STATEMENT\s+SET\s+BEGIN\b", re.I)


def split_statements(sql):
    """Split a script into statements on `;`, dropping comments.

    A character scanner rather than str.split: string literals can hold `;` or `--`,
    and 01_sources_and_catalogs.sql puts `--` comments inside WITH (...) clauses.
    An `EXECUTE STATEMENT SET BEGIN ... END;` stays one statement: the `;` after each
    INSERT inside it does not end it (05_feature_calculations.sql uses them).
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


# The Iceberg catalog left 01 (it would fail without Lakekeeper); complete mode adds it
# back to everything that stands in for `sql-client.sh --init`.
if LAKEHOUSE:
    DDL += split_statements(section(QUERIES_FILE, "Iceberg catalog"))

# Simple mode: the jobs' event views read the Kafka topics, not the Fluss log tables. The
# one FROM line each view has is swapped — the same swap as scripts/kafka-sources.sh,
# which start.sh uses; see 05_feature_calculations.sql.
KAFKA_SOURCE = (re.compile(r"FROM fluss\.betting\.(\w+) /\*\+ OPTIONS\('scan\.startup\.mode' = 'latest'\) \*/"),
                r"FROM \1_kafka /*+ OPTIONS('scan.startup.mode' = 'latest-offset') */")


def event_source(sql):
    return sql if LAKEHOUSE else KAFKA_SOURCE[0].sub(KAFKA_SOURCE[1], sql)


def job_blocks(with_file=False):
    """Every job in the job files, in file order: [(pipeline.name, banner title, sql)].
    Each job sits in a banner section of its own holding exactly one SET 'pipeline.name'
    (and whatever it needs before it, such as a temporary view)."""
    jobs = []
    for rel in JOB_FILES:
        for title, body in load_sections(rel):
            m = PIPELINE_NAME.search(body)
            if m:
                body = event_source(body[:m.start()] + CHECKPOINTING + body[m.start():])
                job = (m.group(1), title, f"-- {title}\n{body}")
                jobs.append(job + (rel,) if with_file else job)
    return jobs


def create_table(name, rel=SOURCES_FILE):
    """One CREATE TABLE of a DDL file, with the comment lines just above it. 01 and 02
    group several tables under one banner, so a banner section is too coarse."""
    text = read_sql(rel)
    m = re.search(rf"^CREATE TABLE (?:IF NOT EXISTS )?{re.escape(name)} \(.*?^\);\n", text, re.S | re.M)
    if not m:
        raise KeyError(f"no CREATE TABLE {name} in {rel}")
    lines = text[:m.start()].splitlines()
    comments = []
    while lines and lines[-1].startswith("--") and not BANNER.match(lines[-1]):
        comments.insert(0, lines.pop())
    return "\n".join(comments + [m.group(0)])


def chunk_title(sql):
    """The first line of the last comment block before the first SQL line: the comment
    that describes this query, not a section introduction further up."""
    block, last = [], None
    for line in sql.splitlines():
        if line.startswith("--"):
            block.append(line[2:].strip())
        elif line.strip():
            return (block or [last])[0] if (block or last) else None
        else:
            if block:
                last = block[0]
            block = []
    return None


SET_STMT = re.compile(r"^\s*SET\s", re.I)
BATCH_MODE = re.compile(r"'execution\.runtime-mode'\s*=\s*'batch'", re.I)


def query_chunks(rel):
    """07's queries one by one: each statement with the comment lines above it, titled
    by the first of them. A SET before a query belongs to it (07 runs its point-in-time
    queries in batch mode), and a query run in batch mode is followed by the switch back
    to streaming, so picking one example never leaves the session in batch mode."""
    out = []
    for title, body in load_sections(rel):
        carry = ""
        for chunk in re.split(r";[ \t]*\n", body):
            stmts = split_statements(chunk)
            if not stmts:
                continue   # only comments (the section's trailing notes)
            chunk = chunk.strip() + ";\n"
            if SET_STMT.match(stmts[0]):
                carry += chunk
                continue
            sql = carry + chunk
            first = chunk_title(sql) or title
            if len(first) > 60:
                first = first[:57].rstrip() + "…"
            if BATCH_MODE.search(carry):
                sql += "SET 'execution.runtime-mode' = 'streaming';\n"
            out.append({"group": "Queries", "title": f"{title}: {first}", "sql": sql})
            carry = ""
    return out


def load_examples():
    """In pipeline order: the DDL files, then every job (one entry per pipeline.name),
    then 07's analytics queries one at a time. The new-feature walkthrough (08) comes
    last, as its own group: its ALTER, then its two jobs. Simple mode has no lake, so it
    leaves out 03, 07's lake queries and 08."""
    examples = [{"group": "DDL", "title": rel, "sql": read_sql(rel)} for rel in (SOURCES_FILE, STORE_FILE)]
    if LAKEHOUSE:
        examples.append({"group": "DDL", "title": f"{INGEST_FILE} — the Fluss log tables",
                         "sql": section(INGEST_FILE, "Event log tables")})
    jobs = [{"title": f"{name} — {title}", "sql": sql.rstrip() + "\n" + RESET_NAME, "rel": rel}
            for name, title, sql, rel in job_blocks(with_file=True)]
    examples += [{"group": "Jobs", **{k: j[k] for k in ("title", "sql")}} for j in jobs if j["rel"] != FEATURE_FILE]
    examples += [q for q in query_chunks(QUERIES_FILE)
                 if LAKEHOUSE or not q["title"].startswith(("Point-in-time", "Iceberg catalog"))]
    if not LAKEHOUSE:
        return examples
    examples.append({"group": "Add a feature", "title": "Step 1 — ALTER TABLE: add avg_stake_7d",
                     "sql": alter_feature_sql()})
    examples += [{"group": "Add a feature", **{k: j[k] for k in ("title", "sql")}} for j in jobs if j["rel"] == FEATURE_FILE]
    return examples


def alter_feature_sql():
    return section(FEATURE_FILE, "Step 1")


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
    # The mode goes into the page itself, so the diagram is drawn right the first time.
    with open(os.path.join(app.static_folder, "pipeline.html")) as f:
        html = f.read()
    mode = "complete" if LAKEHOUSE else "simple"
    return Response(html.replace("<html lang=\"en\">", f"<html lang=\"en\" data-mode=\"{mode}\">", 1),
                    mimetype="text/html")


@app.get("/api/config")
def config():
    return jsonify({"preloadDdl": PRELOAD_DDL, "lakehouse": LAKEHOUSE,
                    "tiering": LAKEHOUSE and tiering_available()})


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
BETTING = ("fluss", "betting")
FEATURE_STORE = BETTING + ("player_feature_store",)
KAFKA_TABLES = ["bets_kafka", "bet_settlements_kafka", "login_events_kafka", "deposits_kafka"]
LOG_TABLES = {"fl_bets": "bets", "fl_settlements": "bet_settlements",
              "fl_logins": "login_events", "fl_deposits": "deposits"}

# Box -> its table(s). `kind` drives the state: a source is live as soon as it exists (the
# generators publish all along); a sink is probed for a first row while the job feeding it
# runs, unless `probe` is False (a Kafka topic: a probe would be an unbounded read).
# `tables` lists every table a box stands for (the Kafka box is four topics); `table` is
# the one its probe reads.
NODES = {
    "kafka":              {"table": DEFAULT_DB + ("bets_kafka",), "tables": [DEFAULT_DB + (t,) for t in KAFKA_TABLES],
                           "kind": "source"},
    "players_cdc":        {"table": DEFAULT_DB + ("players_cdc",), "kind": "source"},
    "player_wallets_cdc": {"table": DEFAULT_DB + ("player_wallets_cdc",), "kind": "source"},
    **{n: {"table": BETTING + (t,), "kind": "sink", "feed": ["ingest"]} for n, t in LOG_TABLES.items()},
    # The Iceberg copy of the tiered log tables, as Fluss exposes it: bets$lake has a row
    # once the tiering service has committed its first snapshot.
    "lake":               {"table": BETTING + ("bets$lake",), "tables": [BETTING + ("bets",)], "kind": "sink",
                           "feed": ["tiering"]},
    "feature_store":      {"table": FEATURE_STORE, "kind": "sink",
                           "feed": ["jobA", "jobB1", "jobB2", "jobB3", "jobC1", "jobC2", "jobD", "jobE"]},
    "risk_scores":        {"table": DEFAULT_DB + ("risk_scores",), "kind": "sink", "feed": ["risk_scoring"],
                           "probe": False},
    "risk_score_log":     {"table": DEFAULT_DB + ("risk_score_log",), "kind": "sink", "feed": ["risk_log"]},
}
# Job boxes, matched to Flink jobs by pipeline.name: all the feature jobs write the same
# Fluss table, so the sink in a job's plan cannot tell them apart. `names` lists every
# name that counts (risk scoring has a v2 in the new-feature walkthrough); `prefix`
# matches the tiering service, whose name carries the lake format.
EDGES = {
    "ingest":       {"from": ["kafka"], "to": list(LOG_TABLES), "names": ["ingest-kafka-to-fluss"]},
    "tiering":      {"from": list(LOG_TABLES), "to": ["lake"], "prefix": "Fluss Lake Tiering Service"},
    "jobA":         {"from": ["players_cdc", "player_wallets_cdc"], "to": ["feature_store"], "names": ["jobA-cdc-dimensions"]},
    "jobB1":        {"from": ["fl_bets"], "to": ["feature_store"], "names": ["jobB1-bet-velocity"]},
    "jobB2":        {"from": ["fl_bets"], "to": ["feature_store"], "names": ["jobB2-live-ratio-avg-odds"]},
    "jobB3":        {"from": ["fl_bets"], "to": ["feature_store"], "names": ["jobB3-stake-sport-variety"]},
    "jobC1":        {"from": ["fl_settlements"], "to": ["feature_store"], "names": ["jobC1-net-pnl"]},
    "jobC2":        {"from": ["fl_settlements"], "to": ["feature_store"], "names": ["jobC2-loss-streak"]},
    "jobD":         {"from": ["fl_logins"], "to": ["feature_store"], "names": ["jobD-geo-mismatches"]},
    "risk_scoring": {"from": ["fl_bets", "feature_store"], "to": ["risk_scores"],
                     "names": ["risk-scoring-kafka", "risk-scoring-kafka-v2"]},
    "risk_log":     {"from": ["risk_scores"], "to": ["risk_score_log"], "names": ["risk-score-log-postgres"]},
    # Optional (08_add_feature_avg_stake.sql): never part of the core pipeline's progress.
    "jobE":         {"from": ["fl_bets", "lake"], "to": ["feature_store"], "names": ["jobE-avg-stake-7d"],
                     "optional": True},
}
# Simple mode: no Fluss log tables, no ingest job, no lake, no tiering service, no job
# E — and the jobs that read a log table read the Kafka topics instead.
if not LAKEHOUSE:
    for n in [*LOG_TABLES, "lake"]:
        del NODES[n]
    for e in ("ingest", "tiering", "jobE"):
        del EDGES[e]
    NODES["feature_store"]["feed"].remove("jobE")
    for spec in EDGES.values():
        spec["from"] = list(dict.fromkeys("kafka" if n in LOG_TABLES else n for n in spec["from"]))

# What every session needs before any box works: the idle-timeout option and the fluss
# and iceberg catalogs. Session-scoped, so run on page load whenever the session lacks them.
BOOTSTRAP = [(SOURCES_FILE, "Session options"), (SOURCES_FILE, "Catalogs")] + (
    [(QUERIES_FILE, "Iceberg catalog")] if LAKEHOUSE else [])


def node_tables(node):
    spec = NODES[node]
    return spec.get("tables", [spec["table"]])


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
    latest = " /*+ OPTIONS('scan.startup.mode' = 'latest') */"
    jobs = {name: sql for name, _, sql in job_blocks()}
    log_notes = {
        "fl_bets": "Every bet in the Fluss log table, from now on ('latest')." + (
            " Without the hint this would be a union read: the whole Iceberg history first." if LAKEHOUSE else ""),
        "fl_settlements": "Every settled bet in the Fluss log table (win, loss or void, stake and payout), from now on.",
        "fl_logins": "Every login in the Fluss log table, with its country, from now on.",
        "fl_deposits": "Every deposit in the Fluss log table, from now on. No feature reads it yet" + (
            "; it is tiered for history." if LAKEHOUSE else "."),
    }
    nodes = {
        "kafka": {"create": "\n".join(create_table(t) for t in KAFKA_TABLES),
                  "view": stream("bets_kafka", "Every bet on the bets topic, from the first one (earliest-offset). A streaming read: it keeps going until you cancel or close.")},
        "players_cdc": {"create": create_table("players_cdc"),
                        "view": cdc("players_cdc", "flink_view_players_slot",
                                    "Every player in PostgreSQL: a snapshot, then each change. It reads through a replication slot of its own, so it can run next to Job A.")},
        "player_wallets_cdc": {"create": create_table("player_wallets_cdc"),
                               "view": cdc("player_wallets_cdc", "flink_view_wallets_slot",
                                           "Every wallet in PostgreSQL: a snapshot, then each balance change as settlements pay out.")},
        **{n: {"create": create_table(f"fluss.betting.{t}", INGEST_FILE) if LAKEHOUSE else "",
               "view": stream(f"fluss.betting.{t}", log_notes[n], latest)} for n, t in LOG_TABLES.items()},
        "lake": {"create": "",
                 "view": batch(
            "What the lake holds: bets in Iceberg alone ($lake) against the union read (Iceberg + the Fluss tail).\n"
            "-- The lake trails by about one checkpoint (60 s); see 07_analytics_and_monitoring.sql for time travel.",
            "SELECT\n"
            "  (SELECT COUNT(*) FROM fluss.betting.`bets$lake`)     AS bets_in_iceberg,\n"
            "  (SELECT COUNT(*) FROM fluss.betting.bets)            AS bets_union_read,\n"
            "  (SELECT MAX(event_ts) FROM fluss.betting.`bets$lake`) AS iceberg_up_to;")},
        "feature_store": {"create": create_table("fluss.betting.player_feature_store", STORE_FILE),
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
    }
    out = {
        "nodes": {n: v for n, v in nodes.items() if n in NODES},
        "edges": {e: {"sql": jobs[spec["names"][0]].rstrip() + "\n" + RESET_NAME}
                  for e, spec in EDGES.items() if "names" in spec},
    }
    if LAKEHOUSE:
        # The new-feature walkthrough: step 1 and step 3 (step 2 is edges.jobE).
        out["feature"] = {"alter": alter_feature_sql(),
                          "scoring": jobs["risk-scoring-kafka-v2"].rstrip() + "\n" + RESET_NAME}
    return jsonify(out)


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


# ── The tiering service: deployed ahead of time, started with one click ──────
# The Fluss tiering service is a Flink job shipped as a JAR (lib/tiering/). "Deploy"
# uploads the JAR to the JobManager (start.sh does it in both modes), so it is listed
# under Submit New Job; "Start" runs it with scripts/tiering.args — the same arguments
# scripts/tiering.sh passes from a terminal. Both go through the JobManager's REST API,
# so the job inherits the cluster's 60 s checkpoint interval, which is when it commits.
TIERING_JOB = "Fluss Lake Tiering Service"


def tiering_jar_file():
    jars = sorted(glob.glob(os.path.join(TIERING_JAR_DIR, "fluss-flink-tiering-*.jar")))
    return jars[-1] if jars else None


def tiering_available():
    return bool(tiering_jar_file()) and os.path.isfile(TIERING_ARGS)


def tiering_args():
    with open(TIERING_ARGS) as f:
        lines = [l.strip() for l in f if l.strip() and not l.lstrip().startswith("#")]
    # programArgsList, not a single programArgs string: no quoting rules to get wrong.
    return [part for line in lines for part in line.split()]


def uploaded_tiering_jar():
    """The id of the tiering JAR already uploaded to the JobManager, or None. Uploads
    live in the JobManager's /tmp, so a JobManager restart forgets them."""
    name = os.path.basename(tiering_jar_file() or "")
    for f in flink("GET", "/jars").get("files", []):
        if f.get("name") == name:
            return f["id"]
    return None


def deploy_tiering():
    jar_id = uploaded_tiering_jar()
    if jar_id:
        return jar_id
    path = tiering_jar_file()
    with open(path, "rb") as f:
        res = flink("POST", "/jars/upload", timeout=120,
                    files={"jarfile": (os.path.basename(path), f, "application/java-archive")})
    return os.path.basename(res["filename"])


def tiering_job():
    jobs = flink("GET", "/jobs/overview").get("jobs", [])
    live = [j for j in jobs if j["name"].startswith(TIERING_JOB) and j["state"] in ACTIVE]
    return max(live, key=lambda j: j["start-time"]) if live else None


def tiering_status():
    job = tiering_job()
    return {
        "available": tiering_available(),
        "deployed": uploaded_tiering_jar() is not None,
        "job": {"id": job["jid"], "state": job["state"]} if job else None,
    }


@app.get("/api/tiering")
def tiering_get():
    if not tiering_available():
        return jsonify({"available": False})
    try:
        return jsonify(tiering_status())
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not reach the Flink JobManager: {e}")


@app.post("/api/tiering/deploy")
def tiering_deploy():
    try:
        jar_id = deploy_tiering()
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(f"could not upload the tiering JAR: {e}")
    return jsonify({"jar": jar_id, **tiering_status()})


@app.post("/api/tiering/start")
def tiering_start():
    """Idempotent: with a tiering job already running this changes nothing — two tiering
    jobs would compete for the same tables. Re-uploads the JAR if a JobManager restart
    lost it, so Start works whether or not start.sh deployed it first."""
    try:
        if tiering_job() is None:
            jar_id = deploy_tiering()
            flink("POST", f"/jars/{jar_id}/run", timeout=120,
                  json={"programArgsList": tiering_args()})
        return jsonify(tiering_status())
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(f"could not start the tiering service: {e}")


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
    spec = EDGES[edge]
    if "prefix" in spec:
        return [j for j in active if j["name"].startswith(spec["prefix"])]
    return [j for j in active if j["name"] in spec["names"]]


def edge_nodes(edge):
    return EDGES[edge]["from"] + EDGES[edge]["to"]


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
    for node in NODES:
        missing = [t[2] for t in node_tables(node) if t[:2] == DEFAULT_DB and t[2] not in listed]
        if missing and any(node in edge_nodes(e) for e in running):
            for name in missing:
                for stmt in split_statements(create_table(name)):
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
        # Not names(): a session the gateway no longer knows (a restarted stack) must fail
        # loudly. Swallowed, it would read as "no tables at all", every box would go grey,
        # and the page would keep polling a dead session instead of opening a new one.
        catalogs = {r[0] for r in query_rows(session, "SHOW CATALOGS")[1]}
    except GatewayError as e:
        return json_error(f"session {session} is gone: {e}", 410)
    except requests.RequestException as e:
        return json_error(e)
    try:
        listed = {
            DEFAULT_DB: names(session, f"SHOW TABLES IN {ident(*DEFAULT_DB)}"),
            BETTING: names(session, f"SHOW TABLES IN {ident(*BETTING)}") if "fluss" in catalogs else set(),
        }
    except requests.RequestException as e:
        return json_error(e)
    exists = {n: all(t[2] in listed.get(t[:2], set()) for t in node_tables(n)) for n in NODES}

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
                    "jobs": jobs, "ready": all(exists[n] for n in edge_nodes(e)),
                    "optional": spec.get("optional", False)}

    nodes = {}
    for n, spec in NODES.items():
        fed = any(edges[e]["state"] == "running" for e in spec.get("feed", []))
        if not exists[n]:
            # Dropped: whatever was confirmed is gone with it. Only for Fluss tables, which
            # exist for everyone once created; a default-catalog table missing from this
            # session says nothing about the data (another session may hold it), and
            # discarding on it made two open pages re-probe every table, every poll.
            if spec["table"][:2] == BETTING and "fluss" in catalogs:
                with probe_lock:
                    probe_ok.discard(n)
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
    missing = "fluss" not in catalogs or (LAKEHOUSE and "iceberg" not in catalogs) or any(
        not exists[n] and NODES[n]["table"][:2] == DEFAULT_DB
        for e in running for n in edge_nodes(e))
    # The new feature's column, for the optional walkthrough's ALTER button.
    feature_added = False
    if LAKEHOUSE and exists["feature_store"]:
        try:
            cols = query_rows(session, f"DESCRIBE {ident(*FEATURE_STORE)}")[1]
            feature_added = any(c[0] == "avg_stake_7d" for c in cols)
        except GatewayError:
            pass
    try:
        tiering = tiering_status() if LAKEHOUSE and tiering_available() else {"available": False}
    except (GatewayError, requests.RequestException):
        tiering = {"available": tiering_available(), "deployed": False, "job": None}
    return jsonify({"nodes": nodes, "edges": edges, "bootstrap": missing,
                    "feature": {"added": feature_added}, "tiering": tiering})


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
