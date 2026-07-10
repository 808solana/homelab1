"""
luv13 Proxy Server (multi-tenant)
=================================
Sits between Cursor (or any OpenAI-compatible client) and Neuralwatt, fronting
it as the `api.luv13.com` product.

- Accepts requests using luv13-branded model slugs (luv13-*)
- Rewrites model names to Neuralwatt's actual model IDs
- Authenticates customers via hashed `sk-luv13-...` API keys (multi-tenant)
- Routes across a ring of NAMED upstream Neuralwatt accounts using an
  active-standby strategy: N-1 keys actively serve traffic while 1 rests in
  reserve. On a 429 rate-limit or 401/402/403 budget error the failing key is
  demoted to standby (cooled + parked), and the previously-resting key rejoins
  the active pool on the next request. Rate-limited accounts go into a
  cooldown (circuit breaker) and are skipped until they recover. (A
  `round-robin` strategy and a configurable `active-reserve` strategy are also
  available.) Whichever account serves, the response always returns to the
  original customer.
- Tracks usage (input / output / cached tokens, cost, revenue) in SQLite, including
  which account ACTUALLY served each request (served_upstream_index)
- Exposes customer usage at GET /usage (branded-key auth)
- Exposes admin dashboards at /admin/* (ADMIN_TOKEN auth)
- /keys/generate is called by the luv13 website using a JWT session
- Runs on port 4000

Setup:
    pip install -r requirements.txt

Local run:
    python proxy.py

In Cursor, set:
    Base URL: http://localhost:4000/v1   (or https://api.luv13.com/v1)
    API Key:  a customer's sk-luv13-... key
    Model:    any name from MODEL_MAP below
"""

import os
import re
import json
import time
import hmac
import random
import queue
import hashlib
import secrets
import sqlite3
import string
import logging
import threading
from datetime import datetime, timedelta, timezone
from functools import wraps

import requests
import jwt as pyjwt
from flask import (Flask, request, jsonify, Response, stream_with_context, g,
                   make_response, redirect)
from flask_cors import CORS

# ── LOGGING ──────────────────────────────────────────────────────────────────
# Never log upstream keys, customer plaintext keys, JWT secrets, or admin tokens.
# This logger is configured to keep those out by construction: we only ever log
# key prefixes, ids, and counts — never the values themselves.
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("luv13-proxy")
log.warning("logging initialized — upstream keys / customer keys are NEVER logged")

# ── CONFIG ───────────────────────────────────────────────────────────────────
NEURALWATT_BASE_URL = os.getenv("NEURALWATT_BASE_URL", "https://api.neuralwatt.com/v1")
PORT = int(os.getenv("PORT", "4000"))

# Only a CONNECT timeout (seconds). Read timeout is None => never cut off a long
# generation. (connect, read) tuple per the requests library.
CONNECT_TIMEOUT = float(os.getenv("PROXY_CONNECT_TIMEOUT", "15"))
UPSTREAM_TIMEOUT = (CONNECT_TIMEOUT, None)

# ── UPSTREAM ACCOUNT POOL ────────────────────────────────────────────────────
# Hardcoded, named Neuralwatt accounts (no .env dependency for these — keys live in
# code by request). Each account has a human NAME purely for identification in logs
# and the admin dashboards. The key strings themselves are NEVER logged, never echoed
# in responses/errors — only the account name and 1-based index are surfaced.
# Indexed 1-based in the api_keys table (upstream_key_index).
UPSTREAM_ACCOUNTS = [
    {"name": "TEST1", "key": "sk-ad4c7bee5bdf4d3e2becfb3398be714caf6ce7b18337a8d628919287562b8bd5"},
    {"name": "TEST2", "key": "sk-406e39654367a8074dda53f31b601b46d107064955ca044a902f839259340af2"},
    {"name": "TEST3", "key": "sk-57a49c0480ed3e37f60aa11cbafa0efebdd4120b97b66efe52495ac08ca13042"},
    {"name": "TEST4", "key": "sk-739a2270395b9edbf6a99b0fa63890067722e2b114d19cb439944df65f880f59"},
    {"name": "TEST5", "key": "sk-2000c689397825e2c4982c8558673cd92ad8c28f520cdfdba946273f58261d9d"},
    {"name": "TEST6", "key": "sk-219c71abe8f66f50e04912597625034227b4c8649a3485caaa5241a563d5af03"},
    {"name": "TEST7", "key": "sk-18c3f9527413308c037681b40cf973046ec18586b9fb8ff5fc73a937eb32058e"},
    {"name": "TEST8", "key": "sk-eba9066625b3f98359914774f551d9098b31b56396d680497ca5a471fcc18fd5"},
]
UPSTREAM_KEYS = [a["key"] for a in UPSTREAM_ACCOUNTS]
# 1-based index -> human name (logs/admin only)
ACCOUNT_NAMES = {i + 1: a["name"] for i, a in enumerate(UPSTREAM_ACCOUNTS)}
NUM_UPSTREAM_KEYS = len(UPSTREAM_KEYS)


def account_name(idx: int) -> str:
    """Human name for a 1-based upstream index (for logs/admin only)."""
    return ACCOUNT_NAMES.get(idx, f"account-{idx}")


# ── RING FAILOVER + COOLDOWN (circuit breaker) ───────────────────────────────
# Under active-standby (default), N-1 keys actively serve while 1 rests as
# standby; the ring order puts active keys first and the standby last (see
# _active_standby_order / ring_order). On a 429/401/402/403 the failing key is
# demoted to standby (cooled + parked); the previously-resting key rejoins
# active on the next request. Under active-reserve, a configurable number of
# keys serve actively while the rest rest in reserve; failures demote an active
# key to cooldown and promote the next reserve into active. Under round-robin,
# the starting account rotates across requests and failover cascades around the
# ring from there. In all cases a rate-limited account is put into cooldown and
# skipped until it recovers, so it gets a rest while the ring carries traffic. The
# api_keys.upstream_key_index column is retained for round-robin's initial
# offset and admin reporting, but under active-standby it is not a "home
# account" — that column is ignored for ring ordering.
#
# NOTE: cooldown state is in-process memory (fine under `app.run(threaded=True)`).
# If this is ever run under gunicorn with multiple workers, each worker keeps its
# own view — move cooldowns to SQLite/Redis before doing that.
MAX_RETRIES = int(os.getenv("PROXY_MAX_RETRIES", "5"))
MAX_BACKOFF = float(os.getenv("PROXY_MAX_BACKOFF", "30"))          # seconds cap
ACCOUNT_COOLDOWN = float(os.getenv("PROXY_ACCOUNT_COOLDOWN", "3"))   # after a 429/5xx
BUDGET_COOLDOWN = float(os.getenv("PROXY_BUDGET_COOLDOWN", "300"))   # after auth/budget err
RETRY_STATUSES = {429, 500, 502, 503, 504}   # rate-limited/transient -> cooldown + failover
BUDGET_STATUSES = {401, 402, 403}            # auth/budget exhausted -> long cooldown + failover

# Queue wait: when ALL upstream accounts are in cooldown, the proxy waits for
# one to recover instead of failing fast with 503. 0 = wait indefinitely (the
# proxy stays open, sending SSE heartbeats for streaming, until an account is
# available). Any positive value = max seconds to wait before giving up and
# returning 503 + Retry-After (the seconds until the soonest account recovers).
# Default 0 — wait indefinitely. Reasoning models can sit in queue for longer
# than 30s while all four accounts cool down; bounded waits were 503ing them
# mid-thought. Set PROXY_QUEUE_MAX_WAIT > 0 to re-enable bounded 503 behavior.
QUEUE_MAX_WAIT = float(os.getenv("PROXY_QUEUE_MAX_WAIT", "0"))    # 0 = unlimited (wait until account recovers); >0 = bounded wait then 503 + Retry-After

# ── STREAMING HEARTBEAT + STALL FAILOVER ─────────────────────────────────────
# Heartbeat: send SSE comment lines (": keepalive\n\n") every N seconds before the
# first token arrives. This keeps Cursor from hitting its "taking longer than
# expected" timeout while Neuralwatt is still processing the prompt.
HEARTBEAT_INTERVAL = float(os.getenv("PROXY_HEARTBEAT_INTERVAL", "5"))  # seconds

# Stall: if no SSE chunks arrive for N seconds *during* a stream (after the first
# token), the upstream is considered stalled. The connection is closed, the
# account is cooled, and the request is retried on the next account in the ring.
#
# `STREAM_STALL_TIMEOUT_MS` mirrors the deployment tool used for the queue-wait
# change at PROXY_QUEUE_MAX_WAIT: a *_MS env var with a safe integer parser so
# reasoning models can think for up to a minute without the proxy mistaking "the
# model is reasoning" for "the connection is dead." Validated strictly — any
# non-integer / non-positive value logs a warning and falls back to the default.
# `PROXY_STREAM_STALL_TIMEOUT` (seconds, float) remains as a legacy override for
# back-compat; the *_MS var takes precedence if both are set.
def _parse_stall_timeout_ms() -> float:
    """Parse STREAM_STALL_TIMEOUT_MS into seconds (float), with safe fallback.

    Accepts the legacy PROXY_STREAM_STALL_TIMEOUT (seconds, float) as a fallback.
    Returns the effective stall timeout in seconds. Never raises.
    """
    raw_ms = os.getenv("STREAM_STALL_TIMEOUT_MS")
    if raw_ms is not None and raw_ms.strip():
        try:
            ms = int(raw_ms)
        except (TypeError, ValueError):
            log.warning(
                "STREAM_STALL_TIMEOUT_MS=%r is not a valid integer; "
                "falling back to default 60000ms", raw_ms,
            )
            return 60.0
        if ms <= 0:
            log.warning(
                "STREAM_STALL_TIMEOUT_MS=%d must be positive; "
                "falling back to default 60000ms", ms,
            )
            return 60.0
        return ms / 1000.0
    # Legacy seconds-based override (float, tolerated).
    legacy = os.getenv("PROXY_STREAM_STALL_TIMEOUT")
    if legacy is not None and legacy.strip():
        try:
            v = float(legacy)
            if v > 0:
                return v
            log.warning(
                "PROXY_STREAM_STALL_TIMEOUT=%r must be positive; "
                "falling back to default 60.0s", legacy,
            )
        except (TypeError, ValueError):
            log.warning(
                "PROXY_STREAM_STALL_TIMEOUT=%r is not a valid float; "
                "falling back to default 60.0s", legacy,
            )
    return 60.0


STREAM_STALL_TIMEOUT = _parse_stall_timeout_ms()   # seconds
MAX_STREAM_RETRIES = int(os.getenv("PROXY_MAX_STREAM_RETRIES", "50"))        # mid-stream retries — high so a saturated ring grinds through cooldowns rather than 503ing the client

# Overload protection: if an active upstream key takes longer than this to emit
# its FIRST token (TTFB), it is treated as overloaded — instantly demoted
# (active-reserve rotation) and the request is retried on the next active key.
# This is faster than waiting the full STREAM_STALL_TIMEOUT, so Cursor stops
# seeing "taking longer than expected" while a saturated key sits on the prompt.
# Set PROXY_FIRST_TOKEN_TIMEOUT_MS=0 to disable (fall back to full stall timeout).
def _parse_first_token_timeout_ms() -> float:
    raw = os.getenv("PROXY_FIRST_TOKEN_TIMEOUT_MS", "10000")
    try:
        ms = int(raw)
    except (TypeError, ValueError):
        log.warning("PROXY_FIRST_TOKEN_TIMEOUT_MS=%r is not a valid integer; "
                    "falling back to default 10000ms", raw)
        return 10.0
    if ms < 0:
        log.warning("PROXY_FIRST_TOKEN_TIMEOUT_MS=%d must be >= 0; "
                    "falling back to default 10000ms", ms)
        return 10.0
    if ms == 0:
        return 0.0  # disabled — full stall timeout governs
    return ms / 1000.0

FIRST_TOKEN_TIMEOUT = _parse_first_token_timeout_ms()
OVERLOAD_COOLDOWN = float(os.getenv("PROXY_OVERLOAD_COOLDOWN", "120"))  # 2 min rest after overload
# Non-streaming requests get a read timeout so an overloaded upstream can't
# hang the request forever. Streaming uses FIRST_TOKEN_TIMEOUT + stall detection
# instead (None read timeout so long generations aren't cut off mid-stream).
NONSTREAM_READ_TIMEOUT = float(os.getenv(
    "PROXY_NONSTREAM_READ_TIMEOUT",
    str(int(FIRST_TOKEN_TIMEOUT if FIRST_TOKEN_TIMEOUT > 0 else 30)),
))
UPSTREAM_TIMEOUT_NONSTREAM = (CONNECT_TIMEOUT, NONSTREAM_READ_TIMEOUT)

_cooldowns = {}                 # 1-based upstream idx -> epoch time it's cooling until
_cooldown_lock = threading.Lock()


# ── UPSTREAM ROUTING STRATEGY ───────────────────────────────────────────────
# "active-standby" (default): N-1 keys actively serve traffic while 1 rests as
#   standby. On a 429/401/402/403 the failing key is demoted to standby (cooled
#   + parked); the previously-resting key rejoins active on the next request.
#   5xx / connection errors / mid-stream stalls still cool the key (existing
#   behavior) but do NOT rotate the standby. The `api_keys.upstream_key_index`
#   column is retained for round-robin's initial offset and admin reporting but
#   is no longer treated as a "home account" under this strategy.
# "round-robin": rotate the starting account across all requests so concurrent
#   sessions spread evenly across the pool. Failover still cascades around the
#   ring from whichever account was picked first.
# "active-reserve": a configurable number of keys actively serve traffic
#   (PROXY_ACTIVE_COUNT, default 3) while the rest rest in reserve
#   (PROXY_RESERVE_COUNT, default 3). Active keys are shared evenly via a
#   per-position counter (not a naive global counter). When an active key is
#   rate-limited or hits an auth/budget error it is demoted to cooldown (red);
#   after cooling it joins the reserve pool. The next reserve key rotates into
#   active to replace it.
UPSTREAM_STRATEGY = os.getenv("PROXY_UPSTREAM_STRATEGY", "active-standby").lower()
ACTIVE_RESERVE_ACTIVE_COUNT = int(os.getenv("PROXY_ACTIVE_COUNT", "3"))
ACTIVE_RESERVE_RESERVE_COUNT = int(os.getenv("PROXY_RESERVE_COUNT", "3"))
if ACTIVE_RESERVE_ACTIVE_COUNT + ACTIVE_RESERVE_RESERVE_COUNT > NUM_UPSTREAM_KEYS:
    raise ValueError(
        f"PROXY_ACTIVE_COUNT ({ACTIVE_RESERVE_ACTIVE_COUNT}) + "
        f"PROXY_RESERVE_COUNT ({ACTIVE_RESERVE_RESERVE_COUNT}) = "
        f"{ACTIVE_RESERVE_ACTIVE_COUNT + ACTIVE_RESERVE_RESERVE_COUNT}, "
        f"which exceeds NUM_UPSTREAM_KEYS ({NUM_UPSTREAM_KEYS})"
    )
if ACTIVE_RESERVE_ACTIVE_COUNT < 1:
    raise ValueError("PROXY_ACTIVE_COUNT must be at least 1")
_round_robin_counter = 0
_round_robin_lock = threading.Lock()

log.info("strategy: %s", UPSTREAM_STRATEGY)
if UPSTREAM_STRATEGY == "active-reserve":
    log.info("active-reserve pool: %d active, %d reserve",
              ACTIVE_RESERVE_ACTIVE_COUNT, ACTIVE_RESERVE_RESERVE_COUNT)


def _next_round_robin_idx() -> int:
    """Atomically increment and return the next 1-based upstream index."""
    global _round_robin_counter
    with _round_robin_lock:
        idx = ((_round_robin_counter % NUM_UPSTREAM_KEYS) + 1)
        _round_robin_counter += 1
        return idx


def _cooldown_remaining(idx: int) -> float:
    with _cooldown_lock:
        return max(0.0, _cooldowns.get(idx, 0.0) - time.time())


# In-memory log of cooldown START timestamps per account, so recovery can be
# emitted as an event when _cooldown_remaining crosses zero (lazy detection in
# _admin_summary_data). Keyed by 1-based upstream idx.
_cooldown_started_at = {}                 # idx -> epoch when cooldown was set
_cooldown_started_lock = threading.Lock()


def record_event(upstream_key_index, event_type: str, *,
                 http_status: int | None = None, message: str = "") -> None:
    """Persist an event row. Safe from any thread — opens its own short-lived
    SQLite connection so it can't collide with the per-request `g.db`.

    event_type is one of: cooldown_start, cooldown_recover, error_429,
    error_budget, error_5xx, error_conn, error_stall, error_overload,
    error_timeout, queue_wait, info.
    """
    try:
        name = account_name(upstream_key_index) if upstream_key_index else None
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, isolation_level=None,
                               check_same_thread=False, timeout=5)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """INSERT INTO events
                   (timestamp, upstream_key_index, account_name,
                    event_type, http_status, message)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (datetime.now(timezone.utc).isoformat(),
                 upstream_key_index, name, event_type, http_status, message),
            )
            # Bounded retention.
            conn.execute(
                "DELETE FROM events WHERE id NOT IN "
                "(SELECT id FROM events ORDER BY id DESC LIMIT 5000)"
            )
        finally:
            conn.close()
    except Exception as e:  # never let logging break the request path
        log.warning("record_event failed: %s", e)


def _set_cooldown(idx: int, seconds: float, *,
                  reason: str | None = None, http_status: int | None = None) -> None:
    """Set (or extend) a cooldown for upstream account idx. Persists a
    cooldown_start event so the admin dashboard can show cooldown timestamps
    and the test harness can correlate 429 timing."""
    with _cooldown_lock:
        _cooldowns[idx] = max(_cooldowns.get(idx, 0.0), time.time() + seconds)
    with _cooldown_started_lock:
        _cooldown_started_at[idx] = time.time()
    ev_type = "cooldown_start"
    msg = reason or f"cooling {seconds:.1f}s"
    if http_status:
        ev_type = f"error_{http_status}" if http_status in (429, 401, 402, 403) else ev_type
    record_event(idx, ev_type, http_status=http_status, message=msg)


# ── ACTIVE-STANDBY + ACTIVE-RESERVE POOLS ───────────────────────────────────
# active-standby: one upstream key rests as STANDBY while the others actively
#   serve. On a 429/401/402/403 the failing key is demoted to standby (cooled).
#   5xx/connection errors/mid-stream stalls cool the key but do NOT rotate the
#   standby.
# active-reserve: configurable active/reserve split. Active keys serve traffic
#   and are shared evenly via a per-position counter (not a naive global
#   counter). Reserve keys rest. When an active key is demoted it is cooled; the
#   next reserve key in the ring rotates into active. After cooldown the demoted
#   key becomes a reserve.
#
# In-process state: each gunicorn worker keeps its own view. All workers
# bootstrap to the same default pools, so they tend to agree. See the cooldown
# caveat above.
_STANDBY_IDX = NUM_UPSTREAM_KEYS  # 1-based idx of resting key; key N rests at boot
_POOL_LOCK = threading.Lock()     # guards standby / active-reserve swaps

# active-reserve state
_ACTIVE_RESERVE_SLOTS = {
    "active": set(range(1, ACTIVE_RESERVE_ACTIVE_COUNT + 1)),      # 1-based
    "reserve": set(range(
        ACTIVE_RESERVE_ACTIVE_COUNT + 1,
        ACTIVE_RESERVE_ACTIVE_COUNT + ACTIVE_RESERVE_RESERVE_COUNT + 1,
    )),
}
# Counter for perfectly even rotation among active keys. The counter stores the
# index into the sorted active list at which the next request will start; after
# each request it advances by one and wraps, so every active key gets exactly
# 1/N of requests.
_ACTIVE_RESERVE_COUNTER = 0
_ACTIVE_RESERVE_COUNTER_LOCK = threading.Lock()


def _active_standby_order() -> list:
    """1-based indices, active keys first (standby excluded), standby last as a
    last-resort fallback. If no standby is set, returns plain 1..N order.
    """
    with _POOL_LOCK:
        standby = _STANDBY_IDX
    order = [i for i in range(1, NUM_UPSTREAM_KEYS + 1) if i != standby]
    if standby is not None:
        order.append(standby)
    return order


def _demote_to_standby(idx: int, cooldown_s: float, *,
                       reason: str | None = None,
                       http_status: int | None = None) -> None:
    """429/403 received under active-standby: cool the key and make it the new
    standby. The previously-resting key automatically rejoins active on the next
    request."""
    global _STANDBY_IDX
    _set_cooldown(idx, cooldown_s, reason=reason, http_status=http_status)
    with _POOL_LOCK:
        _STANDBY_IDX = idx


def _active_reserve_state() -> tuple:
    """Return (active, reserve) sets of 1-based indices under _POOL_LOCK.

    If cooldown expired for demoted keys, move them back into reserve if there
    is room, restoring the configured reserve size.
    """
    with _POOL_LOCK:
        global _ACTIVE_RESERVE_SLOTS
        active = set(_ACTIVE_RESERVE_SLOTS["active"])
        reserve = set(_ACTIVE_RESERVE_SLOTS["reserve"])
        cooling = set(range(1, NUM_UPSTREAM_KEYS + 1)) - active - reserve
        now = time.time()
        recovered = {i for i in cooling if _cooldowns.get(i, 0) <= now}
        if recovered:
            for i in sorted(recovered):
                if len(reserve) < ACTIVE_RESERVE_RESERVE_COUNT:
                    reserve.add(i)
                else:
                    active.add(i)
            # If active dipped below configured, top it off from reserve.
            while len(active) < ACTIVE_RESERVE_ACTIVE_COUNT and reserve:
                next_reserve = min(reserve)
                reserve.remove(next_reserve)
                active.add(next_reserve)
            _ACTIVE_RESERVE_SLOTS = {"active": active, "reserve": reserve}
        return active, reserve


def _active_reserve_order() -> list:
    """Order: active keys (rotated evenly), then reserve, then any others."""
    active, reserve = _active_reserve_state()
    active_sorted = sorted(active)
    with _ACTIVE_RESERVE_COUNTER_LOCK:
        global _ACTIVE_RESERVE_COUNTER
        if active_sorted:
            start = _ACTIVE_RESERVE_COUNTER % len(active_sorted)
            _ACTIVE_RESERVE_COUNTER += 1
            active_order = active_sorted[start:] + active_sorted[:start]
        else:
            active_order = []
    reserve_order = sorted(reserve)
    others = [i for i in range(1, NUM_UPSTREAM_KEYS + 1)
              if i not in active and i not in reserve]
    return active_order + reserve_order + others


def _active_reserve_demote(idx: int, cooldown_s: float, *,
                           reason: str | None = None,
                           http_status: int | None = None) -> None:
    """Demote active key idx: cooldown, then promote next reserve to active."""
    global _ACTIVE_RESERVE_SLOTS
    _set_cooldown(idx, cooldown_s, reason=reason, http_status=http_status)
    with _POOL_LOCK:
        slots = {"active": set(_ACTIVE_RESERVE_SLOTS["active"]),
                 "reserve": set(_ACTIVE_RESERVE_SLOTS["reserve"])}
        slots["active"].discard(idx)
        available_reserve = sorted(r for r in slots["reserve"]
                                    if _cooldown_remaining(r) <= 0)
        if available_reserve:
            replacement = available_reserve[0]
            slots["reserve"].remove(replacement)
            slots["active"].add(replacement)
        # Recovered cooling keys may refill reserve.
        now = time.time()
        cooling = (set(range(1, NUM_UPSTREAM_KEYS + 1))
                   - slots["active"] - slots["reserve"])
        recovered = sorted(i for i in cooling if _cooldowns.get(i, 0) <= now)
        for i in recovered:
            if len(slots["reserve"]) < ACTIVE_RESERVE_RESERVE_COUNT:
                slots["reserve"].add(i)
        _ACTIVE_RESERVE_SLOTS = slots


def _retry_delay(resp) -> float:
    """Seconds to cool an account: prefer the server's Retry-After, else backoff."""
    if resp is not None:
        ra = resp.headers.get("Retry-After")
        if ra:
            try:
                return min(float(ra), MAX_BACKOFF)
            except ValueError:
                pass
    return min(2.0 + random.random(), MAX_BACKOFF)


def _wait_for_available_account(order, heartbeat_cb=None) -> int:
    """Wait for an account in `order` to come off cooldown and return its idx.

    Called when all accounts are cooling — prevents a 503 by queueing the
    request until an account recovers. Waits indefinitely if QUEUE_MAX_WAIT=0,
    otherwise waits up to QUEUE_MAX_WAIT seconds.

    `heartbeat_cb()` is called periodically while waiting (for streaming, it
    sends SSE keepalive comments so NPM/Cloudflare/Cursor don't time out).
    Returns the 1-based idx of the first available account, or 0 if the wait
    was exhausted.
    """
    deadline = None if QUEUE_MAX_WAIT <= 0 else time.time() + QUEUE_MAX_WAIT
    poll_interval = min(HEARTBEAT_INTERVAL, 5.0)
    while True:
        # Check for any available account
        for idx in order:
            if _cooldown_remaining(idx) <= 0 and UPSTREAM_KEYS[idx - 1]:
                return idx
        # All in cooldown — wait and heartbeat
        if deadline is not None and time.time() >= deadline:
            return 0
        if heartbeat_cb is not None:
            heartbeat_cb()
        # Sleep in small slices so the heartbeat fires regularly
        soonest = min((_cooldown_remaining(i) for i in order), default=poll_interval)
        time.sleep(min(soonest, poll_interval))


def ring_order(primary_idx: int) -> list:
    """1-based indices to try, in the order they should be attempted.

    - active-standby (default): N-1 keys actively serve while 1 rests as
      standby. Active keys come first (in ascending order), the standby is
      appended last as a last-resort fallback. On a 429/401/402/403 the
      failing key is demoted to standby (see _demote_to_standby); the
      previously-resting key rejoining active is just the natural consequence
      of it no longer being the standby. `primary_idx` is ignored under this
      strategy.
    - active-reserve: PROXY_ACTIVE_COUNT keys actively serve while
      PROXY_RESERVE_COUNT keys rest in reserve. Active keys are rotated evenly
      so each gets exactly 1/N of traffic. On failure an active key is demoted
      to cooldown and the next reserve key is promoted into active. After
      cooldown the demoted key becomes a reserve.
    - round-robin: rotate the starting point across all requests, spreading
      load evenly. Failover still cascades from there. Also the implicit
      fallback for any unrecognized strategy value.
    """
    n = NUM_UPSTREAM_KEYS
    if UPSTREAM_STRATEGY == "active-standby":
        return _active_standby_order()
    if UPSTREAM_STRATEGY == "active-reserve":
        return _active_reserve_order()
    # round-robin (safe fallthrough default for unrecognized strategy values)
    start = _next_round_robin_idx()
    order = [start]
    i = start
    for _ in range(n - 1):
        i = i % n + 1
        order.append(i)
    return order


def post_upstream(order, body, stream):
    """Send to Neuralwatt across the ring. Returns (response_or_None, used_idx).

    Tries each non-cooling account in `order`; on 429/5xx/conn error it cools that
    account (honoring Retry-After) and cascades to the next; on auth/budget error it
    cools it for much longer. If the whole ring is cooling, waits for the soonest to
    recover, up to MAX_RETRIES rounds. For streaming, status is checked before any
    bytes are yielded so failover happens pre-stream.
    """
    last_resp, last_idx = None, order[-1]
    rounds = 0
    queue_deadline = None if QUEUE_MAX_WAIT <= 0 else time.time() + QUEUE_MAX_WAIT
    while True:
        available = [i for i in order if _cooldown_remaining(i) <= 0 and UPSTREAM_KEYS[i - 1]]
        if not available:
            # All accounts cooling — wait for one to recover (queue behavior)
            if queue_deadline is not None and time.time() >= queue_deadline:
                return last_resp, last_idx
            soonest = min((_cooldown_remaining(i) for i in order), default=1.0)
            time.sleep(min(max(soonest, 0.1), MAX_BACKOFF))
            rounds += 1
            continue
        for idx in available:
            headers = {
                "Authorization": f"Bearer {UPSTREAM_KEYS[idx - 1]}",
                "Content-Type": "application/json",
            }
            try:
                resp = requests.post(
                    f"{NEURALWATT_BASE_URL}/chat/completions",
                    headers=headers, json=body, stream=stream,
                    timeout=UPSTREAM_TIMEOUT_NONSTREAM if not stream else UPSTREAM_TIMEOUT,
                )
            except requests.exceptions.ReadTimeout as e:
                # Overload/latency on non-streaming: cool long, rotate reserve.
                _set_cooldown(idx, OVERLOAD_COOLDOWN,
                              reason="read timeout (overloaded)", http_status=None)
                if UPSTREAM_STRATEGY == "active-reserve":
                    _active_reserve_demote(idx, OVERLOAD_COOLDOWN,
                                           reason="read timeout (overloaded)")
                elif UPSTREAM_STRATEGY == "active-standby":
                    _demote_to_standby(idx, OVERLOAD_COOLDOWN,
                                       reason="read timeout (overloaded)")
                last_idx = idx
                log.warning("account '%s' (idx %d) READ TIMEOUT (overloaded); cooling %.0fs, rotating",
                            account_name(idx), idx, OVERLOAD_COOLDOWN)
                continue
            except requests.exceptions.RequestException as e:
                _set_cooldown(idx, ACCOUNT_COOLDOWN,
                              reason=f"conn error: {type(e).__name__}")
                if UPSTREAM_STRATEGY == "active-reserve":
                    _active_reserve_demote(idx, ACCOUNT_COOLDOWN,
                                           reason=f"conn error: {type(e).__name__}")
                last_idx = idx
                log.warning("account '%s' (idx %d) connection error (%s); cooling %ss",
                            account_name(idx), idx, type(e).__name__, ACCOUNT_COOLDOWN)
                continue
            code = resp.status_code
            if code not in RETRY_STATUSES and code not in BUDGET_STATUSES:
                return resp, idx                      # success (or non-retryable 4xx)
            last_resp, last_idx = resp, idx
            if stream:
                resp.close()
            if code in RETRY_STATUSES:
                cd = max(_retry_delay(resp), ACCOUNT_COOLDOWN)
                reason = f"retryable status {code}"
                if UPSTREAM_STRATEGY == "active-standby" and code == 429:
                    _demote_to_standby(idx, cd, reason=reason, http_status=code)
                elif UPSTREAM_STRATEGY == "active-reserve":
                    _active_reserve_demote(idx, cd, reason=reason, http_status=code)
                else:
                    _set_cooldown(idx, cd, reason=reason, http_status=code)
                log.warning("account '%s' (idx %d) status %d; cooling %.1fs, failing over",
                            account_name(idx), idx, code, cd)
            else:
                reason = f"auth/budget status {code}"
                if UPSTREAM_STRATEGY == "active-standby":
                    _demote_to_standby(idx, BUDGET_COOLDOWN, reason=reason, http_status=code)
                elif UPSTREAM_STRATEGY == "active-reserve":
                    _active_reserve_demote(idx, BUDGET_COOLDOWN, reason=reason, http_status=code)
                else:
                    _set_cooldown(idx, BUDGET_COOLDOWN, reason=reason, http_status=code)
                log.warning("account '%s' (idx %d) auth/budget status %d; cooling %ss",
                            account_name(idx), idx, code, BUDGET_COOLDOWN)
        # Loop back — if all accounts are now cooling, the top of the loop
        # will wait for one to recover (queue behavior) instead of failing.


# ── STREAMING EVENT TYPES ─────────────────────────────────────────────────────
# Every value yielded by stream_upstream() is an instance of one of these. The
# caller does `match event:` over them, so adding a new event type is safe —
# type-checkers (and runtime, with a `case _:` arm) will surface any caller that
# forgets to handle the new kind. This replaces the old positional (kind, data)
# tuple convention, which crashed if a yield arity didn't match the unpack.
class StreamEvent:
    """Base. Carries no payload — subclasses add their own fields."""
    __slots__ = ()


class ChunkEvent(StreamEvent):
    """A raw SSE byte chunk from upstream. Forward to the client verbatim."""
    __slots__ = ("data",)

    def __init__(self, data: bytes):
        self.data = data


class AccountEvent(StreamEvent):
    """Which upstream account idx is serving this stream (for usage tracking)."""
    __slots__ = ("idx",)

    def __init__(self, idx: int):
        self.idx = idx


class HeartbeatEvent(StreamEvent):
    """Idle keepalive — no token yet, or queue wait. Emit a SSE comment."""
    __slots__ = ()


class DoneEvent(StreamEvent):
    """Normal end of stream. Carries the accumulated usage state."""
    __slots__ = ("state",)

    def __init__(self, state: dict):
        self.state = state


class ErrorEvent(StreamEvent):
    """Terminal error. Carries a human-readable message and optional retry hint."""
    __slots__ = ("message", "retry_after")

    def __init__(self, message: str, retry_after: float | None = None):
        self.message = message
        # Seconds until the soonest account recovers, when known. Used to emit
        # an HTTP Retry-After header (non-streaming) or include it in the SSE
        # error payload (streaming) so the client knows when to retry.
        self.retry_after = retry_after


# ── STREAMING WITH HEARTBEAT + STALL FAILOVER ────────────────────────────────
def stream_upstream(order, body):
    """Generator yielding StreamEvent instances for the streaming path.

    Yields one of:
      ChunkEvent(bytes)     — forward these bytes to the client verbatim
      AccountEvent(int)     — which upstream idx is serving (for usage tracking)
      HeartbeatEvent()      — send a keepalive comment (idle, no token yet)
      DoneEvent(dict)       — normal end of stream, carries the usage state
      ErrorEvent(str)       — terminal error message

    Handles:
      - Pre-stream failover (429/5xx/conn error before any bytes)
      - SSE heartbeat comments every HEARTBEAT_INTERVAL before first token
      - Mid-stream stall detection: if no chunks for STREAM_STALL_TIMEOUT seconds,
        cool the account and retry on the next in the ring (up to MAX_STREAM_RETRIES)
      - Usage extraction from SSE text
    """
    state = {"buf": "", "prompt_tokens": 0, "completion_tokens": 0,
             "cached_tokens": 0}
    body_copy = json.loads(json.dumps(body))  # deep copy — requests may consume it
    tried = set()
    attempt = 0

    while attempt <= MAX_STREAM_RETRIES:
        # Pick next account: walk the ring order (active-standby or round-robin)
        # skipping anything we already tried this request, then any non-cooling.
        remaining = [i for i in order if i not in tried and _cooldown_remaining(i) <= 0]
        if not remaining:
            # Try accounts we already tried (maybe they've cooled down)
            remaining = [i for i in order if _cooldown_remaining(i) <= 0]
        if not remaining:
            # All accounts in cooldown — wait for one to recover instead of
            # failing. Yield heartbeats so the client/proxies don't time out.
            log.info("all accounts cooling; queueing stream until one recovers")
            deadline = None if QUEUE_MAX_WAIT <= 0 else time.time() + QUEUE_MAX_WAIT
            poll_interval = min(HEARTBEAT_INTERVAL, 5.0)
            while True:
                yield HeartbeatEvent()
                available = [i for i in order if _cooldown_remaining(i) <= 0 and UPSTREAM_KEYS[i - 1]]
                if available:
                    remaining = available
                    break
                if deadline is not None and time.time() >= deadline:
                    retry_after = min(
                        (_cooldown_remaining(i) for i in order if UPSTREAM_KEYS[i - 1]),
                        default=ACCOUNT_COOLDOWN,
                    )
                    yield ErrorEvent(
                        "all upstream accounts unavailable (queue wait exhausted)",
                        retry_after=retry_after,
                    )
                    return
                soonest = min((_cooldown_remaining(i) for i in order), default=poll_interval)
                time.sleep(min(soonest, poll_interval))

        idx = remaining[0]
        tried.add(idx)
        headers = {
            "Authorization": f"Bearer {UPSTREAM_KEYS[idx - 1]}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(
                f"{NEURALWATT_BASE_URL}/chat/completions",
                headers=headers, json=body_copy, stream=True, timeout=UPSTREAM_TIMEOUT,
            )
        except requests.exceptions.RequestException as e:
            _set_cooldown(idx, ACCOUNT_COOLDOWN,
                          reason=f"stream conn error: {type(e).__name__}")
            if UPSTREAM_STRATEGY == "active-reserve":
                _active_reserve_demote(idx, ACCOUNT_COOLDOWN,
                                       reason=f"stream conn error: {type(e).__name__}")
            log.warning("account '%s' (idx %d) connect error (%s); cooling %ss",
                        account_name(idx), idx, type(e).__name__, ACCOUNT_COOLDOWN)
            attempt += 1
            continue

        if resp.status_code in RETRY_STATUSES or resp.status_code in BUDGET_STATUSES:
            code = resp.status_code
            resp.close()
            if code in RETRY_STATUSES:
                cd = max(_retry_delay(resp), ACCOUNT_COOLDOWN)
                reason = f"pre-stream retryable {code}"
                if UPSTREAM_STRATEGY == "active-standby" and code == 429:
                    _demote_to_standby(idx, cd, reason=reason, http_status=code)
                elif UPSTREAM_STRATEGY == "active-reserve":
                    _active_reserve_demote(idx, cd, reason=reason, http_status=code)
                else:
                    _set_cooldown(idx, cd, reason=reason, http_status=code)
                log.warning("account '%s' (idx %d) pre-stream %d; cooling %.1fs, failing over",
                            account_name(idx), idx, code, cd)
            else:
                reason = f"pre-stream auth/budget {code}"
                if UPSTREAM_STRATEGY == "active-standby":
                    _demote_to_standby(idx, BUDGET_COOLDOWN, reason=reason, http_status=code)
                elif UPSTREAM_STRATEGY == "active-reserve":
                    _active_reserve_demote(idx, BUDGET_COOLDOWN, reason=reason, http_status=code)
                else:
                    _set_cooldown(idx, BUDGET_COOLDOWN, reason=reason, http_status=code)
                log.warning("account '%s' (idx %d) pre-stream auth/budget %d; cooling %ss",
                            account_name(idx), idx, code, BUDGET_COOLDOWN)
            attempt += 1
            continue

        if resp.status_code >= 400:
            detail = resp.content.decode("utf-8", "replace")[:500]
            resp.close()
            yield ErrorEvent(f"upstream {resp.status_code}: {detail}")
            return

        # ── Stream is live — yield chunks with heartbeat + stall detection
        yield AccountEvent(idx)
        first_token_received = False
        request_started_at = time.time()
        last_chunk_time = time.time()
        stalled = False
        overloaded = False

        try:
            with resp:
                for chunk in _iter_with_heartbeat(resp, HEARTBEAT_INTERVAL,
                                                   STREAM_STALL_TIMEOUT):
                    if chunk is None:
                        # Heartbeat timeout — no data for HEARTBEAT_INTERVAL
                        if not first_token_received:
                            # Overload check: if first token has taken too long,
                            # treat this account as overloaded — instantly
                            # demote (active-reserve rotation) and fail over.
                            if (FIRST_TOKEN_TIMEOUT > 0
                                    and time.time() - request_started_at >= FIRST_TOKEN_TIMEOUT):
                                overloaded = True
                                break
                            yield HeartbeatEvent()
                        continue
                    if chunk is _STALL_SENTINEL:
                        # No data for STREAM_STALL_TIMEOUT — stalled
                        stalled = True
                        break

                    first_token_received = True
                    last_chunk_time = time.time()
                    try:
                        _extract_usage_from_sse_text(
                            chunk.decode("utf-8", "replace"), state
                        )
                    except Exception:
                        pass
                    yield ChunkEvent(chunk)
        except Exception as e:
            log.warning("stream exception on '%s' (idx %d): %s",
                        account_name(idx), idx, type(e).__name__)

        if overloaded:
            # Instant demotion: this key is saturated — give it a long rest and
            # rotate the reserve in. Faster than waiting out the stall timeout.
            _set_cooldown(idx, OVERLOAD_COOLDOWN,
                          reason=f"overloaded: no first token in {FIRST_TOKEN_TIMEOUT:.1f}s")
            if UPSTREAM_STRATEGY == "active-reserve":
                _active_reserve_demote(idx, OVERLOAD_COOLDOWN,
                                       reason="overloaded: no first token")
            log.warning("account '%s' (idx %d) OVERLOADED (no first token in %.1fs); "
                        "cooling %.0fs, instant rotation (attempt %d/%d)",
                        account_name(idx), idx, FIRST_TOKEN_TIMEOUT,
                        OVERLOAD_COOLDOWN, attempt + 1, MAX_STREAM_RETRIES)
            resp.close()
            attempt += 1
            continue

        if stalled:
            _set_cooldown(idx, ACCOUNT_COOLDOWN,
                          reason=f"stalled mid-stream: no data {STREAM_STALL_TIMEOUT:.0f}s")
            if UPSTREAM_STRATEGY == "active-reserve":
                _active_reserve_demote(idx, ACCOUNT_COOLDOWN,
                                       reason="stalled mid-stream")
            log.warning("account '%s' (idx %d) stalled mid-stream (no data %ss); "
                        "cooling and failing over (attempt %d/%d)",
                        account_name(idx), idx, STREAM_STALL_TIMEOUT,
                        ACCOUNT_COOLDOWN, attempt + 1, MAX_STREAM_RETRIES)
            resp.close()
            attempt += 1
            continue

        # Normal end of stream
        resp.close()
        yield DoneEvent(state)
        return

    yield ErrorEvent(f"stream failed after {attempt} attempts (stall failover exhausted)")


_STALL_SENTINEL = object()
_STREAM_CLOSED = object()


def _iter_with_heartbeat(resp, heartbeat_interval, stall_timeout):
    """Yield raw chunks from resp.iter_content, with None for heartbeat gaps
    and _STALL_SENTINEL if no data for stall_timeout seconds.

    Implemented with a background reader thread + a queue, because
    resp.iter_content() reads through urllib3's internal buffer. Calling
    select() on the underlying socket misses bytes already pulled into that
    buffer, which made the proxy think a healthy stream was stalled and
    wrongly fail it over — producing empty/truncated responses in Cursor.
    """
    out = queue.Queue()

    def _reader():
        try:
            for chunk in resp.iter_content(chunk_size=None):
                if chunk:
                    out.put(chunk)
        except Exception:
            pass
        finally:
            out.put(_STREAM_CLOSED)

    t = threading.Thread(target=_reader, daemon=True)
    t.start()

    last_data = time.time()
    heartbeat_at = last_data + heartbeat_interval

    while True:
        try:
            item = out.get(timeout=min(heartbeat_interval, stall_timeout))
        except queue.Empty:
            if time.time() - last_data >= stall_timeout:
                yield _STALL_SENTINEL
                return
            if time.time() >= heartbeat_at:
                heartbeat_at = time.time() + heartbeat_interval
                yield None
            continue

        if item is _STREAM_CLOSED:
            return
        last_data = time.time()
        heartbeat_at = last_data + heartbeat_interval
        yield item

# ── AUTH SECRETS ─────────────────────────────────────────────────────────────
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").encode("utf-8")
JWT_SECRET = os.getenv("JWT_SECRET", "").encode("utf-8")
JWT_ALG = "HS256"
JWT_TTL_DAYS = 30

# Browser login for /admin/* — username/password in .env (constant-time compare).
# Falls back to no-login if unset (header-based ADMIN_TOKEN still works).
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
ADMIN_SESSION_COOKIE = "luv13_admin"
ADMIN_SESSION_TTL_DAYS = int(os.getenv("ADMIN_SESSION_TTL_DAYS", "7"))

# ── MODEL MAP ────────────────────────────────────────────────────────────────
# Left  = what luv13 customers put in Cursor (branded slugs, luv13- prefix)
# Right = what gets sent to Neuralwatt's API
MODEL_MAP = {
    "luv13-glm-5.2":                  "glm-5.2",
    "luv13-glm-5.2-fast":             "glm-5.2-fast",
    "luv13-kimi-code":                "moonshotai/Kimi-K2.7-Code",
    "luv13-qwen3":                    "Qwen/Qwen3.6-35B-A3B",
    # Pass through real names unchanged (fallback) so bare model names still work
    "glm-5.2":                        "glm-5.2",
    "glm-5.2-fast":                   "glm-5.2-fast",
    "moonshotai/Kimi-K2.7-Code":      "moonshotai/Kimi-K2.7-Code",
    "Qwen/Qwen3.6-35B-A3B":           "Qwen/Qwen3.6-35B-A3B",
}

# ── PRICING ──────────────────────────────────────────────────────────────────
# Flat-rate revenue model: $0.23 per million tokens billed on the FULL token
# count (prompt_tokens + completion_tokens). The OpenAI spec's `prompt_tokens`
# already includes cached tokens as a subset (cached_tokens is a detail of
# prompt_tokens, NOT an addition), so revenue = (input + output) / 1M * $0.23.
# Cached tokens are billed at the same flat rate as part of the prompt count —
# no separate cached price, no free tier in v1. Keeping a single rate on a
# single axis makes the capacity-test math simple and avoids double-counting.
YOUR_INPUT_PRICE_PER_M = float(os.getenv("YOUR_INPUT_PRICE_PER_M", "0.23"))
YOUR_OUTPUT_PRICE_PER_M = float(os.getenv("YOUR_OUTPUT_PRICE_PER_M", "0.23"))
YOUR_CACHED_INPUT_PRICE_PER_M = float(os.getenv("YOUR_CACHED_INPUT_PRICE_PER_M", "0.23"))

# Blended upstream cost fallback ($/M) when the response omits cost.request_cost_usd
# (streamed responses never include a cost field). $0.10/M is a conservative
# blended estimate across glm/kimi/qwen. compute_cost() prefers the upstream's
# reported cost when present, so this only fills in for streamed/edge cases.
BLENDED_COST_PER_M = float(os.getenv("BLENDED_COST_PER_M", "0.10"))

# ── KEY GENERATION ───────────────────────────────────────────────────────────
KEY_PREFIX = "sk-luv13-"
KEY_RANDOM_LEN = 32  # chars after the prefix
KEY_RANDOM_ALPHABET = string.ascii_letters + string.digits
MAX_KEYS_PER_CUSTOMER = int(os.getenv("MAX_KEYS_PER_CUSTOMER", "5"))

# ── DB ───────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "data", "luv13.db"))

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# ── FLASK APP ───────────────────────────────────────────────────────────────
app = Flask(__name__)
# OpenAI-compatible clients (Cursor, etc.) call this from browsers/Electron; allow
# all origins so the CORS preflight passes and "Failed to fetch" goes away.
CORS(app, supports_credentials=False,
     expose_headers=["X-Served-Account", "X-Served-Index"])


# ── DB HELPERS ──────────────────────────────────────────────────────────────
def get_db() -> sqlite3.Connection:
    """Per-request SQLite connection. WAL mode + busy_timeout for concurrency."""
    if "db" not in g:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA foreign_keys=ON")
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    """Create tables if missing. Safe to call on every boot."""
    db = get_db()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            email         TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at    TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS customers (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS api_keys (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            key_hash           TEXT UNIQUE NOT NULL,
            key_prefix         TEXT NOT NULL,
            customer_id        INTEGER NOT NULL REFERENCES customers(id),
            upstream_key_index INTEGER NOT NULL,  -- 1..4
            created_at         TEXT NOT NULL,
            active             INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys(key_hash);
        CREATE INDEX IF NOT EXISTS idx_api_keys_customer ON api_keys(customer_id);
        CREATE TABLE IF NOT EXISTS usage (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key_id          INTEGER NOT NULL REFERENCES api_keys(id),
            timestamp           TEXT NOT NULL,
            input_tokens        INTEGER NOT NULL DEFAULT 0,
            output_tokens       INTEGER NOT NULL DEFAULT 0,
            cached_input_tokens INTEGER NOT NULL DEFAULT 0,
            cost_usd            REAL    NOT NULL DEFAULT 0,
            revenue_usd         REAL    NOT NULL DEFAULT 0,
            served_upstream_index INTEGER  -- which account ACTUALLY served (after failover)
        );
        CREATE INDEX IF NOT EXISTS idx_usage_api_key ON usage(api_key_id);
        CREATE INDEX IF NOT EXISTS idx_usage_ts ON usage(timestamp);
        CREATE INDEX IF NOT EXISTS idx_usage_served ON usage(served_upstream_index);
        """
    )
    # Idempotent migration: add served_upstream_index to pre-existing usage tables.
    cols = {r["name"] for r in db.execute("PRAGMA table_info(usage)")}
    if "served_upstream_index" not in cols:
        db.execute("ALTER TABLE usage ADD COLUMN served_upstream_index INTEGER")
        log.info("migrated: added usage.served_upstream_index")
    # Events log: cooldown starts/recoveries, errors, retries. Bounded by
    # _prune_events() so a stress test can't grow this unbounded.
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS events (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp           TEXT NOT NULL,
            upstream_key_index  INTEGER,
            account_name        TEXT,
            event_type          TEXT NOT NULL,
            http_status         INTEGER,
            message             TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts ON events(timestamp);
        CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
        CREATE INDEX IF NOT EXISTS idx_events_acct ON events(upstream_key_index);
        """
    )
    log.info("database ready at %s", DB_PATH)


def _prune_events(db, max_rows: int = 5000) -> None:
    """Cap the events table so a long stress test can't grow it unbounded."""
    cur = db.execute(
        "DELETE FROM events WHERE id NOT IN "
        "(SELECT id FROM events ORDER BY id DESC LIMIT ?)",
        (max_rows,),
    )
    del_count = cur.rowcount
    if del_count > 0:
        log.info("pruned %d old event rows", del_count)


# ── AUTH HELPERS ─────────────────────────────────────────────────────────────
def hash_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


def key_prefix_for(plaintext: str) -> str:
    """Last 4 chars for display, e.g. sk-luv13-...ab12"""
    return f"{KEY_PREFIX}...{plaintext[-4:]}"


def generate_random_key_suffix() -> str:
    return "".join(secrets.choice(KEY_RANDOM_ALPHABET) for _ in range(KEY_RANDOM_LEN))


def require_admin(f):
    """Admin auth via constant-time comparison.

    Accepts EITHER:
      - X-Admin-Token header (for API/poller clients), OR
      - luv13_admin session cookie (for browsers, set by /admin/login)
    Returns 401 if neither/invalid.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        token = request.headers.get("X-Admin-Token", "").encode("utf-8")
        if token and ADMIN_TOKEN and hmac.compare_digest(token, ADMIN_TOKEN):
            return f(*args, **kwargs)
        if _check_admin_cookie():
            return f(*args, **kwargs)
        # Browser request: redirect to login instead of bare JSON 401
        if _is_browser_request():
            return redirect("/admin/login", code=302)
        return jsonify({"error": "unauthorized"}), 401
    return wrapper


def _is_browser_request() -> bool:
    """Heuristic: is the caller a browser (HTML) rather than an API client?"""
    accept = request.headers.get("Accept", "").lower()
    return "text/html" in accept and "application/json" not in accept


def _admin_session_secret() -> bytes:
    """Secret used to sign admin session cookies: prefer JWT_SECRET, fall back
    to ADMIN_TOKEN so the browser login works even when only ADMIN_TOKEN is set."""
    return (JWT_SECRET or ADMIN_TOKEN)


def _check_admin_cookie() -> bool:
    """Verify the luv13_admin session cookie. Returns True if valid."""
    cookie = request.cookies.get(ADMIN_SESSION_COOKIE, "")
    secret = _admin_session_secret()
    if not cookie or not secret:
        return False
    try:
        payload = pyjwt.decode(
            cookie, secret, algorithms=[JWT_ALG],
            options={"require": ["exp", "sub"]},
        )
    except Exception:
        return False
    return payload.get("sub") == "admin"


def _make_admin_session() -> str:
    """Sign a short-lived admin session JWT."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": "admin",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=ADMIN_SESSION_TTL_DAYS)).timestamp()),
    }
    return pyjwt.encode(payload, _admin_session_secret(), algorithm=JWT_ALG)


ADMIN_LOGIN_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>luv13 Admin Login</title>
<style>
  :root { color-scheme: light dark; }
  * { box-sizing: border-box; }
  body {
    margin: 0; min-height: 100vh; display: flex; align-items: center;
    justify-content: center; font-family: -apple-system, BlinkMacSystemFont,
      "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    background: #0b1020; color: #e6e9f0;
  }
  .card {
    width: 100%; max-width: 360px; padding: 32px 28px; border-radius: 14px;
    background: #131a2e; box-shadow: 0 10px 40px rgba(0,0,0,.5);
    border: 1px solid #23304d;
  }
  h1 { font-size: 20px; margin: 0 0 6px; font-weight: 600; }
  p.sub { margin: 0 0 22px; font-size: 13px; color: #8a93a6; }
  label { display: block; font-size: 13px; margin: 0 0 6px; color: #aeb4c4; }
  input[type=text], input[type=password] {
    width: 100%; padding: 11px 12px; border-radius: 8px; border: 1px solid #2a3650;
    background: #0b1020; color: #e6e9f0; font-size: 14px; margin-bottom: 16px;
  }
  input[type=text]:focus, input[type=password]:focus {
    outline: none; border-color: #4a7cff; box-shadow: 0 0 0 3px rgba(74,124,255,.2);
  }
  button {
    width: 100%; padding: 11px; border: 0; border-radius: 8px; cursor: pointer;
    background: #4a7cff; color: #fff; font-size: 14px; font-weight: 600;
  }
  button:hover { background: #3d6ae0; }
  .err { color: #ff6b6b; font-size: 13px; margin: 0 0 14px; min-height: 18px; }
</style>
</head>
<body>
<form class="card" method="POST" action="/admin/login">
  <h1>luv13 Admin</h1>
  <p class="sub">Sign in to access the dashboard</p>
  <div class="err">{{ error }}</div>
  <label for="u">Username</label>
  <input id="u" name="username" type="text" autocomplete="username" autofocus required>
  <label for="p">Password</label>
  <input id="p" name="password" type="password" autocomplete="current-password" required>
  <button type="submit">Sign in</button>
</form>
</body>
</html>"""


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    """Browser login for /admin/*. Sets a signed session cookie on success."""
    if request.method == "GET":
        return Response(ADMIN_LOGIN_PAGE.replace("{{ error }}", ""),
                        content_type="text/html")

    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""

    ok_user = bool(ADMIN_USERNAME) and hmac.compare_digest(
        username.encode("utf-8"), ADMIN_USERNAME.encode("utf-8"))
    ok_pass = bool(ADMIN_PASSWORD) and hmac.compare_digest(
        password.encode("utf-8"), ADMIN_PASSWORD.encode("utf-8"))

    if not (ok_user and ok_pass):
        resp = Response(
            ADMIN_LOGIN_PAGE.replace("{{ error }}", "Invalid username or password"),
            content_type="text/html", status=401,
        )
        return resp

    token = _make_admin_session()
    resp = make_response(redirect("/admin/summary", code=302))
    resp.set_cookie(
        ADMIN_SESSION_COOKIE, token,
        max_age=ADMIN_SESSION_TTL_DAYS * 86400,
        httponly=True, secure=request.is_secure, samesite="Lax",
    )
    return resp


@app.route("/admin/logout", methods=["POST", "GET"])
def admin_logout():
    """Clear the admin session cookie."""
    resp = make_response(redirect("/admin/login", code=302))
    resp.delete_cookie(ADMIN_SESSION_COOKIE)
    return resp


def openai_error(message: str, etype: str = "server_error",
                 status: int | None = None, retry_after: int | None = None) -> dict:
    """Build an OpenAI-schema-shaped error dict.

    VS Code and Cursor validate every response against OpenAI's API schema,
    which requires `error` to be an OBJECT (not a string) on the error branch
    of the union. Emiting `{"error": "<string>"}` makes the client reject the
    whole response as "Type validation failed". Wrap the message+type in an
    object so the error branch validates cleanly.
    """
    err = {"message": message, "type": etype}
    if status is not None:
        err["code"] = status
    if retry_after is not None:
        err["retry_after"] = retry_after
    return {"error": err}


def decode_jwt(token: str) -> dict:
    """Verify JWT signature + expiry. Raises jwt.* on failure."""
    return pyjwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])


def resolve_branded_key() -> tuple:
    """Resolve the customer's sk-luv13-... key from the Authorization header.

    Returns (api_key_row, customer_row) on success, or (None, error_response)
    on failure where error_response is a (dict, status) tuple.
    """
    auth = request.headers.get("Authorization", "")
    plaintext = auth.replace("Bearer ", "").strip()
    if not plaintext.startswith(KEY_PREFIX):
        return None, (openai_error("unauthorized", "invalid_auth", 401), 401)
    kh = hash_key(plaintext)
    db = get_db()
    row = db.execute(
        "SELECT * FROM api_keys WHERE key_hash = ? AND active = 1", (kh,)
    ).fetchone()
    if row is None:
        return None, (openai_error("unauthorized", "invalid_auth", 401), 401)
    cust = db.execute("SELECT * FROM customers WHERE id = ?", (row["customer_id"],)).fetchone()
    if cust is None:
        return None, (openai_error("unauthorized", "invalid_auth", 401), 401)
    return (row, cust), None


def require_branded_key(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        res, err = resolve_branded_key()
        if err is not None:
            return err
        g.api_key_row, g.customer_row = res
        return f(*args, **kwargs)
    return wrapper


# ── ROUND-ROBIN ASSIGNMENT ───────────────────────────────────────────────────
# This is the core mechanism tying customer keys back to the upstream pool.
# For each new customer key we count how many keys are already assigned to each
# upstream_key_index across ALL customers, then pick the index with the lowest
# count. Ties broken by lowest index. This balances load across the pool so no
# single upstream key approaches Neuralwatt's rate limit before the others.
def pick_upstream_key_index(db: sqlite3.Connection) -> int:
    counts = {i: 0 for i in range(1, NUM_UPSTREAM_KEYS + 1)}
    for row in db.execute(
        "SELECT upstream_key_index, COUNT(*) AS c FROM api_keys GROUP BY upstream_key_index"
    ):
        counts[row["upstream_key_index"]] = row["c"]
    # Lowest count; ties → lowest index (min over (count, index) tuple)
    return min(range(1, NUM_UPSTREAM_KEYS + 1), key=lambda i: (counts[i], i))


# ── USAGE TRACKING ──────────────────────────────────────────────────────────
def compute_revenue(input_tokens: int, output_tokens: int, cached_tokens: int) -> float:
    """Customer billing. Flat-rate: $0.23/M on every billable token.

    `input_tokens` is the OpenAI spec's `prompt_tokens`, which ALREADY INCLUDES
    `cached_tokens` as a subset (cached is a component of prompt, not an
    addition). So total billable = prompt_tokens + completion_tokens. Cached
    tokens have no separate price (they're billed at the same flat $0.23/M as
    part of the prompt token count). Earlier versions added `cached_tokens` on
    top of `input_tokens`, which double-counted them and produced ~2x revenue
    on cache-heavy workloads (Cursor) — and the aggregates landed below cost.
    """
    return (
        (input_tokens + output_tokens) / 1_000_000 * YOUR_INPUT_PRICE_PER_M
    )


def compute_cost(prompt_tokens: int, completion_tokens: int, neuralwatt_cost) -> float:
    """Upstream cost. Prefer the upstream's reported cost when present
    (cost.request_cost_usd); fall back to a blended $0.10/M on the total token
    count for streamed responses (which never include a cost field)."""
    if neuralwatt_cost is not None:
        try:
            return float(neuralwatt_cost)
        except (TypeError, ValueError):
            pass
    return (prompt_tokens + completion_tokens) / 1_000_000 * BLENDED_COST_PER_M


def record_usage(api_key_id: int, prompt_tokens: int, completion_tokens: int,
                 cached_tokens: int, neuralwatt_cost, served_upstream_index=None) -> None:
    revenue = compute_revenue(prompt_tokens, completion_tokens, cached_tokens)
    cost = compute_cost(prompt_tokens, completion_tokens, neuralwatt_cost)
    db = get_db()
    db.execute(
        """INSERT INTO usage
           (api_key_id, timestamp, input_tokens, output_tokens,
            cached_input_tokens, cost_usd, revenue_usd, served_upstream_index)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            api_key_id,
            datetime.now(timezone.utc).isoformat(),
            prompt_tokens or 0,
            completion_tokens or 0,
            cached_tokens or 0,
            cost,
            revenue,
            served_upstream_index,
        ),
    )


def _extract_usage_from_sse_text(text: str, state: dict) -> None:
    """Best-effort parse of `usage` from streamed SSE text without altering bytes.

    `state` carries a partial trailing line buffer and running token counts.
    NOTE: Neuralwatt's streamed usage chunk omits cost; cost falls back to blended.
    """
    state["buf"] += text
    while "\n" in state["buf"]:
        line, state["buf"] = state["buf"].split("\n", 1)
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]":
            continue
        try:
            data = json.loads(payload)
        except Exception:
            continue
        usage = data.get("usage")
        if isinstance(usage, dict):
            state["prompt_tokens"] = usage.get("prompt_tokens", state["prompt_tokens"])
            state["completion_tokens"] = usage.get(
                "completion_tokens", state["completion_tokens"]
            )
            details = usage.get("prompt_tokens_details") or {}
            if "cached_tokens" in details:
                state["cached_tokens"] = details.get(
                    "cached_tokens", state["cached_tokens"]
                )


# ── ROUTES: PUBLIC API ──────────────────────────────────────────────────────
@app.route("/v1/models", methods=["GET"])
def list_models():
    """Return luv13-branded model list to Cursor."""
    models = [
        {"id": slug, "object": "model", "created": 1700000000, "owned_by": "luv13"}
        for slug in MODEL_MAP.keys()
    ]
    return jsonify({"object": "list", "data": models})


def _client_disconnected():
    """Best-effort detection of whether the downstream client (Cursor, curl,
    etc.) has closed the connection mid-stream.

    Flask's streaming generator runs inside the request context, so we can
    inspect the WSGI environ for the underlying socket. If the socket's file
    descriptor has been closed (recv returns b''), the client is gone.

    Works across werkzeug dev server (WSGIRequestHandler) and gunicorn/eventlet
    workers — each puts the raw socket somewhere slightly different; we check
    all known locations. Returns False if we can't tell (prefer false negative
    over killing a healthy stream).
    """
    env = request.environ
    sock = env.get("werkzeug.socket")
    if sock is None:
        obj = env.get("eventlet.input") or env.get("gunicorn.sock")
        sock = getattr(obj, "sock", None) or getattr(obj, "get_socket", lambda: None)()
    if sock is None:
        return False
    try:
        fd = sock.fileno()
    except (ValueError, OSError):
        return True
    import select as _select
    try:
        r, _, _ = _select.select([fd], [], [], 0)
        if not r:
            return False
        return sock.recv(1, 0x40) == b""  # MSG_DONTWAIT = 0x40 on Linux
    except (BlockingIOError, InterruptedError):
        return False
    except (OSError, ValueError):
        return True


@app.route("/v1/chat/completions", methods=["POST"])
def chat_completions():
    res, err = resolve_branded_key()
    if err is not None:
        return err
    api_key_row, _customer_row = res
    # Ring order (active-standby by default; round-robin if configured).
    order = ring_order(api_key_row["upstream_key_index"])

    body = request.get_json(silent=True)
    if not body:
        return jsonify(openai_error("Invalid JSON body", "invalid_request_error", 400)), 400

    requested_model = body.get("model", "glm-5.2")
    body["model"] = MODEL_MAP.get(requested_model, requested_model)

    is_streaming = bool(body.get("stream", False))
    if is_streaming:
        opts = body.get("stream_options") or {}
        opts.setdefault("include_usage", True)
        body["stream_options"] = opts

    try:
        if is_streaming:
            DONE_BYTES = b"data: [DONE]\n\n"

            def generate():
                served_idx = None
                done_sent = False
                state = {"buf": "", "prompt_tokens": 0, "completion_tokens": 0,
                         "cached_tokens": 0}
                upstream_gen = stream_upstream(order, body)
                try:
                    for event in upstream_gen:
                        if _client_disconnected():
                            log.info("client disconnected mid-stream (account idx %s); "
                                     "stopping generator", served_idx)
                            return
                        match event:
                            case ChunkEvent(data=data):
                                # If the upstream already sent [DONE], remember so we
                                # don't duplicate it after the done event.
                                if b"[DONE]" in data:
                                    done_sent = True
                                yield data
                            case HeartbeatEvent():
                                yield b": keepalive\n\n"
                            case AccountEvent(idx=idx):
                                served_idx = idx
                            case DoneEvent(state=state):
                                record_usage(
                                    api_key_row["id"],
                                    state["prompt_tokens"],
                                    state["completion_tokens"],
                                    state["cached_tokens"],
                                    neuralwatt_cost=None,
                                    served_upstream_index=served_idx,
                                )
                                if not done_sent:
                                    done_sent = True
                                    yield DONE_BYTES
                            case ErrorEvent(message=message):
                                # VS Code / Cursor validate every response against the
                                # OpenAI schema union: success requires `choices: []`,
                                # error requires `error: {message, type, ...}`. Emiting
                                # `{"error": "<string>"}` (string) makes zod reject the
                                # whole response as "Type validation failed". Use the
                                # shared openai_error() helper so the shape stays
                                # consistent with the non-streaming paths.
                                retry_after = (int(max(1, round(event.retry_after)))
                                               if event.retry_after is not None else None)
                                err_payload = openai_error(
                                    message, "server_error", 503, retry_after=retry_after,
                                )
                                yield ("data: " + json.dumps(err_payload) + "\n\n").encode()
                                if not done_sent:
                                    done_sent = True
                                    yield DONE_BYTES
                            case _:
                                # Exhaustiveness guard: any new StreamEvent
                                # subclass that isn't handled above lands here.
                                # Production behavior: log + fail closed (don't
                                # forward an unknown event silently).
                                log.error("unhandled StreamEvent from upstream_gen: %r",
                                          type(event).__name__)
                                if not done_sent:
                                    done_sent = True
                                    yield ("data: " + json.dumps(
                                        openai_error("internal: unhandled stream event",
                                                     "server_error", 500)) + "\n\n").encode()
                                    yield DONE_BYTES
                except GeneratorExit:
                    # Client closed the connection (Flask translates that into
                    # GeneratorExit raised inside the generator). Close the
                    # upstream generator explicitly so its `with resp:` block
                    # runs resp.close() promptly instead of waiting for GC.
                    log.info("GeneratorExit on chat stream (account idx %s) — "
                             "client gone", served_idx)
                finally:
                    upstream_gen.close()

            return Response(
                stream_with_context(generate()),
                content_type="text/event-stream",
                headers={
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                    "Cache-Control": "no-cache",
                },
            )

        # ── NON-STREAMING ──────────────────────────────────────────────────
        # Ring failover + cooldown.
        resp, served_idx = post_upstream(order, body, stream=False)
        if resp is None:
            # Whole ring cooling and queue wait elapsed — emit 503 + Retry-After
            # so the client knows when the soonest account recovers. Error must
            # be an OBJECT (OpenAI schema) so VS Code / Cursor don't reject the
            # response as "Type validation failed" (string-vs-object union fail).
            retry_after = int(max(1, round(min(
                (_cooldown_remaining(i) for i in order if UPSTREAM_KEYS[i - 1]),
                default=ACCOUNT_COOLDOWN,
            ))))
            response = jsonify(openai_error(
                "all upstream accounts unavailable", "server_error",
                503, retry_after=retry_after,
            ))
            response.headers["Retry-After"] = str(retry_after)
            return response, 503
        try:
            data = resp.json()
        except ValueError:
            return Response(
                resp.content,
                status=resp.status_code,
                content_type=resp.headers.get("content-type", "text/plain"),
            )

        usage = data.get("usage", {}) or {}
        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        cached_tokens = (usage.get("prompt_tokens_details") or {}).get("cached_tokens", 0)

        neuralwatt_cost = None
        cost_data = data.get("cost", {}) or {}
        if cost_data:
            neuralwatt_cost = cost_data.get("request_cost_usd")

        record_usage(
            api_key_row["id"],
            prompt_tokens,
            completion_tokens,
            cached_tokens,
            neuralwatt_cost,
            served_upstream_index=served_idx,
        )
        return jsonify(data), resp.status_code, {
            "X-Served-Account": account_name(served_idx),
            "X-Served-Index": str(served_idx),
        }

    except requests.exceptions.ConnectTimeout:
        return jsonify(openai_error("Could not connect to Neuralwatt (connect timeout)",
                                    "server_error", 504)), 504
    except Exception as e:
        log.exception("proxy error")
        return jsonify(openai_error(str(e), "server_error", 500)), 500


# ── ROUTES: KEY GENERATION (JWT auth — called by luv13 website) ─────────────
@app.route("/keys/generate", methods=["POST"])
def generate_key():
    auth = request.headers.get("Authorization", "")
    token = auth.replace("Bearer ", "").strip()
    if not token:
        return jsonify({"error": "unauthorized"}), 401
    try:
        payload = decode_jwt(token)
    except Exception:
        return jsonify({"error": "unauthorized"}), 401

    token_email = payload.get("email", "").lower()

    body = request.get_json(silent=True) or {}
    email = (body.get("email") or "").strip().lower()
    if not email or not EMAIL_RE.match(email):
        return jsonify({"error": "invalid email"}), 400
    # The body email must match the JWT payload's email — don't let a logged-in
    # user mint keys for a different account.
    if email != token_email:
        return jsonify({"error": "email does not match session"}), 403

    db = get_db()
    # Create or look up the customer by email.
    cust = db.execute("SELECT * FROM customers WHERE email = ?", (email,)).fetchone()
    if cust is None:
        cur = db.execute(
            "INSERT INTO customers (email, created_at) VALUES (?, ?)",
            (email, datetime.now(timezone.utc).isoformat()),
        )
        customer_id = cur.lastrowid
    else:
        customer_id = cust["id"]

    # Abuse control: max N keys per customer.
    existing = db.execute(
        "SELECT COUNT(*) AS c FROM api_keys WHERE customer_id = ?", (customer_id,)
    ).fetchone()["c"]
    if existing >= MAX_KEYS_PER_CUSTOMER:
        return jsonify({
            "error": f"max {MAX_KEYS_PER_CUSTOMER} keys per customer reached"
        }), 403

    # Round-robin assignment to one of the 5 upstream keys (see helper comment).
    upstream_idx = pick_upstream_key_index(db)

    plaintext = KEY_PREFIX + generate_random_key_suffix()
    db.execute(
        """INSERT INTO api_keys
           (key_hash, key_prefix, customer_id, upstream_key_index, created_at, active)
           VALUES (?, ?, ?, ?, ?, 1)""",
        (
            hash_key(plaintext),
            key_prefix_for(plaintext),
            customer_id,
            upstream_idx,
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    log.info(
        "generated key for customer_id=%s upstream_idx=%d prefix=%s",
        customer_id, upstream_idx, key_prefix_for(plaintext),
    )
    # Plaintext returned ONCE. Never retrievable again — only the hash is stored.
    return jsonify({
        "key": plaintext,
        "customer_id": customer_id,
        "key_prefix": key_prefix_for(plaintext),
        "upstream_key_index": upstream_idx,
    })


# ── ROUTES: CUSTOMER USAGE (branded-key auth) ───────────────────────────────
@app.route("/usage", methods=["GET"])
@require_branded_key
def customer_usage():
    api_key_row = g.api_key_row
    db = get_db()

    agg = db.execute(
        """SELECT
               COUNT(*)              AS request_count,
               COALESCE(SUM(input_tokens), 0)        AS total_input,
               COALESCE(SUM(output_tokens), 0)       AS total_output,
               COALESCE(SUM(cached_input_tokens), 0) AS total_cached,
               COALESCE(SUM(revenue_usd), 0)        AS total_revenue
           FROM usage WHERE api_key_id = ?""",
        (api_key_row["id"],)
    ).fetchone()

    total_input = agg["total_input"]
    total_cached = agg["total_cached"]
    cache_rate = (total_cached / total_input * 100) if total_input > 0 else 0

    # Daily breakdown for last 30 days.
    since = (datetime.now(timezone.utc) - timedelta(days=30)).date()
    daily = []
    for row in db.execute(
        """SELECT
               DATE(timestamp)                  AS date,
               COUNT(*)                         AS requests,
               COALESCE(SUM(input_tokens), 0)   AS input,
               COALESCE(SUM(output_tokens), 0)  AS output,
               COALESCE(SUM(cached_input_tokens), 0) AS cached,
               COALESCE(SUM(revenue_usd), 0)    AS revenue_usd
           FROM usage
           WHERE api_key_id = ? AND DATE(timestamp) >= ?
           GROUP BY DATE(timestamp)
           ORDER BY DATE(timestamp) DESC""",
        (api_key_row["id"], since.isoformat())
    ):
        daily.append({
            "date": row["date"],
            "input": row["input"],
            "output": row["output"],
            "cached": row["cached"],
            "requests": row["requests"],
            "revenue_usd": round(row["revenue_usd"], 6),
        })

    # NOTE: never expose cost_usd, upstream_key_index, or any other customer's
    # data on this endpoint — customer-facing means customer-safe.
    return jsonify({
        "total_input_tokens": total_input,
        "total_output_tokens": agg["total_output"],
        "total_cached_tokens": total_cached,
        "cache_rate_pct": round(cache_rate, 2),
        "total_revenue_usd": round(agg["total_revenue"], 6),
        "request_count": agg["request_count"],
        "daily": daily,
    })


def _admin_summary_data(db: sqlite3.Connection) -> dict:
    """Compute the full admin summary. Shared by the HTML dashboard and the
    JSON endpoint so they can never diverge."""
    totals = db.execute(
        """SELECT
               (SELECT COUNT(*) FROM customers) AS total_customers,
               COUNT(u.id)                      AS total_requests,
               COALESCE(SUM(u.input_tokens + u.output_tokens), 0) AS total_tokens,
               COALESCE(SUM(u.cost_usd), 0)    AS total_cost,
               COALESCE(SUM(u.revenue_usd), 0) AS total_revenue
           FROM usage u"""
    ).fetchone()
    total_revenue = totals["total_revenue"] or 0
    total_cost = totals["total_cost"] or 0

    customers = []
    for row in db.execute(
        """SELECT
               c.id,
               c.email,
               COUNT(DISTINCT k.id)                 AS key_count,
               COUNT(u.id)                         AS requests,
               COALESCE(SUM(u.input_tokens), 0)    AS input_tokens,
               COALESCE(SUM(u.output_tokens), 0)   AS output_tokens,
               COALESCE(SUM(u.cached_input_tokens), 0) AS cached_tokens,
               COALESCE(SUM(u.cost_usd), 0)        AS cost_usd,
               COALESCE(SUM(u.revenue_usd), 0)     AS revenue_usd
           FROM customers c
           LEFT JOIN api_keys k ON k.customer_id = c.id
           LEFT JOIN usage u    ON u.api_key_id = k.id
           GROUP BY c.id
           ORDER BY revenue_usd DESC"""
    ):
        rev = row["revenue_usd"] or 0
        cst = row["cost_usd"] or 0
        customers.append({
            "customer_id": row["id"],
            "email": row["email"],
            "key_count": row["key_count"],
            "requests": row["requests"],
            "input_tokens": row["input_tokens"],
            "output_tokens": row["output_tokens"],
            "cached_tokens": row["cached_tokens"],
            "cost_usd": round(cst, 6),
            "revenue_usd": round(rev, 6),
            "profit_usd": round(rev - cst, 6),
        })

    upstream = []
    for i in range(1, NUM_UPSTREAM_KEYS + 1):
        assigned = db.execute(
            """SELECT COUNT(DISTINCT id) AS keys_assigned,
                      COUNT(DISTINCT customer_id) AS customers_assigned
               FROM api_keys WHERE upstream_key_index = ?""",
            (i,)
        ).fetchone()
        served = db.execute(
            """SELECT COUNT(*) AS requests,
                      COALESCE(SUM(input_tokens + output_tokens), 0) AS total_tokens,
                      COALESCE(SUM(cost_usd), 0) AS total_cost
               FROM usage WHERE served_upstream_index = ?""",
            (i,)
        ).fetchone()
        # Per-account event counts (429s, cooldowns, errors) for the test
        # harness. "last_event_ts" is the most recent event for this account.
        evstats = db.execute(
            """SELECT
                   COUNT(*) AS total_events,
                   SUM(CASE WHEN event_type = 'cooldown_start' THEN 1 ELSE 0 END) AS cooldowns,
                   SUM(CASE WHEN event_type = 'error_429' THEN 1 ELSE 0 END) AS err_429,
                   SUM(CASE WHEN event_type LIKE 'error_%' THEN 1 ELSE 0 END) AS errors,
                   MAX(timestamp) AS last_event_ts
               FROM events WHERE upstream_key_index = ?""",
            (i,)
        ).fetchone()
        entry = {
            "upstream_key_index": i,
            "account_name": account_name(i),
            "customers_assigned": assigned["customers_assigned"],
            "keys_assigned": assigned["keys_assigned"],
            "served_requests": served["requests"],
            "served_tokens": served["total_tokens"],
            "served_cost_usd": round(served["total_cost"] or 0, 6),
            "cooling_down_s": round(_cooldown_remaining(i), 1),
            "is_standby": (UPSTREAM_STRATEGY == "active-standby"
                           and _STANDBY_IDX == i),
            "cooldown_count": evstats["cooldowns"] or 0,
            "error_429_count": evstats["err_429"] or 0,
            "error_count": evstats["errors"] or 0,
            "last_event_ts": evstats["last_event_ts"],
        }
        if UPSTREAM_STRATEGY == "active-reserve":
            ar_active, ar_reserve = _active_reserve_state()
            if i in ar_active:
                entry["pool_role"] = "active"
            elif _cooldown_remaining(i) > 0:
                entry["pool_role"] = "cooling"
            else:
                entry["pool_role"] = "reserve"
        elif UPSTREAM_STRATEGY == "active-standby":
            entry["pool_role"] = (
                "standby" if _STANDBY_IDX == i else "active"
            )
        else:
            entry["pool_role"] = "active"
        upstream.append(entry)

    # Recent error/cooldown events — the "error logs" the test harness needs
    # to correlate 429 timing, cooldown start, and recovery per account.
    recent_events = []
    for row in db.execute(
        """SELECT timestamp, upstream_key_index, account_name,
                  event_type, http_status, message
           FROM events
           ORDER BY id DESC LIMIT 200"""
    ):
        recent_events.append({
            "timestamp": row["timestamp"],
            "upstream_key_index": row["upstream_key_index"],
            "account_name": row["account_name"],
            "event_type": row["event_type"],
            "http_status": row["http_status"],
            "message": row["message"],
        })

    # Recent requests — "requests per timestamp for each API." Each row is one
    # completed request with its account, tokens, and timestamp. Capped so the
    # dashboard payload stays light; the test harness can query /usage for full.
    recent_requests = []
    for row in db.execute(
        """SELECT u.timestamp, u.served_upstream_index,
                  u.input_tokens, u.output_tokens, u.cached_input_tokens,
                  u.cost_usd, u.revenue_usd, k.key_prefix, c.email
           FROM usage u
           LEFT JOIN api_keys k ON k.id = u.api_key_id
           LEFT JOIN customers c ON c.id = k.customer_id
           ORDER BY u.id DESC LIMIT 200"""
    ):
        recent_requests.append({
            "timestamp": row["timestamp"],
            "upstream_key_index": row["served_upstream_index"],
            "account_name": (account_name(row["served_upstream_index"])
                             if row["served_upstream_index"] else None),
            "input_tokens": row["input_tokens"],
            "output_tokens": row["output_tokens"],
            "cached_tokens": row["cached_input_tokens"],
            "total_tokens": (row["input_tokens"] or 0) + (row["output_tokens"] or 0),
            "cost_usd": round(row["cost_usd"] or 0, 6),
            "revenue_usd": round(row["revenue_usd"] or 0, 6),
            "key_prefix": row["key_prefix"],
            "email": row["email"],
        })

    return {
        "strategy": UPSTREAM_STRATEGY,
        "pricing": {
            "input_price_per_m": YOUR_INPUT_PRICE_PER_M,
            "output_price_per_m": YOUR_OUTPUT_PRICE_PER_M,
            "cached_input_price_per_m": YOUR_CACHED_INPUT_PRICE_PER_M,
            "blended_cost_per_m": BLENDED_COST_PER_M,
        },
        "total_customers": totals["total_customers"],
        "total_requests": totals["total_requests"],
        "total_tokens": totals["total_tokens"],
        "total_revenue_usd": round(total_revenue, 6),
        "total_cost_usd": round(total_cost, 6),
        "total_profit_usd": round(total_revenue - total_cost, 6),
        "gross_margin_pct": round(
            ((total_revenue - total_cost) / total_revenue * 100)
            if total_revenue > 0 else 0, 2),
        "per_customer": customers,
        "per_upstream_key": upstream,
        "recent_events": recent_events,
        "recent_requests": recent_requests,
    }


ADMIN_SUMMARY_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>luv13 Admin Summary</title>
<style>
  :root {
    color-scheme: dark;
    --bg: #0b1020;
    --surface: #131a2e;
    --surface-2: #1a2238;
    --border: #23304d;
    --text: #e6e9f0;
    --text-dim: #8a93a6;
    --text-faint: #6b7390;
    --accent: #4a7cff;
    --accent-dim: rgba(74,124,255,.18);
    --positive: #34d399;
    --positive-dim: rgba(52,211,153,.14);
    --negative: #f87171;
    --warning: #fbbf24;
    --cooling: #f59e0b;
    --cool: #60a5fa;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; min-height: 100vh;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
      Helvetica, Arial, sans-serif;
    background: var(--bg); color: var(--text);
    line-height: 1.5; font-size: 14px;
    -webkit-font-smoothing: antialiased;
  }
  .num, .mono { font-variant-numeric: tabular-nums; }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }

  .wrap { max-width: 1180px; margin: 0 auto; padding: 28px 20px 64px; }

  header.top {
    display: flex; align-items: center; justify-content: space-between;
    gap: 16px; flex-wrap: wrap; margin-bottom: 28px;
  }
  header.top h1 { margin: 0; font-size: 22px; font-weight: 600; letter-spacing: -.01em; }
  header.top .meta { font-size: 13px; color: var(--text-dim); display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
  header.top .meta form { margin: 0; }
  header.top .meta button {
    background: var(--surface-2); border: 1px solid var(--border); color: var(--text-dim);
    padding: 6px 12px; border-radius: 7px; font-size: 12px; cursor: pointer;
    min-height: 32px; transition: background .15s, color .15s;
  }
  header.top .meta button:hover { background: var(--border); color: var(--text); }
  header.top .meta .dot {
    width: 8px; height: 8px; border-radius: 50%; background: var(--positive);
    box-shadow: 0 0 0 3px var(--positive-dim);
  }

  /* Overview tiles */
  .overview {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
    gap: 12px; margin-bottom: 32px;
  }
  .tile {
    background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
    padding: 16px 18px;
  }
  .tile .label { font-size: 11px; color: var(--text-dim); text-transform: uppercase;
    letter-spacing: .04em; margin-bottom: 6px; }
  .tile .value { font-size: 22px; font-weight: 600; letter-spacing: -.01em; }
  .tile .sub { font-size: 12px; color: var(--text-faint); margin-top: 2px; }
  .tile.pos .value { color: var(--positive); }

  /* Section headings */
  .section { margin-bottom: 32px; }
  .section h2 {
    margin: 0 0 14px; font-size: 15px; font-weight: 600; color: var(--text);
    display: flex; align-items: center; gap: 10px;
  }
  .section h2 .strat {
    font-size: 11px; font-weight: 500; color: var(--accent);
    background: var(--accent-dim); padding: 3px 8px; border-radius: 999px;
    text-transform: uppercase; letter-spacing: .03em;
  }
  .section h2 .count {
    font-size: 12px; color: var(--text-dim); font-weight: 400;
  }

  /* Upstream account cards */
  .accounts {
    display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 12px;
  }
  .acc {
    background: var(--surface); border: 1px solid var(--border); border-radius: 12px;
    padding: 16px; position: relative; overflow: hidden;
    transition: border-color .15s, transform .15s;
  }
  .acc:hover { border-color: var(--accent); }
  .acc::before {
    content: ""; position: absolute; left: 0; top: 0; bottom: 0; width: 4px;
    background: var(--hue, var(--accent));
  }
  .acc .head {
    display: flex; align-items: center; justify-content: space-between;
    gap: 10px; margin-bottom: 12px; padding-left: 8px;
  }
  .acc .name { font-weight: 600; font-size: 15px; display: flex; align-items: center; gap: 8px; }
  .acc .idx { font-size: 11px; color: var(--text-faint); }
  .pill {
    display: inline-flex; align-items: center; gap: 6px;
    font-size: 11px; font-weight: 500; padding: 3px 9px; border-radius: 999px;
    letter-spacing: .02em; text-transform: capitalize; white-space: nowrap;
  }
  .pill svg { width: 12px; height: 12px; }
  .pill.active { color: var(--positive); background: var(--positive-dim); }
  .pill.standby, .pill.reserve { color: var(--cool); background: rgba(96,165,250,.14); }
  .pill.cooling { color: var(--cooling); background: rgba(245,158,11,.14); }

  .acc .cooldown {
    font-size: 12px; color: var(--cooling); margin-left: 8px; padding-left: 8px;
    border-left: 1px solid var(--border); display: inline-flex; align-items: center; gap: 5px;
  }

  .stats { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; padding-left: 8px; }
  .stat .k { font-size: 11px; color: var(--text-dim); text-transform: uppercase; letter-spacing: .03em; }
  .stat .v { font-size: 15px; font-weight: 500; }

  /* Bar inside card showing relative load */
  .loadbar { height: 4px; background: var(--surface-2); border-radius: 2px; overflow: hidden; margin: 10px 0 0 8px; }
  .loadbar > span {
    display: block; height: 100%; background: var(--hue, var(--accent));
    transition: width .3s ease; border-radius: 2px;
  }

  /* Customers table */
  .tablewrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 12px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  thead th {
    text-align: left; padding: 11px 14px; color: var(--text-dim); font-weight: 500;
    font-size: 11px; text-transform: uppercase; letter-spacing: .04em;
    background: var(--surface); border-bottom: 1px solid var(--border); white-space: nowrap;
  }
  tbody td { padding: 12px 14px; border-bottom: 1px solid var(--border); white-space: nowrap; }
  tbody tr:last-child td { border-bottom: 0; }
  tbody tr:hover { background: var(--surface); }
  tbody td.email { color: var(--text); }
  tbody td.profit.pos { color: var(--positive); }
  tbody td.profit.neg { color: var(--negative); }
  tbody td.ts { color: var(--text-dim); font-size: 12px; }
  tbody td.acct-cell { font-weight: 500; }
  tbody td.acct-cell .swatch {
    display: inline-block; width: 8px; height: 8px; border-radius: 2px;
    margin-right: 6px; vertical-align: middle;
  }
  td.action { text-align: center; }
  .del-btn {
    display: inline-flex; align-items: center; justify-content: center; gap: 4px;
    background: transparent; border: 1px solid transparent; color: var(--negative);
    padding: 5px 8px; border-radius: 7px; cursor: pointer; font-size: 12px;
    transition: background .15s, border-color .15s, color .15s;
  }
  .del-btn:hover { background: rgba(248,113,113,.14); border-color: rgba(248,113,113,.35); }
  .del-btn svg { width: 14px; height: 14px; }
  .del-btn.confirm {
    background: var(--negative); color: #fff; border-color: var(--negative);
    font-weight: 500;
  }
  .del-btn.confirm:hover { background: #ef4444; }

  /* Event type badges in the activity log */
  .ev {
    display: inline-flex; align-items: center; gap: 5px;
    font-size: 11px; font-weight: 500; padding: 2px 8px; border-radius: 999px;
    text-transform: lowercase; letter-spacing: .02em; white-space: nowrap;
  }
  .ev.cooldown_start { color: var(--cooling); background: rgba(245,158,11,.14); }
  .ev.cooldown_recover { color: var(--positive); background: var(--positive-dim); }
  .ev[class*="error_"] { color: var(--negative); background: rgba(248,113,113,.14); }
  .ev.info { color: var(--cool); background: rgba(96,165,250,.14); }
  .ev.warning { color: var(--warning); background: rgba(251,191,36,.14); }

  .empty { padding: 32px; text-align: center; color: var(--text-dim); font-size: 13px; }

  footer { margin-top: 36px; color: var(--text-faint); font-size: 12px; text-align: center; }

  @media (max-width: 540px) {
    .stats { grid-template-columns: 1fr; }
    .acc .head { flex-wrap: wrap; }
  }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after { transition: none !important; animation: none !important; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header class="top">
    <h1>luv13 Admin Summary</h1>
    <div class="meta">
      <span class="dot" aria-hidden="true"></span>
      <span id="fresh">loading…</span>
      <form action="/admin/logout" method="get">
        <button type="submit">Sign out</button>
      </form>
    </div>
  </header>

  <div class="overview" id="overview"></div>

  <div class="section">
    <h2>Upstream Accounts <span class="strat" id="strat">—</span> <span class="count" id="acc-count"></span></h2>
    <div class="accounts" id="accounts"><div class="empty">loading…</div></div>
  </div>

  <div class="section">
    <h2>Customers <span class="count" id="cust-count"></span></h2>
    <div class="tablewrap">
      <table id="cust-table">
        <thead>
          <tr>
            <th scope="col">Email</th>
            <th scope="col">Keys</th>
            <th scope="col">Requests</th>
            <th scope="col">Input tok</th>
            <th scope="col">Output tok</th>
            <th scope="col">Cached</th>
            <th scope="col">Cost</th>
            <th scope="col">Revenue</th>
            <th scope="col">Profit</th>
            <th scope="col">Remove</th>
          </tr>
        </thead>
        <tbody id="cust-tbody"><tr><td colspan="10" class="empty">loading…</td></tr></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Recent Requests <span class="count" id="req-count"></span></h2>
    <div class="tablewrap">
      <table id="req-table">
        <thead>
          <tr>
            <th scope="col">Timestamp</th>
            <th scope="col">Account</th>
            <th scope="col">Customer</th>
            <th scope="col">In tok</th>
            <th scope="col">Out tok</th>
            <th scope="col">Cached</th>
            <th scope="col">Total tok</th>
            <th scope="col">Cost</th>
            <th scope="col">Revenue</th>
            <th scope="col">Profit</th>
          </tr>
        </thead>
        <tbody id="req-tbody"><tr><td colspan="10" class="empty">loading…</td></tr></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>Activity Log <span class="count" id="evt-count"></span></h2>
    <div class="tablewrap">
      <table id="evt-table">
        <thead>
          <tr>
            <th scope="col">Timestamp</th>
            <th scope="col">Account</th>
            <th scope="col">Event</th>
            <th scope="col">HTTP</th>
            <th scope="col">Message</th>
          </tr>
        </thead>
        <tbody id="evt-tbody"><tr><td colspan="5" class="empty">loading…</td></tr></tbody>
      </table>
    </div>
  </div>

  <footer>Auto-refreshes every 15s. Cooldowns tick down live.</footer>
</div>

<noscript>
  <p style="text-align:center;color:var(--text-dim);padding:48px;">
    This dashboard needs JavaScript. Raw JSON is available at this URL via
    <code>curl -H "X-Admin-Token: …"</code>.
  </p>
</noscript>

<script id="data" type="application/json">__DATA__</script>
<script>
(function () {
  // Stable hue per account so the same name always gets the same color.
  // Gold-angle spread (~45°) gives visually distinct hues for small N.
  var HUES = [210, 160, 280, 35, 320, 95, 0, 245, 175, 50];
  function hueFor(name, idx) {
    var h = 0;
    for (var i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
    return HUES[h % HUES.length];
  }

  function fmtNum(n) { return (n || 0).toLocaleString(undefined); }
  function fmtUsd(n) { return "$" + (n || 0).toFixed(4); }
  function fmtUsd6(n) { return "$" + (n || 0).toFixed(6); }

  var ICONS = {
    active: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>',
    reserve: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>',
    standby: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>',
    cooling: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"/><circle cx="12" cy="12" r="4"/></svg>'
  };

  function el(tag, cls, html) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (html !== undefined) e.innerHTML = html;
    return e;
  }

  function renderOverview(d) {
    var ov = document.getElementById("overview");
    ov.innerHTML = "";
    var p = d.pricing || {};
    var rateStr = "$" + (p.input_price_per_m || 0).toFixed(2) + "/M";
    if ((p.cached_input_price_per_m || 0) !== (p.input_price_per_m || 0)) {
      rateStr += " · cached $" + (p.cached_input_price_per_m || 0).toFixed(2) + "/M";
    }
    var tiles = [
      { label: "Customers", value: fmtNum(d.total_customers) },
      { label: "Total Requests", value: fmtNum(d.total_requests) },
      { label: "Total Tokens", value: fmtNum(d.total_tokens) },
      { label: "Revenue", value: fmtUsd(d.total_revenue_usd), sub: "cost " + fmtUsd6(d.total_cost_usd) },
      { label: "Profit", value: fmtUsd(d.total_profit_usd), pos: true,
        sub: "margin " + (d.gross_margin_pct || 0).toFixed(1) + "%" },
      { label: "Rate (rev)", value: rateStr,
        sub: "cost $" + (p.blended_cost_per_m || 0).toFixed(2) + "/M blended" }
    ];
    for (var i = 0; i < tiles.length; i++) {
      var t = tiles[i];
      var tile = el("div", "tile" + (t.pos ? " pos" : ""));
      tile.appendChild(el("div", "label", t.label));
      tile.appendChild(el("div", "value num", t.value));
      if (t.sub) tile.appendChild(el("div", "sub", t.sub));
      ov.appendChild(tile);
    }
  }

  function renderAccounts(d) {
    var box = document.getElementById("accounts");
    document.getElementById("strat").textContent = d.strategy || "—";
    var ups = d.per_upstream_key || [];
    document.getElementById("acc-count").textContent = ups.length + " accounts";
    if (!ups.length) { box.innerHTML = '<div class="empty">No upstream accounts configured.</div>'; return; }
    var maxTok = Math.max.apply(null, ups.map(function (u) { return u.served_tokens || 0; }).concat([1]));
    box.innerHTML = "";
    for (var i = 0; i < ups.length; i++) {
      var u = ups[i];
      var role = u.pool_role || "active";
      var card = el("div", "acc");
      // Green when active; red for any inactive/standby/reserve/cooling state.
      var statusColor = (role === "active") ? "var(--positive)" : "var(--negative)";
      card.style.setProperty("--hue", statusColor);

      var head = el("div", "head");
      var nameDiv = el("div", "name");
      nameDiv.appendChild(el("span", null, u.account_name));
      nameDiv.appendChild(el("span", "idx", "#" + u.upstream_key_index));
      head.appendChild(nameDiv);

      var pillExtra = "";
      if ((u.cooling_down_s || 0) > 0) {
        pillExtra = '<span class="cooldown" data-acct="' + u.upstream_key_index + '">' +
          '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:11px;height:11px"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>' +
          '<span class="cd-num">' + u.cooling_down_s + '</span>s</span>';
      }
      head.insertAdjacentHTML("beforeend",
        '<span class="pill ' + role + '">' + (ICONS[role] || "") + role + '</span>' + pillExtra);
      card.appendChild(head);

      var stats = el("div", "stats");
      stats.appendChild(pair("Tokens served", fmtNum(u.served_tokens)));
      stats.appendChild(pair("Requests served", fmtNum(u.served_requests)));
      stats.appendChild(pair("Keys assigned", u.keys_assigned));
      stats.appendChild(pair("Customers", u.customers_assigned));
      // Error/cooldown counters highlighted for the stress test.
      if ((u.cooldown_count || 0) > 0 || (u.error_429_count || 0) > 0) {
        stats.appendChild(pair("429s seen", u.error_429_count || 0));
        stats.appendChild(pair("Cooldowns", u.cooldown_count || 0));
      }
      card.appendChild(stats);

      var pct = Math.round((u.served_tokens || 0) / maxTok * 100);
      card.insertAdjacentHTML("beforeend",
        '<div class="loadbar"><span style="width:' + pct + '%"></span></div>');
      box.appendChild(card);
    }
    // stash base cooldowns for live countdown
    window.__cdBase = {};
    window.__cdStart = Date.now();
    ups.forEach(function (u) { window.__cdBase[u.upstream_key_index] = u.cooling_down_s || 0; });
  }

  function pair(k, v) {
    var d = el("div", "stat");
    d.appendChild(el("div", "k", k));
    d.appendChild(el("div", "v num", String(v)));
    return d;
  }

  function renderCustomers(d) {
    var tb = document.getElementById("cust-tbody");
    var list = d.per_customer || [];
    document.getElementById("cust-count").textContent = list.length + " customers";
    if (!list.length) {
      tb.innerHTML = '<tr><td colspan="10" class="empty">No customers yet.</td></tr>';
      return;
    }
    tb.innerHTML = "";
    var MINUS_ICON = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M8 12h8"/></svg>';
    for (var i = 0; i < list.length; i++) {
      var c = list[i];
      var profit = c.profit_usd || 0;
      var tr = document.createElement("tr");
      tr.appendChild(el("td", "email", c.email || "—"));
      tr.appendChild(el("td", "num", String(c.key_count)));
      tr.appendChild(el("td", "num", fmtNum(c.requests)));
      tr.appendChild(el("td", "num", fmtNum(c.input_tokens)));
      tr.appendChild(el("td", "num", fmtNum(c.output_tokens)));
      tr.appendChild(el("td", "num", fmtNum(c.cached_tokens)));
      tr.appendChild(el("td", "num", fmtUsd6(c.cost_usd)));
      tr.appendChild(el("td", "num", fmtUsd6(c.revenue_usd)));
      var pc = document.createElement("td");
      pc.className = "num profit " + (profit >= 0 ? "pos" : "neg");
      pc.textContent = fmtUsd6(profit);
      tr.appendChild(pc);

      var actionTd = document.createElement("td");
      actionTd.className = "action";
      var btn = document.createElement("button");
      btn.className = "del-btn";
      btn.innerHTML = MINUS_ICON;
      btn.title = "Remove customer";
      btn.onclick = function (customerId) {
        return function (e) {
          e.stopPropagation();
          var b = e.currentTarget;
          if (b.classList.contains("confirm")) {
            b.disabled = true;
            b.textContent = "Deleting…";
            fetch("/admin/customer/" + customerId, { method: "DELETE" })
              .then(function (r) { return r.json(); })
              .then(function (res) {
                if (res.status === "deleted") {
                  window.location.reload();
                } else {
                  alert("Delete failed: " + (res.error || "unknown"));
                  b.disabled = false;
                  b.className = "del-btn";
                  b.innerHTML = MINUS_ICON;
                }
              })
              .catch(function () {
                alert("Delete failed.");
                b.disabled = false;
                b.className = "del-btn";
                b.innerHTML = MINUS_ICON;
              });
            return;
          }
          b.className = "del-btn confirm";
          b.innerHTML = "Delete?";
          b.title = "Click again to confirm deletion";
          setTimeout(function () {
            b.className = "del-btn";
            b.innerHTML = MINUS_ICON;
            b.title = "Remove customer";
          }, 4000);
        };
      }(c.customer_id);
      actionTd.appendChild(btn);
      tr.appendChild(actionTd);

      tb.appendChild(tr);
    }
  }

  function tickCooldowns() {
    if (!window.__cdBase) return;
    var elapsed = (Date.now() - window.__cdStart) / 1000;
    var nodes = document.querySelectorAll(".cooldown[data-acct]");
    for (var i = 0; i < nodes.length; i++) {
      var idx = nodes[i].getAttribute("data-acct");
      var base = window.__cdBase[idx] || 0;
      var rem = Math.max(0, base - elapsed);
      var num = nodes[i].querySelector(".cd-num");
      if (rem > 0) { if (num) num.textContent = rem.toFixed(0); }
      else { nodes[i].style.display = "none"; }
    }
  }

  function fmtTs(s) {
    if (!s) return "—";
    try {
      var d = new Date(s);
      return d.toLocaleString(undefined, {
        month: "short", day: "numeric",
        hour: "2-digit", minute: "2-digit", second: "2-digit",
        hour12: false
      });
    } catch (e) { return s; }
  }

  function accountCell(name, idx, role) {
    if (!name) return '<td class="acct-cell">—</td>';
    // Green = active, red = anything else (reserve/standby/cooling/timeout).
    var color = (role === "active") ? "var(--positive)" : "var(--negative)";
    return '<td class="acct-cell"><span class="swatch" style="background:' +
      color + '"></span>' + name + '</td>';
  }

  function renderRequests(d) {
    var tb = document.getElementById("req-tbody");
    var list = d.recent_requests || [];
    document.getElementById("req-count").textContent = list.length + " recent";
    if (!list.length) {
      tb.innerHTML = '<tr><td colspan="10" class="empty">No requests yet.</td></tr>';
      return;
    }
    tb.innerHTML = "";
    var roleMap = {};
    (d.per_upstream_key || []).forEach(function (u) {
      roleMap[u.upstream_key_index] = u.pool_role || "active";
    });
    for (var i = 0; i < list.length; i++) {
      var r = list[i];
      var role = roleMap[r.upstream_key_index] || "active";
      var tr = document.createElement("tr");
      tr.appendChild(el("td", "ts", fmtTs(r.timestamp)));
      tr.insertAdjacentHTML("beforeend",
        accountCell(r.account_name, r.upstream_key_index, role));
      tr.appendChild(el("td", null, r.email || "—"));
      tr.appendChild(el("td", "num", fmtNum(r.input_tokens)));
      tr.appendChild(el("td", "num", fmtNum(r.output_tokens)));
      tr.appendChild(el("td", "num", fmtNum(r.cached_tokens)));
      tr.appendChild(el("td", "num", fmtNum(r.total_tokens)));
      tr.appendChild(el("td", "num", fmtUsd6(r.cost_usd)));
      tr.appendChild(el("td", "num", fmtUsd6(r.revenue_usd)));
      var profit = (r.revenue_usd || 0) - (r.cost_usd || 0);
      var pc = document.createElement("td");
      pc.className = "num profit " + (profit >= 0 ? "pos" : "neg");
      pc.textContent = fmtUsd6(profit);
      tr.appendChild(pc);
      tb.appendChild(tr);
    }
  }

  function renderEvents(d) {
    var tb = document.getElementById("evt-tbody");
    var list = d.recent_events || [];
    document.getElementById("evt-count").textContent = list.length + " recent";
    if (!list.length) {
      tb.innerHTML = '<tr><td colspan="5" class="empty">No events yet.</td></tr>';
      return;
    }
    tb.innerHTML = "";
    var roleMap = {};
    (d.per_upstream_key || []).forEach(function (u) {
      roleMap[u.upstream_key_index] = u.pool_role || "active";
    });
    for (var i = 0; i < list.length; i++) {
      var e = list[i];
      var role = roleMap[e.upstream_key_index] || "active";
      var tr = document.createElement("tr");
      tr.appendChild(el("td", "ts", fmtTs(e.timestamp)));
      tr.insertAdjacentHTML("beforeend",
        accountCell(e.account_name, e.upstream_key_index, role));
      var et = e.event_type || "info";
      tr.insertAdjacentHTML("beforeend",
        '<td><span class="ev ' + et + '">' + et.replace(/_/g, " ") + "</span></td>");
      tr.appendChild(el("td", "num", e.http_status ? String(e.http_status) : "—"));
      tr.appendChild(el("td", null, e.message || ""));
      tb.appendChild(tr);
    }
  }

  function render(d) {
    renderOverview(d);
    renderAccounts(d);
    renderCustomers(d);
    renderRequests(d);
    renderEvents(d);
    document.getElementById("fresh").textContent =
      "updated " + new Date().toLocaleTimeString();
  }

  // Initial render from embedded data (no extra round-trip).
  try {
    var d = JSON.parse(document.getElementById("data").textContent);
    render(d);
  } catch (e) {
    document.getElementById("accounts").innerHTML =
      '<div class="empty">Failed to load data.</div>';
  }

  // Live cooldown countdown every second.
  setInterval(tickCooldowns, 1000);

  // Auto-refresh from server every 15s.
  setInterval(function () {
    fetch(window.location.pathname, { headers: { "Accept": "application/json" } })
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (d) { if (d) render(d); })
      .catch(function () {});
  }, 15000);
})();
</script>
</body>
</html>"""


# ── ROUTES: ADMIN ───────────────────────────────────────────────────────────
@app.route("/admin/summary", methods=["GET"])
@require_admin
def admin_summary():
    db = get_db()
    data = _admin_summary_data(db)
    # Browsers get a rendered dashboard; API clients (admin_poller.py, curl)
    # keep getting JSON. _is_browser_request already gates the /admin/login
    # redirect in require_admin, so this mirrors that contract.
    if _is_browser_request():
        return Response(
            ADMIN_SUMMARY_PAGE.replace("__DATA__", json.dumps(data)),
            content_type="text/html",
        )
    return jsonify(data)


@app.route("/admin/customer/<int:customer_id>", methods=["DELETE"])
@require_admin
def admin_delete_customer(customer_id: int):
    """Destructive: permanently DELETE a customer and all associated
    API keys plus usage rows from the database."""
    db = get_db()
    exists = db.execute(
        "SELECT 1 FROM customers WHERE id = ?", (customer_id,)
    ).fetchone()
    if not exists:
        return jsonify({"error": "customer not found"}), 404
    db.execute(
        "DELETE FROM usage WHERE api_key_id IN (SELECT id FROM api_keys WHERE customer_id = ?)",
        (customer_id,)
    )
    db.execute("DELETE FROM api_keys WHERE customer_id = ?", (customer_id,))
    db.execute("DELETE FROM customers WHERE id = ?", (customer_id,))
    log.warning("admin delete customer_id=%d", customer_id)
    return jsonify({"status": "deleted", "customer_id": customer_id})


@app.route("/admin/reset/<int:api_key_id>", methods=["POST"])
@require_admin
def admin_reset_for_key(api_key_id: int):
    """Destructive: DELETE FROM usage WHERE api_key_id = ?.
    Scoped per-key only — no full-database wipe route in prod.
    AGENTS.md CONFIRM rule applies before the human triggers this."""
    db = get_db()
    exists = db.execute(
        "SELECT 1 FROM api_keys WHERE id = ?", (api_key_id,)
    ).fetchone()
    if not exists:
        return jsonify({"error": "api_key not found"}), 404
    cur = db.execute(
        "DELETE FROM usage WHERE api_key_id = ?", (api_key_id,)
    )
    deleted = cur.rowcount
    log.warning("admin reset: deleted %d usage rows for api_key_id=%d", deleted, api_key_id)
    return jsonify({"status": "reset", "api_key_id": api_key_id, "rows_deleted": deleted})


# ── ROUTES: ADMIN RECOMPUTE (backfill revenue/cost on historical rows) ──────
@app.route("/admin/recompute-usage", methods=["POST"])
@require_admin
def admin_recompute_usage():
    """Recompute revenue_usd (and cost_usd where it's stale) on every usage row
    from the recorded token counts using the CURRENT pricing. Used to backfill
    historical rows written under an older revenue formula that double-counted
    or under-billed tokens.

    Body (JSON, all optional):
      { "apply": false }   # default = dry-run preview only, no writes
      { "apply": true }    # commit the UPDATE

    Revenue is recomputed from token counts (deterministic, always safe).
    Cost is recomputed with the blended fallback ONLY when the stored value is
    zero or clearly broken (negative / None); rows that have a real upstream
    cost are left untouched since we can't recover the original upstream number
    from token counts alone. The dry-run returns the old→new delta for revenue,
    cost, and profit at both the total and per-customer level so the operator
    can review before committing.
    """
    body = request.get_json(silent=True) or {}
    apply = bool(body.get("apply", False))

    db = get_db()

    # Per-customer totals BEFORE, from stored revenue/cost columns.
    before = db.execute(
        """SELECT k.customer_id,
                  COALESCE(SUM(u.cost_usd), 0)    AS cost,
                  COALESCE(SUM(u.revenue_usd), 0)  AS revenue
           FROM usage u
           JOIN api_keys k ON k.id = u.api_key_id
           GROUP BY k.customer_id"""
    ).fetchall()
    before_map = {r["customer_id"]: (r["cost"] or 0, r["revenue"] or 0)
                  for r in before}
    before_total_cost = sum(v[0] for v in before_map.values())
    before_total_revenue = sum(v[1] for v in before_map.values())

    # Walk every usage row, recompute both fields, and (if apply) write them.
    rows = db.execute(
        """SELECT u.id, k.customer_id,
                  u.input_tokens, u.output_tokens, u.cached_input_tokens,
                  u.cost_usd, u.revenue_usd
           FROM usage u
           JOIN api_keys k ON k.id = u.api_key_id"""
    ).fetchall()

    updates = []  # (row_id, new_rev, new_cost)
    per_customer_new = {}  # customer_id -> [new_rev, new_cost]
    for r in rows:
        new_rev = compute_revenue(
            r["input_tokens"], r["output_tokens"], r["cached_input_tokens"]
        )
        stored_cost = r["cost_usd"]
        # Only recompute cost when there's no usable upstream figure. A real
        # upstream cost is small-but-positive; a broken one is 0/negative/None.
        if stored_cost is None or stored_cost <= 0:
            new_cost = compute_cost(
                r["input_tokens"], r["output_tokens"], None
            )
        else:
            new_cost = stored_cost

        if (abs(new_rev - (r["revenue_usd"] or 0)) > 1e-9
                or abs(new_cost - (stored_cost or 0)) > 1e-9):
            updates.append((r["id"], new_rev, new_cost))

        bucket = per_customer_new.setdefault(r["customer_id"], [0.0, 0.0])
        bucket[0] += new_rev
        bucket[1] += new_cost

    if apply and updates:
        # Use individual execute() calls (not executemany) for parity with
        # record_usage()/admin_reset_for_key(), which persist reliably under
        # the connection's autocommit (isolation_level=None) config.
        for row_id, new_rev, new_cost in updates:
            db.execute(
                "UPDATE usage SET revenue_usd = ?, cost_usd = ? WHERE id = ?",
                (new_rev, new_cost, row_id),
            )
        db.commit()

    after_total_revenue = sum(v[0] for v in per_customer_new.values())
    after_total_cost = sum(v[1] for v in per_customer_new.values())

    per_customer = []
    for cust_id, (rev, cost) in sorted(per_customer_new.items()):
        old = before_map.get(cust_id, (0.0, 0.0))
        per_customer.append({
            "customer_id": cust_id,
            "revenue_before": round(old[1], 6),
            "revenue_after": round(rev, 6),
            "cost_before": round(old[0], 6),
            "cost_after": round(cost, 6),
            "profit_before": round(old[1] - old[0], 6),
            "profit_after": round(rev - cost, 6),
        })

    log.warning("admin recompute-usage: apply=%s rows_changed=%d",
                apply, len(updates))
    return jsonify({
        "applied": apply,
        "rows_changed": len(updates),
        "totals_before": {
            "revenue_usd": round(before_total_revenue, 6),
            "cost_usd": round(before_total_cost, 6),
            "profit_usd": round(before_total_revenue - before_total_cost, 6),
        },
        "totals_after": {
            "revenue_usd": round(after_total_revenue, 6),
            "cost_usd": round(after_total_cost, 6),
            "profit_usd": round(after_total_revenue - after_total_cost, 6),
        },
        "per_customer": per_customer,
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})


# ── MAIN ────────────────────────────────────────────────────────────────────
with app.app_context():
    init_db()

if __name__ == "__main__":
    print(f"""
+==========================================================+
|   luv13 Proxy Server (multi-tenant)                     |
|   Running at:  http://localhost:{PORT}                      |
|   Admin:       http://localhost:{PORT}/admin/summary      |
|   No upstream read-timeout: long generations won't chop. |
+==========================================================+

Upstream account ring (names only — keys never printed):""")
    for i in range(1, NUM_UPSTREAM_KEYS + 1):
        print(f"  [{i}] {account_name(i)}")
    print("\nModel mappings:")
    for k, v in MODEL_MAP.items():
        print(f"  {k:<35} -> {v}")
    print()
    app.run(host="0.0.0.0", port=PORT, threaded=True)
