"""
Live poller for /admin/summary on the luv13 proxy.

Usage:
    python admin_poller.py                  # reads ADMIN_TOKEN from .env
    python admin_poller.py --url https://api.luv13.com
    python admin_poller.py --interval 1.0

Prints a baseline on startup, then only the deltas of the per-upstream
counters (served_requests, served_tokens, cooling_down_s) and per-customer
request counts every tick. Also flags when counters frozen for several
ticks (Cursor retry pile-up vs single stalled stream).

Ctrl+C to stop.
"""
import argparse
import hmac
import json
import os
import re
import sys
import time
import urllib.request


DEFAULT_URL = "https://api.luv13.com"


def load_token(env_path: str) -> str:
    if not os.path.exists(env_path):
        return ""
    with open(env_path, "r", encoding="utf-8") as f:
        for line in f:
            m = re.match(r"^ADMIN_TOKEN=(.*)$", line.strip())
            if m:
                return m.group(1).strip().strip('"').strip("'")
    return ""


def fetch_summary(base_url: str, token: str, timeout: float = 15.0) -> dict:
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/admin/summary",
        headers={"X-Admin-Token": token},
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fmt_delta(name: str, old: float, new: float, unit: str = "") -> str:
    d = new - old
    if d == 0:
        return ""
    sign = "+" if d > 0 else ""
    return f"{name}: {new}{unit} ({sign}{d}{unit})"


def print_state(state: dict, prefix: str = "") -> None:
    ts = time.strftime("%H:%M:%S")
    line = f"[{ts}] {prefix}"
    ups = state.get("per_upstream_key", [])
    custs = state.get("per_customer", [])
    parts = []
    for u in ups:
        parts.append(
            f"{u['account_name']} req={u['served_requests']} "
            f"cool={u['cooling_down_s']}s"
        )
    line += " | ".join(parts)
    me = [c for c in custs if c["email"] == "jgranda693@gmail.com"]
    if me:
        line += " | you: req=" + str(me[0]["requests"])
    print(line, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_URL)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument(
        "--env",
        default=os.path.join(os.path.dirname(__file__), ".env"),
        help="path to .env file containing ADMIN_TOKEN",
    )
    ap.add_argument("--frozen-threshold", type=int, default=5,
                    help="ticks with no delta before flagging frozen")
    args = ap.parse_args()

    token = os.getenv("ADMIN_TOKEN", "") or load_token(args.env)
    if not token:
        print("ERROR: no ADMIN_TOKEN found in env or .env", file=sys.stderr)
        return 2

    print(f"polling {args.url}/admin/summary every {args.interval}s "
          f"(Ctrl+C to stop)\n", flush=True)

    try:
        prev = fetch_summary(args.url, token)
    except Exception as e:
        print(f"initial fetch failed: {e}", file=sys.stderr)
        return 1

    print_state(prev, prefix="baseline")
    print("-" * 60, flush=True)

    frozen_ticks = 0
    try:
        while True:
            time.sleep(args.interval)
            try:
                cur = fetch_summary(args.url, token)
            except Exception as e:
                print(f"[{time.strftime('%H:%M:%S')}] fetch error: {e}",
                      flush=True)
                continue

            # Build deltas
            prev_ups = {u["upstream_key_index"]: u
                        for u in prev.get("per_upstream_key", [])}
            cur_ups = {u["upstream_key_index"]: u
                       for u in cur.get("per_upstream_key", [])}
            deltas = []
            any_change = False
            for idx, cu in cur_ups.items():
                pu = prev_ups.get(idx, cu)
                d_req = cu["served_requests"] - pu["served_requests"]
                d_tok = cu["served_tokens"] - pu["served_tokens"]
                d_cool = cu["cooling_down_s"] - pu["cooling_down_s"]
                if d_req or d_tok or d_cool:
                    any_change = True
                    deltas.append(
                        f"{cu['account_name']}: "
                        f"req +{d_req} (tot {cu['served_requests']}), "
                        f"tok +{d_tok}, cool {cu['cooling_down_s']}s"
                    )

            prev_custs = {c["customer_id"]: c
                          for c in prev.get("per_customer", [])}
            me_idx = next((i for i, c in prev_custs.items()
                           if c["email"] == "jgranda693@gmail.com"), None)
            if me_idx is not None:
                cur_me = next((c for c in cur.get("per_customer", [])
                              if c["customer_id"] == me_idx), None)
                if cur_me:
                    d = cur_me["requests"] - prev_custs[me_idx]["requests"]
                    if d:
                        any_change = True
                        deltas.append(f"YOU jgranda693: req +{d} "
                                      f"(tot {cur_me['requests']})")

            ts = time.strftime("%H:%M:%S")
            if deltas:
                frozen_ticks = 0
                for d in deltas:
                    print(f"[{ts}] DELTA  {d}", flush=True)
            else:
                frozen_ticks += 1
                tag = ""
                if frozen_ticks >= args.frozen_threshold:
                    tag = "  <<< FROZEN (proxy not advancing; "
                    tag += "Cursor hang is client-side or stalled stream)"
                print(f"[{ts}] (no change, tick {frozen_ticks}){tag}",
                      flush=True)

            prev = cur
    except KeyboardInterrupt:
        print("\nstopped.", flush=True)
        return 0


if __name__ == "__main__":
    sys.exit(main())
