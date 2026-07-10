"""
Part B load test for the luv13 proxy concurrency-slot router.

Drives N concurrent streaming chat completions through the proxy for a fixed
duration while sampling /admin/inflight at 1Hz, then reports:
  - steady-state tokens/min (total and output-only; first 2 min excluded)
  - average available accounts (accounts with >= 1 free concurrency slot)
  - % of samples with ZERO available accounts
  - slot-cap violations (any account ever > max_concurrency in flight)
  - queue wait stats + unexpected-429 count from the proxy's own counters
  - request success/error breakdown

Usage:
    .venv/bin/python partb_load_test.py \
        --url http://localhost:4000 --key sk-luv13-... \
        --admin-token ... --concurrency 24 --duration 900
"""
import argparse
import json
import os
import random
import threading
import time
import urllib.request

TOPICS = [
    "photosynthesis", "plate tectonics", "the water cycle", "black holes",
    "the immune system", "semiconductors", "ocean currents", "bird migration",
    "the printing press", "fermentation", "volcanoes", "the silk road",
    "honeybees", "glaciers", "radio waves", "coral reefs", "the roman aqueducts",
    "lightning", "deep sea vents", "the human ear", "antibiotics", "sailing",
    "photovoltaic cells", "earthquakes",
]

stop_event = threading.Event()
lock = threading.Lock()
completions = []   # (end_epoch, prompt_tokens, completion_tokens, duration_s, ttfb_s)
errors = []        # (epoch, kind, detail)
samples = []       # dicts from /admin/inflight + local receive time


def worker(wid: int, url: str, key: str, model: str):
    n = 0
    while not stop_event.is_set():
        n += 1
        topic = random.choice(TOPICS)
        body = json.dumps({
            "model": model,
            "stream": True,
            "messages": [{
                "role": "user",
                "content": (f"[worker {wid} run {n}] Write a clear ~250 word "
                            f"explanation of {topic} for a curious adult."),
            }],
        }).encode()
        req = urllib.request.Request(
            f"{url}/v1/chat/completions", data=body, method="POST",
            headers={"Authorization": f"Bearer {key}",
                     "Content-Type": "application/json"},
        )
        started = time.time()
        ttfb = None
        ptok = ctok = 0
        got_error = None
        try:
            with urllib.request.urlopen(req, timeout=600) as resp:
                buf = b""
                while True:
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    if ttfb is None:
                        ttfb = time.time() - started
                    buf += chunk
                    while b"\n" in buf:
                        line, buf = buf.split(b"\n", 1)
                        line = line.strip()
                        if not line.startswith(b"data:"):
                            continue
                        payload = line[5:].strip()
                        if payload == b"[DONE]":
                            continue
                        try:
                            data = json.loads(payload)
                        except ValueError:
                            continue
                        if isinstance(data, dict):
                            if data.get("error"):
                                got_error = str(data["error"])[:200]
                            usage = data.get("usage")
                            if isinstance(usage, dict):
                                ptok = usage.get("prompt_tokens", ptok)
                                ctok = usage.get("completion_tokens", ctok)
        except Exception as e:
            got_error = f"{type(e).__name__}: {e}"[:200]

        now = time.time()
        with lock:
            if got_error:
                errors.append((now, "stream_error", got_error))
            else:
                completions.append((now, ptok, ctok, now - started,
                                    ttfb if ttfb is not None else -1))


def sampler(url: str, admin_token: str, interval: float = 1.0):
    while not stop_event.is_set():
        t0 = time.time()
        try:
            req = urllib.request.Request(
                f"{url}/admin/inflight",
                headers={"X-Admin-Token": admin_token})
            with urllib.request.urlopen(req, timeout=10) as resp:
                snap = json.loads(resp.read().decode())
            snap["_local_ts"] = t0
            with lock:
                samples.append(snap)
        except Exception as e:
            with lock:
                errors.append((t0, "sampler_error", str(e)[:200]))
        stop_event.wait(max(0.0, interval - (time.time() - t0)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:4000")
    # Credentials come from env (LUV13_KEY / ADMIN_TOKEN) so they never appear
    # on the command line; flags remain as optional overrides.
    ap.add_argument("--key", default=os.getenv("LUV13_KEY", ""))
    ap.add_argument("--admin-token", default=os.getenv("ADMIN_TOKEN", ""))
    ap.add_argument("--model", default="glm-5.2")
    ap.add_argument("--concurrency", type=int, default=24)
    ap.add_argument("--duration", type=float, default=900)
    ap.add_argument("--warmup", type=float, default=120,
                    help="seconds excluded from steady-state tok/min")
    ap.add_argument("--out", default="partb_results.json")
    args = ap.parse_args()
    if not args.key or not args.admin_token:
        raise SystemExit("set LUV13_KEY and ADMIN_TOKEN env vars (or pass flags)")

    t_start = time.time()
    threads = [threading.Thread(target=sampler,
                                args=(args.url, args.admin_token), daemon=True)]
    for w in range(args.concurrency):
        threads.append(threading.Thread(
            target=worker, args=(w, args.url, args.key, args.model),
            daemon=True))
    for t in threads:
        t.start()

    # Progress line every 30s.
    while time.time() - t_start < args.duration:
        time.sleep(30)
        with lock:
            done = len(completions)
            errs = len(errors)
            tok = sum(c[1] + c[2] for c in completions)
            avail = samples[-1]["available_accounts"] if samples else "?"
            infl = samples[-1]["total_in_flight"] if samples else "?"
        el = time.time() - t_start
        print(f"[{el:6.0f}s] done={done} errors={errs} tokens={tok} "
              f"tok/min={tok / (el / 60):.0f} inflight={infl} avail={avail}",
              flush=True)

    stop_event.set()
    print("duration reached; waiting up to 120s for in-flight streams to finish",
          flush=True)
    deadline = time.time() + 120
    for t in threads[1:]:
        t.join(timeout=max(0.0, deadline - time.time()))
    t_end = time.time()

    # ── Report ───────────────────────────────────────────────────────────
    with lock:
        comps = list(completions)
        errs = list(errors)
        snaps = list(samples)

    total_p = sum(c[1] for c in comps)
    total_c = sum(c[2] for c in comps)
    wall_min = (t_end - t_start) / 60

    ss_from = t_start + args.warmup
    ss = [c for c in comps if c[0] >= ss_from]
    ss_min = max((t_end - ss_from) / 60, 1e-9)
    ss_total = sum(c[1] + c[2] for c in ss)
    ss_out = sum(c[2] for c in ss)

    n_acct = len(snaps[0]["accounts"]) if snaps else 0
    avails = [s["available_accounts"] for s in snaps]
    zero_pct = (100.0 * sum(1 for a in avails if a == 0) / len(avails)) if avails else 0
    avg_avail = (sum(avails) / len(avails)) if avails else 0

    over_cap = []
    peak_seen = {}
    for s in snaps:
        for a in s["accounts"]:
            nm = a["account_name"]
            peak_seen[nm] = max(peak_seen.get(nm, 0), a["in_flight"])
            if a["in_flight"] > a["max_concurrency"]:
                over_cap.append((s["_local_ts"], nm, a["in_flight"]))

    last_stats = snaps[-1]["stats"] if snaps else {}
    durations = sorted(c[3] for c in comps)
    ttfbs = sorted(c[4] for c in comps if c[4] >= 0)

    def pct(sorted_list, p):
        if not sorted_list:
            return 0
        return sorted_list[min(len(sorted_list) - 1,
                               int(p / 100 * len(sorted_list)))]

    report = {
        "config": {"concurrency": args.concurrency, "duration_s": args.duration,
                   "model": args.model, "accounts": n_acct,
                   "warmup_excluded_s": args.warmup},
        "wall_clock_min": round(wall_min, 2),
        "requests_completed": len(comps),
        "requests_errored": len(errs),
        "tokens": {"prompt": total_p, "completion": total_c,
                   "total": total_p + total_c},
        "tok_per_min_overall": round((total_p + total_c) / wall_min, 1),
        "tok_per_min_steady_state_total": round(ss_total / ss_min, 1),
        "tok_per_min_steady_state_output": round(ss_out / ss_min, 1),
        "availability": {
            "samples": len(avails),
            "avg_available_accounts": round(avg_avail, 2),
            "pct_time_zero_available": round(zero_pct, 1),
        },
        "slot_cap": {
            "violations": len(over_cap),
            "peak_in_flight_per_account": peak_seen,
        },
        "proxy_stats_final": last_stats,
        "latency_s": {
            "request_p50": round(pct(durations, 50), 2),
            "request_p95": round(pct(durations, 95), 2),
            "ttfb_p50": round(pct(ttfbs, 50), 2),
            "ttfb_p95": round(pct(ttfbs, 95), 2),
        },
        "error_sample": [e for e in errs[:10]],
    }
    with open(args.out, "w") as f:
        json.dump({"report": report,
                   "samples": snaps,
                   "completions": comps}, f)
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    main()
