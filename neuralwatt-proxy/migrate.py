"""
One-time migration: legacy in-memory usage_tracker → SQLite.

Background
----------
Before the multi-tenant rewrite, proxy.py kept per-API-key usage in an in-memory
dict called `usage_tracker` (keyed by the raw API-key string the customer sent).
That structure had no customer email, no timestamps, and no link to the new
JWT/email-based accounts — so those entries cannot be tied to a real user.

Per the build guide, we SKIP these entries (they were internal load testing, not
real customers) and log how many were skipped for operator visibility. Production
usage tracking starts clean at zero for real customers going forward.

Usage
-----
    python migrate.py            # dry-run (default) — prints what would happen
    python migrate.py --apply    # actually write to the DB (no-op for skipped entries)

This script never inserts legacy rows. `--apply` is a no-op kept for symmetry
with the documented migration contract, and so the operator can run it on the
box as a recorded step of the rollout.
"""

import argparse
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def load_legacy_tracker():
    """Best-effort load of the legacy in-memory usage_tracker.

    The current proxy.py no longer defines it, so this returns {} in practice.
    Kept here so the script is honest about what it's looking for.
    """
    try:
        spec = importlib.util.spec_from_file_location("_legacy_proxy", HERE / "proxy.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return getattr(mod, "usage_tracker", {})
    except Exception:
        return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="actually write to DB (no-op for skipped entries)")
    args = ap.parse_args()

    tracker = load_legacy_tracker()
    print(f"== migrate: legacy usage_tracker has {len(tracker)} entries ==")

    if not tracker:
        print("== nothing to migrate — production usage tracking starts clean ==")
        return 0

    print("== SKIPPING all entries (no customer email / timestamp; legacy load-testing data) ==")
    for k, v in tracker.items():
        # Mask the key — never print full API keys, even legacy ones.
        masked = (k[:8] + "..." + k[-4:]) if len(k) > 12 else "(short key)"
        print(f"   skipped: key={masked}  requests={v.get('total_requests', 0)}")

    print(f"== migration complete: 0 rows inserted, {len(tracker)} entries skipped ==")
    if not args.apply:
        print("== (dry-run — re-run with --apply to record this step on the box) ==")
    else:
        print("== --apply: no DB writes performed (per migration contract) ==")
    return 0


if __name__ == "__main__":
    sys.exit(main())
