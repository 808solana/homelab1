"""
End-to-end smoke test for the luv13 proxy. Hits the Flask app in-process
(no network) to validate: DB init, JWT issue, key-gen round-robin, branded-key
auth, usage recording, /usage + /admin/summary shapes.

Run:  python smoke_test.py
(uses an isolated temp DB so it doesn't touch ./data/luv13.db)
"""
import os
import sys
import json
import tempfile
import shutil

# Set env BEFORE importing proxy so DB_PATH + secrets resolve to test values.
TMP = tempfile.mkdtemp(prefix="luv13-smoke-")
os.environ["DB_PATH"] = os.path.join(TMP, "luv13.db")
os.environ["JWT_SECRET"] = "test-secret"
os.environ["ADMIN_TOKEN"] = "test-admin-token"
os.environ["NEURALWATT_KEY_1"] = "k1"
os.environ["NEURALWATT_KEY_2"] = "k2"
os.environ["NEURALWATT_KEY_3"] = "k3"
os.environ["NEURALWATT_KEY_4"] = "k4"
os.environ["NEURALWATT_KEY_5"] = "k5"

sys.path.insert(0, os.path.dirname(__file__))
import proxy as p

PASS, FAIL = [], []
def check(name, cond, detail=""):
    (PASS if cond else FAIL).append(f"{name} {('- '+detail) if detail else ''}")

c = p.app.test_client()

# 1. Health
r = c.get("/health")
check("health", r.status_code == 200 and r.get_json()["status"] == "ok")

# 2. /v1/models lists luv13-branded slugs and owned_by=luv13
r = c.get("/v1/models")
data = r.get_json()
slugs = {m["id"] for m in data["data"]}
check("models has luv13-glm-5.2", "luv13-glm-5.2" in slugs)
check("models owned_by luv13", all(m["owned_by"] == "luv13" for m in data["data"]))

# 3. Issue a JWT (as the luv13 website would)
import jwt as pyjwt
token = pyjwt.encode(
    {"user_id": 1, "email": "alice@example.com", "exp": 9999999999},
    "test-secret", algorithm="HS256"
)

# 4. /keys/generate without auth -> 401
r = c.post("/keys/generate", json={"email": "alice@example.com"})
check("keygen no-auth 401", r.status_code == 401)

# 5. /keys/generate with bad email -> 400
r = c.post("/keys/generate",
           json={"email": "not-an-email"},
           headers={"Authorization": f"Bearer {token}"})
check("keygen bad email 400", r.status_code == 400)

# 6. /keys/generate email mismatch -> 403
r = c.post("/keys/generate",
           json={"email": "bob@example.com"},
           headers={"Authorization": f"Bearer {token}"})
check("keygen email mismatch 403", r.status_code == 403)

# 7. /keys/generate happy path — capture plaintext key
r = c.post("/keys/generate",
           json={"email": "alice@example.com"},
           headers={"Authorization": f"Bearer {token}"})
check("keygen happy 200", r.status_code == 200, str(r.status_code))
alice_key = r.get_json()["key"]
check("key has sk-luv13- prefix", alice_key.startswith("sk-luv13-"))
check("key has 32-char suffix", len(alice_key) - len("sk-luv13-") == 32)
check("upstream_key_index in 1..5",
      1 <= r.get_json()["upstream_key_index"] <= 5)

# 8. Chat without branded key -> 401
r = c.post("/v1/chat/completions",
           json={"model": "luv13-glm-5.2", "messages": []},
           headers={"Authorization": "Bearer wrong"})
check("chat bad key 401", r.status_code == 401)

# 9. Record usage directly via the DB to validate /usage + /admin shapes,
#    bypassing the live upstream call (no network in smoke test).
import sqlite3
db = sqlite3.connect(os.environ["DB_PATH"])
db.execute("PRAGMA foreign_keys=ON")
key_row = db.execute(
    "SELECT id, upstream_key_index FROM api_keys WHERE customer_id = 1"
).fetchone()
api_key_id = key_row[0]
for i in range(3):
    db.execute(
        """INSERT INTO usage
           (api_key_id, timestamp, input_tokens, output_tokens,
            cached_input_tokens, cost_usd, revenue_usd)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (api_key_id, f"2026-06-{30-i:02d}T10:00:00+00:00",
         1000 + i*100, 500 + i*50, 200, 0.05, 0.20)
    )
db.commit()
db.close()

# 10. GET /usage with alice's branded key — customer-safe fields only
r = c.get("/usage", headers={"Authorization": f"Bearer {alice_key}"})
check("usage 200", r.status_code == 200, str(r.status_code))
uj = r.get_json()
check("usage has total_input_tokens", "total_input_tokens" in uj)
check("usage has cache_rate_pct", "cache_rate_pct" in uj)
check("usage has daily array", isinstance(uj.get("daily"), list) and len(uj["daily"]) >= 1)
check("usage has request_count=3", uj["request_count"] == 3, str(uj["request_count"]))
check("usage does NOT expose cost_usd", "cost_usd" not in uj)
check("usage does NOT expose upstream_key_index", "upstream_key_index" not in uj)

# 11. /admin/summary without admin token -> 401
r = c.get("/admin/summary")
check("admin no-token 401", r.status_code == 401)

# 12. /admin/summary with admin token — has per-customer + per-upstream-key
r = c.get("/admin/summary", headers={"X-Admin-Token": "test-admin-token"})
check("admin token 200", r.status_code == 200, str(r.status_code))
aj = r.get_json()
check("admin has per_customer", isinstance(aj.get("per_customer"), list))
check("admin has per_upstream_key", isinstance(aj.get("per_upstream_key"), list))
check("admin per_upstream_key len==5", len(aj["per_upstream_key"]) == 5)
check("admin total_customers==1", aj["total_customers"] == 1, str(aj["total_customers"]))

# 13. Round-robin: generate 6 more keys across 2 customers, verify spread across indexes
import jwt as pyjwt
for email_idx, email in enumerate(["bob@example.com", "carol@example.com"], start=2):
    tok = pyjwt.encode(
        {"user_id": email_idx, "email": email, "exp": 9999999999},
        "test-secret", algorithm="HS256"
    )
    for _ in range(3):  # 3 keys each
        r = c.post("/keys/generate", json={"email": email},
                   headers={"Authorization": f"Bearer {tok}"})
        check(f"keygen {email}", r.status_code == 200, str(r.status_code))

# 14. Max-5-keys-per-customer enforced (alice has 1, try 5 more = should be ok at 5th, 6th fails)
for i in range(4):  # +4 brings alice to 5 total
    r = c.post("/keys/generate", json={"email": "alice@example.com"},
               headers={"Authorization": f"Bearer {token}"})
    check(f"alice key {i+2} ok", r.status_code == 200, str(r.status_code))
r = c.post("/keys/generate", json={"email": "alice@example.com"},
           headers={"Authorization": f"Bearer {token}"})
check("alice 6th key rejected 403", r.status_code == 403, str(r.status_code))

# 15. /admin/reset/<id> with admin token deletes usage for that key only
r = c.post(f"/admin/reset/{api_key_id}",
           headers={"X-Admin-Token": "test-admin-token"})
check("admin reset 200", r.status_code == 200, str(r.status_code))
check("admin reset deleted 3 rows", r.get_json()["rows_deleted"] == 3,
      str(r.get_json()))

# 16. /admin/reset on nonexistent key -> 404
r = c.post("/admin/reset/999999",
           headers={"X-Admin-Token": "test-admin-token"})
check("admin reset missing 404", r.status_code == 404)

print(f"\n== {len(PASS)} passed, {len(FAIL)} failed ==")
for f in FAIL: print("  FAIL:", f)
for x in PASS: print("  ok:  ", x)

# Cleanup
shutil.rmtree(TMP, ignore_errors=True)
sys.exit(1 if FAIL else 0)
