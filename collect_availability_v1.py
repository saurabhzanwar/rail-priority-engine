"""v1 availability test for rail-priority-engine.

Question this answers: does the Apify IRCTC actor return a usable seat COUNT
for the Vande Bharat, Shatabdi and other trains on a corridor, for tomorrow
and for a week out?

One actor run = one origin, destination, date, quota and class. Each run
returns every train on that corridor, billed at $0.001 per train row.
Eight runs below, so expect roughly 8 x 10 rows = about $0.08.
Nothing is printed except counts. The token is never printed.
"""
import csv
import json
import os
import sys
from datetime import date, timedelta

import requests

TOKEN = os.environ.get("APIFY_TOKEN", "").strip()
ACTOR = "maximedupre~irctc"
URL = f"https://api.apify.com/v2/acts/{ACTOR}/run-sync-get-dataset-items"
TIMEOUT = 240
OUT = "out"
MAX_RUNS = 10

# (origin, destination, label). Edit freely. Codes are official station codes.
CORRIDORS = [
    ("NDLS", "LKO", "Delhi-Lucknow"),
    ("MAS", "MYS", "Chennai-Mysuru"),
    ("NDLS", "CDG", "Delhi-Chandigarh"),
    ("BCT", "ADI", "Mumbai-Ahmedabad"),
]
LEADS = [1, 7]       # days ahead of today
TRAVEL_CLASS = "CC"  # chair car. If the actor rejects CC, change to 3A to test
QUOTA = "GN"

runs = 0
rows = []
summary = []


def run_actor(origin, dest, day):
    global runs
    if runs >= MAX_RUNS:
        print("STOP: run cap reached")
        sys.exit(1)
    runs += 1
    body = {
        "origin": origin,
        "destination": dest,
        "journeyDate": day,
        "quota": QUOTA,
        "travelClass": TRAVEL_CLASS,
    }
    print(f"RUN {runs}: {origin}-{dest} {day} {QUOTA} {TRAVEL_CLASS}", flush=True)
    try:
        r = requests.post(
            URL, params={"token": TOKEN}, json=body, timeout=TIMEOUT
        )
    except requests.RequestException as e:
        print(f"  network error: {type(e).__name__}", flush=True)
        return None
    print(f"  status {r.status_code}", flush=True)
    if r.status_code in (401, 403):
        print("STOP: token rejected. Check the APIFY_TOKEN secret.")
        sys.exit(1)
    if r.status_code >= 400:
        return None
    try:
        return r.json()
    except ValueError:
        return None


def main():
    if not TOKEN:
        print("STOP: APIFY_TOKEN is empty. Add it as a repository secret.")
        sys.exit(1)
    os.makedirs(OUT, exist_ok=True)
    today = date.today()
    saved_raw = False

    for origin, dest, label in CORRIDORS:
        for lead in LEADS:
            day = (today + timedelta(days=lead)).isoformat()
            items = run_actor(origin, dest, day)
            if not isinstance(items, list):
                summary.append((label, lead, 0, 0, 0))
                continue
            if items and not saved_raw:
                with open(f"{OUT}/raw_first_run.json", "w") as f:
                    json.dump(items[:3], f, indent=1)
                saved_raw = True
            n_status = n_seats = n_berths = 0
            for it in items:
                tr = it.get("train") or {}
                av = it.get("availability") or {}
                n_status += bool(av.get("status"))
                n_seats += av.get("seats") is not None
                n_berths += av.get("berths") is not None
                rows.append({
                    "corridor": label, "journey_date": day, "lead_days": lead,
                    "train_number": tr.get("number"), "train_name": tr.get("name"),
                    "train_type": tr.get("type"), "class": TRAVEL_CLASS, "quota": QUOTA,
                    "avail_status": av.get("status"), "avail_seats": av.get("seats"),
                    "avail_berths": av.get("berths"),
                })
            summary.append((label, lead, len(items), n_status, n_seats + n_berths))

    if rows:
        with open(f"{OUT}/availability_sample.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    print("\ncorridor | lead | trains | with status | with a count")
    for s in summary:
        print(" | ".join(str(x) for x in s))
    total = sum(s[2] for s in summary)
    with_status = sum(s[3] for s in summary)
    with_count = sum(s[4] for s in summary)
    ok_rows = total > 0
    ok_status = total > 0 and with_status / total >= 0.8
    ok_count = total > 0 and with_count / total >= 0.5
    print(f"\nPASS A rows returned: {ok_rows} ({total})")
    print(f"PASS B status text on >=80% of rows: {ok_status}")
    print(f"PASS C numeric seat/berth count on >=50% of rows: {ok_count}")
    print(f"Runs used: {runs}. Approx cost: ${total * 0.001:.3f}")
    with open(f"{OUT}/availability_report.json", "w") as f:
        json.dump({"runs": runs, "rows": total, "with_status": with_status,
                   "with_count": with_count}, f, indent=1)
    if not (ok_rows and ok_status):
        sys.exit(2)


if __name__ == "__main__":
    main()
