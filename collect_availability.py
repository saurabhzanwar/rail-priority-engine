"""v2 availability collector for rail-priority-engine.

What it does, in order:
  1. Builds a de-duplicated list of queries (corridor x lead time).
  2. Runs them 4 at a time against the Apify IRCTC actor. One query returns
     every train on that origin-destination pair, billed per train row.
  3. Parses each status text into a number: open seats or waitlist depth.
  4. Removes duplicate rows the actor returns.
  5. Segment probe: when a train is waitlisted end to end, checks one shorter
     leg (origin to second-last halt) to see if seats exist on that leg.
  6. Writes CSVs and a report. Never prints the token or any raw data.

No-repeat-work rules built in:
  * each (origin, destination, date, class, quota) is queried once per run
  * duplicate rows are dropped before anything is counted
  * failed queries are listed, never silently retried (a retry could bill twice)
  * a row cap stops the run if the cost would run away
"""
import csv
import json
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

TOKEN = os.environ.get("APIFY_TOKEN", "").strip()
ACTOR = "maximedupre~irctc"
URL = f"https://api.apify.com/v2/acts/{ACTOR}/run-sync-get-dataset-items"
IST = ZoneInfo("Asia/Kolkata")
TIMEOUT = 280                       # Apify's sync limit is 300s
OUT = "out"
LEADS = [int(x) for x in os.environ.get("LEADS", "1,7").split(",") if x.strip()]
WORKERS = int(os.environ.get("WORKERS", "4"))
MAX_ROWS = int(os.environ.get("MAX_ROWS", "600"))   # about $0.60 at $0.001/row
QUOTA = "GN"

# (origin, destination, label, class, via)
# via = second-last halt, used only for the segment probe. None = no probe.
# Station codes are from my knowledge and NOT verified. Zero-row corridors are
# listed in the report so wrong codes show up cheaply.
CORRIDORS = [
    ("NDLS", "CDG", "Delhi-Chandigarh", "CC", None),
    ("NDLS", "LKO", "Delhi-Lucknow", "CC", None),
    ("NDLS", "CNB", "Delhi-Kanpur", "CC", None),
    ("NDLS", "RKMP", "Delhi-Bhopal", "CC", "BPL"),
    ("NDLS", "JP", "Delhi-Jaipur", "CC", None),
    ("NDLS", "DDN", "Delhi-Dehradun", "CC", None),
    ("NDLS", "ASR", "Delhi-Amritsar", "CC", None),
    ("NDLS", "BSB", "Delhi-Varanasi", "CC", None),
    ("MAS", "MYS", "Chennai-Mysuru", "CC", "SBC"),
    ("MAS", "SBC", "Chennai-Bengaluru", "CC", None),
    ("MAS", "CBE", "Chennai-Coimbatore", "CC", None),
    ("MMCT", "ADI", "Mumbai-Ahmedabad", "CC", None),
    ("HWH", "NJP", "Howrah-Siliguri", "CC", None),
    ("SC", "VSKP", "Secunderabad-Visakhapatnam", "CC", None),
    ("NDLS", "BSB", "Delhi-Varanasi SLEEPER", "3A", None),
    ("CSMT", "SMVB", "Mumbai-Bengaluru SLEEPER", "3A", None),
]

lock = threading.Lock()
stop = threading.Event()
state = {"rows_billed": 0}


# ---------- parsing ----------
RE_AVL = re.compile(r"^AVAILABLE\s*-?\s*(\d+)")
RE_CURR = re.compile(r"^CURR_AVBL\s*-?\s*(\d+)")
RE_RAC = re.compile(r"^[A-Z]{0,3}RAC\s*-?\s*(\d+)")
RE_WL2 = re.compile(r"^([A-Z]{0,4}?)WL\s*(\d+)\s*/\s*WL\s*(\d+)")
RE_WL1 = re.compile(r"^([A-Z]{0,4}?)WL\s*-?\s*(\d+)")


def parse_status(raw):
    """Returns (kind, open_seats, wl_prefix, wl_booking_pos, wl_current_pos)."""
    s = (raw or "").strip().upper()
    m = RE_AVL.match(s)
    if m:
        return "AVAILABLE", int(m.group(1)), "", None, None
    m = RE_CURR.match(s)
    if m:
        return "CURRENT", int(m.group(1)), "", None, None
    m = RE_RAC.match(s)
    if m:
        return "RAC", None, "", int(m.group(1)), None
    m = RE_WL2.match(s)
    if m:
        return "WL", None, m.group(1), int(m.group(2)), int(m.group(3))
    m = RE_WL1.match(s)
    if m:
        return "WL", None, m.group(1), int(m.group(2)), None
    if s == "NOT AVAILABLE":
        return "NOT_AVAILABLE", None, "", None, None
    return "OTHER", None, "", None, None


def classify(name):
    """Class from the train name. The actor's own type code is SHB for all of
    these, so it cannot be used. Spellings vary (SHATABADI, SHTBDI, JANSHATABDI)."""
    n = re.sub(r"[^A-Z]", "", str(name).upper())
    if "VANDEBHARAT" in n:
        return "VB_SLEEPER" if "SLEEPER" in n else "VANDE_BHARAT"
    if "JANSHAT" in n or "JANSHTB" in n or "JANSHAB" in n:
        return "JAN_SHATABDI"
    if any(t in n for t in ("SHATABDI", "SHATABADI", "SHTBDI", "SHTABDI", "SHATBDI")):
        return "SHATABDI"
    return "OTHER"


# ---------- querying ----------
def run_query(q):
    """One billed query. Returns (query, outcome, items). Never retries."""
    if stop.is_set():
        return q, "SKIPPED", None
    body = {
        "origin": q["origin"], "destination": q["dest"], "journeyDate": q["date"],
        "quota": QUOTA, "travelClass": q["cls"],
    }
    try:
        r = requests.post(URL, params={"token": TOKEN}, json=body, timeout=TIMEOUT)
    except requests.RequestException as e:
        return q, f"NETERR {type(e).__name__}", None
    if r.status_code in (401, 402, 403, 429):
        stop.set()
        return q, f"STOPPING HTTP {r.status_code}", None
    if r.status_code >= 400:
        return q, f"HTTP {r.status_code}", None
    try:
        items = r.json()
    except ValueError:
        return q, "BADJSON", None
    if not isinstance(items, list):
        return q, "BADSHAPE", None
    with lock:
        state["rows_billed"] += len(items)
        if state["rows_billed"] >= MAX_ROWS:
            stop.set()
    return q, "OK", items


def make_query(origin, dest, label, cls, day, lead, pair_type):
    return {"origin": origin, "dest": dest, "label": label, "cls": cls,
            "date": day, "lead": lead, "pair_type": pair_type}


def qkey(q):
    return (q["origin"], q["dest"], q["date"], q["cls"], QUOTA)


def to_row(q, it, snap_ts):
    tr = it.get("train") or {}
    av = it.get("availability") or {}
    rt = it.get("route") or {}
    fare = it.get("fare") or {}
    dyn = next((c.get("amount") for c in (fare.get("components") or [])
                if c.get("name") == "dynamicFare"), None)
    status = str(av.get("status") or "").strip()
    kind, seats, pfx, wl_b, wl_n = parse_status(status)
    return {
        "snapshot_ts": snap_ts, "snapshot_date": snap_ts[:10],
        "corridor": q["label"], "pair_type": q["pair_type"],
        "origin": q["origin"], "dest": q["dest"],
        "journey_date": q["date"], "lead_days": q["lead"],
        "class": q["cls"], "quota": QUOTA,
        "train_number": str(tr.get("number") or ""),
        "train_name": tr.get("name"), "train_class": classify(tr.get("name") or ""),
        "actor_type": tr.get("type"),
        "status_raw": status, "kind": kind, "open_seats": seats,
        "wl_prefix": pfx, "wl_booking_pos": wl_b, "wl_current_pos": wl_n,
        "actor_seats": av.get("seats"),
        "fare_total": fare.get("total"), "dynamic_fare": dyn,
        "departure": rt.get("departureTime"), "arrival": rt.get("arrivalTime"),
        "duration_min": rt.get("durationMinutes"), "distance_km": rt.get("distanceKm"),
    }


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main():
    if not TOKEN:
        print("STOP: APIFY_TOKEN is empty. Add it as a repository secret.")
        sys.exit(1)
    os.makedirs(OUT, exist_ok=True)
    now = datetime.now(IST)
    snap_ts = now.isoformat(timespec="seconds")
    today = now.date()

    # 1. De-duplicated query list
    seen, queries = set(), []
    for origin, dest, label, cls, _via in CORRIDORS:
        for lead in LEADS:
            q = make_query(origin, dest, label, cls,
                           (today + timedelta(days=lead)).isoformat(), lead, "full")
            if qkey(q) not in seen:
                seen.add(qkey(q))
                queries.append(q)
    print(f"{len(queries)} unique queries, {WORKERS} at a time, row cap {MAX_ROWS}",
          flush=True)

    rows, dedup = {}, {"raw_rows": 0, "dropped": 0, "conflicts": 0}
    failures, zero_rows = [], []

    def ingest(q, items):
        n_new = 0
        for it in items:
            row = to_row(q, it, snap_ts)
            k = (row["journey_date"], row["origin"], row["dest"], row["class"],
                 row["quota"], row["train_number"])
            dedup["raw_rows"] += 1
            if k in rows:
                dedup["dropped"] += 1
                if rows[k]["status_raw"] != row["status_raw"]:
                    dedup["conflicts"] += 1
                continue
            rows[k] = row
            n_new += 1
        return n_new

    # 2. Main phase, in parallel
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(run_query, queries))
    for q, outcome, items in results:
        tag = f"{q['label']} +{q['lead']}d"
        if outcome != "OK":
            failures.append((tag, outcome))
            if outcome != "SKIPPED":
                print(f"  FAIL {tag}: {outcome}", flush=True)
            continue
        if ingest(q, items) == 0:
            zero_rows.append(tag)

    # 3. Segment probe: only where a train is waitlisted end to end
    probe_log = []
    if not stop.is_set():
        for origin, dest, label, cls, via in CORRIDORS:
            if not via:
                continue
            for lead in LEADS:
                day = (today + timedelta(days=lead)).isoformat()
                full = [r for r in rows.values() if r["corridor"] == label
                        and r["journey_date"] == day and r["pair_type"] == "full"]
                if not any(r["kind"] == "WL" for r in full):
                    continue
                pq = make_query(origin, via, label, cls, day, lead, "probe")
                # Reuse rows if this exact leg was already queried as a corridor
                if qkey(pq) not in seen:
                    seen.add(qkey(pq))
                    q, outcome, items = run_query(pq)
                    if outcome != "OK":
                        failures.append((f"{label} probe +{lead}d", outcome))
                        continue
                    ingest(q, items)
                by_train = {r["train_number"]: r for r in rows.values()
                            if r["origin"] == origin and r["dest"] == via
                            and r["journey_date"] == day and r["class"] == cls}
                for fr in full:
                    pr = by_train.get(fr["train_number"])
                    if pr:
                        probe_log.append({
                            "corridor": label, "journey_date": day, "lead_days": lead,
                            "train_number": fr["train_number"], "train_name": fr["train_name"],
                            "train_class": fr["train_class"], "via": via,
                            "full_status": fr["status_raw"], "leg_status": pr["status_raw"],
                            "full_is_waitlist": fr["kind"] == "WL",
                            "leg_has_seats": pr["kind"] in ("AVAILABLE", "CURRENT"),
                            "leg_open_seats": pr["open_seats"],
                        })

    # 4. Outputs
    all_rows = sorted(rows.values(), key=lambda r: (r["corridor"], r["pair_type"],
                      r["journey_date"], r["train_number"]))
    write_csv(f"{OUT}/availability_rows.csv", all_rows)
    write_csv(f"{OUT}/segment_probe.csv", probe_log)

    full_rows = [r for r in all_rows if r["pair_type"] == "full"]
    by_class, by_kind, unknown = {}, {}, {}
    for r in full_rows:
        by_class[r["train_class"]] = by_class.get(r["train_class"], 0) + 1
        by_kind[r["kind"]] = by_kind.get(r["kind"], 0) + 1
        if r["kind"] == "OTHER":
            unknown[r["status_raw"]] = unknown.get(r["status_raw"], 0) + 1
    mismatch = sum(1 for r in full_rows if r["actor_seats"] is not None
                   and r["open_seats"] is not None and r["actor_seats"] != r["open_seats"])
    waitlisted = [p for p in probe_log if p["full_is_waitlist"]]
    flips = [p for p in waitlisted if p["leg_has_seats"]]
    ok_q = len(queries) - len([f for f in failures if "probe" not in f[0]])

    print("\n=== SUMMARY ===")
    print("unique full-pair rows by train class:", by_class)
    print("rows by status kind:", by_kind)
    print(f"duplicates dropped: {dedup['dropped']} of {dedup['raw_rows']} rows "
          f"({dedup['conflicts']} with differing status)")
    print(f"queries ok: {ok_q} of {len(queries)}; failures: {len(failures)}")
    print("zero-row queries (check station codes):", sorted(set(zero_rows)))
    print("unknown status texts:", unknown)
    print(f"actor seat field disagreed with parsed count: {mismatch}")
    print(f"segment probe: {len(waitlisted)} waitlisted trains compared, "
          f"{len(flips)} had seats on the shorter leg")
    cost = state["rows_billed"] * 0.001
    print(f"rows billed: {state['rows_billed']}, approx cost ${cost:.3f}")

    n_unique = len(full_rows)
    n_parsed = n_unique - by_kind.get("OTHER", 0)
    checks = {
        "D1 queries ok >= 90%": len(queries) > 0 and ok_q / len(queries) >= 0.9,
        "D2 VB and Shatabdi both found": by_class.get("VANDE_BHARAT", 0) > 0
                                        and by_class.get("SHATABDI", 0) > 0,
        "D3 status parsed >= 95%": n_unique > 0 and n_parsed / n_unique >= 0.95,
    }
    for k, v in checks.items():
        print(("PASS " if v else "FAIL ") + k)
    with open(f"{OUT}/v2_report.json", "w") as f:
        json.dump({"snapshot_ts": snap_ts, "queries": len(queries), "failures": failures,
                   "zero_rows": zero_rows, "by_class": by_class, "by_kind": by_kind,
                   "dedup": dedup, "unknown_status": unknown,
                   "probe": {"waitlisted": len(waitlisted), "flips": len(flips)},
                   "rows_billed": state["rows_billed"], "cost_usd": round(cost, 3),
                   "stopped_early": stop.is_set()}, f, indent=1)
    if not all(checks.values()):
        sys.exit(2)


if __name__ == "__main__":
    main()
