"""v3 availability collector for rail-priority-engine.

Reads corridors.csv, queries the Apify IRCTC actor for each corridor and lead
time, parses seat availability, and writes CSVs to ./out.

Settings (all environment variables, all optional):
  LEADS        days ahead to sample, default "1,7,30"
  CORRIDOR_GROUPS  corridor groups to run, default "A,B,C" (D is road-heavy, off in the list)
  WORKERS      parallel queries, default 4 (free Apify plan allows about 5 at once)
  MAX_ROWS     stop when this many rows have been billed (about $0.001 each)
  MAX_MINUTES  stop starting new queries after this many minutes, default 200
  PROBE        "1" to also test the shorter leg when a pair is waitlisted
  TRIGGER      "schedule" makes the run obey RUN_DATES; anything else always runs
  RUN_DATES    comma separated YYYY-MM-DD run days, used only when TRIGGER=schedule

No-repeat-work rules:
  * each (origin, destination, date, class, quota) is queried once per run
  * duplicate rows are dropped before anything is counted
  * a failed query is listed, never retried (a retry could bill twice)
  * the only second query is the fallback class, and only after a zero-row
    result, which is not billed
  * a row cap and a time cap stop the run if cost or duration runs away
Never prints the token or any raw data.
"""
import csv
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

TOKEN = os.environ.get("APIFY_TOKEN", "").strip()
ACTOR = "maximedupre~irctc"
URL = f"https://api.apify.com/v2/acts/{ACTOR}/run-sync-get-dataset-items"
IST = ZoneInfo("Asia/Kolkata")
TIMEOUT = 280                       # Apify's sync limit is 300s
OUT = os.environ.get("OUT_DIR", "out")
CORRIDORS_FILE = os.environ.get("CORRIDORS_FILE", "corridors.csv")
LEADS = [int(x) for x in (os.environ.get("LEADS") or "1,7,30").split(",") if x.strip()]
GROUPS = [g.strip().upper() for g in (os.environ.get("CORRIDOR_GROUPS") or "A,B,C").split(",") if g.strip()]
WORKERS = int(os.environ.get("WORKERS") or "4")
MAX_ROWS = int(os.environ.get("MAX_ROWS") or "5500")
MAX_MINUTES = float(os.environ.get("MAX_MINUTES") or "200")
PROBE = (os.environ.get("PROBE") or "0") == "1"
TRIGGER = os.environ.get("TRIGGER", "")
RUN_DATES = [d.strip() for d in (os.environ.get("RUN_DATES") or "").split(",") if d.strip()]
RUN_TAG = os.environ.get("RUN_TAG", "")
QUOTA = "GN"
PRIORITY = {"B": 0, "A": 1, "C": 2, "D": 3}

lock = threading.Lock()
stop = threading.Event()
state = {"rows_billed": 0, "deadline": None, "stop_reason": ""}


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
    if s == "TRAIN CANCELLED":
        return "CANCELLED", None, "", None, None
    if s == "REGRET":
        return "REGRET", None, "", None, None        # booking closed for this train and date
    if s == "CLASS NOT EXIST":
        return "NO_CLASS", None, "", None, None
    if s.startswith("CHARTING DONE"):
        return "CHARTED", None, "", None, None   # chart prepared, different pool
    return "OTHER", None, "", None, None


def classify(name):
    """Class from the train name. The actor's own type code is SHB for all of
    these, so it cannot be used. Spellings vary (SHATABADI, SHTBDI, JANSHATABDI,
    'VANDE BHARAT SL', 'VB SL')."""
    up = str(name).upper()
    n = re.sub(r"[^A-Z]", "", up)
    is_vb = "VANDEBHARAT" in n or bool(re.search(r"\bV\.?B\.?\b", up))
    if is_vb:
        sleeper = "SLEEPER" in n or bool(re.search(r"\b(SL|SLPR)\b", up)) or n.endswith("VBSL")
        return "VB_SLEEPER" if sleeper else "VANDE_BHARAT"
    if "JANSHAT" in n or "JANSHTB" in n or "JANSHAB" in n:
        return "JAN_SHATABDI"
    if any(t in n for t in ("SHATABDI", "SHATABADI", "SHTBDI", "SHTABDI", "SHATBDI")):
        return "SHATABDI"
    return "OTHER"


def flat(v):
    """List, dict or string to a compact text for CSV."""
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        out = []
        for x in v:
            if isinstance(x, dict):
                x = x.get("code") or x.get("name") or json.dumps(x, sort_keys=True)
            out.append(str(x))
        return "|".join(out)
    if isinstance(v, dict):
        return json.dumps(v, sort_keys=True)
    return str(v)


# ---------- querying ----------
def run_query(q):
    """One billed query. Returns (query, outcome, items). Never retries."""
    if stop.is_set():
        return q, "SKIPPED", None
    if state["deadline"] and time.time() > state["deadline"]:
        state["stop_reason"] = state["stop_reason"] or "time cap"
        stop.set()
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
        state["stop_reason"] = state["stop_reason"] or f"HTTP {r.status_code}"
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
            state["stop_reason"] = state["stop_reason"] or "row cap"
            stop.set()
    return q, "OK", items


def do_query(q):
    """Main query, then the fallback class once if the first returned zero rows
    (zero rows are not billed, so this cannot double bill)."""
    q1, outcome, items = run_query(q)
    if outcome == "OK" and not items and q.get("fallback_cls") and q["fallback_cls"] != q["cls"]:
        q2 = dict(q, cls=q["fallback_cls"], fallback_cls="", used_fallback=True)
        return run_query(q2)
    return q1, outcome, items


def make_query(c, day, lead, pair_type="full", dest=None):
    return {"origin": c["origin"], "dest": dest or c["dest"], "label": c["label"],
            "group": c["group"], "cls": c["cls"], "fallback_cls": c.get("fallback_cls", ""),
            "coaches": c.get("coaches", ""), "date": day, "lead": lead,
            "pair_type": pair_type, "used_fallback": False}


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
        "corridor": q["label"], "group": q["group"], "pair_type": q["pair_type"],
        "origin": q["origin"], "dest": q["dest"],
        "journey_date": q["date"], "lead_days": q["lead"],
        "class": q["cls"], "quota": QUOTA,
        "train_number": str(tr.get("number") or ""),
        "train_name": tr.get("name"), "train_class": classify(tr.get("name") or ""),
        "actor_type": tr.get("type"),
        "operating_days": flat(tr.get("operatingDays")),
        "available_classes": flat(tr.get("availableClasses")),
        "coaches_listed": q.get("coaches", ""),
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


def load_corridors():
    with open(CORRIDORS_FILE, newline="") as f:
        rows = list(csv.DictReader(f))
    rows = [r for r in rows if str(r.get("enabled", "1")).strip() == "1"
            and r["group"].strip().upper() in GROUPS and r["origin"].strip() and r["dest"].strip()]
    rows.sort(key=lambda r: PRIORITY.get(r["group"].strip().upper(), 9))
    return rows


def main():
    now = datetime.now(IST)
    if TRIGGER == "schedule" and now.date().isoformat() not in RUN_DATES:
        print(f"{now.date()} is not in RUN_DATES. No queries sent, nothing billed.")
        return
    if not TOKEN:
        print("STOP: APIFY_TOKEN is empty. Add it as a repository secret.")
        sys.exit(1)
    os.makedirs(OUT, exist_ok=True)
    snap_ts = now.isoformat(timespec="seconds")
    today = now.date()
    t0 = time.time()
    state["deadline"] = t0 + MAX_MINUTES * 60

    corridors = load_corridors()
    # 1. De-duplicated query list, existing corridors first
    seen, queries = set(), []
    for c in corridors:
        for lead in LEADS:
            q = make_query(c, (today + timedelta(days=lead)).isoformat(), lead)
            if qkey(q) not in seen:
                seen.add(qkey(q))
                queries.append(q)
    print(f"{len(corridors)} corridors (groups {','.join(GROUPS)}), leads {LEADS}: "
          f"{len(queries)} unique queries, {WORKERS} at a time, row cap {MAX_ROWS}, "
          f"time cap {MAX_MINUTES:.0f} min", flush=True)

    rows, dedup = {}, {"raw_rows": 0, "dropped": 0, "conflicts": 0}
    failures, zero_rows, qlog = [], [], []

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

    # 2. Main phase, in parallel, with progress lines
    done = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(do_query, q) for q in queries]
        for fut in as_completed(futs):
            q, outcome, items = fut.result()
            done += 1
            tag = f"{q['label']} +{q['lead']}d"
            n_rows = 0
            if outcome != "OK":
                failures.append((tag, outcome))
                if outcome != "SKIPPED":
                    print(f"  FAIL {tag}: {outcome}", flush=True)
            else:
                n_rows = ingest(q, items)
                if n_rows == 0:
                    zero_rows.append((q["group"], q["origin"], q["dest"], q["cls"], q["label"]))
            qlog.append({"corridor": q["label"], "group": q["group"], "origin": q["origin"],
                         "dest": q["dest"], "lead_days": q["lead"], "journey_date": q["date"],
                         "class_used": q["cls"], "fallback_used": q["used_fallback"],
                         "outcome": outcome, "rows_returned": len(items) if items else 0,
                         "new_rows": n_rows})
            if done % 25 == 0 or done == len(queries):
                mins = (time.time() - t0) / 60
                print(f"  {done}/{len(queries)} done, {state['rows_billed']} rows billed "
                      f"(${state['rows_billed'] * 0.001:.2f}), {mins:.1f} min", flush=True)

    # 3. Optional segment probe: only where a train is waitlisted end to end
    probe_log = []
    if PROBE and not stop.is_set():
        for c in corridors:
            via = (c.get("via") or "").strip()
            if not via:
                continue
            for lead in LEADS:
                day = (today + timedelta(days=lead)).isoformat()
                full = [r for r in rows.values() if r["corridor"] == c["label"]
                        and r["origin"] == c["origin"] and r["dest"] == c["dest"]
                        and r["journey_date"] == day and r["pair_type"] == "full"]
                if not any(r["kind"] == "WL" for r in full):
                    continue
                pq = make_query(c, day, lead, pair_type="probe", dest=via)
                if qkey(pq) not in seen:             # reuse rows if leg already queried
                    seen.add(qkey(pq))
                    q, outcome, items = run_query(pq)
                    if outcome != "OK":
                        failures.append((f"{c['label']} probe +{lead}d", outcome))
                        continue
                    ingest(q, items)
                by_train = {r["train_number"]: r for r in rows.values()
                            if r["origin"] == c["origin"] and r["dest"] == via
                            and r["journey_date"] == day and r["class"] == c["cls"]}
                for fr in full:
                    pr = by_train.get(fr["train_number"])
                    if pr:
                        probe_log.append({
                            "corridor": c["label"], "journey_date": day, "lead_days": lead,
                            "train_number": fr["train_number"], "train_name": fr["train_name"],
                            "train_class": fr["train_class"], "via": via,
                            "full_status": fr["status_raw"], "leg_status": pr["status_raw"],
                            "full_is_waitlist": fr["kind"] == "WL",
                            "leg_has_seats": pr["kind"] in ("AVAILABLE", "CURRENT"),
                            "leg_open_seats": pr["open_seats"],
                        })

    # 4. Outputs
    all_rows = sorted(rows.values(), key=lambda r: (r["group"], r["corridor"], r["pair_type"],
                      r["journey_date"], r["train_number"]))
    write_csv(f"{OUT}/availability_rows.csv", all_rows)
    write_csv(f"{OUT}/segment_probe.csv", probe_log)
    write_csv(f"{OUT}/query_log.csv", sorted(qlog, key=lambda r: (r["group"], r["corridor"], r["lead_days"])))
    cor_sum = {}
    for r in qlog:
        k = (r["group"], r["origin"], r["dest"], r["corridor"])
        s = cor_sum.setdefault(k, {"group": k[0], "origin": k[1], "dest": k[2], "corridor": k[3],
                                   "queries": 0, "rows": 0, "failed": 0})
        s["queries"] += 1
        s["rows"] += r["rows_returned"]
        s["failed"] += r["outcome"] != "OK"
    for s in cor_sum.values():
        s["status"] = ("FAILED" if s["failed"] == s["queries"] else
                       "NO_TRAINS_OR_BAD_CODE" if s["rows"] == 0 else "OK")
    write_csv(f"{OUT}/corridor_summary.csv", sorted(cor_sum.values(), key=lambda r: (r["group"], r["corridor"])))

    full_rows = [r for r in all_rows if r["pair_type"] == "full"]
    by_class, by_kind, unknown = {}, {}, {}
    for r in full_rows:
        by_class[r["train_class"]] = by_class.get(r["train_class"], 0) + 1
        by_kind[r["kind"]] = by_kind.get(r["kind"], 0) + 1
        if r["kind"] == "OTHER":
            unknown[r["status_raw"]] = unknown.get(r["status_raw"], 0) + 1
    waitlisted = [p for p in probe_log if p["full_is_waitlist"]]
    flips = [p for p in waitlisted if p["leg_has_seats"]]
    skipped = len([f for f in failures if f[1] == "SKIPPED"])
    attempted = len(queries) - skipped
    ok_q = attempted - len([f for f in failures if "probe" not in f[0] and f[1] != "SKIPPED"])
    cost = state["rows_billed"] * 0.001
    mins = (time.time() - t0) / 60
    zero_codes = sorted({(g, o, d, c, lab) for g, o, d, c, lab in zero_rows})
    rows_per_query = state["rows_billed"] / attempted if attempted else 0

    print("\n=== SUMMARY ===")
    print(f"queries attempted: {attempted} of {len(queries)} (skipped {skipped}); ok: {ok_q}")
    print(f"stopped early: {stop.is_set()} {state['stop_reason']}")
    print(f"rows billed: {state['rows_billed']}, about ${cost:.2f}, "
          f"{rows_per_query:.2f} rows per query, {mins:.1f} minutes")
    print("unique full-pair rows by train class:", by_class)
    print("rows by status kind:", by_kind)
    print(f"duplicates dropped: {dedup['dropped']} of {dedup['raw_rows']} rows "
          f"({dedup['conflicts']} with differing status)")
    print(f"zero-row corridors: {len(zero_codes)} (listed in corridor_summary.csv)")
    by_grp = {}
    for g, *_ in zero_codes:
        by_grp[g] = by_grp.get(g, 0) + 1
    print("zero-row corridors by group:", by_grp)
    print("unknown status texts:", unknown)
    if PROBE:
        print(f"segment probe: {len(waitlisted)} waitlisted trains compared, "
              f"{len(flips)} had seats on the shorter leg")

    n_unique = len(full_rows)
    n_parsed = n_unique - by_kind.get("OTHER", 0)
    checks = {
        "D1 queries ok >= 90%": attempted > 0 and ok_q / attempted >= 0.9,
        "D3 status parsed >= 95%": n_unique > 0 and n_parsed / n_unique >= 0.95,
    }
    if {"A", "B"} <= set(GROUPS):
        checks["D2 VB and Shatabdi both found"] = (by_class.get("VANDE_BHARAT", 0) > 0
                                                  and by_class.get("SHATABDI", 0) > 0)
    for k, v in checks.items():
        print(("PASS " if v else "FAIL ") + k)
    with open(f"{OUT}/run_report.json", "w") as f:
        json.dump({"snapshot_ts": snap_ts, "tag": RUN_TAG, "groups": GROUPS, "leads": LEADS,
                   "corridors": len(corridors), "queries": len(queries), "attempted": attempted,
                   "failures": failures[:200], "zero_row_corridors": len(zero_codes),
                   "by_class": by_class, "by_kind": by_kind, "dedup": dedup,
                   "unknown_status": unknown, "rows_billed": state["rows_billed"],
                   "cost_usd": round(cost, 3), "rows_per_query": round(rows_per_query, 2),
                   "minutes": round(mins, 1), "stopped_early": stop.is_set(),
                   "stop_reason": state["stop_reason"]}, f, indent=1)
    if not all(checks.values()):
        sys.exit(2)


if __name__ == "__main__":
    main()
