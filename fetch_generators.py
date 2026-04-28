"""
fetch_generators.py
-------------------
Fetches Arkansas generator data from the EIA Form 860M API,
deduplicates to the most recent record per generator, derives
a human-readable status field, and writes generators.geojson.

Run manually:  EIA_API_KEY=your_key python fetch_generators.py
Run via CI:    GitHub Action sets EIA_API_KEY from repository secrets
"""

import json
import os
import sys
from datetime import datetime, timezone
from urllib.request import urlopen
from urllib.error import URLError
from urllib.parse import quote

# ── Config ────────────────────────────────────────────────────────────────────
API_BASE = "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/"
OUTPUT   = "generators.geojson"
STATE    = "AR"
MAX_ROWS = 5000
TODAY    = datetime.now(timezone.utc).strftime("%Y-%m")


def build_url(api_key, offset=0):
    """
    Build the EIA API URL for a given offset.
    Brackets in parameter names must NOT be percent-encoded —
    urlencode() would encode them, so we build the query string manually.
    """
    parts = [
        "frequency=monthly",
        "data[0]=county",
        "data[1]=latitude",
        "data[2]=longitude",
        "data[3]=nameplate-capacity-mw",
        "data[4]=operating-year-month",
        "data[5]=planned-retirement-year-month",
        "data[6]=technology",
        f"facets[stateid][]={STATE}",
        "sort[0][column]=period",
        "sort[0][direction]=desc",
        f"offset={offset}",
        f"length={MAX_ROWS}",
        f"api_key={quote(api_key, safe='')}",
    ]
    return API_BASE + "?" + "&".join(parts)


def fetch_all(api_key):
    """Fetch all records, paginating if necessary."""
    all_records = []
    offset = 0

    while True:
        url = build_url(api_key, offset)
        print(f"  Fetching offset {offset}...")
        print(f" URL: {url}")
        try:
            with urlopen(url, timeout=30) as resp:
                payload = json.loads(resp.read().decode())
        except URLError as e:
            print(f"ERROR: API request failed — {e}")
            sys.exit(1)

        data  = payload.get("response", {}).get("data", [])
        total = payload.get("response", {}).get("total", 0)

        if not data:
            break

        all_records.extend(data)
        print(f"  Got {len(data)} records (total available: {total})")

        offset += len(data)
        if offset >= total or len(data) < MAX_ROWS:
            break

    return all_records


def derive_status(record):
    """
    Derive a human-readable status from the retirement date vs today.
      - No retirement date        → Operating
      - Retirement date in future → Planned Retirement (YYYY-MM)
      - Retirement date in past   → Retired (YYYY-MM)
    """
    retirement = record.get("planned-retirement-year-month") or ""
    if not retirement or retirement.strip() == "":
        return "Operating"
    if retirement >= TODAY:
        return f"Planned Retirement ({retirement})"
    return f"Retired ({retirement})"


def deduplicate(records):
    """
    Keep only the most recent record per unique generator.
    Records are already sorted desc by period so first-seen wins.
    """
    seen = {}
    for r in records:
        key = (r.get("plantCode", ""), r.get("generatorId", ""))
        if key not in seen:
            seen[key] = r
    return list(seen.values())


def to_geojson(records):
    """Convert deduplicated records to a GeoJSON FeatureCollection."""
    features = []
    skipped  = 0

    for r in records:
        try:
            lat = float(r["latitude"])
            lon = float(r["longitude"])
        except (TypeError, ValueError, KeyError):
            skipped += 1
            continue

        if lat == 0.0 and lon == 0.0:
            skipped += 1
            continue

        cap = r.get("nameplate-capacity-mw")
        try:
            cap = round(float(cap), 2) if cap is not None else None
        except (TypeError, ValueError):
            cap = None

        props = {
            "plantName":     r.get("plantName")     or "—",
            "entityName":    r.get("entityName")    or "—",
            "technology":    r.get("technology")    or "—",
            "county":        r.get("county")        or "—",
            "capacityMW":    cap,
            "operatingDate": r.get("operating-year-month")           or "—",
            "retirementDate":r.get("planned-retirement-year-month")  or "—",
            "status":        derive_status(r),
            "generatorId":   r.get("generatorId")   or "—",
            "period":        r.get("period")         or "—",
        }

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props
        })

    if skipped:
        print(f"  Skipped {skipped} records with missing/invalid coordinates")

    return {"type": "FeatureCollection", "features": features}


def main():
    api_key = os.environ.get("EIA_API_KEY", "").strip()
    if not api_key:
        print("ERROR: EIA_API_KEY environment variable is not set.")
        print("Usage: EIA_API_KEY=your_key python fetch_generators.py")
        sys.exit(1)

    print(f"Fetching EIA 860M generator data for {STATE}...")
    raw_records = fetch_all(api_key)
    print(f"Total records fetched: {len(raw_records)}")

    print("Deduplicating to most recent record per generator...")
    deduped = deduplicate(raw_records)
    print(f"Unique generators: {len(deduped)}")

    print("Building GeoJSON...")
    geojson = to_geojson(deduped)
    print(f"Features with valid coordinates: {len(geojson['features'])}")

    with open(OUTPUT, "w") as f:
        json.dump(geojson, f)

    print(f"Written to {OUTPUT}")

    statuses = {}
    for feat in geojson["features"]:
        s = feat["properties"]["status"].split(" (")[0]
        statuses[s] = statuses.get(s, 0) + 1
    print("Status breakdown:")
    for s, count in sorted(statuses.items()):
        print(f"  {s}: {count}")


if __name__ == "__main__":
    main()
