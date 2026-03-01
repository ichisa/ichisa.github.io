#!/usr/bin/env python3
"""
fetch_ebird_data.py
===================
Fetch monthly observation data for Vermilion Flycatcher (Pyrocephalus rubinus)
from the eBird API v2 and save as static JSON files for the migration map.

Usage
-----
    # Set your API key as an environment variable (recommended):
    export EBIRD_API_KEY="your-api-key-here"
    python3 fetch_ebird_data.py

    # Or pass it as an argument:
    python3 fetch_ebird_data.py --key YOUR_API_KEY

    # Specify a different year:
    python3 fetch_ebird_data.py --year 2024

Output
------
Creates a ``data/`` directory containing:
    month-01.json  …  month-12.json

Each file is a JSON array of objects: { lat, lng, count, loc, date }

These files are loaded by map.html automatically when hosted on GitHub Pages.
"""

import argparse
import json
import os
import sys
import time

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library is required.  Install it with:")
    print("       pip install requests")
    sys.exit(1)

# ── Configuration ────────────────────────────────────────────────
SPECIES_CODE = "verfly"                     # eBird code for Vermilion Flycatcher
SPECIES_SCI  = "Pyrocephalus rubinus"

# Regions covering the species' range (Americas)
# We use subnational1 (state-level) regions for Mexico and Brazil
# to avoid response truncation on country-level queries.
REGIONS = [
    # USA – key states in breeding / wintering / vagrant range
    "US-AZ", "US-TX", "US-NM", "US-CA", "US-OK", "US-LA", "US-FL",
    "US-NV", "US-UT", "US-CO", "US-KS", "US-AR", "US-MS",
    "US-GA", "US-SC", "US-AL", "US-NE", "US-MO",
    # Mexico – individual states (species is resident/common across most of MX)
    "MX-AGU", "MX-BCN", "MX-BCS", "MX-CAM", "MX-CHH", "MX-CHP",
    "MX-COA", "MX-COL", "MX-DIF", "MX-DUR", "MX-GRO", "MX-GUA",
    "MX-HID", "MX-JAL", "MX-MEX", "MX-MIC", "MX-MOR", "MX-NAY",
    "MX-NLE", "MX-OAX", "MX-PUE", "MX-QUE", "MX-ROO", "MX-SIN",
    "MX-SLP", "MX-SON", "MX-TAB", "MX-TAM", "MX-TLA", "MX-VER",
    "MX-YUC", "MX-ZAC",
    # Central America
    "GT", "BZ", "HN", "SV", "NI", "CR", "PA",
    # South America – major countries with subnational for Brazil
    "CO", "VE", "EC", "PE", "BO", "PY", "AR", "UY", "CL",
    "GY", "SR", "TT",
    # Brazil – key states where the species occurs
    "BR-SP", "BR-RJ", "BR-MG", "BR-GO", "BR-DF", "BR-MS", "BR-MT",
    "BR-BA", "BR-PR", "BR-SC", "BR-RS", "BR-TO", "BR-MA", "BR-PI",
    "BR-CE", "BR-PE", "BR-PA",
]

# Days to sample per month — more days = much better coverage
SAMPLE_DAYS = [1, 5, 10, 15, 20, 25, 28]

API_BASE = "https://api.ebird.org/v2"
DELAY    = 0.15         # seconds between API calls (respect rate limits)

# ── Helpers ──────────────────────────────────────────────────────

def ebird_get(endpoint, api_key, params=None):
    """Make a GET request to the eBird API and return (status_code, data)."""
    url = f"{API_BASE}{endpoint}"
    headers = {"X-eBirdApiToken": api_key}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            return r.status_code, r.json()
        else:
            return r.status_code, []
    except requests.RequestException as exc:
        print(f"  ⚠  Request failed: {exc}")
        return 0, []


def fetch_historic(api_key, region, year, month, day):
    """
    Fetch observations on a specific date using the general historic endpoint,
    then filter for our target species.

    Endpoint: GET /v2/data/obs/{regionCode}/historic/{y}/{m}/{d}

    The eBird API does NOT have a species-specific variant of the historic
    endpoint, so we must fetch all species and filter client-side.
    We use maxResults=10000 to avoid response truncation.
    """
    endpoint = f"/data/obs/{region}/historic/{year}/{month}/{day}"
    params = {
        "maxResults": 10000,
        "includeProvisional": "true",
    }
    status, data = ebird_get(endpoint, api_key, params=params)
    if not isinstance(data, list):
        return []
    # Filter for our target species
    return [
        o for o in data
        if o.get("speciesCode") == SPECIES_CODE
        or "pyrocephalus" in (o.get("sciName", "")).lower()
    ]


def fetch_recent(api_key, region):
    """
    Fetch recent observations (last 30 days) of the species in a region.

    Endpoint: GET /v2/data/obs/{regionCode}/recent/{speciesCode}

    This IS a species-specific endpoint and works reliably.
    """
    endpoint = f"/data/obs/{region}/recent/{SPECIES_CODE}"
    params = {
        "back": 30,
        "includeProvisional": "true",
    }
    status, data = ebird_get(endpoint, api_key, params=params)
    return data if isinstance(data, list) else []


def to_record(obs):
    """Normalise an eBird observation to a compact dict."""
    return {
        "lat":   obs.get("lat"),
        "lng":   obs.get("lng"),
        "count": obs.get("howMany", 1),
        "loc":   obs.get("locName", ""),
        "date":  obs.get("obsDt", ""),
    }


def dedup(records):
    """Remove duplicate lat/lng keeping the highest count."""
    best = {}
    for r in records:
        if r["lat"] is None or r["lng"] is None:
            continue
        key = f"{r['lat']:.4f},{r['lng']:.4f}"
        if key not in best or r["count"] > best[key]["count"]:
            best[key] = r
    return list(best.values())


# ── Main ─────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch eBird data for Vermilion Flycatcher migration map"
    )
    parser.add_argument("--key", default=os.environ.get("EBIRD_API_KEY", ""),
                        help="eBird API key (or set EBIRD_API_KEY env var)")
    parser.add_argument("--year", type=int, default=2025,
                        help="Year to fetch historic data for (default: 2025)")
    parser.add_argument("--outdir", default="data",
                        help="Output directory (default: data)")
    args = parser.parse_args()

    api_key = args.key
    if not api_key:
        api_key = input("Enter your eBird API key: ").strip()
    if not api_key:
        print("ERROR: API key is required.")
        sys.exit(1)

    year   = args.year
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    total_regions = len(REGIONS)
    total_api_calls = total_regions * 12 * len(SAMPLE_DAYS) + total_regions
    est_minutes = total_api_calls * DELAY / 60

    print(f"Species:   {SPECIES_SCI} ({SPECIES_CODE})")
    print(f"Year:      {year}")
    print(f"Regions:   {total_regions}")
    print(f"Days/mo:   {SAMPLE_DAYS}")
    print(f"API calls: ~{total_api_calls}  (est. {est_minutes:.0f} min)")
    print(f"Output:    {outdir}/")
    print()
    print("Strategy: General historic endpoint + species filter")
    print("          (eBird API has no species-specific historic endpoint)")
    print()

    # Quick API key validation
    print("🔍 Validating API key… ", end="", flush=True)
    status, data = ebird_get(f"/data/obs/US-TX/recent/{SPECIES_CODE}",
                              api_key, params={"back": 1, "maxResults": 1})
    if status == 200:
        print(f"✅ Valid (got {len(data)} record(s) from US-TX)")
    elif status == 403:
        print("❌ Invalid API key! Check your key and try again.")
        sys.exit(1)
    else:
        print(f"⚠  Unexpected status {status}, continuing anyway…")
    print()

    total_obs = 0

    for month in range(1, 13):
        month_records = []
        month_name = [
            "Jan","Feb","Mar","Apr","May","Jun",
            "Jul","Aug","Sep","Oct","Nov","Dec"
        ][month - 1]
        print(f"\n── {month_name} {year} ──")

        # Historic queries for this month
        queries_done = 0
        for day in SAMPLE_DAYS:
            for region in REGIONS:
                data = fetch_historic(api_key, region, year, month, day)
                if data:
                    month_records.extend(to_record(o) for o in data)
                queries_done += 1
                time.sleep(DELAY)

            unique_so_far = len(dedup(month_records))
            print(f"  Day {day:2d}: {unique_so_far} unique obs "
                  f"({queries_done}/{total_regions * len(SAMPLE_DAYS)} queries)")

        # Also fetch recent observations and bucket by month
        recent_added = 0
        for region in REGIONS:
            recent = fetch_recent(api_key, region)
            for o in recent:
                dt = o.get("obsDt", "")
                if dt and "-" in dt:
                    try:
                        obs_month = int(dt.split("-")[1])
                    except (ValueError, IndexError):
                        continue
                    if obs_month == month:
                        month_records.append(to_record(o))
                        recent_added += 1
            time.sleep(DELAY)
        if recent_added:
            print(f"  Recent: +{recent_added} observations from last 30 days")

        # Deduplicate
        month_records = dedup(month_records)
        total_obs += len(month_records)
        print(f"  ✅ {month_name}: {len(month_records)} unique observations")

        # Save
        outfile = os.path.join(outdir, f"month-{month:02d}.json")
        with open(outfile, "w") as f:
            json.dump(month_records, f, separators=(",", ":"))
        print(f"  💾 Saved to {outfile}")

    print(f"\n{'='*55}")
    print(f"✅ Done! {total_obs} total observations across 12 months.")
    print(f"   Files saved in {outdir}/")
    print()
    for month in range(1, 13):
        mn = ["Jan","Feb","Mar","Apr","May","Jun",
              "Jul","Aug","Sep","Oct","Nov","Dec"][month - 1]
        fpath = os.path.join(outdir, f"month-{month:02d}.json")
        with open(fpath) as f:
            n = len(json.load(f))
        bar = "█" * (n // 10) if n else "·"
        print(f"   {mn}: {n:>5} obs  {bar}")
    print()
    print("Next steps:")
    print("   git add data/")
    print("   git commit -m 'Update bird migration data'")
    print("   git push")


if __name__ == "__main__":
    main()
