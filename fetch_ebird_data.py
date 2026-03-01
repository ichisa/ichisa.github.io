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
REGIONS = [
    # USA – key states
    "US-AZ", "US-TX", "US-NM", "US-CA", "US-OK", "US-LA", "US-FL",
    "US-NV", "US-UT", "US-CO", "US-KS", "US-AR", "US-MS",
    # Mexico
    "MX",
    # Central America
    "GT", "BZ", "HN", "SV", "NI", "CR", "PA",
    # South America
    "CO", "VE", "EC", "PE", "BR", "BO", "PY", "AR", "UY", "CL", "GY", "SR", "TT",
]

# Days to sample per month (more days = more coverage but more API calls)
SAMPLE_DAYS = [5, 15, 25]

API_BASE = "https://api.ebird.org/v2"
DELAY    = 0.2          # seconds between API calls (respect rate limits)

# ── Helpers ──────────────────────────────────────────────────────

def ebird_get(endpoint, api_key, params=None):
    """Make a GET request to the eBird API and return parsed JSON (or [])."""
    url = f"{API_BASE}{endpoint}"
    headers = {"X-eBirdApiToken": api_key}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        else:
            return []
    except requests.RequestException as exc:
        print(f"  ⚠  Request failed: {exc}")
        return []


def test_species_historic(api_key, year):
    """Check if the species-specific historic endpoint works."""
    endpoint = f"/data/obs/US-TX/historic/{year}/6/15/{SPECIES_CODE}"
    data = ebird_get(endpoint, api_key)
    if isinstance(data, list):
        return True
    return False


def fetch_historic_species(api_key, region, year, month, day):
    """Fetch using species-specific historic endpoint."""
    endpoint = f"/data/obs/{region}/historic/{year}/{month}/{day}/{SPECIES_CODE}"
    return ebird_get(endpoint, api_key)


def fetch_historic_general(api_key, region, year, month, day):
    """Fetch all observations on a date, then filter for target species."""
    endpoint = f"/data/obs/{region}/historic/{year}/{month}/{day}"
    data = ebird_get(endpoint, api_key)
    if not isinstance(data, list):
        return []
    return [
        o for o in data
        if o.get("speciesCode") == SPECIES_CODE
        or "pyrocephalus" in (o.get("sciName", "")).lower()
    ]


def fetch_recent(api_key, region):
    """Fetch recent observations (last 30 days) of the species in a region."""
    endpoint = f"/data/obs/{region}/recent/{SPECIES_CODE}"
    return ebird_get(endpoint, api_key, params={"back": 30})


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

    print(f"Species:  {SPECIES_SCI} ({SPECIES_CODE})")
    print(f"Year:     {year}")
    print(f"Regions:  {len(REGIONS)}")
    print(f"Days/mo:  {SAMPLE_DAYS}")
    print(f"Output:   {outdir}/")
    print()

    # Test species-specific endpoint
    print("🔍 Testing species-specific historic endpoint… ", end="", flush=True)
    use_species = test_species_historic(api_key, year)
    if use_species:
        print("✅ Available (fast mode)")
        fetch_fn = fetch_historic_species
    else:
        print("❌ Not available — using general endpoint with filter (slower)")
        fetch_fn = fetch_historic_general

    total_obs = 0

    for month in range(1, 13):
        month_records = []
        month_name = [
            "Jan","Feb","Mar","Apr","May","Jun",
            "Jul","Aug","Sep","Oct","Nov","Dec"
        ][month - 1]
        print(f"\n── {month_name} {year} ──")

        # Historic queries
        for day in SAMPLE_DAYS:
            for region in REGIONS:
                data = fetch_fn(api_key, region, year, month, day)
                if data:
                    month_records.extend(to_record(o) for o in data)
                time.sleep(DELAY)
            print(f"  Day {day:2d}: {len(month_records)} cumulative obs")

        # Also fetch recent for current-month boost
        for region in REGIONS:
            recent = fetch_recent(api_key, region)
            for o in recent:
                obs_month = None
                dt = o.get("obsDt", "")
                if dt and "-" in dt:
                    try:
                        obs_month = int(dt.split("-")[1])
                    except (ValueError, IndexError):
                        pass
                if obs_month == month:
                    month_records.append(to_record(o))
            time.sleep(DELAY)

        # Deduplicate
        month_records = dedup(month_records)
        total_obs += len(month_records)
        print(f"  ✅ {month_name}: {len(month_records)} unique observations")

        # Save
        outfile = os.path.join(outdir, f"month-{month:02d}.json")
        with open(outfile, "w") as f:
            json.dump(month_records, f, separators=(",", ":"))
        print(f"  💾 Saved to {outfile}")

    print(f"\n{'='*50}")
    print(f"✅ Done! {total_obs} total observations across 12 months.")
    print(f"   Files saved in {outdir}/")
    print(f"   Commit and push to GitHub to see them on your map.")


if __name__ == "__main__":
    main()
