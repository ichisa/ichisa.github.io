#!/usr/bin/env python3
"""
build_monthly_geojson.py
========================
Read the eBird text files from data/pyrocephalus_rubinus/ and produce
12 GeoJSON files (month-01.geojson … month-12.geojson) in data/.

Each point is aggregated per grid cell (~0.1° ≈ 11 km) so the output
stays small enough for a web map.  The "count" property holds the total
number of individual birds observed in that cell for that month (across
all years), and "nobs" holds how many separate observation records fell
in the cell.

Usage
-----
    python3 build_monthly_geojson.py
"""

import csv
import glob
import json
import os
import sys
from collections import defaultdict

# ── Configuration ────────────────────────────────────────────────
INPUT_DIR  = os.path.join("data", "pyrocephalus_rubinus")
OUTPUT_DIR = "data"
GRID_RES   = 0.1          # degrees – aggregation grid resolution

MONTH_NAMES = [
    "January","February","March","April","May","June",
    "July","August","September","October","November","December",
]

# ── Read all observation files ───────────────────────────────────

def read_ebd_files():
    """
    Yield (lat, lng, count, month) for every valid observation row
    in all out_ebd_*.txt files.
    """
    pattern = os.path.join(INPUT_DIR, "out_ebd_*.txt")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"ERROR: no files matching {pattern}")
        sys.exit(1)

    print(f"Found {len(files)} observation files")

    total = 0
    skipped_x = 0
    skipped_other = 0

    for fpath in files:
        fname = os.path.basename(fpath)
        file_rows = 0
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter="\t")
            header = next(reader)  # skip header

            # Find column indices by name (robust to column order changes)
            col = {h.strip(): i for i, h in enumerate(header)}
            i_count = col.get("OBSERVATION COUNT")
            i_lat   = col.get("LATITUDE")
            i_lng   = col.get("LONGITUDE")
            i_date  = col.get("OBSERVATION DATE")

            if None in (i_count, i_lat, i_lng, i_date):
                print(f"  ⚠  Skipping {fname}: missing required columns")
                continue

            for row in reader:
                try:
                    raw_count = row[i_count].strip()
                    lat = float(row[i_lat].strip())
                    lng = float(row[i_lng].strip())
                    date_str = row[i_date].strip()  # YYYY-MM-DD

                    # Parse month from date
                    month = int(date_str.split("-")[1])
                    if month < 1 or month > 12:
                        raise ValueError

                    # Handle count: "X" = presence (count as 1), else integer
                    if raw_count.upper() == "X":
                        count = 1
                        skipped_x += 1  # count but don't skip
                    else:
                        count = max(1, int(raw_count))

                    total += 1
                    file_rows += 1
                    yield (lat, lng, count, month)
                except (IndexError, ValueError):
                    skipped_other += 1
                    continue

        print(f"  {fname}: {file_rows:,} rows")

    print(f"\nTotal valid observations: {total:,}")
    print(f"  'X' values (counted as 1): {skipped_x:,}")
    print(f"  Skipped (parse errors):    {skipped_other:,}")


# ── Aggregate into grid cells per month ──────────────────────────

def aggregate(observations):
    """
    Aggregate observations into grid cells.
    Returns { month: { (grid_lat, grid_lng): {"count": N, "nobs": M} } }
    """
    grid = defaultdict(lambda: defaultdict(lambda: {"count": 0, "nobs": 0}))

    for lat, lng, count, month in observations:
        # Snap to grid cell center
        glat = round(round(lat / GRID_RES) * GRID_RES, 4)
        glng = round(round(lng / GRID_RES) * GRID_RES, 4)
        cell = grid[month][(glat, glng)]
        cell["count"] += count
        cell["nobs"]  += 1

    return grid


# ── Write GeoJSON ────────────────────────────────────────────────

def write_geojson(grid):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for month in range(1, 13):
        cells = grid.get(month, {})
        features = []
        for (lat, lng), info in cells.items():
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]   # GeoJSON is [lng, lat]
                },
                "properties": {
                    "count": info["count"],
                    "nobs":  info["nobs"],
                }
            })

        geojson = {
            "type": "FeatureCollection",
            "features": features,
        }

        outpath = os.path.join(OUTPUT_DIR, f"month-{month:02d}.geojson")
        with open(outpath, "w") as f:
            json.dump(geojson, f, separators=(",", ":"))

        size_kb = os.path.getsize(outpath) / 1024
        bar = "█" * (len(features) // 20) if features else "·"
        print(f"  {MONTH_NAMES[month-1]:>9}: {len(features):>5} cells, "
              f"{size_kb:>7.0f} KB  {bar}")


# ── Main ─────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("Building monthly GeoJSON for Pyrocephalus rubinus")
    print("=" * 55)
    print()

    print("Reading observation files…")
    obs = list(read_ebd_files())
    print()

    print("Aggregating into grid cells…")
    grid = aggregate(obs)
    print()

    print("Writing GeoJSON files…")
    write_geojson(grid)

    total_cells = sum(len(grid.get(m, {})) for m in range(1, 13))
    print(f"\n✅ Done! {total_cells:,} total grid cells across 12 months.")
    print(f"   Files in {OUTPUT_DIR}/month-XX.geojson")


if __name__ == "__main__":
    main()
