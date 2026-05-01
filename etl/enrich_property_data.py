"""
enrich_melbourne_data.py
========================
Enriches the Melbourne property dataset with:
  1. ABS Census 2021 — median income, population, median age by postcode
  2. Victoria Crime Statistics — offence rate by suburb
  3. PTV GTFS — distance to nearest train station (lat/lon)

Run:
    python enrich_melbourne_data.py \
        --input  data/melbourne_price_data.csv \
        --output data/melbourne_price_data_enriched.csv

Safe to rerun: caches data in .cache_enrich/ for 6 days.
Delete that folder to force a full re-download.

Dependencies (add to requirements.txt):
    pandas requests openpyxl tqdm
"""

import argparse
import io
import json
import math
import os
import re
import time
import zipfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ── optional progress bar ──────────────────────────────────────────────────────
try:
    from tqdm import tqdm
    TQDM = True
except ImportError:
    TQDM = False


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

# Cache directory — override with --cache-dir CLI arg if C: is full
CACHE_DIR = Path(".cache_enrich")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ABS Census 2021 GCP — Postcode (POA) only for VIC — ~3 MB (not the 144 MB 'all' pack)
# Contains G01 (population) and G02 (medians) for every VIC postcode.
ABS_G02_URL = (
    "https://www.abs.gov.au/census/find-census-data/datapacks/download/"
    "2021_GCP_POA_for_VIC_short-header.zip"
)

# Crime Statistics Agency Victoria — LGA Recorded Offences, Year Ending September 2024
# The LGA file contains a Postcode sheet for suburb/postcode-level data.
# Source: https://www.crimestatistics.vic.gov.au/crime-statistics/download-crime-data/download-data-19
CRIME_STATS_URL = (
    "https://files.crimestatistics.vic.gov.au/2025-03/"
    "Data_Tables_LGA_Recorded_Offences_Year_Ending_September_2024.xlsx"
)

# PTV GTFS static feed — all Melbourne public transport modes
PTV_GTFS_URL = (
    "https://data.ptv.vic.gov.au/downloads/gtfs.zip"
)

CACHE_MAX_DAYS = 6          # re-download if cache older than this many days


# ══════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _cache_path(name: str) -> Path:
    return CACHE_DIR / name


def _cache_fresh(name: str) -> bool:
    p = _cache_path(name)
    if not p.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)
    return age.days < CACHE_MAX_DAYS


def _save_cache(name: str, data: bytes) -> None:
    _cache_path(name).write_bytes(data)


def _load_cache(name: str) -> bytes:
    return _cache_path(name).read_bytes()


def _download(url: str, cache_name: str, timeout: int = 60, retries: int = 3) -> bytes:
    """Download with caching, retry, and streaming to avoid OOM on large files."""
    if _cache_fresh(cache_name):
        print(f"   [cache] {cache_name}")
        return _load_cache(cache_name)

    print(f"   [download] {url}")
    cache_file = _cache_path(cache_name)

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
            r.raise_for_status()
            # Stream to disk in 1 MB chunks — avoids loading entire file into RAM
            with open(cache_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            return cache_file.read_bytes()
        except Exception as e:
            if cache_file.exists():
                cache_file.unlink()          # remove partial file
            wait = 5 * (attempt + 1)
            print(f"   Attempt {attempt+1}/{retries} failed: {e}. Retrying in {wait}s…")
            time.sleep(wait)
    raise RuntimeError(f"Failed to download {url} after {retries} attempts")


# ══════════════════════════════════════════════════════════════════════════════
# 1. ABS CENSUS 2021 — G02 (Medians & Averages by Postcode)
# ══════════════════════════════════════════════════════════════════════════════

def load_abs_g02() -> pd.DataFrame:
    """
    Returns DataFrame indexed by postcode (int) with columns:
        abs_median_income_weekly, abs_population, abs_median_age

    - Median income + age  → G02 (Selected Medians and Averages)
    - Population           → G01 (Selected Person Characteristics)
      G01 column: Tot_P_P  (Total persons, persons)
    """
    print("\n[1/3] Loading ABS Census G02 + G01 …")
    try:
        raw = _download(ABS_G02_URL, "abs_g02_vic_all.zip", timeout=300)

        df_g02 = None
        df_g01 = None

        with zipfile.ZipFile(io.BytesIO(raw)) as outer:
            all_files = outer.namelist()

            # Helper: read a CSV either directly from outer zip or from a nested POA sub-zip
            def _read_poa_csv(pattern):
                # Direct match in outer zip
                matches = [n for n in all_files
                           if re.search(pattern, n) and 'POA' in n and n.endswith('.csv')]
                if matches:
                    with outer.open(matches[0]) as f:
                        return pd.read_csv(f, dtype=str)
                # Nested sub-zip
                poa_zips = [n for n in all_files if 'POA' in n and n.endswith('.zip')]
                for pz in poa_zips:
                    inner_raw = outer.read(pz)
                    with zipfile.ZipFile(io.BytesIO(inner_raw)) as inner:
                        inner_matches = [n for n in inner.namelist()
                                         if re.search(pattern, n) and n.endswith('.csv')]
                        if inner_matches:
                            with inner.open(inner_matches[0]) as f:
                                return pd.read_csv(f, dtype=str)
                return None

            df_g02 = _read_poa_csv(r'G02')
            df_g01 = _read_poa_csv(r'G01')

        if df_g02 is None:
            print("   ⚠  G02 POA CSV not found — skipping ABS.")
            return pd.DataFrame()

        print(f"   G02 columns: {df_g02.columns.tolist()[:10]}")

        # ── Map G02 columns ──────────────────────────────────────────────────
        g02_map = {}
        for c in df_g02.columns:
            cl = c.lower()
            if cl.startswith('poa_code') or cl == 'region_id':
                g02_map[c] = '_postcode_raw'
            elif cl in ('median_tot_prsnl_inc_weekly', 'med_tot_prsnl_inc_wk'):
                g02_map[c] = 'abs_median_income_weekly'
            elif 'median' in cl and ('prsnl' in cl or 'personal' in cl) and 'inc' in cl:
                g02_map[c] = 'abs_median_income_weekly'
            elif cl in ('median_age_persons', 'med_age_persons', 'median_age_persons'):
                g02_map[c] = 'abs_median_age'
            elif 'median' in cl and 'age' in cl:
                g02_map[c] = 'abs_median_age'

        df_g02 = df_g02.rename(columns=g02_map)
        
        df_g02 = df_g02.loc[:, ~df_g02.columns.duplicated(keep='first')]
        
        df_g02['_postcode_raw'] = df_g02['_postcode_raw'].str.extract(r'(\d{4})')
        
        df_g02 = df_g02.dropna(subset=['_postcode_raw'])
        df_g02['Postcode'] = df_g02['_postcode_raw'].astype(int)

        # Build result on df_g02 BEFORE setting index (avoids index-alignment bug)
        for col in ['abs_median_income_weekly', 'abs_median_age']:
            if col in df_g02.columns:
                df_g02[col] = pd.to_numeric(df_g02[col], errors='coerce')
            else:
                df_g02[col] = np.nan

        result = df_g02[['Postcode', 'abs_median_income_weekly', 'abs_median_age']].copy()
        result = result.set_index('Postcode')

        # ── Get population from G01 ──────────────────────────────────────────
        if df_g01 is not None:
            print(f"   G01 columns (first 15): {df_g01.columns.tolist()[:15]}")
            # Find postcode col and total population col (Tot_P_P)
            g01_pc_col  = next((c for c in df_g01.columns
                                if c.lower().startswith('poa_code') or c.lower() == 'region_id'), None)
            g01_pop_col = next((c for c in df_g01.columns
                                if c.lower() == 'tot_p_p'), None)
            if not g01_pop_col:
                # Broader search
                g01_pop_col = next((c for c in df_g01.columns
                                    if re.search(r'^tot_p', c.lower())), None)
            print(f"   G01 pop col: {g01_pop_col}")
            if g01_pc_col and g01_pop_col:
                pop = df_g01[[g01_pc_col, g01_pop_col]].copy()
                pop['Postcode'] = pop[g01_pc_col].str.extract(r'(\d{4})').astype(float).astype('Int64')
                pop['abs_population'] = pd.to_numeric(pop[g01_pop_col], errors='coerce')
                pop = pop[['Postcode', 'abs_population']].dropna().set_index('Postcode')
                result = result.join(pop, how='left')
                print(f"   ✓  Population joined for {result['abs_population'].notna().sum()} postcodes.")
            else:
                result['abs_population'] = np.nan
        else:
            print("   ⚠  G01 not found — population will be NaN.")
            result['abs_population'] = np.nan

        print(f"   ✓  {len(result)} postcodes loaded from ABS.")
        return result

    except Exception as e:
        print(f"   ⚠  ABS load failed: {e}")
        import traceback; traceback.print_exc()
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# 2. VICTORIA CRIME STATISTICS (Suburb level, Recorded Offences)
# ══════════════════════════════════════════════════════════════════════════════

def load_crime_stats() -> pd.DataFrame:
    """
    Returns DataFrame indexed by POSTCODE (int) with column:
        crime_rate_per_100k  — recorded offences per 100k population

    Source: Crime Statistics Agency Victoria
    File: Data_Tables_LGA_Recorded_Offences_Year_Ending_September_2024 (17 MB)

    CSA does NOT publish a standalone suburb file. The LGA file contains
    a 'Postcode' sheet (or similar) with postcode-level offence counts.
    We join this to the property data by Postcode.
    """
    print("\n[2/3] Loading Victoria Crime Statistics …")
    try:
        raw = _download(CRIME_STATS_URL, "vic_crime_lga.xlsx", timeout=120)
        xl = pd.ExcelFile(io.BytesIO(raw))
        print(f"   Sheets: {xl.sheet_names}")

        # Find the postcode-level sheet
        # CSA LGA file typically has sheets: Contents, LGA, Postcode, Suburb (varies)
        target_sheet = None
        priority = ['postcode', 'suburb', 'poa']
        for keyword in priority:
            for s in xl.sheet_names:
                if keyword in s.lower():
                    target_sheet = s
                    break
            if target_sheet:
                break

        # Fallback: scan all data sheets for one containing 'postcode' or 'suburb' column
        if not target_sheet:
            for s in xl.sheet_names:
                if s.lower() in ('contents', 'footnotes', 'notes'):
                    continue
                try:
                    probe = pd.read_excel(io.BytesIO(raw), sheet_name=s,
                                          header=None, nrows=5, dtype=str)
                    flat = ' '.join(probe.values.flatten().astype(str)).lower()
                    if 'postcode' in flat or 'suburb' in flat:
                        target_sheet = s
                        print(f"   Found geo data in sheet: '{s}'")
                        break
                except Exception:
                    continue

        if not target_sheet:
            print("   ⚠  No postcode/suburb sheet found in crime file.")
            return pd.DataFrame()

        print(f"   Using sheet: '{target_sheet}'")

        # Detect header row
        raw_df = pd.read_excel(io.BytesIO(raw), sheet_name=target_sheet,
                               header=None, dtype=str, nrows=15)
        header_row = 0
        for ridx, row in raw_df.iterrows():
            vals_lower = ' '.join(str(v).lower() for v in row.values)
            if any(kw in vals_lower for kw in ['postcode', 'suburb', 'offence', 'rate']):
                header_row = ridx
                break

        df = pd.read_excel(io.BytesIO(raw), sheet_name=target_sheet,
                           skiprows=header_row, dtype=str)
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\s+', '_', regex=True)
        print(f"   Columns (first 10): {df.columns.tolist()[:10]}")

        # Find postcode col (preferred) or suburb col
        pc_col      = next((c for c in df.columns if 'postcode' in c), None)
        suburb_col  = next((c for c in df.columns if 'suburb' in c), None)
        rate_col    = next((c for c in df.columns if re.search(r'rate.*100|per.*100', c)), None)
        count_col   = next((c for c in df.columns if re.search(r'offence_count|incident_count|number', c)), None)
        year_col    = next((c for c in df.columns if c == 'year'), None)

        geo_col = pc_col or suburb_col
        if not geo_col:
            print(f"   ⚠  No postcode/suburb column. Cols: {df.columns.tolist()}")
            return pd.DataFrame()

        print(f"   geo_col={geo_col}, rate_col={rate_col}, count_col={count_col}")

        # Filter to most recent year
        if year_col:
            years = pd.to_numeric(df[year_col], errors='coerce')
            df = df[years == years.max()].copy()
            print(f"   Filtered to year: {years.max()}")

        if pc_col:
            df['_pc'] = pd.to_numeric(df[pc_col], errors='coerce').astype('Int64')
            group_col = '_pc'
        else:
            df['_geo'] = df[suburb_col].str.upper().str.strip()
            group_col = '_geo'

        if rate_col:
            df['_val'] = pd.to_numeric(df[rate_col], errors='coerce')
        elif count_col:
            df['_val'] = pd.to_numeric(df[count_col], errors='coerce')
        else:
            print("   ⚠  No rate or count column found.")
            return pd.DataFrame()

        result = df.groupby(group_col)['_val'].sum().round(1)
        result = result[result > 0].rename('crime_rate_per_100k').to_frame()

        if pc_col:
            result.index.name = 'Postcode'
        else:
            result.index.name = 'Suburb'

        print(f"   ✓  {len(result)} areas in crime stats (keyed by {'postcode' if pc_col else 'suburb'}).")
        return result

    except Exception as e:
        print(f"   ⚠  Crime stats load failed: {e}")
        import traceback; traceback.print_exc()
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# 3. PTV GTFS — Distance to Nearest Train Station
# ══════════════════════════════════════════════════════════════════════════════

def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon/2)**2)
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)), 3)


def load_ptv_train_stops() -> pd.DataFrame:
    """
    Returns DataFrame of train station lat/lon from PTV GTFS feed.
    Only Route Type 2 = Rail (metro + regional).
    """
    print("\n[3/3] Loading PTV GTFS train stops …")
    try:
        raw = _download(PTV_GTFS_URL, "ptv_gtfs.zip", timeout=180)
        with zipfile.ZipFile(io.BytesIO(raw)) as outer:
            # PTV zip may contain sub-zips per mode
            sub_zips = [n for n in outer.namelist()
                        if n.endswith('.zip')]
            train_stops_dfs = []

            # Load stops from sub-zips (look for rail/metro) or top-level
            sources = sub_zips if sub_zips else ['']
            for sub in sources:
                try:
                    if sub:
                        inner_bytes = outer.read(sub)
                        inner_zip = zipfile.ZipFile(io.BytesIO(inner_bytes))
                    else:
                        inner_zip = outer

                    if 'stops.txt' not in inner_zip.namelist():
                        continue

                    with inner_zip.open('stops.txt') as f:
                        stops = pd.read_csv(f, dtype=str)

                    stops['stop_lat'] = pd.to_numeric(stops.get('stop_lat'), errors='coerce')
                    stops['stop_lon'] = pd.to_numeric(stops.get('stop_lon'), errors='coerce')
                    stops = stops.dropna(subset=['stop_lat', 'stop_lon'])

                    # Filter for Metro train modes if routes.txt available
                    if 'routes.txt' in inner_zip.namelist():
                        with inner_zip.open('routes.txt') as f:
                            routes = pd.read_csv(f, dtype=str)
                        rail_routes = routes[routes.get('route_type', pd.Series()) == '2']
                        if len(rail_routes) and 'trips.txt' in inner_zip.namelist():
                            with inner_zip.open('trips.txt') as f:
                                trips = pd.read_csv(f, dtype=str)
                            with inner_zip.open('stop_times.txt') as f:
                                st = pd.read_csv(f, dtype=str,
                                                 usecols=['trip_id', 'stop_id'])
                            rail_trips = trips[trips['route_id'].isin(rail_routes['route_id'])]
                            rail_stop_ids = st[st['trip_id'].isin(
                                rail_trips['trip_id'])]['stop_id'].unique()
                            stops = stops[stops['stop_id'].isin(rail_stop_ids)]

                    if len(stops):
                        train_stops_dfs.append(stops[['stop_id', 'stop_name',
                                                       'stop_lat', 'stop_lon']])
                    if sub:
                        inner_zip.close()
                except Exception as inner_e:
                    print(f"      sub-zip {sub}: {inner_e}")

            if not train_stops_dfs:
                print("   ⚠  No train stops found in GTFS feed.")
                return pd.DataFrame()

            all_stops = pd.concat(train_stops_dfs).drop_duplicates('stop_id')
            # Filter to Melbourne bounding box
            all_stops = all_stops[
                (all_stops['stop_lat'].between(-38.6, -37.4)) &
                (all_stops['stop_lon'].between(144.3, 145.6))
            ]
            print(f"   ✓  {len(all_stops)} train stops in Melbourne area.")
            return all_stops.reset_index(drop=True)

    except Exception as e:
        print(f"   ⚠  PTV GTFS load failed: {e}")
        return pd.DataFrame()


def compute_nearest_station(df_props: pd.DataFrame,
                             df_stops: pd.DataFrame) -> pd.Series:
    """
    Vectorised nearest-station lookup.
    Returns Series of distances (km) aligned to df_props index.
    """
    if df_stops.empty:
        return pd.Series(np.nan, index=df_props.index)

    stops_lat = df_stops['stop_lat'].values
    stops_lon = df_stops['stop_lon'].values

    prop_lat = df_props['Latitude'].values
    prop_lon = df_props['Longitude'].values

    distances = []
    # Process in chunks to avoid OOM on large arrays
    CHUNK = 500
    n = len(prop_lat)
    iterator = range(0, n, CHUNK)
    if TQDM:
        iterator = tqdm(iterator, desc="   nearest station", unit="chunk")

    for i in iterator:
        pl = prop_lat[i:i+CHUNK]
        pn = prop_lon[i:i+CHUNK]
        chunk_dists = []
        for lat, lon in zip(pl, pn):
            if np.isnan(lat) or np.isnan(lon):
                chunk_dists.append(np.nan)
                continue
            # Vectorised haversine for all stops
            dlat = np.radians(stops_lat - lat)
            dlon = np.radians(stops_lon - lon)
            a = (np.sin(dlat/2)**2
                 + np.cos(np.radians(lat)) * np.cos(np.radians(stops_lat))
                 * np.sin(dlon/2)**2)
            d = 6371.0 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
            chunk_dists.append(round(float(np.min(d)), 3))
        distances.extend(chunk_dists)

    return pd.Series(distances, index=df_props.index, name='dist_nearest_train_km')


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENRICHMENT PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Enrich Melbourne property data with ABS, crime, and PTV data."
    )
    parser.add_argument('--input',     default='data/melbourne_price_data.csv')
    parser.add_argument('--output',    default='data/melbourne_price_data_enriched.csv')
    parser.add_argument('--cache-dir', default='.cache_enrich',
                        help='Directory for cached downloads. '
                             'Use a path on a drive with free space, e.g. D:\\.cache_enrich')
    args = parser.parse_args()

    # Set global cache directory BEFORE any downloads
    global CACHE_DIR
    CACHE_DIR = Path(args.cache_dir)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Cache dir : {CACHE_DIR.resolve()}")

    print("=" * 65)
    print("  Melbourne Property Enrichment Script")
    print(f"  Input : {args.input}")
    print(f"  Output: {args.output}")
    print("=" * 65)

    # ── Load base property data ───────────────────────────────────────────────
    print("\n[0/4] Loading property data …")
    df = None
    for enc in ('utf-8-sig', 'cp1252', 'latin-1'):
        for sep in ('\t', ','):
            try:
                _df = pd.read_csv(args.input, sep=sep, encoding=enc,
                                  on_bad_lines='skip', low_memory=False)
                # Accept if we get the expected columns
                if 'Postcode' in _df.columns and 'Suburb' in _df.columns:
                    df = _df
                    print(f"   Detected: sep={'TAB' if sep==chr(9) else 'COMMA'}, "
                          f"encoding={enc}")
                    break
            except Exception:
                continue
        if df is not None:
            break

    if df is None:
        raise RuntimeError(
            "Could not load the CSV with any known encoding/separator combo. "
            "Check the file path and format."
        )
    print(f"   ✓  {len(df):,} properties loaded, {len(df.columns)} columns.")
    print(f"   Columns: {df.columns.tolist()[:8]} …")

    df['Postcode'] = pd.to_numeric(df['Postcode'], errors='coerce').astype('Int64')

    # ── Fetch all enrichment sources ─────────────────────────────────────────
    abs_df   = load_abs_g02()           # postcode-indexed DataFrame
    crime_df = load_crime_stats()       # suburb-indexed DataFrame
    stops_df = load_ptv_train_stops()   # all train stops

    # ── Merge ABS (by Postcode) ───────────────────────────────────────────────
    if not abs_df.empty:
        df = df.join(abs_df, on='Postcode', how='left')
        print(f"\n   ABS: filled "
              f"{df['abs_median_income_weekly'].notna().sum():,} rows.")
    else:
        for col in ['abs_median_income_weekly', 'abs_population', 'abs_median_age']:
            df[col] = np.nan

    # ── Merge Crime (by Postcode or Suburb) ──────────────────────────────────
    if not crime_df.empty:
        if crime_df.index.name == 'Postcode':
            df = df.join(crime_df, on='Postcode', how='left')
            print(f"   Crime (by postcode): filled "
                  f"{df['crime_rate_per_100k'].notna().sum():,} rows.")
        else:
            # Suburb-keyed fallback
            df['_suburb_key'] = df['Suburb'].str.upper().str.strip()
            df = df.join(crime_df, on='_suburb_key', how='left')
            df = df.drop(columns=['_suburb_key'])
            print(f"   Crime (by suburb): filled "
                  f"{df['crime_rate_per_100k'].notna().sum():,} rows.")
    else:
        df['crime_rate_per_100k'] = np.nan

    # ── Compute nearest train distance ────────────────────────────────────────
    if not stops_df.empty:
        df['dist_nearest_train_km'] = compute_nearest_station(df, stops_df)
        print(f"\n   PTV: filled "
              f"{df['dist_nearest_train_km'].notna().sum():,} rows.")
    else:
        df['dist_nearest_train_km'] = np.nan

    # ── Add enrichment timestamp ──────────────────────────────────────────────
    df['Enriched_Date'] = datetime.now().strftime('%Y-%m-%d')

    # ── Save output ───────────────────────────────────────────────────────────
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"\n{'='*65}")
    print(f"  ✅  Enriched dataset saved → {out_path}")
    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")
    print(f"  New columns added:")
    new_cols = ['abs_median_income_weekly', 'abs_population', 'abs_median_age',
                'crime_rate_per_100k', 'dist_nearest_train_km', 'Enriched_Date']
    for c in new_cols:
        filled = df[c].notna().sum() if c in df else 0
        print(f"    {c:<35} {filled:>7,} / {len(df):,} filled")
    print('='*65)


if __name__ == '__main__':
    main()