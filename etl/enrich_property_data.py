"""
enrich_melbourne_data.py
========================
Enriches the Melbourne property dataset with:
  1. ABS Census 2021 — median income, population, median age by postcode
  2. Victoria Crime Statistics — offence count + rate by postcode
  3. PTV GTFS — distance to nearest train station (lat/lon)

Run:
    python enrich_melbourne_data.py \
        --input  data/melbourne_price_data.csv \
        --output data/melbourne_price_data_enriched.csv

    # If C: drive is full, put cache on another drive:
    python enrich_melbourne_data.py --cache-dir "D:\\.cache_enrich"

Safe to rerun: caches downloads for 6 days.
Delete .cache_enrich/ to force a full re-download.

Dependencies (add to requirements.txt):
    pandas requests openpyxl tqdm
"""

import argparse
import io
import math
import re
import time
import zipfile
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests

try:
    from tqdm import tqdm
    TQDM = True
except ImportError:
    TQDM = False


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS  (CACHE_DIR is overridden in main() via --cache-dir)
# ══════════════════════════════════════════════════════════════════════════════

CACHE_DIR = Path(".cache_enrich")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# ABS Census 2021 GCP — Postcode (POA) only for VIC — ~3 MB
ABS_G02_URL = (
    "https://www.abs.gov.au/census/find-census-data/datapacks/download/"
    "2021_GCP_POA_for_VIC_short-header.zip"
)

# Crime Statistics Agency Victoria — LGA Recorded Offences, Year Ending Sep 2024
# Table 03 inside has: Year, Postcode, Suburb/Town Name, Offence Count
CRIME_STATS_URL = (
    "https://files.crimestatistics.vic.gov.au/2025-03/"
    "Data_Tables_LGA_Recorded_Offences_Year_Ending_September_2024.xlsx"
)

# PTV GTFS static feed — all Melbourne public transport modes
PTV_GTFS_URL = "https://data.ptv.vic.gov.au/downloads/gtfs.zip"

CACHE_MAX_DAYS = 6


# ══════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _cache_path(name):
    return CACHE_DIR / name


def _cache_fresh(name):
    p = _cache_path(name)
    if not p.exists():
        return False
    age = datetime.now() - datetime.fromtimestamp(p.stat().st_mtime)
    return age.days < CACHE_MAX_DAYS


def _load_cache(name):
    return _cache_path(name).read_bytes()


def _download(url, cache_name, timeout=60, retries=3):
    """Download with caching, retry, and chunk-streaming."""
    if _cache_fresh(cache_name):
        print(f"   [cache] {cache_name}")
        return _load_cache(cache_name)

    print(f"   [download] {url}")
    cache_file = _cache_path(cache_name)

    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, stream=True)
            r.raise_for_status()
            with open(cache_file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            return cache_file.read_bytes()
        except Exception as e:
            if cache_file.exists():
                cache_file.unlink()
            wait = 5 * (attempt + 1)
            print(f"   Attempt {attempt+1}/{retries} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed to download {url} after {retries} attempts")


# ══════════════════════════════════════════════════════════════════════════════
# 1. ABS CENSUS 2021
# ══════════════════════════════════════════════════════════════════════════════

def load_abs_g02():
    print("\n[1/3] Loading ABS Census G02 + G01 ...")
    try:
        raw = _download(ABS_G02_URL, "abs_poa_vic.zip", timeout=300)

        def _read_poa_csv(pattern):
            with zipfile.ZipFile(io.BytesIO(raw)) as outer:
                all_files = outer.namelist()
                matches = [n for n in all_files
                           if re.search(pattern, n) and 'POA' in n and n.endswith('.csv')]
                if matches:
                    with outer.open(matches[0]) as f:
                        return pd.read_csv(f, dtype=str)
                poa_zips = [n for n in all_files if 'POA' in n and n.endswith('.zip')]
                for pz in poa_zips:
                    with zipfile.ZipFile(io.BytesIO(outer.read(pz))) as inner:
                        inner_matches = [n for n in inner.namelist()
                                         if re.search(pattern, n) and n.endswith('.csv')]
                        if inner_matches:
                            with inner.open(inner_matches[0]) as f:
                                return pd.read_csv(f, dtype=str)
            return None

        df_g02 = _read_poa_csv(r'G02')
        df_g01 = _read_poa_csv(r'G01')

        if df_g02 is None:
            print("   WARNING: G02 CSV not found in ABS zip - skipping ABS.")
            return pd.DataFrame()

        print(f"   G02 columns: {df_g02.columns.tolist()[:10]}")

        g02_map = {}
        for c in df_g02.columns:
            cl = c.lower()
            if cl.startswith('poa_code') or cl == 'region_id':
                g02_map[c] = '_postcode_raw'
            elif cl in ('median_tot_prsnl_inc_weekly', 'med_tot_prsnl_inc_wk'):
                g02_map[c] = 'abs_median_income_weekly'
            elif 'median' in cl and ('prsnl' in cl or 'personal' in cl) and 'inc' in cl:
                g02_map[c] = 'abs_median_income_weekly'
            elif cl in ('median_age_persons', 'med_age_persons'):
                g02_map[c] = 'abs_median_age'
            elif 'median' in cl and 'age' in cl:
                g02_map[c] = 'abs_median_age'

        df_g02 = df_g02.rename(columns=g02_map)
        df_g02 = df_g02.loc[:, ~df_g02.columns.duplicated(keep='first')]

        if '_postcode_raw' not in df_g02.columns:
            print(f"   WARNING: Postcode column not mapped. Cols: {df_g02.columns.tolist()}")
            return pd.DataFrame()

        df_g02['_postcode_raw'] = df_g02['_postcode_raw'].str.extract(r'(\d{4})')
        df_g02 = df_g02.dropna(subset=['_postcode_raw'])
        df_g02['Postcode'] = df_g02['_postcode_raw'].astype(int)

        for col in ['abs_median_income_weekly', 'abs_median_age']:
            if col in df_g02.columns:
                df_g02[col] = pd.to_numeric(df_g02[col], errors='coerce')
            else:
                df_g02[col] = np.nan

        result = df_g02[['Postcode', 'abs_median_income_weekly', 'abs_median_age']].copy()
        result = result.set_index('Postcode')

        if df_g01 is not None:
            print(f"   G01 columns (first 15): {df_g01.columns.tolist()[:15]}")
            g01_pc_col  = next((c for c in df_g01.columns
                                if c.lower().startswith('poa_code') or c.lower() == 'region_id'), None)
            g01_pop_col = next((c for c in df_g01.columns if c.lower() == 'tot_p_p'), None)
            if not g01_pop_col:
                g01_pop_col = next((c for c in df_g01.columns
                                    if re.search(r'^tot_p', c.lower())), None)
            print(f"   G01 pop col: {g01_pop_col}")
            if g01_pc_col and g01_pop_col:
                pop = df_g01[[g01_pc_col, g01_pop_col]].copy()
                pop['Postcode'] = (pop[g01_pc_col].str.extract(r'(\d{4})')
                                   .astype(float).astype('Int64'))
                pop['abs_population'] = pd.to_numeric(pop[g01_pop_col], errors='coerce')
                pop = pop[['Postcode', 'abs_population']].dropna().set_index('Postcode')
                result = result.join(pop, how='left')
                print(f"   Population joined for {result['abs_population'].notna().sum()} postcodes.")
            else:
                result['abs_population'] = np.nan
        else:
            print("   WARNING: G01 not found - population will be NaN.")
            result['abs_population'] = np.nan

        print(f"   OK: {len(result)} postcodes loaded from ABS.")
        return result

    except Exception as e:
        print(f"   ERROR in ABS load: {e}")
        import traceback; traceback.print_exc()
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# 2. VICTORIA CRIME STATISTICS
# ══════════════════════════════════════════════════════════════════════════════

def load_crime_stats():
    """
    Returns DataFrame indexed by Postcode (Int64) with columns:
        crime_offence_count  — total recorded offences (latest year)
        crime_suburb_ref     — most common suburb name for that postcode

    crime_rate_per_100k is computed in main() after joining ABS population.
    """
    print("\n[2/3] Loading Victoria Crime Statistics ...")
    try:
        raw = _download(CRIME_STATS_URL, "vic_crime_lga.xlsx", timeout=120)
        xl = pd.ExcelFile(io.BytesIO(raw))
        print(f"   Sheets: {xl.sheet_names}")

        # Find sheet whose header row 0 contains 'Postcode'
        target_sheet = None
        for s in xl.sheet_names:
            try:
                probe = pd.read_excel(io.BytesIO(raw), sheet_name=s,
                                      header=None, nrows=1, dtype=str)
                if 'postcode' in ' '.join(probe.iloc[0].astype(str).str.lower()):
                    target_sheet = s
                    break
            except Exception:
                continue

        if not target_sheet:
            print("   WARNING: No postcode-level sheet found in crime file.")
            return pd.DataFrame()

        print(f"   Using sheet: '{target_sheet}'")
        df = pd.read_excel(io.BytesIO(raw), sheet_name=target_sheet, dtype=str)
        df.columns = df.columns.str.strip()
        print(f"   Columns: {df.columns.tolist()}")
        print(f"   Total rows: {len(df):,}")

        pc_col     = next((c for c in df.columns if c.lower() == 'postcode'), None)
        year_col   = next((c for c in df.columns if c.lower() == 'year'), None)
        suburb_col = next((c for c in df.columns
                           if 'suburb' in c.lower() or 'town' in c.lower()), None)
        count_col  = next((c for c in df.columns
                           if 'offence count' in c.lower() or 'incident count' in c.lower()), None)

        print(f"   pc_col={pc_col}, year_col={year_col}, count_col={count_col}")

        if not pc_col or not count_col:
            print("   WARNING: Required columns not found - skipping crime data.")
            return pd.DataFrame()

        if year_col:
            df[year_col] = pd.to_numeric(df[year_col], errors='coerce')
            latest_year = int(df[year_col].max())
            df = df[df[year_col] == latest_year].copy()
            print(f"   Filtered to year {latest_year}: {len(df):,} rows")

        df[pc_col]    = pd.to_numeric(df[pc_col], errors='coerce').astype('Int64')
        df[count_col] = pd.to_numeric(df[count_col], errors='coerce')
        df = df.dropna(subset=[pc_col, count_col])

        result = (df.groupby(pc_col)[count_col]
                  .sum().round(0)
                  .rename('crime_offence_count')
                  .to_frame())
        result = result[result['crime_offence_count'] > 0]
        result.index.name = 'Postcode'

        if suburb_col:
            top_suburb = (df.groupby(pc_col)[suburb_col]
                          .agg(lambda x: x.value_counts().index[0] if len(x) > 0 else '')
                          .rename('crime_suburb_ref'))
            result = result.join(top_suburb)

        print(f"   OK: {len(result):,} postcodes with crime data.")
        return result

    except Exception as e:
        print(f"   ERROR in crime stats: {e}")
        import traceback; traceback.print_exc()
        return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# 3. PTV GTFS — Distance to Nearest Train Station
# ══════════════════════════════════════════════════════════════════════════════

def load_ptv_train_stops():
    print("\n[3/3] Loading PTV GTFS train stops ...")
    try:
        raw = _download(PTV_GTFS_URL, "ptv_gtfs.zip", timeout=180)
        with zipfile.ZipFile(io.BytesIO(raw)) as outer:
            sub_zips = [n for n in outer.namelist() if n.endswith('.zip')]
            train_stops_dfs = []

            for sub in (sub_zips if sub_zips else ['']):
                try:
                    inner_zip = zipfile.ZipFile(io.BytesIO(outer.read(sub))) if sub else outer
                    if 'stops.txt' not in inner_zip.namelist():
                        continue

                    with inner_zip.open('stops.txt') as f:
                        stops = pd.read_csv(f, dtype=str)
                    stops['stop_lat'] = pd.to_numeric(stops.get('stop_lat'), errors='coerce')
                    stops['stop_lon'] = pd.to_numeric(stops.get('stop_lon'), errors='coerce')
                    stops = stops.dropna(subset=['stop_lat', 'stop_lon'])

                    if 'routes.txt' in inner_zip.namelist():
                        with inner_zip.open('routes.txt') as f:
                            routes = pd.read_csv(f, dtype=str)
                        rail_routes = routes[routes.get('route_type', pd.Series()) == '2']
                        if len(rail_routes) and 'trips.txt' in inner_zip.namelist():
                            with inner_zip.open('trips.txt') as f:
                                trips = pd.read_csv(f, dtype=str)
                            with inner_zip.open('stop_times.txt') as f:
                                st = pd.read_csv(f, dtype=str, usecols=['trip_id', 'stop_id'])
                            rail_trips = trips[trips['route_id'].isin(rail_routes['route_id'])]
                            rail_stop_ids = st[st['trip_id'].isin(rail_trips['trip_id'])]['stop_id'].unique()
                            stops = stops[stops['stop_id'].isin(rail_stop_ids)]

                    if len(stops):
                        train_stops_dfs.append(stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']])
                    if sub:
                        inner_zip.close()
                except Exception as inner_e:
                    print(f"      sub-zip {sub}: {inner_e}")

            if not train_stops_dfs:
                print("   WARNING: No train stops found.")
                return pd.DataFrame()

            all_stops = pd.concat(train_stops_dfs).drop_duplicates('stop_id')
            all_stops = all_stops[
                all_stops['stop_lat'].between(-38.6, -37.4) &
                all_stops['stop_lon'].between(144.3, 145.6)
            ]
            print(f"   OK: {len(all_stops)} train stops in Melbourne area.")
            return all_stops.reset_index(drop=True)

    except Exception as e:
        print(f"   ERROR in PTV load: {e}")
        return pd.DataFrame()


def compute_nearest_station(df_props, df_stops):
    if df_stops.empty:
        return pd.Series(np.nan, index=df_props.index)

    stops_lat = df_stops['stop_lat'].values
    stops_lon = df_stops['stop_lon'].values
    prop_lat  = df_props['Latitude'].values
    prop_lon  = df_props['Longitude'].values

    distances = []
    CHUNK = 500
    iterator = range(0, len(prop_lat), CHUNK)
    if TQDM:
        iterator = tqdm(iterator, desc="   nearest station", unit="chunk")

    for i in iterator:
        for lat, lon in zip(prop_lat[i:i+CHUNK], prop_lon[i:i+CHUNK]):
            if np.isnan(lat) or np.isnan(lon):
                distances.append(np.nan)
                continue
            dlat = np.radians(stops_lat - lat)
            dlon = np.radians(stops_lon - lon)
            a = (np.sin(dlat/2)**2
                 + np.cos(np.radians(lat)) * np.cos(np.radians(stops_lat))
                 * np.sin(dlon/2)**2)
            d = 6371.0 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
            distances.append(round(float(np.min(d)), 3))

    return pd.Series(distances, index=df_props.index, name='dist_nearest_train_km')


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',     default='data/melbourne_price_data.csv')
    parser.add_argument('--output',    default='data/melbourne_price_data_enriched.csv')
    parser.add_argument('--cache-dir', default='.cache_enrich',
                        help='Cache dir. Use another drive if C: is full: --cache-dir "D:\\.cache_enrich"')
    args = parser.parse_args()

    global CACHE_DIR
    CACHE_DIR = Path(args.cache_dir)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Cache dir : {CACHE_DIR.resolve()}")

    print("=" * 65)
    print("  Melbourne Property Enrichment Script")
    print(f"  Input : {args.input}")
    print(f"  Output: {args.output}")
    print("=" * 65)

    # Load base data
    print("\n[0/3] Loading property data ...")
    df = None
    for enc in ('utf-8-sig', 'cp1252', 'latin-1'):
        for sep in (',', '\t'):
            try:
                _df = pd.read_csv(args.input, sep=sep, encoding=enc,
                                  on_bad_lines='skip', low_memory=False)
                if 'Postcode' in _df.columns and 'Suburb' in _df.columns:
                    df = _df
                    print(f"   Detected: sep={'TAB' if sep == chr(9) else 'COMMA'}, encoding={enc}")
                    break
            except Exception:
                continue
        if df is not None:
            break

    if df is None:
        raise RuntimeError("Could not load CSV. Check the file path and format.")

    print(f"   OK: {len(df):,} properties loaded, {len(df.columns)} columns.")
    df['Postcode'] = pd.to_numeric(df['Postcode'], errors='coerce').astype('Int64')

    # Enrich
    abs_df   = load_abs_g02()
    crime_df = load_crime_stats()
    stops_df = load_ptv_train_stops()

    # Merge ABS
    if not abs_df.empty:
        df = df.join(abs_df, on='Postcode', how='left')
        print(f"\n   ABS: filled {df['abs_median_income_weekly'].notna().sum():,} rows.")
    else:
        for col in ['abs_median_income_weekly', 'abs_median_age', 'abs_population']:
            df[col] = np.nan

    # Merge Crime
    if not crime_df.empty:
        df = df.join(crime_df, on='Postcode', how='left')
        df['crime_rate_per_100k'] = np.nan
        mask = (df['abs_population'].notna() & df['crime_offence_count'].notna()
                & (df['abs_population'] > 0))
        df.loc[mask, 'crime_rate_per_100k'] = (
            df.loc[mask, 'crime_offence_count'] / df.loc[mask, 'abs_population'] * 100000
        ).round(1)
        print(f"   Crime: filled {df['crime_offence_count'].notna().sum():,} rows "
              f"({df['crime_rate_per_100k'].notna().sum():,} with rate/100k).")
    else:
        df['crime_offence_count'] = np.nan
        df['crime_rate_per_100k'] = np.nan

    # Nearest train
    if not stops_df.empty:
        df['dist_nearest_train_km'] = compute_nearest_station(df, stops_df)
        print(f"\n   PTV: filled {df['dist_nearest_train_km'].notna().sum():,} rows.")
    else:
        df['dist_nearest_train_km'] = np.nan

    df['Enriched_Date'] = datetime.now().strftime('%Y-%m-%d')

    # Save
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')

    print(f"\n{'='*65}")
    print(f"  DONE: Saved -> {out_path}")
    print(f"  Rows: {len(df):,}  |  Columns: {len(df.columns)}")
    print("  New columns:")
    new_cols = [
        'abs_median_income_weekly', 'abs_median_age', 'abs_population',
        'crime_offence_count', 'crime_rate_per_100k',
        'dist_nearest_train_km', 'Enriched_Date',
    ]
    for c in new_cols:
        filled = df[c].notna().sum() if c in df.columns else 0
        print(f"    {c:<35} {filled:>7,} / {len(df):,} filled")
    print('='*65)


if __name__ == '__main__':
    main()