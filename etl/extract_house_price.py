"""
Domain.com.au scraper using Camoufox (Firefox stealth) to bypass Akamai Bot Manager.

Why Camoufox:
- Domain.com.au uses Akamai (errors.edgesuite.net = Akamai EdgeSuite)
- Akamai is significantly harder than Cloudflare; Patchright/Playwright fail
- Camoufox patches Firefox at the C++ layer (not JS hooks), passes JA4 + canvas + WebGL
- Has been tested working against Akamai-protected sites in 2026

Setup (run once):
    pip install camoufox[geoip] pandas beautifulsoup4 numpy
    python -m camoufox fetch

Usage:
    python etl/extract_house_price.py

Important behavioral notes:
- The script does real human-like browsing: visits homepage, scrolls, waits,
  then navigates to search URLs while keeping the same session.
- Akamai cookies (_abck, bm_sz) accumulate trust score over time, so DON'T
  restart browser between cells — keep the same session for the entire run.
- If Akamai blocks after warm-up succeeded, it usually means the access
  pattern is too bot-like (too fast, no scroll, etc.) — increase delays.
"""

import os
import sys
import json
import time
import math
import random
import re
import pandas as pd
import numpy as np
from camoufox.sync_api import Camoufox
from playwright.sync_api import TimeoutError as PlaywrightTimeout

print("STARTING SCRAPER (Camoufox vs Akamai)")

# ==========================================
# CONFIGURATION
# ==========================================
FILE_NAME = 'data/melbourne_price_data.csv'
GRID_SIZE = 14
LAT_NORTH, LAT_SOUTH = -37.5, -38.5
LNG_WEST, LNG_EAST = 144.35, 145.40

# Headless: false locally (better fingerprint), true on CI
HEADLESS = os.getenv('HEADLESS', 'false').lower() == 'true'

# Optional proxy — set PROXY_URL env var to enable, e.g.
#   PROXY_URL='http://user:pass@proxy.example.com:8080'
PROXY_URL = os.getenv('PROXY_URL')

NAV_TIMEOUT_MS = 45_000  # Akamai challenges can take 5-10s
CELL_MAX_STRIKES = 3
EARLY_ABORT_CELLS = 5
DELAY_BETWEEN_REQUESTS = (4.0, 8.0)  # seconds, Akamai is strict on rate

SEARCH_MODES = [
    ('For Sale', 'sale', 'excludeunderoffer=1'),
    ('Sold', 'sold-listings', ''),
]

# ==========================================
# HELPERS — price parsing, distance, save
# ==========================================
def parse_raw_price(raw_price):
    if not isinstance(raw_price, str) or not raw_price.strip():
        return np.nan
    normalized = raw_price.lower().strip()
    if '$' not in normalized:
        return np.nan
    normalized = re.sub(r'([.,])(\d{4,})', lambda m: m.group(1) + m.group(2)[:3], normalized)
    normalized = normalized.replace(',', '').replace('–', '-').replace(' to ', '-')
    matches = re.findall(r'\$\s*(\d+\.?\d*[km]?)', normalized)
    if not matches:
        return np.nan
    parsed_vals = []
    for val_str in matches:
        mult = 1
        if val_str.endswith('m'):
            mult = 1_000_000
            val_str = val_str[:-1]
        elif val_str.endswith('k'):
            num = float(val_str[:-1])
            if num < 1000:
                mult = 1000
            val_str = val_str[:-1]
        try:
            parsed_vals.append(float(val_str) * mult)
        except ValueError:
            pass
    if not parsed_vals:
        return np.nan
    if 'fhog' in normalized:
        return parsed_vals[0]
    if len(parsed_vals) >= 2 and '-' in normalized:
        return (parsed_vals[0] + parsed_vals[1]) / 2
    return parsed_vals[0]


def calculate_distance_to_cbd(lat2, lon2):
    if pd.isna(lat2) or pd.isna(lon2):
        return np.nan
    lat1, lon1 = -37.8136, 144.9631
    R = 6371.0
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlon, dlat = lon2_rad - lon1_rad, lat2_rad - lat1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)


def human_delay(min_sec, max_sec):
    """Sleep with random duration."""
    time.sleep(random.uniform(min_sec, max_sec))


def save_incremental_data(new_data_list, file_path):
    if not new_data_list:
        return
    df_new = pd.DataFrame(new_data_list)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        try:
            df_old = pd.read_csv(file_path)
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_final = df_combined.drop_duplicates(subset=['Property_ID'], keep='last')
            suburb_counts = df_final.groupby('Suburb')['Property_ID'].transform('count')
            df_final['Propertycount'] = suburb_counts
            df_final.to_csv(file_path, index=False, encoding='utf-8-sig')
        except pd.errors.EmptyDataError:
            df_new.to_csv(file_path, index=False, encoding='utf-8-sig')
    else:
        df_new['Propertycount'] = df_new.groupby('Suburb')['Property_ID'].transform('count')
        df_new.to_csv(file_path, index=False, encoding='utf-8-sig')


# ==========================================
# BEHAVIORAL SIMULATION — fool Akamai's behavior analysis
# ==========================================
def simulate_human_behavior(page):
    """
    Light behavior sim — just scroll. We let Camoufox's built-in humanize=True
    handle cursor movement (mouse.move() can deadlock with humanize).
    """
    try:
        scroll_distance = random.randint(200, 800)
        page.evaluate(f"window.scrollBy(0, {scroll_distance})")
        human_delay(0.5, 1.2)
        if random.random() < 0.3:
            page.evaluate(f"window.scrollBy(0, -{random.randint(50, 200)})")
            human_delay(0.3, 0.6)
    except Exception as e:
        print(f"   (behavior sim skipped: {e})")


# ==========================================
# PAYLOAD PARSING — extract from __NEXT_DATA__
# ==========================================
def parse_listings_payload(payload, status_label, seen_records):
    props = payload.get('props', {}).get('pageProps', {}).get('componentProps', {})
    listings = props.get('listingsMap', {})
    total_pages = props.get('totalPages', 1)
    records = []

    for pid, item in listings.items():
        pid_str = str(pid)
        m = item.get('listingModel', {})
        raw_price = str(m.get('price', 'N/A'))

        if pid_str in seen_records:
            old = seen_records[pid_str]
            if old['Status'] == status_label and old['Price'] == raw_price:
                continue

        a = m.get('address', {})
        f = m.get('features', {})
        street = a.get('street', 'N/A')
        suburb = str(a.get('suburb', 'Map Area')).upper()
        postcode = a.get('postcode', '')
        url_path = m.get('url', '')
        lat = a.get('lat', m.get('geolocation', {}).get('latitude'))
        lng = a.get('lng', m.get('geolocation', {}).get('longitude'))

        full_address = f"{street}, {suburb} VIC {postcode}".strip() if street != 'N/A' else None
        if not full_address:
            continue

        date_val = m.get('dateSold', m.get('dateListed'))
        if not date_val:
            date_val = m.get('status', {}).get('date')
        if not date_val:
            item_str = json.dumps(item)
            date_match = re.search(r'([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})', item_str)
            if date_match:
                date_val = date_match.group(1)
            else:
                iso_match = re.search(r'"[A-Za-z]*[dD]ate[A-Za-z]*"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})', item_str)
                if iso_match:
                    date_val = iso_match.group(1)
        if not date_val:
            date_val = 'N/A'

        full_url = f"https://www.domain.com.au{url_path}" if url_path else "N/A"

        raw_land_size = f.get('landSize', np.nan)
        land_unit = str(f.get('landUnit', '')).lower()
        try:
            if pd.notna(raw_land_size):
                raw_land_size = float(raw_land_size)
                if 'ha' in land_unit or 'hectare' in land_unit:
                    raw_land_size = raw_land_size * 10000
        except Exception:
            pass

        record = {
            'Property_ID': pid_str,
            'Status': status_label,
            'Full_Address': full_address,
            'Suburb': suburb,
            'Postcode': postcode,
            'Property_Type': f.get('propertyTypeFormatted', f.get('propertyType', 'N/A')),
            'Date': date_val,
            'Beds': f.get('beds', 0),
            'Baths': f.get('baths', 0),
            'Car_Spaces': f.get('parking', f.get('carspaces', 0)),
            'LandSize_sqm': raw_land_size,
            'Propertycount': np.nan,
            'Raw_Price': raw_price,
            'Numeric_Price': parse_raw_price(raw_price),
            'Latitude': lat,
            'Longitude': lng,
            'Distance_to_CBD_km': calculate_distance_to_cbd(lat, lng),
            'URL': full_url,
            'Last_Updated': pd.Timestamp.now().strftime('%Y-%m-%d'),
        }
        records.append(record)
        seen_records[pid_str] = {'Status': status_label, 'Price': raw_price}

    return records, total_pages


# ==========================================
# NAVIGATION WITH AKAMAI HANDLING
# ==========================================
def is_access_denied(page):
    """Check if Akamai returned 'Access Denied' page."""
    try:
        title = (page.title() or '').lower()
        if 'access denied' in title:
            return True
        # Akamai sometimes shows challenge inline; check body
        body = page.evaluate("() => document.body ? document.body.innerText.slice(0, 300) : ''")
        if 'access denied' in body.lower() or 'pardon our interruption' in body.lower():
            return True
    except Exception:
        pass
    return False


def get_next_data(page, url):
    """
    Navigate to URL, handle Akamai, extract __NEXT_DATA__ JSON.
    Returns dict or None.
    """
    try:
        page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
    except PlaywrightTimeout:
        return None
    except Exception as e:
        print(f"   ⚠️ Navigation error: {e}")
        return None

    # Give Akamai challenge time to resolve
    try:
        page.wait_for_load_state('networkidle', timeout=20_000)
    except PlaywrightTimeout:
        pass

    # Check for Access Denied
    if is_access_denied(page):
        return None

    # Simulate browsing behavior (helps trust score)
    simulate_human_behavior(page)

    # Extract __NEXT_DATA__
    try:
        content = page.evaluate(
            "() => { const t = document.getElementById('__NEXT_DATA__'); return t ? t.textContent : null; }"
        )
    except Exception:
        return None

    if not content:
        return None
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return None


# ==========================================
# MAIN
# ==========================================
def main():
    # Load existing data
    seen_records = {}
    if os.path.exists(FILE_NAME) and os.path.getsize(FILE_NAME) > 0:
        try:
            df_existing = pd.read_csv(FILE_NAME)
            for _, row in df_existing.iterrows():
                pid = str(row.get('Property_ID', ''))
                seen_records[pid] = {
                    'Status': str(row.get('Status', '')),
                    'Price': str(row.get('Raw_Price', '')),
                }
            print(f"   📚 Loaded {len(seen_records)} existing records for dedup")
        except pd.errors.EmptyDataError:
            pass

    lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
    lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE
    total_cells = GRID_SIZE * GRID_SIZE
    total_records = 0
    cells_with_any_data = 0
    cells_completely_failed = 0
    cell_idx = 0

    # Camoufox config — Australian locale + viewport.
    # humanize=False because it can deadlock in headful mode on Windows
    # when there's no real mouse interaction; we simulate behavior via scroll only.
    camoufox_kwargs = {
        'headless': HEADLESS,
        'humanize': False,
        'locale': 'en-AU',
        'os': 'windows',
    }
    if PROXY_URL:
        camoufox_kwargs['proxy'] = {'server': PROXY_URL}
        camoufox_kwargs['geoip'] = True  # only useful with a proxy
        print(f"   🛡️ Using proxy: {PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL}")

    with Camoufox(**camoufox_kwargs) as browser:
        page = browser.new_page()

        # Warm-up sequence: visit homepage, browse a bit, then start searching
        print("   🌐 Warm-up: visiting homepage...")
        try:
            page.goto('https://www.domain.com.au/', timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
            try:
                page.wait_for_load_state('networkidle', timeout=20_000)
            except PlaywrightTimeout:
                pass

            if is_access_denied(page):
                print("   ❌ Even homepage blocked. Akamai is rejecting this IP/fingerprint.")
                print("   Try: residential proxy via PROXY_URL env var, or use a paid scraping API.")
                sys.exit(1)

            print(f"   ✅ Homepage: '{page.title()[:60]}'")

            # Browse for a few seconds to build trust score
            print("   🧍 Simulating browsing behavior...")
            for k in range(3):
                print(f"      step {k+1}/3: scroll...")
                simulate_human_behavior(page)
                print(f"      step {k+1}/3: wait...")
                human_delay(1.5, 3.0)
            print("   ✅ Behavior simulation done")

            # Verify Akamai cookie is set
            cookies = page.context.cookies()
            cookie_names = [c['name'] for c in cookies]
            has_abck = '_abck' in cookie_names
            has_bm_sz = 'bm_sz' in cookie_names
            print(f"   🍪 Akamai cookies: _abck={has_abck}, bm_sz={has_bm_sz}")
            if not has_abck:
                print("   ⚠️ _abck cookie missing — Akamai may not trust this session yet")

        except Exception as e:
            print(f"   ❌ Warm-up failed: {e}")
            sys.exit(1)

        human_delay(3.0, 6.0)

        # Main scraping loop
        for i in range(GRID_SIZE):
            for j in range(GRID_SIZE):
                cell_idx += 1
                t_lat = round(LAT_NORTH - (i * lat_step), 4)
                b_lat = round(LAT_NORTH - ((i + 1) * lat_step), 4)
                l_lng = round(LNG_WEST + (j * lng_step), 4)
                r_lng = round(LNG_WEST + ((j + 1) * lng_step), 4)
                print(f"\n📍 Cell [{cell_idx}/{total_cells}] | {t_lat},{l_lng} → {b_lat},{r_lng}")

                cell_records = 0
                cell_strikes = 0

                for status_label, mode_path, mode_extra in SEARCH_MODES:
                    if cell_strikes >= CELL_MAX_STRIKES:
                        break

                    base_query = f"startloc={t_lat}%2C{l_lng}&endloc={b_lat}%2C{r_lng}"
                    if mode_extra:
                        base_query = f"{mode_extra}&{base_query}"

                    page1_url = f"https://www.domain.com.au/{mode_path}/?{base_query}&page=1"
                    payload = get_next_data(page, page1_url)
                    if payload is None:
                        cell_strikes += 1
                        print(f"   ⚠️ Failed on {status_label} page 1 (strike {cell_strikes}/{CELL_MAX_STRIKES})")
                        human_delay(*DELAY_BETWEEN_REQUESTS)
                        continue

                    page_records, total_pages = parse_listings_payload(payload, status_label, seen_records)
                    if page_records:
                        save_incremental_data(page_records, FILE_NAME)
                        cell_records += len(page_records)
                        print(f"   + {len(page_records)} records ({status_label}, page 1/{total_pages})")

                    for pg in range(2, total_pages + 1):
                        if cell_strikes >= CELL_MAX_STRIKES:
                            break
                        human_delay(*DELAY_BETWEEN_REQUESTS)
                        url = f"https://www.domain.com.au/{mode_path}/?{base_query}&page={pg}"
                        payload = get_next_data(page, url)
                        if payload is None:
                            cell_strikes += 1
                            print(f"   ⚠️ Failed on {status_label} page {pg} (strike {cell_strikes}/{CELL_MAX_STRIKES})")
                            continue
                        page_records, _ = parse_listings_payload(payload, status_label, seen_records)
                        if page_records:
                            save_incremental_data(page_records, FILE_NAME)
                            cell_records += len(page_records)
                            print(f"   + {len(page_records)} records ({status_label}, page {pg}/{total_pages})")

                    human_delay(*DELAY_BETWEEN_REQUESTS)

                if cell_records > 0:
                    cells_with_any_data += 1
                    total_records += cell_records
                else:
                    cells_completely_failed += 1

                # Early abort on systemic block
                if cell_idx == EARLY_ABORT_CELLS and cells_with_any_data == 0:
                    print(f"\n❌ SYSTEMIC BLOCK: {EARLY_ABORT_CELLS} cells failed.")
                    print("   Akamai is rejecting all search requests despite Camoufox.")
                    print("   Next step: try a residential proxy via PROXY_URL env var.")
                    sys.exit(1)

    print(f"\n✅ SCRAPING COMPLETED")
    print(f"   Cells with data: {cells_with_any_data}/{total_cells}")
    print(f"   Cells failed:    {cells_completely_failed}/{total_cells}")
    print(f"   New records:     {total_records}")

    if total_records == 0 and len(seen_records) == 0:
        print("❌ Zero records scraped — failing job")
        sys.exit(1)


if __name__ == '__main__':
    main()