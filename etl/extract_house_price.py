"""
Domain.com.au scraper — FREE DAILY-BATCH STRATEGY (v3)

Config:
- 28 cells per run × 7 days = 196 cells covered weekly
- 5 pages per query (with 30% random abandon at page 4+)
- humanize=False, no mouse.move (causes hangs)
- Multi-step warm-up to build Akamai trust

Schedule:
  Day 0: cells 0, 7, 14, 21, ...   (every 7th cell)
  Day 1: cells 1, 8, 15, 22, ...
  ...
  Day 6: cells 6, 13, 20, ...
  Day 7: back to cells 0, 7, 14, ... (weekly refresh)

Manual override (env vars):
  MANUAL_OFFSET=5         → use group 5
  MANUAL_OFFSET=random    → random group 0-6
  CELLS_PER_RUN=5         → limit to 5 cells for testing
"""

import os
import sys
import json
import time
import math
import random
import re
import datetime as dt
import pandas as pd
import numpy as np
from camoufox.sync_api import Camoufox
from playwright.sync_api import TimeoutError as PlaywrightTimeout

print("STARTING SCRAPER (Free daily-batch strategy v3)")

# ==========================================
# CONFIGURATION
# ==========================================
FILE_NAME = 'data/melbourne_price_data.csv'
GRID_SIZE = 14
LAT_NORTH, LAT_SOUTH = -37.5, -38.5
LNG_WEST, LNG_EAST = 144.35, 145.40

HEADLESS = os.getenv('HEADLESS', 'false').lower() == 'true'
PROXY_URL = os.getenv('PROXY_URL')   # optional

NAV_TIMEOUT_MS = 45_000

# Daily slice size — 28 cells × 7 days = 196 total
CELLS_PER_RUN = int(os.getenv('CELLS_PER_RUN', '28'))
ROTATION_STRIDE = 7

# Pacing
DELAY_BETWEEN_REQUESTS = (10.0, 22.0)
PAGES_BEFORE_REST = 10
REST_DURATION = (90.0, 180.0)

# Pages per query — 5 with random abandon at deep pages
MAX_PAGES_PER_QUERY = 5
DEEP_PAGE_ABANDON_PROB = 0.3   # 30% chance to stop after page 3

# Cell strike budget
CELL_MAX_STRIKES = 2

# Stop early if too many consecutive blocks
MAX_CONSECUTIVE_BLOCKS = 3

# Real suburb names for warm-up
WARMUP_SUBURBS = [
    'richmond-vic-3121', 'st-kilda-vic-3182', 'brunswick-vic-3056',
    'fitzroy-vic-3065', 'south-yarra-vic-3141', 'carlton-vic-3053',
    'footscray-vic-3011', 'brighton-vic-3186', 'hawthorn-vic-3122',
    'prahran-vic-3181', 'collingwood-vic-3066', 'northcote-vic-3070',
]

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
# BEHAVIORAL SIMULATION — scroll only
# ==========================================
def simulate_human_behavior(page):
    """Random scroll. No mouse.move (hangs with Camoufox)."""
    try:
        for _ in range(random.randint(1, 3)):
            scroll_distance = random.randint(200, 900)
            page.evaluate(f"window.scrollBy(0, {scroll_distance})")
            human_delay(0.6, 1.8)
        if random.random() < 0.4:
            page.evaluate(f"window.scrollBy(0, -{random.randint(100, 400)})")
            human_delay(0.4, 1.0)
    except Exception:
        pass


# ==========================================
# PAYLOAD PARSING
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
# NAVIGATION + AKAMAI HANDLING
# ==========================================
def is_access_denied(page):
    try:
        title = (page.title() or '').lower()
        if 'access denied' in title or 'pardon our interruption' in title:
            return True
        body = page.evaluate("() => document.body ? document.body.innerText.slice(0, 300) : ''")
        if 'access denied' in body.lower() or 'pardon our interruption' in body.lower():
            return True
    except Exception:
        pass
    return False


def get_next_data(page, url):
    """Returns dict payload, or 'BLOCKED'."""
    try:
        page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
    except (PlaywrightTimeout, Exception):
        return 'BLOCKED'

    try:
        page.wait_for_load_state('networkidle', timeout=20_000)
    except PlaywrightTimeout:
        pass

    if is_access_denied(page):
        return 'BLOCKED'

    simulate_human_behavior(page)

    try:
        content = page.evaluate(
            "() => { const t = document.getElementById('__NEXT_DATA__'); return t ? t.textContent : null; }"
        )
    except Exception:
        return 'BLOCKED'

    if not content:
        return 'BLOCKED'
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return 'BLOCKED'


# ==========================================
# BROWSER LIFECYCLE
# ==========================================
def make_camoufox_kwargs():
    kwargs = {
        'headless': HEADLESS,
        'humanize': False,          # MUST be False — True causes hangs
        'locale': 'en-AU',
        'os': random.choice(['windows', 'macos']),
    }
    if PROXY_URL:
        kwargs['proxy'] = {'server': PROXY_URL}
        kwargs['geoip'] = True
    return kwargs


def warm_up(page):
    """Multi-step warm-up: homepage → suburb profile → real search."""
    print("   🌐 Warm-up step 1: homepage...")
    try:
        page.goto('https://www.domain.com.au/', timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=20_000)
        except PlaywrightTimeout:
            pass
        if is_access_denied(page):
            print("   ❌ Homepage blocked. IP rejected.")
            return False
        print(f"   ✅ '{page.title()[:60]}'")
        simulate_human_behavior(page)
        human_delay(3.0, 6.0)
    except Exception as e:
        print(f"   ⚠️ Homepage exception: {e}")
        return False

    # Step 2: suburb profile
    suburb = random.choice(WARMUP_SUBURBS)
    print(f"   🌐 Warm-up step 2: /suburb-profile/{suburb}")
    try:
        page.goto(f'https://www.domain.com.au/suburb-profile/{suburb}',
                  timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=15_000)
        except PlaywrightTimeout:
            pass
        if is_access_denied(page):
            print("   ⚠️ Suburb profile blocked — continuing anyway")
        else:
            simulate_human_behavior(page)
            human_delay(4.0, 8.0)
    except Exception as e:
        print(f"   ⚠️ Suburb profile exception: {e}")

    # Step 3: real suburb search
    try:
        print(f"   🌐 Warm-up step 3: real search for {suburb}")
        page.goto(f'https://www.domain.com.au/sale/{suburb}/',
                  timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=15_000)
        except PlaywrightTimeout:
            pass
        if is_access_denied(page):
            print("   ⚠️ Search blocked — continuing")
        else:
            simulate_human_behavior(page)
            human_delay(3.0, 7.0)
    except Exception as e:
        print(f"   ⚠️ Search exception: {e}")

    # Check Akamai cookies
    cookies = page.context.cookies()
    cookie_names = [c['name'] for c in cookies]
    has_abck = '_abck' in cookie_names
    print(f"   🍪 Akamai _abck cookie: {has_abck}")
    return has_abck


# ==========================================
# CELL SELECTION — daily rotation + manual override
# ==========================================
def select_cells_for_today(all_cells):
    """Pick a rotating slice. Supports manual override via MANUAL_OFFSET env var."""
    manual_offset = os.getenv('MANUAL_OFFSET', '').strip()

    if manual_offset.isdigit():
        offset = int(manual_offset) % ROTATION_STRIDE
        print(f"   🎯 Manual override: using offset {offset}")
    elif manual_offset.lower() == 'random':
        offset = random.randint(0, ROTATION_STRIDE - 1)
        print(f"   🎲 Random offset selected: {offset}")
    else:
        today = dt.date.today()
        offset = today.toordinal() % ROTATION_STRIDE
        print(f"   📅 Auto offset {offset} for {today}")

    selected = [(i, c) for i, c in enumerate(all_cells) if i % ROTATION_STRIDE == offset]
    random.shuffle(selected)
    selected = selected[:CELLS_PER_RUN]

    cell_ids = sorted([idx for idx, _ in selected])
    print(f"   🗺️  Cells to scrape: {cell_ids}")

    return selected


# ==========================================
# MAIN
# ==========================================
def main():
    # Load existing dedup data
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

    # Build full cell list
    lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
    lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE
    all_cells = []
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            t_lat = round(LAT_NORTH - (i * lat_step), 4)
            b_lat = round(LAT_NORTH - ((i + 1) * lat_step), 4)
            l_lng = round(LNG_WEST + (j * lng_step), 4)
            r_lng = round(LNG_WEST + ((j + 1) * lng_step), 4)
            all_cells.append((t_lat, b_lat, l_lng, r_lng))

    # Pick today's slice
    todays_cells = select_cells_for_today(all_cells)
    print(f"   📊 Today's slice: {len(todays_cells)} cells out of {len(all_cells)} total")

    if PROXY_URL:
        print(f"   🛡️ Proxy: {PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL}")
    else:
        print("   ⚠️  No proxy — relying on Camoufox fingerprinting alone")

    total_records = 0
    cells_done = 0
    consecutive_blocks = 0

    try:
        with Camoufox(**make_camoufox_kwargs()) as browser:
            page = browser.new_page()

            if not warm_up(page):
                print("\n❌ Warm-up failed. IP likely flagged.")
                print("   Try again later, or use a residential proxy.")
                sys.exit(1)

            human_delay(4.0, 8.0)

            for idx, (cell_idx, cell) in enumerate(todays_cells, 1):
                if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                    print(f"\n🛑 {consecutive_blocks} blocked cells in a row — stopping early")
                    print(f"   Saving partial data. Try again tomorrow.")
                    break

                t_lat, b_lat, l_lng, r_lng = cell
                print(f"\n📍 [{idx}/{len(todays_cells)}] cell #{cell_idx} | {t_lat},{l_lng} → {b_lat},{r_lng}")

                cell_records = 0
                cell_strikes = 0
                pages_in_cell = 0

                # Randomise mode order
                modes = list(SEARCH_MODES)
                random.shuffle(modes)

                for status_label, mode_path, mode_extra in modes:
                    if cell_strikes >= CELL_MAX_STRIKES:
                        break

                    base_query = f"startloc={t_lat}%2C{l_lng}&endloc={b_lat}%2C{r_lng}"
                    if mode_extra:
                        base_query = f"{mode_extra}&{base_query}"

                    for pg in range(1, MAX_PAGES_PER_QUERY + 1):
                        if cell_strikes >= CELL_MAX_STRIKES:
                            break

                        # Random abandon at deep pages (humans don't always paginate to the end)
                        if pg >= 4 and random.random() < DEEP_PAGE_ABANDON_PROB:
                            print(f"   🚪 Stopping at page {pg} (human-like abandon)")
                            break

                        # Pacing rest
                        if pages_in_cell >= PAGES_BEFORE_REST:
                            rest = random.uniform(*REST_DURATION)
                            print(f"   ☕ Rest {rest:.0f}s")
                            time.sleep(rest)
                            pages_in_cell = 0
                        elif pg > 1:
                            human_delay(*DELAY_BETWEEN_REQUESTS)

                        url = f"https://www.domain.com.au/{mode_path}/?{base_query}&page={pg}"
                        payload = get_next_data(page, url)
                        pages_in_cell += 1

                        if payload == 'BLOCKED':
                            cell_strikes += 1
                            print(f"   ⚠️ Block: {status_label} pg {pg} (strike {cell_strikes}/{CELL_MAX_STRIKES})")
                            continue

                        page_records, total_pages = parse_listings_payload(payload, status_label, seen_records)
                        if page_records:
                            save_incremental_data(page_records, FILE_NAME)
                            cell_records += len(page_records)
                            print(f"   + {len(page_records)} ({status_label} pg {pg}/{total_pages})")
                        elif pg == 1:
                            print(f"   ◌ No {status_label} listings")
                            break

                        if pg >= total_pages:
                            break

                    human_delay(*DELAY_BETWEEN_REQUESTS)

                if cell_records > 0:
                    total_records += cell_records
                    cells_done += 1
                    consecutive_blocks = 0
                elif cell_strikes > 0:
                    consecutive_blocks += 1
                    print(f"   🚫 Cell blocked (run: {consecutive_blocks} in a row)")
                else:
                    consecutive_blocks = 0
    except Exception as e:
        print(f"\n❌ Session exception: {e}")

    # Final report
    print(f"\n{'='*60}")
    print("✅ DONE")
    print(f"{'='*60}")
    print(f"   Cells done:  {cells_done}/{len(todays_cells)}")
    print(f"   New records: {total_records}")

    if total_records == 0 and consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
        print("⚠️  Stopped early due to blocks — partial run")
        sys.exit(1)


if __name__ == '__main__':
    main()