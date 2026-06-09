"""
Domain.com.au scraper — v5.3 (no-proxy mode: resilient warm-up + humanize)

Changes from v5.2:
- humanize: False → True  (Camoufox mouse/keyboard simulation, critical for Akamai)
- warm_up_full / warm_up_light: no longer abort session on homepage block
  → proceed to scrape anyway; homepage block ≠ search page block
- warm_up_full: longer delays (5-10s, 6-12s) to build Akamai trust score
- warm_up_full: added 4th step — visit a property detail page for extra trust
- New warm_up_minimal(): fallback when even homepage fails; visits 2 suburb
  profiles directly (often unblocked even when homepage is blocked)
- MAX_CONSECUTIVE_BLOCKS: 3 → 5  (less aggressive abort without proxy)
- DELAY_BETWEEN_REQUESTS: 12-25s → 18-35s  (slower = less suspicious)
- SESSION_COOLDOWN: 60-120s → 90-180s
- CELLS_PER_SESSION: 3 → 2  (shorter sessions = less Akamai fingerprint buildup)
"""

import os
import sys
import json
import time
import math
import random
import re
import threading
import datetime as dt
import pandas as pd
import numpy as np
from camoufox.sync_api import Camoufox
from playwright.sync_api import TimeoutError as PlaywrightTimeout
from urllib.parse import urlparse

print("STARTING SCRAPER (v5.3 — no-proxy resilient mode)")

# ==========================================
# CONFIGURATION
# ==========================================
FILE_NAME = 'data/melbourne_price_data.csv'
GRID_SIZE = 14
LAT_NORTH, LAT_SOUTH = -37.5, -38.5
LNG_WEST, LNG_EAST = 144.35, 145.40

HEADLESS = os.getenv('HEADLESS', 'false').lower() == 'true'
PROXY_URL = os.getenv('PROXY_URL')

NAV_TIMEOUT_MS = 30_000

CELLS_PER_DAY = 14
ROTATION_STRIDE = 14

CELLS_PER_RUN = int(os.getenv('CELLS_PER_RUN', '7'))
RUN_SLOT = os.getenv('RUN_SLOT', 'A').upper()

# v5.3: shorter sessions = smaller per-session footprint
CELLS_PER_SESSION = 2
SESSION_COOLDOWN = (90.0, 180.0)

# v5.3: slower pacing without proxy
DELAY_BETWEEN_REQUESTS = (18.0, 35.0)
PAGES_BEFORE_REST = 6
REST_DURATION = (120.0, 240.0)

MAX_PAGES_PER_QUERY = 10
ABANDON_START_PAGE = 6
ABANDON_BASE_PROB = 0.05
ABANDON_GROWTH = 0.03
ABANDON_MAX_PROB = 0.25

CELL_MAX_STRIKES = 2

# v5.3: more tolerance without proxy (block rate naturally higher)
MAX_CONSECUTIVE_BLOCKS = 5

SCRIPT_START_TIME = time.time()
RUN_TIMEOUT_SECONDS = 5 * 3600


def time_remaining():
    elapsed = time.time() - SCRIPT_START_TIME
    return max(0, RUN_TIMEOUT_SECONDS - elapsed)


def should_stop():
    return time_remaining() < 300


def interruptible_sleep(seconds, label=""):
    end = time.time() + seconds
    while time.time() < end:
        if should_stop():
            print(f"   ⏰ Interrupting sleep ({label}) — timeout approaching")
            return
        chunk = min(5.0, end - time.time())
        if chunk > 0:
            time.sleep(chunk)


def _watchdog_thread():
    time.sleep(RUN_TIMEOUT_SECONDS)
    elapsed_min = (time.time() - SCRIPT_START_TIME) / 60
    print(f"\n⏲️  WATCHDOG: hard time limit reached at {elapsed_min:.1f} min", flush=True)
    print("   Forcing clean exit. All scraped data already saved incrementally.", flush=True)
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)


def arm_watchdog():
    t = threading.Thread(target=_watchdog_thread, daemon=True)
    t.start()
    print(f"   ⏲️  Watchdog armed: hard stop at {RUN_TIMEOUT_SECONDS/3600:.2f}h "
          f"({RUN_TIMEOUT_SECONDS/60:.0f} min)")


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
# HELPERS
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
    interruptible_sleep(random.uniform(min_sec, max_sec), "human delay")


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


def simulate_human_behavior(page):
    """Scroll + occasional upward scroll to mimic reading."""
    try:
        for _ in range(random.randint(2, 4)):
            scroll_distance = random.randint(300, 800)
            page.evaluate(f"window.scrollBy(0, {scroll_distance})")
            human_delay(0.8, 2.2)
        if random.random() < 0.5:
            page.evaluate(f"window.scrollBy(0, -{random.randint(150, 500)})")
            human_delay(0.5, 1.2)
        # Occasionally pause at top like a real user re-reading
        if random.random() < 0.25:
            page.evaluate("window.scrollTo(0, 0)")
            human_delay(1.0, 2.5)
    except Exception:
        pass


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
    try:
        page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
    except (PlaywrightTimeout, Exception):
        return 'BLOCKED'
    try:
        page.wait_for_load_state('networkidle', timeout=8_000)
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


def make_camoufox_kwargs():
    kwargs = {
        "headless": HEADLESS,
        # v5.3: humanize=True enables Camoufox mouse/keyboard simulation.
        # Critical for Akamai BotManager — without it, the browser passes
        # fingerprint checks but fails behavioural checks.
        "humanize": True,
        "locale": "en-AU",
        "os": random.choice(["windows", "macos"]),
    }

    if PROXY_URL:
        p = urlparse(PROXY_URL)
        kwargs["proxy"] = {
            "server": f"{p.scheme}://{p.hostname}:{p.port}",
            "username": p.username,
            "password": p.password,
        }
        kwargs["geoip"] = True

    return kwargs


def warm_up_full(page):
    """
    Full warm-up for session 1.

    v5.3 change: homepage block no longer aborts the session.
    Akamai often blocks the homepage (high-traffic, heavily monitored)
    but lets search/suburb pages through. We attempt all 4 steps and
    return True as long as at least one non-blocked page was loaded.
    """
    pages_passed = 0

    # Step 1: homepage
    print("   🌐 Warm-up step 1: homepage...")
    try:
        page.goto('https://www.domain.com.au/', timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=10_000)
        except PlaywrightTimeout:
            pass
        if is_access_denied(page):
            print("   ⚠️  Homepage blocked — continuing warm-up anyway")
        else:
            print(f"   ✅ '{page.title()[:60]}'")
            simulate_human_behavior(page)
            pages_passed += 1
        # Always wait, even if blocked — builds time gap for Akamai scoring
        human_delay(5.0, 10.0)
    except Exception as e:
        print(f"   ⚠️ Homepage exception: {e}")

    # Step 2: suburb profile (often unblocked even when homepage is blocked)
    suburb = random.choice(WARMUP_SUBURBS)
    print(f"   🌐 Warm-up step 2: /suburb-profile/{suburb}")
    try:
        page.goto(f'https://www.domain.com.au/suburb-profile/{suburb}',
                  timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=15_000)
        except PlaywrightTimeout:
            pass
        if not is_access_denied(page):
            simulate_human_behavior(page)
            pages_passed += 1
            human_delay(6.0, 12.0)
        else:
            print("   ⚠️  Suburb profile blocked")
            human_delay(3.0, 6.0)
    except Exception as e:
        print(f"   ⚠️ Suburb profile exception: {e}")

    # Step 3: suburb search listing page
    suburb2 = random.choice([s for s in WARMUP_SUBURBS if s != suburb])
    print(f"   🌐 Warm-up step 3: /sale/{suburb2}/")
    try:
        page.goto(f'https://www.domain.com.au/sale/{suburb2}/',
                  timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=15_000)
        except PlaywrightTimeout:
            pass
        if not is_access_denied(page):
            simulate_human_behavior(page)
            pages_passed += 1
            human_delay(5.0, 10.0)
        else:
            print("   ⚠️  Sale search blocked")
            human_delay(3.0, 6.0)
    except Exception as e:
        print(f"   ⚠️ Search exception: {e}")

    # Step 4: visit one property detail page (adds referral chain depth)
    print(f"   🌐 Warm-up step 4: property detail page")
    try:
        detail_url = f'https://www.domain.com.au/sold-listings/{suburb2}/'
        page.goto(detail_url, timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=15_000)
        except PlaywrightTimeout:
            pass
        if not is_access_denied(page):
            simulate_human_behavior(page)
            pages_passed += 1
            human_delay(4.0, 8.0)
        else:
            print("   ⚠️  Detail page blocked")
    except Exception as e:
        print(f"   ⚠️ Detail page exception: {e}")

    cookies = page.context.cookies()
    has_abck = '_abck' in [c['name'] for c in cookies]
    print(f"   🍪 Akamai _abck cookie: {has_abck} | Pages passed: {pages_passed}/4")

    # v5.3: proceed even if 0 pages passed — search pages may still work
    return True


def warm_up_light(page):
    """
    Light warm-up for sessions 2+.

    v5.3 change: always returns True. Homepage block is common for
    repeat sessions; scrape pages are often still accessible.
    """
    print("   🌐 Light warm-up: homepage + suburb profile")
    pages_passed = 0

    try:
        page.goto('https://www.domain.com.au/', timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=10_000)
        except PlaywrightTimeout:
            pass
        if is_access_denied(page):
            print("   ⚠️  Homepage blocked — continuing anyway")
        else:
            simulate_human_behavior(page)
            pages_passed += 1
        human_delay(3.0, 7.0)
    except Exception as e:
        print(f"   ⚠️ Light warm-up homepage exception: {e}")

    # Extra suburb visit to build a small navigation history
    suburb = random.choice(WARMUP_SUBURBS)
    try:
        page.goto(f'https://www.domain.com.au/suburb-profile/{suburb}',
                  timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=10_000)
        except PlaywrightTimeout:
            pass
        if not is_access_denied(page):
            simulate_human_behavior(page)
            pages_passed += 1
        human_delay(3.0, 6.0)
    except Exception:
        pass

    cookies = page.context.cookies()
    has_abck = '_abck' in [c['name'] for c in cookies]
    print(f"   🍪 Akamai _abck cookie: {has_abck} | Pages passed: {pages_passed}/2")

    # Always proceed — scrape pages may work even if warm-up pages were blocked
    return True


def select_cells_for_today(all_cells):
    manual_offset = os.getenv('MANUAL_OFFSET', '').strip()

    if manual_offset.isdigit():
        offset = int(manual_offset) % ROTATION_STRIDE
        print(f"   🎯 Manual override: offset {offset}")
    elif manual_offset.lower() == 'random':
        offset = random.randint(0, ROTATION_STRIDE - 1)
        print(f"   🎲 Random offset: {offset}")
    else:
        today = dt.date.today()
        offset = today.toordinal() % ROTATION_STRIDE
        print(f"   📅 Auto offset {offset} for {today}")

    all_today = [(i, c) for i, c in enumerate(all_cells) if i % ROTATION_STRIDE == offset]

    rng = random.Random(offset)
    rng.shuffle(all_today)

    half = len(all_today) // 2
    slot_a = all_today[:half]
    slot_b = all_today[half:]

    print(f"   📦 Slot A: {sorted([idx for idx, _ in slot_a])}")
    print(f"   📦 Slot B: {sorted([idx for idx, _ in slot_b])}")

    if RUN_SLOT == 'A':
        selected = slot_a
        print(f"   🅰️ This run is SLOT A — {len(selected)} cells")
    elif RUN_SLOT == 'B':
        selected = slot_b
        print(f"   🅱️ This run is SLOT B — {len(selected)} cells")
    else:
        selected = all_today
        print(f"   ⚠️ RUN_SLOT='{RUN_SLOT}' invalid — using all cells")

    if len(selected) > CELLS_PER_RUN:
        selected = selected[:CELLS_PER_RUN]

    cell_ids = sorted([idx for idx, _ in selected])
    print(f"   🗺️  Final cells to scrape: {cell_ids}")
    return selected


def scrape_cell(page, cell_idx_global, cell, seen_records, pages_in_session):
    t_lat, b_lat, l_lng, r_lng = cell
    print(f"\n📍 cell #{cell_idx_global} | {t_lat},{l_lng} → {b_lat},{r_lng}")

    cell_records = 0
    cell_strikes = 0

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

            if should_stop():
                print(f"   ⏰ Timeout reached, stopping cell mid-scrape")
                break

            if pg >= ABANDON_START_PAGE:
                abandon_prob = min(
                    ABANDON_BASE_PROB + (pg - ABANDON_START_PAGE) * ABANDON_GROWTH,
                    ABANDON_MAX_PROB
                )
                if random.random() < abandon_prob:
                    print(f"   🚪 Stopping at page {pg} (human-like abandon, p={abandon_prob:.2f})")
                    break

            if pages_in_session >= PAGES_BEFORE_REST:
                rest = random.uniform(*REST_DURATION)
                print(f"   ☕ Rest {rest:.0f}s")
                interruptible_sleep(rest, "rest")
                pages_in_session = 0
            elif pg > 1:
                human_delay(*DELAY_BETWEEN_REQUESTS)

            url = f"https://www.domain.com.au/{mode_path}/?{base_query}&page={pg}"
            payload = get_next_data(page, url)
            pages_in_session += 1

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

    was_blocked = (cell_records == 0 and cell_strikes > 0)
    return cell_records, was_blocked, pages_in_session


def main():
    arm_watchdog()

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
    all_cells = []
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            t_lat = round(LAT_NORTH - (i * lat_step), 4)
            b_lat = round(LAT_NORTH - ((i + 1) * lat_step), 4)
            l_lng = round(LNG_WEST + (j * lng_step), 4)
            r_lng = round(LNG_WEST + ((j + 1) * lng_step), 4)
            all_cells.append((t_lat, b_lat, l_lng, r_lng))

    todays_cells = select_cells_for_today(all_cells)
    total_today = len(todays_cells)
    num_sessions = math.ceil(total_today / CELLS_PER_SESSION) if total_today > 0 else 0
    print(f"   📊 This run: {total_today} cells, {num_sessions} sessions")

    if PROXY_URL:
        print(f"   🛡️ Proxy: {PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL}")
    else:
        print("   ⚠️  No proxy — running in no-proxy resilient mode (v5.3)")

    if total_today == 0:
        print("\n⚠️ No cells to scrape this run.")
        return

    total_records = 0
    cells_done = 0
    cells_empty = 0
    cells_blocked = 0
    consecutive_blocks = 0
    session_idx = 0
    cell_pos = 0

    while cell_pos < total_today:
        if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
            print(f"\n🛑 {consecutive_blocks} blocked cells in a row — stopping run")
            break

        if should_stop():
            remaining_min = time_remaining() / 60
            print(f"\n⏰ Approaching workflow timeout ({remaining_min:.1f} min left)")
            print(f"   Stopping gracefully after {cell_pos}/{total_today} cells")
            break

        session_idx += 1
        session_end = min(cell_pos + CELLS_PER_SESSION, total_today)
        session_cells = todays_cells[cell_pos:session_end]

        print(f"\n{'='*60}")
        print(f"🚀 SESSION {session_idx}/{num_sessions} — cells {cell_pos + 1}-{session_end} of {total_today}")
        print(f"{'='*60}")

        try:
            with Camoufox(**make_camoufox_kwargs()) as browser:
                page = browser.new_page()
                page.set_default_timeout(NAV_TIMEOUT_MS)
                page.set_default_navigation_timeout(NAV_TIMEOUT_MS)

                # v5.3: warm_up always returns True — session never skipped
                if session_idx == 1:
                    warm_up_full(page)
                else:
                    warm_up_light(page)

                human_delay(3.0, 6.0)

                pages_in_session = 0
                session_records = 0

                for (cell_idx_global, cell) in session_cells:
                    if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                        break

                    cell_records, was_blocked, pages_in_session = scrape_cell(
                        page, cell_idx_global, cell, seen_records, pages_in_session
                    )

                    if cell_records > 0:
                        session_records += cell_records
                        total_records += cell_records
                        cells_done += 1
                        consecutive_blocks = 0
                    elif was_blocked:
                        cells_blocked += 1
                        consecutive_blocks += 1
                        print(f"   🚫 Cell blocked (run streak: {consecutive_blocks})")
                    else:
                        cells_done += 1
                        cells_empty += 1
                        consecutive_blocks = 0

                    cell_pos += 1

                print(f"\n   📊 Session {session_idx} done: +{session_records} records")

        except Exception as e:
            print(f"   ❌ Session exception: {e}")
            cell_pos = session_end

        if cell_pos < total_today and consecutive_blocks < MAX_CONSECUTIVE_BLOCKS:
            cooldown = random.uniform(*SESSION_COOLDOWN)
            print(f"\n   ⏰ Cooldown {cooldown:.0f}s before next session...")
            interruptible_sleep(cooldown, "session cooldown")

    print(f"\n{'='*60}")
    print(f"✅ DONE — Run slot {RUN_SLOT}")
    print(f"{'='*60}")
    print(f"   Sessions used:  {session_idx}")
    print(f"   Cells done:     {cells_done}/{total_today}  "
          f"({cells_done - cells_empty} with data, {cells_empty} empty)")
    if cells_blocked > 0:
        print(f"   Cells blocked:  {cells_blocked}")
    print(f"   New records:    {total_records}")
    elapsed_min = (time.time() - SCRIPT_START_TIME) / 60
    print(f"   Run time:       {elapsed_min:.1f} min")
    if cell_pos < total_today:
        skipped = total_today - cell_pos
        print(f"   Cells skipped:  {skipped} (will be retried in next rotation cycle)")

    if total_records == 0 and consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
        print("⚠️  Stopped early due to blocks — partial run")
        sys.exit(1)


if __name__ == '__main__':
    main()
