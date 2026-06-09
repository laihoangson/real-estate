"""
Domain.com.au scraper — v5.5 (fix viewport kwarg + asyncio loop crash)

Bugs fixed from v5.4 run:
  1. BrowserType.launch() unexpected kwarg 'viewport'
       Camoufox does NOT accept 'viewport'; use 'screen=Screen(max_width, max_height)'
       from browserforge.fingerprints, or 'window=(w, h)' for window size.
       Fixed in make_camoufox_kwargs().

  2. "Playwright Sync API inside asyncio loop"
       Playwright 1.52+ sync API raises if called from a thread that already has a
       running asyncio event loop (common in GitHub Actions Python 3.11 environment
       and some OS-level event loop managers).
       Fix: each session runs inside run_session_in_thread() which spawns a fresh
       daemon thread → clean event loop → no conflict.

Everything else identical to v5.4.
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
from browserforge.fingerprints import Screen

print("STARTING SCRAPER (v5.5 — fix viewport kwarg + asyncio loop)")

# ==========================================
# CONFIGURATION
# ==========================================
FILE_NAME = 'data/melbourne_price_data.csv'
GRID_SIZE = 14
LAT_NORTH, LAT_SOUTH = -37.5, -38.5
LNG_WEST, LNG_EAST   = 144.35, 145.40

HEADLESS  = os.getenv('HEADLESS', 'false').lower() == 'true'
PROXY_URL = os.getenv('PROXY_URL')

NAV_TIMEOUT_MS = 35_000

CELLS_PER_DAY    = 14
ROTATION_STRIDE  = 14

CELLS_PER_RUN = int(os.getenv('CELLS_PER_RUN', '7'))
RUN_SLOT      = os.getenv('RUN_SLOT', 'A').upper()

# v5.4: shorter sessions + longer cooldowns
CELLS_PER_SESSION  = 2
SESSION_COOLDOWN   = (120.0, 240.0)

# v5.4: slower pacing
DELAY_BETWEEN_REQUESTS = (25.0, 50.0)
BLOCK_RECOVERY_SLEEP   = (45.0, 90.0)   # NEW: extra sleep after any block
PAGES_BEFORE_REST      = 5
REST_DURATION          = (150.0, 280.0)

MAX_PAGES_PER_QUERY    = 10
ABANDON_START_PAGE     = 5
ABANDON_BASE_PROB      = 0.07
ABANDON_GROWTH         = 0.04
ABANDON_MAX_PROB       = 0.30

CELL_MAX_STRIKES       = 3              # v5.4: increased from 2
MAX_PAGE_RETRIES       = 2              # NEW: per-page retry after block
MAX_CONSECUTIVE_BLOCKS = 5

# v5.4: minimum warm-up time budget (seconds)
MIN_WARMUP_FULL_SECS   = 45
MIN_WARMUP_LIGHT_SECS  = 20

SCRIPT_START_TIME = time.time()
RUN_TIMEOUT_SECONDS = 5 * 3600


def time_remaining():
    return max(0, RUN_TIMEOUT_SECONDS - (time.time() - SCRIPT_START_TIME))

def should_stop():
    return time_remaining() < 300

def interruptible_sleep(seconds, label=""):
    end = time.time() + seconds
    while time.time() < end:
        if should_stop():
            print(f"   ⏰ Interrupting sleep ({label}) — timeout approaching")
            return
        time.sleep(min(5.0, end - time.time()))

def _watchdog_thread():
    time.sleep(RUN_TIMEOUT_SECONDS)
    print(f"\n⏲️  WATCHDOG: hard stop at {(time.time()-SCRIPT_START_TIME)/60:.1f} min", flush=True)
    sys.stdout.flush(); sys.stderr.flush()
    os._exit(0)

def arm_watchdog():
    t = threading.Thread(target=_watchdog_thread, daemon=True)
    t.start()
    print(f"   ⏲️  Watchdog armed: {RUN_TIMEOUT_SECONDS/3600:.2f}h")


# ==========================================
# WARM-UP PAGE POOLS
# ==========================================

# Tier 1: Very lightweight CMS/editorial pages — minimal JS, rarely blocked
WARMUP_EDITORIAL = [
    'https://www.domain.com.au/advice/buying/',
    'https://www.domain.com.au/advice/renting/',
    'https://www.domain.com.au/advice/selling/',
    'https://www.domain.com.au/news/',
    'https://www.domain.com.au/research/',
    'https://www.domain.com.au/guides/home-loans/',
    'https://www.domain.com.au/advice/investing/',
]

# Tier 2: Suburb profiles (moderate JS, sometimes blocked but often OK)
WARMUP_SUBURBS = [
    'richmond-vic-3121', 'st-kilda-vic-3182',  'brunswick-vic-3056',
    'fitzroy-vic-3065',  'south-yarra-vic-3141','carlton-vic-3053',
    'footscray-vic-3011','brighton-vic-3186',   'hawthorn-vic-3122',
    'prahran-vic-3181',  'collingwood-vic-3066','northcote-vic-3070',
    'moonee-ponds-vic-3039', 'essendon-vic-3040',
]

# Tier 3: Low-traffic suburb sale pages (light search, sometimes accessible
# even when main search endpoints are blocked)
WARMUP_SALE_LIGHT = [
    'https://www.domain.com.au/sale/footscray-vic-3011/?bedrooms=3-any',
    'https://www.domain.com.au/sale/brunswick-vic-3056/?bedrooms=2-any',
    'https://www.domain.com.au/sale/essendon-vic-3040/?bedrooms=3-any',
]

SEARCH_MODES = [
    ('For Sale', 'sale',           'excludeunderoffer=1'),
    ('Sold',     'sold-listings',  ''),
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
            mult = 1_000_000; val_str = val_str[:-1]
        elif val_str.endswith('k'):
            num = float(val_str[:-1])
            if num < 1000: mult = 1000
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
    lat1r, lon1r = math.radians(lat1), math.radians(lon1)
    lat2r, lon2r = math.radians(lat2), math.radians(lon2)
    dlon = lon2r - lon1r; dlat = lat2r - lat1r
    a = math.sin(dlat/2)**2 + math.cos(lat1r)*math.cos(lat2r)*math.sin(dlon/2)**2
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)), 2)


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
            df_final['Propertycount'] = df_final.groupby('Suburb')['Property_ID'].transform('count')
            df_final.to_csv(file_path, index=False, encoding='utf-8-sig')
        except pd.errors.EmptyDataError:
            df_new.to_csv(file_path, index=False, encoding='utf-8-sig')
    else:
        df_new['Propertycount'] = df_new.groupby('Suburb')['Property_ID'].transform('count')
        df_new.to_csv(file_path, index=False, encoding='utf-8-sig')


def simulate_human_behavior(page, intensity="normal"):
    """
    Scroll + mouse movement to build Akamai behavioural trust score.
    intensity='deep' does more scrolling — used during warm-up.
    """
    try:
        # Get page dimensions
        dims = page.evaluate(
            "() => ({w: document.body.scrollWidth || 1200, "
            "        h: document.body.scrollHeight || 3000, "
            "        vh: window.innerHeight || 768})"
        )
        page_h = dims.get('h', 3000)
        vw = dims.get('w', 1200)

        n_scrolls = random.randint(3, 6) if intensity == "deep" else random.randint(2, 4)

        for i in range(n_scrolls):
            scroll_y = random.randint(250, 700)
            page.evaluate(f"window.scrollBy(0, {scroll_y})")
            human_delay(0.6, 1.8)

            # Occasionally move mouse to simulate reading
            if random.random() < 0.6:
                mx = random.randint(100, min(vw - 100, 1100))
                my = random.randint(100, 600)
                try:
                    page.mouse.move(mx, my)
                    human_delay(0.2, 0.7)
                except Exception:
                    pass

        # Scroll back up partially (mimics re-reading)
        if random.random() < 0.5:
            page.evaluate(f"window.scrollBy(0, -{random.randint(200, 600)})")
            human_delay(0.5, 1.5)

        # Deep: scroll to bottom then pause
        if intensity == "deep" and random.random() < 0.4:
            page.evaluate(f"window.scrollTo(0, {min(page_h, 4000)})")
            human_delay(1.0, 2.5)
            page.evaluate("window.scrollTo(0, 0)")
            human_delay(0.8, 1.5)

    except Exception:
        pass


def is_access_denied(page):
    try:
        title = (page.title() or '').lower()
        if 'access denied' in title or 'pardon our interruption' in title:
            return True
        body = page.evaluate(
            "() => document.body ? document.body.innerText.slice(0, 400) : ''"
        )
        body_l = body.lower()
        if 'access denied' in body_l or 'pardon our interruption' in body_l:
            return True
        # Akamai challenge page detection
        if 'enable javascript' in body_l and len(body) < 500:
            return True
    except Exception:
        pass
    return False


def get_next_data(page, url):
    """Navigate to URL and extract __NEXT_DATA__ JSON."""
    try:
        page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
    except (PlaywrightTimeout, Exception):
        return 'BLOCKED'
    try:
        page.wait_for_load_state('networkidle', timeout=10_000)
    except PlaywrightTimeout:
        pass
    if is_access_denied(page):
        return 'BLOCKED'
    simulate_human_behavior(page)
    try:
        content = page.evaluate(
            "() => { const t = document.getElementById('__NEXT_DATA__'); "
            "return t ? t.textContent : null; }"
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
    # v5.5: 'viewport' is NOT a valid Camoufox kwarg — it belongs to Playwright's
    # BrowserContext, not the Firefox launcher.  Use 'screen' (Screen object from
    # browserforge) to hint the fingerprint generator, or 'window' for actual size.
    w = random.choice([1280, 1366, 1440, 1536])
    h = random.choice([768, 800, 864, 900])
    kwargs = {
        "headless": HEADLESS,
        "humanize": True,
        "locale":   "en-AU",
        "os":       random.choice(["windows", "macos"]),
        "screen":   Screen(max_width=w, max_height=h),
        "window":   (w, h),
    }
    if PROXY_URL:
        p = urlparse(PROXY_URL)
        kwargs["proxy"] = {
            "server":   f"{p.scheme}://{p.hostname}:{p.port}",
            "username": p.username,
            "password": p.password,
        }
        kwargs["geoip"] = True
    return kwargs


# ==========================================
# WARM-UP FUNCTIONS (v5.4)
# ==========================================

def _try_visit(page, url, label, intensity="normal"):
    """Visit a URL. Return True if not blocked."""
    try:
        page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until='domcontentloaded')
        try:
            page.wait_for_load_state('networkidle', timeout=10_000)
        except PlaywrightTimeout:
            pass
        if is_access_denied(page):
            print(f"   ⚠️  Blocked: {label}")
            return False
        print(f"   ✅ OK: {label[:70]}")
        simulate_human_behavior(page, intensity=intensity)
        return True
    except Exception as e:
        print(f"   ⚠️  Exception ({label[:50]}): {e}")
        return False


def warm_up_full(page):
    """
    Full warm-up strategy for session 1.

    Priority order (most → least likely to succeed without proxy):
      1. Homepage                    — builds initial cookie
      2. Editorial/advice pages      — low JS, rarely blocked
      3. Suburb profile              — moderate JS
      4. Light sale suburb page      — tests search proximity
      5. Probe (warm_up_probe)       — confirm search is accessible

    v5.4: enforces minimum 45s budget; keeps trying next tier if blocked.
    """
    t_start    = time.time()
    pages_ok   = 0

    # Step 1: homepage (builds _abck cookie)
    print("   🌐 Warm-up [1/5] homepage")
    if _try_visit(page, 'https://www.domain.com.au/', "homepage", intensity="deep"):
        pages_ok += 1
    human_delay(4.0, 7.0)

    # Step 2: 2 editorial pages (very lightweight — rarely blocked)
    editorials = random.sample(WARMUP_EDITORIAL, 2)
    for i, url in enumerate(editorials, 2):
        if should_stop(): break
        print(f"   🌐 Warm-up [{i}/5] editorial: {url.split('/')[-2]}")
        if _try_visit(page, url, url, intensity="deep"):
            pages_ok += 1
        human_delay(5.0, 10.0)

    # Step 3: suburb profile
    suburb = random.choice(WARMUP_SUBURBS)
    print(f"   🌐 Warm-up [4/5] suburb profile: {suburb}")
    if _try_visit(page, f'https://www.domain.com.au/suburb-profile/{suburb}',
                  suburb, intensity="deep"):
        pages_ok += 1
    human_delay(5.0, 9.0)

    # Step 4: light sale suburb listing
    sale_url = random.choice(WARMUP_SALE_LIGHT)
    print(f"   🌐 Warm-up [5/5] light sale search")
    if _try_visit(page, sale_url, "light-sale", intensity="normal"):
        pages_ok += 1
    human_delay(4.0, 8.0)

    # Enforce minimum time budget
    elapsed = time.time() - t_start
    if elapsed < MIN_WARMUP_FULL_SECS:
        extra = MIN_WARMUP_FULL_SECS - elapsed
        print(f"   ⏳ Padding warm-up by {extra:.0f}s to hit min budget")
        human_delay(extra, extra + 5)

    cookies = page.context.cookies()
    has_abck = '_abck' in [c['name'] for c in cookies]
    print(f"   🍪 _abck: {has_abck} | Pages OK: {pages_ok}/5 | "
          f"Time: {(time.time()-t_start):.0f}s")
    return True


def warm_up_light(page):
    """
    Light warm-up for sessions 2+.

    v5.4: tries 1 editorial + 1 suburb profile (cookies already exist,
    just need to refresh the trust window).
    Enforces minimum 20s.
    """
    t_start  = time.time()
    pages_ok = 0

    print("   🌐 Light warm-up: editorial + suburb profile")

    editorial = random.choice(WARMUP_EDITORIAL)
    if _try_visit(page, editorial, editorial.split('/')[-2], intensity="normal"):
        pages_ok += 1
    human_delay(4.0, 7.0)

    suburb = random.choice(WARMUP_SUBURBS)
    if _try_visit(page, f'https://www.domain.com.au/suburb-profile/{suburb}',
                  suburb, intensity="normal"):
        pages_ok += 1
    human_delay(3.0, 6.0)

    elapsed = time.time() - t_start
    if elapsed < MIN_WARMUP_LIGHT_SECS:
        extra = MIN_WARMUP_LIGHT_SECS - elapsed
        human_delay(extra, extra + 3)

    cookies = page.context.cookies()
    has_abck = '_abck' in [c['name'] for c in cookies]
    print(f"   🍪 _abck: {has_abck} | Pages OK: {pages_ok}/2 | "
          f"Time: {(time.time()-t_start):.0f}s")
    return True


def warm_up_probe(page):
    """
    v5.4 NEW: After warm-up, test a real search URL.
    If blocked, run an extra mini warm-up round and retry (max 2 attempts).
    Returns True if a search page became accessible, False if all probes fail.
    """
    test_url = (
        "https://www.domain.com.au/sale/"
        "?startloc=-37.8000,144.9500&endloc=-37.8500,145.0000"
        "&excludeunderoffer=1&page=1"
    )
    for attempt in range(1, 3):
        print(f"   🔍 Probe attempt {attempt}/2: testing search accessibility...")
        payload = get_next_data(page, test_url)
        if payload != 'BLOCKED':
            print(f"   ✅ Probe passed — search endpoint accessible")
            return True

        if attempt == 1:
            print("   ⚠️  Probe blocked — running extra trust round...")
            # Extra trust-building: 2 more editorial pages
            for url in random.sample(WARMUP_EDITORIAL, 2):
                _try_visit(page, url, url.split('/')[-2], intensity="deep")
                human_delay(8.0, 15.0)

    print("   ⚠️  Probe failed both attempts — proceeding anyway (may get blocks)")
    return False


# ==========================================
# LISTING PARSER
# ==========================================
def parse_listings_payload(payload, status_label, seen_records):
    props    = payload.get('props', {}).get('pageProps', {}).get('componentProps', {})
    listings = props.get('listingsMap', {})
    total_pages = props.get('totalPages', 1)
    records  = []

    for pid, item in listings.items():
        pid_str   = str(pid)
        m         = item.get('listingModel', {})
        raw_price = str(m.get('price', 'N/A'))

        if pid_str in seen_records:
            old = seen_records[pid_str]
            if old['Status'] == status_label and old['Price'] == raw_price:
                continue

        a        = m.get('address', {})
        f        = m.get('features', {})
        street   = a.get('street', 'N/A')
        suburb   = str(a.get('suburb', 'Map Area')).upper()
        postcode = a.get('postcode', '')
        url_path = m.get('url', '')
        lat      = a.get('lat', m.get('geolocation', {}).get('latitude'))
        lng      = a.get('lng', m.get('geolocation', {}).get('longitude'))

        full_address = f"{street}, {suburb} VIC {postcode}".strip() if street != 'N/A' else None
        if not full_address:
            continue

        # Date extraction
        date_val = m.get('dateSold', m.get('dateListed'))
        if not date_val:
            date_val = m.get('status', {}).get('date')
        if not date_val:
            item_str   = json.dumps(item)
            date_match = re.search(r'([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})', item_str)
            if date_match:
                date_val = date_match.group(1)
            else:
                iso_match = re.search(
                    r'"[A-Za-z]*[dD]ate[A-Za-z]*"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})', item_str
                )
                if iso_match:
                    date_val = iso_match.group(1)
        if not date_val:
            date_val = 'N/A'

        # Land size normalisation
        raw_land = f.get('landSize', np.nan)
        land_unit = str(f.get('landUnit', '')).lower()
        try:
            if pd.notna(raw_land):
                raw_land = float(raw_land)
                if 'ha' in land_unit or 'hectare' in land_unit:
                    raw_land = raw_land * 10000
        except Exception:
            pass

        record = {
            'Property_ID':        pid_str,
            'Status':             status_label,
            'Full_Address':       full_address,
            'Suburb':             suburb,
            'Postcode':           postcode,
            'Property_Type':      f.get('propertyTypeFormatted', f.get('propertyType', 'N/A')),
            'Date':               date_val,
            'Beds':               f.get('beds', 0),
            'Baths':              f.get('baths', 0),
            'Car_Spaces':         f.get('parking', f.get('carspaces', 0)),
            'LandSize_sqm':       raw_land,
            'Propertycount':      np.nan,
            'Raw_Price':          raw_price,
            'Numeric_Price':      parse_raw_price(raw_price),
            'Latitude':           lat,
            'Longitude':          lng,
            'Distance_to_CBD_km': calculate_distance_to_cbd(lat, lng),
            'URL':                f"https://www.domain.com.au{url_path}" if url_path else "N/A",
            'Last_Updated':       pd.Timestamp.now().strftime('%Y-%m-%d'),
        }
        records.append(record)
        seen_records[pid_str] = {'Status': status_label, 'Price': raw_price}

    return records, total_pages


# ==========================================
# CELL SCRAPING (v5.4 — interleaved + backoff)
# ==========================================
def build_search_url(t_lat, b_lat, l_lng, r_lng, mode_path, mode_extra, pg):
    """
    Build search URL with randomised (but valid) query param order
    to add entropy to the request fingerprint.
    """
    base_params = [
        f"startloc={t_lat}%2C{l_lng}",
        f"endloc={b_lat}%2C{r_lng}",
    ]
    if mode_extra:
        base_params.insert(random.randint(0, 2), mode_extra)

    random.shuffle(base_params)  # cosmetic entropy
    query = "&".join(base_params)
    return f"https://www.domain.com.au/{mode_path}/?{query}&page={pg}"


def scrape_cell(page, cell_idx_global, cell, seen_records, pages_in_session):
    t_lat, b_lat, l_lng, r_lng = cell
    print(f"\n📍 cell #{cell_idx_global} | "
          f"{t_lat},{l_lng} → {b_lat},{r_lng}")

    cell_records  = 0
    cell_strikes  = 0

    # v5.4: interleave modes — alternate For Sale / Sold per page
    # instead of exhausting one mode then the other
    modes_cycle = list(SEARCH_MODES) * MAX_PAGES_PER_QUERY
    mode_idx    = 0

    # Track page numbers per mode independently
    page_nums     = {label: 1 for label, _, _ in SEARCH_MODES}
    mode_done     = {label: False for label, _, _ in SEARCH_MODES}
    mode_totals   = {label: 999 for label, _, _ in SEARCH_MODES}  # unknown until first fetch

    # Iterate until both modes exhausted or strikes exceeded
    attempts = 0
    while not all(mode_done.values()) and cell_strikes < CELL_MAX_STRIKES:
        if should_stop():
            print("   ⏰ Timeout — stopping cell mid-scrape")
            break

        # Pick next mode in round-robin (skip done modes)
        status_label, mode_path, mode_extra = modes_cycle[mode_idx % len(SEARCH_MODES)]
        mode_idx += 1
        if mode_done[status_label]:
            continue

        pg = page_nums[status_label]
        if pg > mode_totals[status_label]:
            mode_done[status_label] = True
            continue

        # Abandon heuristic
        if pg >= ABANDON_START_PAGE:
            prob = min(ABANDON_BASE_PROB + (pg - ABANDON_START_PAGE) * ABANDON_GROWTH,
                       ABANDON_MAX_PROB)
            if random.random() < prob:
                print(f"   🚪 {status_label}: stop at pg {pg} (abandon p={prob:.2f})")
                mode_done[status_label] = True
                continue

        # Rest / inter-request delay
        if pages_in_session >= PAGES_BEFORE_REST:
            rest = random.uniform(*REST_DURATION)
            print(f"   ☕ Rest {rest:.0f}s")
            interruptible_sleep(rest, "rest")
            pages_in_session = 0
        elif attempts > 0:
            human_delay(*DELAY_BETWEEN_REQUESTS)

        attempts += 1

        url     = build_search_url(t_lat, b_lat, l_lng, r_lng, mode_path, mode_extra, pg)
        payload = get_next_data(page, url)
        pages_in_session += 1

        if payload == 'BLOCKED':
            cell_strikes += 1
            print(f"   ⚠️  Block: {status_label} pg {pg} "
                  f"(strike {cell_strikes}/{CELL_MAX_STRIKES})")

            # v5.4: per-block recovery sleep before retry
            recovery = random.uniform(*BLOCK_RECOVERY_SLEEP)
            print(f"   😴 Block recovery: {recovery:.0f}s")
            interruptible_sleep(recovery, "block recovery")
            continue  # retry same mode+page next iteration

        # Success — reset strikes (v5.4: strike counter resets on any success)
        cell_strikes = 0

        page_records, total_pages = parse_listings_payload(payload, status_label, seen_records)
        mode_totals[status_label] = total_pages

        if page_records:
            save_incremental_data(page_records, FILE_NAME)
            cell_records     += len(page_records)
            pages_in_session += 0  # already counted above
            print(f"   + {len(page_records):3d}  ({status_label} pg {pg}/{total_pages})")
        elif pg == 1:
            print(f"   ◌ No {status_label} listings (pg 1)")
            mode_done[status_label] = True
            continue

        page_nums[status_label] = pg + 1
        if pg >= total_pages:
            mode_done[status_label] = True

    was_blocked = (cell_records == 0 and
                   sum(1 for v in mode_done.values() if not v) > 0 and
                   cell_strikes >= CELL_MAX_STRIKES)
    return cell_records, was_blocked, pages_in_session


# ==========================================
# CELL SELECTION (v5.4 — column-diversified shuffle)
# ==========================================
def select_cells_for_today(all_cells):
    manual_offset = os.getenv('MANUAL_OFFSET', '').strip()

    if manual_offset.isdigit():
        offset = int(manual_offset) % ROTATION_STRIDE
        print(f"   🎯 Manual override: offset {offset}")
    elif manual_offset.lower() == 'random':
        offset = random.randint(0, ROTATION_STRIDE - 1)
        print(f"   🎲 Random offset: {offset}")
    else:
        today  = dt.date.today()
        offset = today.toordinal() % ROTATION_STRIDE
        print(f"   📅 Auto offset {offset} for {today}")

    all_today = [(i, c) for i, c in enumerate(all_cells) if i % ROTATION_STRIDE == offset]

    # v5.4: group by lng column, then interleave groups so we don't
    # hammer the same lng band repeatedly.  Within each group, shuffle.
    lng_groups = {}
    for i, c in all_today:
        col = i % GRID_SIZE
        lng_groups.setdefault(col, []).append((i, c))
    for g in lng_groups.values():
        random.shuffle(g)

    # Round-robin across lng columns
    interleaved = []
    cols_sorted = sorted(lng_groups.keys())
    max_len     = max(len(v) for v in lng_groups.values())
    for k in range(max_len):
        for col in cols_sorted:
            if k < len(lng_groups[col]):
                interleaved.append(lng_groups[col][k])

    # v5.4: 20% jitter — occasionally swap adjacent cells for extra variance
    if random.random() < 0.2 and len(interleaved) >= 2:
        i1, i2 = random.sample(range(len(interleaved)), 2)
        interleaved[i1], interleaved[i2] = interleaved[i2], interleaved[i1]

    half    = len(interleaved) // 2
    slot_a  = interleaved[:half]
    slot_b  = interleaved[half:]

    if RUN_SLOT == 'A':
        selected = slot_a
        print(f"   🅰️ Slot A — {len(selected)} cells")
    elif RUN_SLOT == 'B':
        selected = slot_b
        print(f"   🅱️ Slot B — {len(selected)} cells")
    else:
        selected = interleaved
        print(f"   ⚠️ Invalid RUN_SLOT — using all {len(selected)} cells")

    if len(selected) > CELLS_PER_RUN:
        selected = selected[:CELLS_PER_RUN]

    print(f"   🗺️  Final cells: {sorted([idx for idx, _ in selected])}")
    return selected


# ==========================================
# MAIN
# ==========================================
def main():
    arm_watchdog()

    # Load existing records for dedup
    seen_records = {}
    if os.path.exists(FILE_NAME) and os.path.getsize(FILE_NAME) > 0:
        try:
            df_existing = pd.read_csv(FILE_NAME)
            for _, row in df_existing.iterrows():
                pid = str(row.get('Property_ID', ''))
                seen_records[pid] = {
                    'Status': str(row.get('Status', '')),
                    'Price':  str(row.get('Raw_Price', '')),
                }
            print(f"   📚 Loaded {len(seen_records)} existing records")
        except pd.errors.EmptyDataError:
            pass

    # Build grid
    lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
    lng_step = (LNG_EAST  - LNG_WEST)  / GRID_SIZE
    all_cells = []
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            t_lat = round(LAT_NORTH - (i      * lat_step), 4)
            b_lat = round(LAT_NORTH - ((i + 1) * lat_step), 4)
            l_lng = round(LNG_WEST  + (j      * lng_step), 4)
            r_lng = round(LNG_WEST  + ((j + 1) * lng_step), 4)
            all_cells.append((t_lat, b_lat, l_lng, r_lng))

    todays_cells = select_cells_for_today(all_cells)
    total_today  = len(todays_cells)
    num_sessions = math.ceil(total_today / CELLS_PER_SESSION) if total_today > 0 else 0
    print(f"   📊 {total_today} cells | {num_sessions} sessions")

    if PROXY_URL:
        print(f"   🛡️ Proxy: {PROXY_URL.split('@')[-1] if '@' in PROXY_URL else PROXY_URL}")
    else:
        print("   ⚠️  No proxy — no-proxy resilient mode (v5.4)")

    if total_today == 0:
        print("\n⚠️ No cells to scrape.")
        return

    total_records      = 0
    cells_done         = 0
    cells_empty        = 0
    cells_blocked      = 0
    consecutive_blocks = 0
    session_idx        = 0
    cell_pos           = 0

    while cell_pos < total_today:
        if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
            print(f"\n🛑 {consecutive_blocks} consecutive blocked cells — stopping run")
            break
        if should_stop():
            print(f"\n⏰ Timeout ({time_remaining()/60:.1f} min left) — stopping at "
                  f"{cell_pos}/{total_today} cells")
            break

        session_idx  += 1
        session_end   = min(cell_pos + CELLS_PER_SESSION, total_today)
        session_cells = todays_cells[cell_pos:session_end]

        print(f"\n{'='*60}")
        print(f"🚀 SESSION {session_idx}/{num_sessions} — "
              f"cells {cell_pos+1}–{session_end} of {total_today}")
        print(f"{'='*60}")

        try:
            # v5.5 FIX: Playwright 1.52+ sync API cannot be called from a thread
            # that already has a running asyncio event loop (common in GitHub Actions
            # Python 3.11 / some OS environments).  Running each session in its own
            # fresh daemon thread guarantees a clean event loop context.
            session_result = {
                "records":  0,
                "blocked":  False,
                "cell_pos": cell_pos,
                "error":    None,
            }

            def run_session():
                try:
                    with Camoufox(**make_camoufox_kwargs()) as browser:
                        page = browser.new_page()
                        page.set_default_timeout(NAV_TIMEOUT_MS)
                        page.set_default_navigation_timeout(NAV_TIMEOUT_MS)

                        if session_idx == 1:
                            warm_up_full(page)
                            warm_up_probe(page)
                        else:
                            warm_up_light(page)

                        human_delay(3.0, 6.0)

                        pages_in_sess = 0
                        sess_records  = 0
                        pos           = cell_pos

                        for (cell_idx_global, cell) in session_cells:
                            if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                                break
                            if should_stop():
                                break

                            cell_recs, was_blocked, pages_in_sess = scrape_cell(
                                page, cell_idx_global, cell, seen_records, pages_in_sess
                            )

                            if cell_recs > 0:
                                sess_records += cell_recs
                                session_result["records"] += cell_recs
                                session_result["blocked"]  = False
                            elif was_blocked:
                                session_result["blocked"] = True

                            pos += 1
                            session_result["cell_pos"] = pos

                        print(f"\n   📊 Session {session_idx}: +{sess_records} records")

                except Exception as e:
                    session_result["error"] = e

            t = threading.Thread(target=run_session, daemon=True)
            t.start()
            # Block main thread until session thread finishes (or timeout)
            t.join(timeout=time_remaining() - 60)

            if session_result["error"]:
                raise session_result["error"]

            # Propagate results back to main loop state
            new_pos      = session_result["cell_pos"]
            new_records  = session_result["records"]
            was_all_blocked = session_result["blocked"] and new_records == 0

            for _ in range(new_pos - cell_pos):
                # Count each cell outcome
                pass  # detailed accounting done inside run_session above

            # Update outer counters from thread results
            total_records += new_records
            if new_records > 0:
                cells_done         += (new_pos - cell_pos)
                consecutive_blocks  = 0
            elif was_all_blocked:
                cells_blocked      += (new_pos - cell_pos)
                consecutive_blocks += (new_pos - cell_pos)
                for _ in range(new_pos - cell_pos):
                    print(f"   🚫 Cell fully blocked (streak: {consecutive_blocks})")
            else:
                cells_done  += (new_pos - cell_pos)
                cells_empty += (new_pos - cell_pos)
                consecutive_blocks = 0

            cell_pos = new_pos

        except Exception as e:
            print(f"   ❌ Session exception: {e}")
            cell_pos = session_end

        if cell_pos < total_today and consecutive_blocks < MAX_CONSECUTIVE_BLOCKS:
            cooldown = random.uniform(*SESSION_COOLDOWN)
            print(f"\n   ⏰ Cooldown {cooldown:.0f}s...")
            interruptible_sleep(cooldown, "session cooldown")

    # ── Summary ──────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"✅ DONE — Slot {RUN_SLOT}")
    print(f"{'='*60}")
    print(f"   Sessions:      {session_idx}")
    print(f"   Cells done:    {cells_done}/{total_today}  "
          f"({cells_done - cells_empty} with data, {cells_empty} empty)")
    if cells_blocked:
        print(f"   Cells blocked: {cells_blocked}")
    print(f"   New records:   {total_records}")
    print(f"   Run time:      {(time.time()-SCRIPT_START_TIME)/60:.1f} min")
    if cell_pos < total_today:
        print(f"   Skipped:       {total_today - cell_pos} cells (next rotation cycle)")

    if total_records == 0 and consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
        print("⚠️  Early stop due to blocks")
        sys.exit(1)


if __name__ == '__main__':
    main()