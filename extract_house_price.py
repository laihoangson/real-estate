import os
import json
import time
import pandas as pd
import random
import re
import math
from bs4 import BeautifulSoup
from curl_cffi import requests
import numpy as np

print("🚀 STARTING FAST SCRAPER 🚀")

FILE_NAME = 'data/melbourne_price_data.csv'
GRID_SIZE = 14  

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
def parse_raw_price(raw_price):
    if not isinstance(raw_price, str) or not raw_price.strip():
        return np.nan
    
    normalized = raw_price.lower().strip()
    if '$' not in normalized: 
        return np.nan
    
    # 1. FIX TYPO WITH EXTRA DIGITS (e.g., 3,0000,000 -> extract 3000000)
    # Whenever there are 4 or more digits after a ',' or '.', keep only the first 3 digits of that group
    normalized = re.sub(r'([.,])(\d{4,})', lambda m: m.group(1) + m.group(2)[:3], normalized)
    
    # Normalize separators
    normalized = normalized.replace(',', '').replace('–', '-').replace(' to ', '-')
    
    # 2. ONLY EXTRACT NUMBERS IMMEDIATELY FOLLOWING THE '$' SIGN (Ignore 10% deposit, years, etc.)
    matches = re.findall(r'\$\s*(\d+\.?\d*[km]?)', normalized)
    if not matches:
        return np.nan
        
    parsed_vals = []
    for val_str in matches:
        mult = 1
        if val_str.endswith('m'):
            mult = 1000000
            val_str = val_str[:-1]
        elif val_str.endswith('k'):
            # Ignore 'k' if the agent mistakenly typed something like 575000k
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
        
    # 3. HANDLE FHOG CASE
    if 'fhog' in normalized:
        return parsed_vals[0] # Always prioritize extracting the first number
        
    # 4. HANDLE PRICE RANGES (Contains a hyphen and has at least 2 extracted numbers)
    if len(parsed_vals) >= 2 and '-' in normalized:
        return (parsed_vals[0] + parsed_vals[1]) / 2
        
    # 5. DEFAULT
    return parsed_vals[0]

def calculate_distance_to_cbd(lat2, lon2):
    if pd.isna(lat2) or pd.isna(lon2): return np.nan
    lat1, lon1 = -37.8136, 144.9631
    R = 6371.0 
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlon, dlat = lon2_rad - lon1_rad, lat2_rad - lat1_rad
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return round(R * c, 2)

def human_delay(min_sec=0.5, max_sec=1.5):
    time.sleep(random.uniform(min_sec, max_sec))

def save_incremental_data(new_data_list, file_path):
    if not new_data_list: return
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
# 2. BROWSER SESSION & SETUP
# ==========================================
# NARROWED COORDINATES TO FIT GREATER MELBOURNE (Increases scraping speed)
LAT_NORTH, LAT_SOUTH = -37.5, -38.5
LNG_WEST, LNG_EAST = 144.35, 145.40
lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE

session = requests.Session(impersonate="chrome120")
seen_ids = set()

if os.path.exists(FILE_NAME) and os.path.getsize(FILE_NAME) > 0:
    try:
        df_existing = pd.read_csv(FILE_NAME)
        if 'Property_ID' in df_existing.columns:
            seen_ids = set(df_existing['Property_ID'].astype(str))
    except pd.errors.EmptyDataError: pass

SEARCH_MODES = [
    ('For Sale', 'sale/?excludeunderoffer=1'),
    ('Sold', 'sold-listings/?')
]
block_counter = 0

# ==========================================
# 3. MAIN SCRAPING LOOP 
# ==========================================
try:
    cell_idx = 0
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            cell_idx += 1
            t_lat = round(LAT_NORTH - (i * lat_step), 4)
            b_lat = round(LAT_NORTH - ((i + 1) * lat_step), 4)
            l_lng = round(LNG_WEST + (j * lng_step), 4)
            r_lng = round(LNG_WEST + ((j + 1) * lng_step), 4)
            
            print(f"\n📍 Cell [{cell_idx}/{GRID_SIZE*GRID_SIZE}] | Coordinates: {t_lat},{l_lng} to {b_lat},{r_lng}")
            
            for status_label, mode_url in SEARCH_MODES:
                grid_url = f"https://www.domain.com.au/{mode_url}&startloc={t_lat}%2C{l_lng}&endloc={b_lat}%2C{r_lng}"
                
                try:
                    response = session.get(f"{grid_url}&page=1", timeout=15)
                except Exception: continue

                soup = BeautifulSoup(response.text, 'html.parser')
                script_tag = soup.find('script', id='__NEXT_DATA__')
                
                if not script_tag: 
                    if "We couldn't find anything" not in response.text:
                        block_counter += 1
                        print(f"   -> ⚠️ Cloudflare Blocked (Strike {block_counter}/3).")
                        if block_counter >= 3: break
                        time.sleep(10)
                    continue
                
                block_counter = 0 
                data = json.loads(script_tag.string)
                props = data.get('props', {}).get('pageProps', {}).get('componentProps', {})
                total_pages = props.get('totalPages', 1)
                
                for page in range(1, total_pages + 1):
                    page_scraped_data = [] 
                    if page > 1:
                        human_delay(0.5, 1.5)
                        try:
                            response = session.get(f"{grid_url}&page={page}", timeout=15)
                            soup = BeautifulSoup(response.text, 'html.parser')
                            script_tag = soup.find('script', id='__NEXT_DATA__')
                        except: continue
                    
                    if script_tag:
                        page_data = json.loads(script_tag.string)
                        page_props = page_data.get('props', {}).get('pageProps', {}).get('componentProps', {})
                        listings = page_props.get('listingsMap', {})
                        
                        for pid, item in listings.items():
                            pid_str = str(pid)
                            if pid_str not in seen_ids:
                                m = item.get('listingModel', {})
                                a = m.get('address', {})
                                f = m.get('features', {})
                                
                                street = a.get('street', 'N/A')
                                suburb = str(a.get('suburb', 'Map Area')).upper()
                                postcode = a.get('postcode', '')
                                raw_price = str(m.get('price', 'N/A'))
                                url_path = m.get('url', '')
                                lat = a.get('lat', m.get('geolocation', {}).get('latitude'))
                                lng = a.get('lng', m.get('geolocation', {}).get('longitude'))
                                
                                full_address = f"{street}, {suburb} VIC {postcode}".strip() if street != 'N/A' else None
                                
                                # 1. DATE SOLD / DATE LISTED (BRUTE-FORCE REGEX)
                                date_val = m.get('dateSold', m.get('dateListed'))
                                if not date_val: date_val = m.get('status', {}).get('date')
                                
                                if not date_val:
                                    item_str = json.dumps(item)
                                    date_match = re.search(r'([0-9]{1,2}\s+[A-Za-z]{3,9}\s+[0-9]{4})', item_str)
                                    if date_match:
                                        date_val = date_match.group(1)
                                    else:
                                        iso_match = re.search(r'"[A-Za-z]*[dD]ate[A-Za-z]*"\s*:\s*"([0-9]{4}-[0-9]{2}-[0-9]{2})', item_str)
                                        if iso_match: date_val = iso_match.group(1)
                                
                                if not date_val: date_val = 'N/A'
                                
                                # 2. DETAILED METHOD CLASSIFICATION (AUCTION OR PRIVATE TREATY ONLY)
                                tags = [str(t).lower() for t in m.get('tags', [])]
                                # Capture the ENTIRE status object, saleMode string, and auction dict to guarantee we don't miss the word 'auction'
                                status_str = json.dumps(m.get('status', {})).lower()
                                sale_mode = str(m.get('saleMode', '')).lower()
                                auction_info = json.dumps(m.get('auction', {})).lower()
                                
                                combined_text = (raw_price + " " + " ".join(tags) + " " + status_str + " " + sale_mode + " " + auction_info).lower()
                                
                                method = "N/A"
                                if 'auction' in combined_text: 
                                    method = "Auction"
                                elif 'private treaty' in combined_text or 'sale' in combined_text or 'sold' in combined_text or 'under offer' in combined_text:
                                    method = "Private Treaty"
                                else:
                                    # Fallback to Private Treaty as the default for non-auctions in Australia
                                    method = "Private Treaty"
                                
                                # 3. CONVERT HA TO SQM
                                raw_land_size = f.get('landSize', np.nan)
                                land_unit = str(f.get('landUnit', '')).lower()
                                try:
                                    if pd.notna(raw_land_size):
                                        raw_land_size = float(raw_land_size)
                                        if 'ha' in land_unit or 'hectare' in land_unit:
                                            raw_land_size = raw_land_size * 10000
                                except:
                                    pass

                                if full_address:
                                    record = {
                                        'Property_ID': pid_str,
                                        'Status': status_label, 
                                        'Full_Address': full_address,
                                        'Suburb': suburb,
                                        'Postcode': postcode,
                                        'Property_Type': f.get('propertyTypeFormatted', f.get('propertyType', 'N/A')),
                                        'Method': method,
                                        'Date': date_val,
                                        'Beds': f.get('beds', 0),
                                        'Baths': f.get('baths', 0),
                                        'Car_Spaces': f.get('parking', f.get('carspaces', 0)),
                                        'LandSize_sqm': raw_land_size,
                                        'Propertycount': np.nan, # Auto updated in save_incremental_data
                                        'Raw_Price': raw_price,
                                        'Numeric_Price': parse_raw_price(raw_price),
                                        'Latitude': lat,
                                        'Longitude': lng,
                                        'Distance_to_CBD_km': calculate_distance_to_cbd(lat, lng),
                                        'URL': f"https://www.domain.com.au{url_path}" if url_path else "N/A",
                                        'Last_Updated': pd.Timestamp.now().strftime('%Y-%m-%d')
                                    }
                                    
                                    page_scraped_data.append(record)
                                    seen_ids.add(pid_str)
                    
                    if page_scraped_data:
                        save_incremental_data(page_scraped_data, FILE_NAME)
                        print(f"      + {len(page_scraped_data)} records saved (Page {page} - {status_label})")

            if block_counter >= 3: break
        if block_counter >= 3: break

finally:
    print("\n✅ SCRAPING COMPLETED.")