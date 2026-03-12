import os
import json
import time
import random
import pandas as pd
import re
import math # Đã thêm math cho công thức Haversine
from bs4 import BeautifulSoup
from curl_cffi import requests 
import numpy as np

print("🚀 STARTING CLOUDFLARE-BYPASS SCRAPER (CURL_CFFI MODE WITH DEEP EXTRACT & CBD DIST) 🚀")

FILE_NAME = 'data/melbourne_full_hybrid_data.csv'
GRID_SIZE = 14  

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
def parse_raw_price(raw_price):
    """
    Parses the Raw_Price string to extract a numeric price.
    Handles ranges by taking the average, single values directly.
    Supports 'M' for millions (e.g., 7M = 7000000, 7.7M = 7700000).
    Ignores non-price text and returns NaN if no valid price found or no $ sign.
    """
    if not isinstance(raw_price, str) or not raw_price.strip():
        return np.nan
    
    # Normalize the string: remove commas, replace en-dash with hyphen, lowercase
    normalized = raw_price.replace(',', '').replace('–', '-').lower().strip()
    
    # Check if there's a $ sign; if not, it's not money
    if '$' not in normalized:
        return np.nan
    
    # Handle 'M' or 'm' for millions, including decimals (e.g., 7.7m -> 7700000)
    normalized = re.sub(r'(\d+\.?\d*|\.\d+)m', lambda m: str(float(m.group(1)) * 1000000), normalized)
    
    # Handle 'k' or 'K' for thousands, including decimals (e.g., 25k -> 25000)
    normalized = re.sub(r'(\d+\.?\d*|\.\d+)k', lambda m: str(float(m.group(1)) * 1000), normalized)
    
    # Extract range: $123456 - $789101 or similar (optional $)
    range_match = re.search(r'[\$]?(\d+\.?\d*)\s*-\s*[\$]?(\d+\.?\d*)', normalized)
    if range_match:
        try:
            low = float(range_match.group(1))
            high = float(range_match.group(2))
            if low < high:
                return (low + high) / 2
            else:
                # Likely a discount or non-range, take the first (larger) value
                return low
        except ValueError:
            pass
    
    # Extract single value: $123456 or similar
    single_match = re.search(r'[\$]?(\d+\.?\d*)', normalized)
    if single_match:
        try:
            return float(single_match.group(1))
        except ValueError:
            pass
    
    # If no match, return NaN
    return np.nan

def extract_numeric(text):
    if pd.isna(text) or not str(text).strip(): return np.nan
    numbers = re.findall(r'\d+', str(text).replace(',', '').replace('.', ''))
    return float(numbers[0]) if numbers else np.nan

def calculate_distance_to_cbd(lat2, lon2):
    """
    Tính khoảng cách (tính bằng km) từ một tọa độ đến Melbourne CBD sử dụng công thức Haversine.
    Tọa độ Melbourne CBD: -37.8136, 144.9631
    """
    if pd.isna(lat2) or pd.isna(lon2):
        return np.nan
        
    lat1 = -37.8136
    lon1 = 144.9631
    R = 6371.0 # Bán kính Trái Đất tính bằng km

    # Chuyển đổi tọa độ từ độ sang radian
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    # Sự khác biệt về tọa độ
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    # Công thức Haversine
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    distance = R * c
    return round(distance, 2)

def get_deep_property_data(property_url, session_obj):
    """
    Vào trang chi tiết để lấy thông vị trí, Features, Demographics, Street Profile.
    Được gọi bên trong vòng lặp chính.
    """
    details = {
        'Year_Built': np.nan, 'Building_Size_sqm': np.nan, 'Is_New_Construction': 0,
        'Days_on_Market': np.nan,
        'Has_Ensuite': 0, 'Has_Dishwasher': 0, 'Has_Floorboards': 0,
        'Has_Ducted_Heating': 0, 'Has_Ducted_Cooling': 0, 'Has_Secure_Parking': 0,
        'Has_Courtyard': 0, 'Has_Balcony': 0,
        'Neighbour_Age_Under20_Pct': np.nan, 'Neighbour_Age_20_39_Pct': np.nan,
        'Neighbour_Age_40_59_Pct': np.nan, 'Neighbour_Age_60Plus_Pct': np.nan,
        'Neighbour_LongTermRes_Pct': np.nan, 'Neighbour_Owner_Pct': np.nan, 'Neighbour_Renter_Pct': np.nan,
        'Neighbour_Family_Pct': np.nan, 'Neighbour_Single_Pct': np.nan,
        'Street_Total_Properties': np.nan, 'Street_Recently_Sold': np.nan,
        'Street_Owner_Pct': np.nan, 'Street_Renter_Pct': np.nan
    }
    
    if not property_url or property_url == "N/A":
        return details
        
    try:
        response = session_obj.get(property_url, timeout=15)
        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find('script', id='__NEXT_DATA__')
        
        if script_tag:
            data = json.loads(script_tag.string)
            listing_data = data['props']['pageProps']['componentProps'].get('listing', {})
            
            # 1. Basic Details
            details['Is_New_Construction'] = 1 if listing_data.get('isNewDevelopment') else 0
            
            listed_date_str = listing_data.get('dateListed')
            if listed_date_str:
                try:
                    listed = pd.to_datetime(listed_date_str).tz_localize(None)
                    details['Days_on_Market'] = (pd.Timestamp.now() - listed).days
                except: pass
            
            property_details = listing_data.get('propertyDetails', [])
            for item in property_details:
                label = str(item.get('label', '')).lower()
                val = item.get('value')
                if 'year built' in label: details['Year_Built'] = extract_numeric(val)
                elif 'building size' in label: details['Building_Size_sqm'] = extract_numeric(val)

            # 2. Features (One-Hot)
            features = listing_data.get('features', [])
            f_str = " ".join(features).lower()
            if 'ensuite' in f_str: details['Has_Ensuite'] = 1
            if 'dishwasher' in f_str: details['Has_Dishwasher'] = 1
            if 'floorboards' in f_str: details['Has_Floorboards'] = 1
            if 'ducted heating' in f_str: details['Has_Ducted_Heating'] = 1
            if 'ducted cooling' in f_str or 'airconditioning' in f_str: details['Has_Ducted_Cooling'] = 1
            if 'secure parking' in f_str: details['Has_Secure_Parking'] = 1
            if 'courtyard' in f_str: details['Has_Courtyard'] = 1
            if 'balcony' in f_str or 'deck' in f_str: details['Has_Balcony'] = 1

            # 3. Demographics & Street
            insights = listing_data.get('insights', {})
            demographics = insights.get('demographics', {})
            if demographics:
                details['Neighbour_Age_Under20_Pct'] = extract_numeric(demographics.get('ageUnder20'))
                details['Neighbour_Age_20_39_Pct'] = extract_numeric(demographics.get('age20To39'))
                details['Neighbour_Age_40_59_Pct'] = extract_numeric(demographics.get('age40To59'))
                details['Neighbour_Age_60Plus_Pct'] = extract_numeric(demographics.get('age60Plus'))
                details['Neighbour_LongTermRes_Pct'] = extract_numeric(demographics.get('longTermResidents'))
                details['Neighbour_Owner_Pct'] = extract_numeric(demographics.get('ownerOccupier'))
                details['Neighbour_Renter_Pct'] = extract_numeric(demographics.get('renter'))
                details['Neighbour_Family_Pct'] = extract_numeric(demographics.get('family'))
                details['Neighbour_Single_Pct'] = extract_numeric(demographics.get('single'))

            street_profile = insights.get('streetProfile', {})
            if street_profile:
                details['Street_Total_Properties'] = extract_numeric(street_profile.get('totalProperties'))
                details['Street_Recently_Sold'] = extract_numeric(street_profile.get('recentlySold'))
                details['Street_Owner_Pct'] = extract_numeric(street_profile.get('ownerOccupier'))
                details['Street_Renter_Pct'] = extract_numeric(street_profile.get('renter'))

    except Exception:
        pass # Silently fail on individual properties to keep the main loop running
        
    return details

def human_delay(min_sec=1.0, max_sec=2.5):
    time.sleep(random.uniform(min_sec, max_sec))

# ==========================================
# 2. COORDINATES & SESSION
# ==========================================
LAT_NORTH = -37.4000
LAT_SOUTH = -38.2000
LNG_WEST = 144.6000
LNG_EAST = 145.4000

lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE

# Khởi tạo Session đóng giả Chrome phiên bản 120
session = requests.Session(impersonate="chrome120")

# ==========================================
# 3. DATA EXTRACTION PROCESS
# ==========================================
daily_scraped_data = []
seen_ids = set()

# Load ID cũ nếu có để tránh trùng lặp
if os.path.exists(FILE_NAME):
    df_existing = pd.read_csv(FILE_NAME)
    if 'Property_ID' in df_existing.columns:
        seen_ids = set(df_existing['Property_ID'].astype(str))

block_counter = 0

try:
    cell_idx = 0
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            cell_idx += 1
            
            t_lat = round(LAT_NORTH - (i * lat_step), 4)
            b_lat = round(LAT_NORTH - ((i + 1) * lat_step), 4)
            l_lng = round(LNG_WEST + (j * lng_step), 4)
            r_lng = round(LNG_WEST + ((j + 1) * lng_step), 4)
            
            grid_url = f"https://www.domain.com.au/sale/?excludeunderoffer=1&startloc={t_lat}%2C{l_lng}&endloc={b_lat}%2C{r_lng}"
            
            print(f"\n📍 Cell [{cell_idx}/{GRID_SIZE*GRID_SIZE}] | Requesting...")
            
            try:
                # Gửi request siêu tàng hình
                response = session.get(f"{grid_url}&page=1", timeout=15)
            except Exception as e:
                print(f"   -> Lỗi kết nối: {e}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            script_tag = soup.find('script', id='__NEXT_DATA__')
            
            if not script_tag: 
                if "We couldn't find anything" in response.text:
                    print("   -> (Empty) Không có nhà ở ô này.")
                    block_counter = 0
                else:
                    block_counter += 1
                    print(f"   -> ⚠️ Bị Cloudflare chặn (Strike {block_counter}/3).")
                    if block_counter >= 3:
                        print("🚨 DỪNG LẠI! Github IP đã bị khoanh vùng. Lưu data và thoát...")
                        break
                    time.sleep(10)
                continue
            
            block_counter = 0 # Reset
            
            data = json.loads(script_tag.string)
            props = data['props']['pageProps']['componentProps']
            total_pages = props.get('totalPages', 1)
            
            # Quét từng trang của ô
            for page in range(1, total_pages + 1):
                if page > 1:
                    human_delay(1.0, 2.0)
                    try:
                        response = session.get(f"{grid_url}&page={page}", timeout=15)
                        soup = BeautifulSoup(response.text, 'html.parser')
                        script_tag = soup.find('script', id='__NEXT_DATA__')
                    except:
                        continue
                
                if script_tag:
                    listings = json.loads(script_tag.string)['props']['pageProps']['componentProps'].get('listingsMap', {})
                    for pid, item in listings.items():
                        pid_str = str(pid)
                        if pid_str not in seen_ids:
                            m = item.get('listingModel', {})
                            a = m.get('address', {})
                            f = m.get('features', {})
                            
                            street = a.get('street', 'N/A')
                            suburb = str(a.get('suburb', 'Map Area')).upper()
                            postcode = a.get('postcode', '')
                            raw_price = m.get('price', 'N/A')
                            url_path = m.get('url', '')
                            
                            # Lấy tọa độ
                            lat = a.get('lat', m.get('geolocation', {}).get('latitude'))
                            lng = a.get('lng', m.get('geolocation', {}).get('longitude'))
                            
                            full_address = f"{street}, {suburb} VIC {postcode}".strip() if street != 'N/A' else None
                            property_full_url = f"https://www.domain.com.au{url_path}" if url_path else "N/A"

                            if full_address:
                                # TRÍCH XUẤT CƠ BẢN
                                base_record = {
                                    'Property_ID': pid_str,
                                    'Full_Address': full_address,
                                    'Suburb': suburb,
                                    'Postcode': postcode,
                                    'Property_Type': f.get('propertyTypeFormatted', f.get('propertyType', 'N/A')),
                                    'Beds': f.get('beds', 0),
                                    'Baths': f.get('baths', 0),
                                    'Car_Spaces': f.get('parking', f.get('carspaces', 0)),
                                    'Land_Size_sqm': f.get('landSize', 0),
                                    'Raw_Price': raw_price,
                                    'Numeric_Price': parse_raw_price(raw_price),
                                    'Latitude': lat,
                                    'Longitude': lng,
                                    'Distance_to_CBD_km': calculate_distance_to_cbd(lat, lng), # <-- GỌI HÀM TẠI ĐÂY
                                    'URL': property_full_url,
                                    'Last_Updated': pd.Timestamp.now().strftime('%Y-%m-%d')
                                }
                                
                                # TRÍCH XUẤT SÂU
                                if property_full_url != "N/A":
                                    print(f"      -> Khai thác sâu: {pid_str}")
                                    human_delay(1.5, 3.0) 
                                    deep_data = get_deep_property_data(property_full_url, session)
                                    base_record.update(deep_data) 
                                
                                daily_scraped_data.append(base_record)
                                seen_ids.add(pid_str)

        if block_counter >= 3:
            break

finally:
    pass

# ==========================================
# 4. UPSERT LOGIC
# ==========================================
print("\n[3] WRITING TO DATABASE...")

if not daily_scraped_data:
    print("❌ Hôm nay không cào thêm được căn nào (hoặc đã quét hết).")
else:
    df_new = pd.DataFrame(daily_scraped_data)
    if os.path.exists(FILE_NAME):
        df_old = pd.read_csv(FILE_NAME)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_final = df_combined.drop_duplicates(subset=['Property_ID'], keep='last')
        df_final.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"✅ Đã gộp thành công! Tổng số nhà trong kho: {len(df_final)}")
    else:
        df_new.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"✅ Đã tạo file CSV mới. Tổng số nhà: {len(df_new)}")