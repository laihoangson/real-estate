import os
import json
import time
import random
import pandas as pd
import re
from bs4 import BeautifulSoup
from curl_cffi import requests 

print("🚀 STARTING CLOUDFLARE-BYPASS SCRAPER (CURL_CFFI MODE) 🚀")

FILE_NAME = 'data/melbourne_full_hybrid_data.csv'
GRID_SIZE = 12  

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
def extract_numeric_price(price_str):
    if not price_str or "contact" in price_str.lower() or "auction" in price_str.lower():
        return None
    numbers = re.findall(r'\d+', str(price_str).replace(',', '').replace('.', ''))
    numbers = [float(n) for n in numbers if len(n) >= 4]
    if len(numbers) >= 2:
        return (numbers[0] + numbers[1]) / 2 
    elif len(numbers) == 1:
        return numbers[0]
    return None

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
                            
                            full_address = f"{street}, {suburb} VIC {postcode}".strip() if street != 'N/A' else None

                            if full_address:
                                daily_scraped_data.append({
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
                                    'Numeric_Price': extract_numeric_price(raw_price),
                                    'Latitude': a.get('lat', m.get('geolocation', {}).get('latitude')),
                                    'Longitude': a.get('lng', m.get('geolocation', {}).get('longitude')),
                                    'URL': f"https://www.domain.com.au{url_path}" if url_path else "N/A",
                                    'Last_Updated': pd.Timestamp.now().strftime('%Y-%m-%d')
                                })
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