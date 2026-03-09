import os
import json
import time
import random
import pandas as pd
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

print("🚀 STARTING OPTIMIZED HYBRID SCRAPER (15x15 GRID - STEALTH MODE) 🚀")

# ==========================================
# 1. BROWSER & SCRIPT CONFIGURATION
# ==========================================
FILE_NAME = 'melbourne_full_hybrid_data.csv'
GRID_SIZE = 15 

chrome_options = Options()
chrome_options.page_load_strategy = 'eager' 
prefs = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", prefs)

# Mandatory settings for GitHub Actions (Ubuntu Server Environment)
# chrome_options.add_argument("--headless=new") # Uncomment if running on server
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--remote-debugging-port=9222")

# Spoofing User-Agent
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=chrome_options)

# ==========================================
# 2. HELPER FUNCTIONS
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

def human_delay(min_sec=1.5, max_sec=3.5):
    """Giả lập độ trễ ngẫu nhiên của con người để chống bot"""
    time.sleep(random.uniform(min_sec, max_sec))

# ==========================================
# 3. COORDINATES (MELBOURNE METRO BOUNDING BOX)
# ==========================================
LAT_NORTH = -37.4000
LAT_SOUTH = -38.2000
LNG_WEST = 144.6000
LNG_EAST = 145.4000

lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE

daily_scraped_data = []
seen_ids = set()

# ==========================================
# 4. DATA EXTRACTION PROCESS
# ==========================================
try:
    cell_idx = 0
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            cell_idx += 1
            
            t_lat = LAT_NORTH - (i * lat_step)
            b_lat = LAT_NORTH - ((i + 1) * lat_step)
            l_lng = LNG_WEST + (j * lng_step)
            r_lng = LNG_WEST + ((j + 1) * lng_step)
            
            grid_url = f"https://www.domain.com.au/sale/?excludeunderoffer=1&startloc={t_lat}%2C{l_lng}&endloc={b_lat}%2C{r_lng}"
            
            print(f"\n📍 Cell [{cell_idx}/{GRID_SIZE*GRID_SIZE}] | Coordinates: {t_lat:.3f},{l_lng:.3f} to {b_lat:.3f},{r_lng:.3f}")
            driver.get(f"{grid_url}&page=1")
            
            # Tạm nghỉ lâu hơn khi sang ô mới
            human_delay(2.5, 4.5) 
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            script_tag = soup.find('script', id='__NEXT_DATA__')
            
            if not script_tag: 
                # Thử check xem có phải ô trống thật không hay là bị chặn
                if "We couldn't find anything" in driver.page_source:
                    print("   -> (Empty) No properties in this grid cell.")
                else:
                    print("   -> ⚠️ WARNING: Blocked by Anti-Bot. Taking a long break...")
                    time.sleep(15) # Ngủ 15s nếu bị chặn để giải phóng IP
                continue
            
            data = json.loads(script_tag.string)
            props = data['props']['pageProps']['componentProps']
            total_pages = props.get('totalPages', 1)
            
            if total_pages >= 50:
                print(f"   ⚠️ DENSITY ALERT: Hits 50-page limit.")

            for page in range(1, total_pages + 1):
                if page > 1:
                    driver.get(f"{grid_url}&page={page}")
                    # Tạm nghỉ giữa các lần lật trang
                    human_delay(1.0, 2.5)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    script_tag = soup.find('script', id='__NEXT_DATA__')
                
                if script_tag:
                    listings = json.loads(script_tag.string)['props']['pageProps']['componentProps'].get('listingsMap', {})
                    for pid, item in listings.items():
                        if pid not in seen_ids:
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
                                    'Property_ID': pid,
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
                                seen_ids.add(pid)

finally:
    driver.quit()

# ==========================================
# 5. UPSERT LOGIC & DATA LOAD
# ==========================================
print("\n[3] WRITING TO DATABASE...")

if not daily_scraped_data:
    print("❌ Failure: No data collected today.")
else:
    df_new = pd.DataFrame(daily_scraped_data)
    if os.path.exists(FILE_NAME):
        df_old = pd.read_csv(FILE_NAME)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_final = df_combined.drop_duplicates(subset=['Property_ID'], keep='last')
        df_final.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"✅ Upsert successful. Total properties in warehouse: {len(df_final)}")
    else:
        df_new.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"✅ Initial CSV created. Total properties: {len(df_new)}")