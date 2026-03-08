import os
import json
import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

chrome_options = Options()
chrome_options.add_argument("--headless=new") 
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--remote-debugging-port=9222")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

print("🚀 STARTING OPTIMIZED HYBRID SCRAPER (12x12 GRID) 🚀")

# ==========================================
# 1. CONFIGURATION
# ==========================================
FILE_NAME = 'melbourne_full_hybrid_data.csv'
# Increasing density to 12x12 (144 cells) to stay under the 50-page limit
GRID_SIZE = 12 

chrome_options = Options()
chrome_options.page_load_strategy = 'eager' 
prefs = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# ==========================================
# 2. COORDINATES & GRID
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
# 3. EXECUTION
# ==========================================
try:
    cell_idx = 0
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            cell_idx += 1
            t_lat, b_lat = LAT_NORTH - (i * lat_step), LAT_NORTH - ((i + 1) * lat_step)
            l_lng, r_lng = LNG_WEST + (j * lng_step), LNG_WEST + ((j + 1) * lng_step)
            
            grid_url = f"https://www.domain.com.au/sale/?excludeunderoffer=1&startloc={t_lat}%2C{l_lng}&endloc={b_lat}%2C{r_lng}"
            
            print(f"📍 Cell [{cell_idx}/144] | Scanning area...")
            driver.get(f"{grid_url}&page=1")
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            script_tag = soup.find('script', id='__NEXT_DATA__')
            
            if not script_tag: continue
            
            data = json.loads(script_tag.string)
            props = data['props']['pageProps']['componentProps']
            total_pages = props.get('totalPages', 1)
            
            if total_pages >= 50:
                print(f"⚠️  DENSITY ALERT: Cell {cell_idx} still hits 50-page limit. Data might be truncated.")

            for page in range(1, total_pages + 1):
                if page > 1:
                    driver.get(f"{grid_url}&page={page}")
                    time.sleep(1.5)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    script_tag = soup.find('script', id='__NEXT_DATA__')
                
                if script_tag:
                    listings = json.loads(script_tag.string)['props']['pageProps']['componentProps'].get('listingsMap', {})
                    for pid, item in listings.items():
                        if pid not in seen_ids:
                            m = item.get('listingModel', {})
                            a = m.get('address', {})
                            f = m.get('features', {})
                            
                            daily_scraped_data.append({
                                'Property_ID': pid,
                                'Address': f"{a.get('street')}, {a.get('suburb')} VIC",
                                'Price': m.get('price'),
                                'Beds': f.get('beds'),
                                'Baths': f.get('baths'),
                                'Lat': a.get('lat'),
                                'Lng': a.get('lng'),
                                'URL': f"https://www.domain.com.au{m.get('url')}"
                            })
                            seen_ids.add(pid)

finally:
    driver.quit()
    if daily_scraped_data:
        pd.DataFrame(daily_scraped_data).to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"✅ Success! Captured {len(daily_scraped_data)} unique properties.")
        
if not daily_scraped_data:
    print("No data found today.")
else:
    df_new = pd.DataFrame(daily_scraped_data)
    if os.path.exists(FILE_NAME):
        # Logic Update (Upsert) như cũ
        pass
    else:
        # Tạo mới hoàn toàn nếu chưa có file
        df_new.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')