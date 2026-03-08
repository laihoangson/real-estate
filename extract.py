import os
import json
import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

print("🚀 STARTING OPTIMIZED HYBRID SCRAPER (12x12 GRID) 🚀")

# ==========================================
# 1. BROWSER & SCRIPT CONFIGURATION
# ==========================================
FILE_NAME = 'melbourne_full_hybrid_data.csv'
GRID_SIZE = 12 

chrome_options = Options()
# Eager loading: Do not wait for images or CSS to load (Speed optimization)
chrome_options.page_load_strategy = 'eager' 
prefs = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", prefs)

# Mandatory settings for GitHub Actions (Ubuntu Server Environment)
chrome_options.add_argument("--headless=new") 
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("--remote-debugging-port=9222")

# CRITICAL: Spoof the User-Agent to avoid being flagged as a headless bot
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

driver = webdriver.Chrome(options=chrome_options)

# ==========================================
# 2. COORDINATES (MELBOURNE METRO BOUNDING BOX)
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
# 3. DATA EXTRACTION PROCESS
# ==========================================
try:
    cell_idx = 0
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            cell_idx += 1
            
            # Calculate coordinates for the current sub-grid cell
            t_lat = LAT_NORTH - (i * lat_step)
            b_lat = LAT_NORTH - ((i + 1) * lat_step)
            l_lng = LNG_WEST + (j * lng_step)
            r_lng = LNG_WEST + ((j + 1) * lng_step)
            
            grid_url = f"https://www.domain.com.au/sale/?excludeunderoffer=1&startloc={t_lat}%2C{l_lng}&endloc={b_lat}%2C{r_lng}"
            
            print(f"📍 Cell [{cell_idx}/144] | Scanning area coordinates...")
            driver.get(f"{grid_url}&page=1")
            time.sleep(2)
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            script_tag = soup.find('script', id='__NEXT_DATA__')
            
            # If the script tag is missing, the IP might be blocked or the cell is empty
            if not script_tag: 
                print("   -> 🚫 Blocked by Anti-Bot or no data available in this cell.")
                continue
            
            data = json.loads(script_tag.string)
            props = data['props']['pageProps']['componentProps']
            total_pages = props.get('totalPages', 1)
            
            if total_pages >= 50:
                print(f"   ⚠️ DENSITY ALERT: Cell {cell_idx} hits 50-page limit. Some properties may be truncated.")

            # Iterate through all available pages in the current grid cell
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
    # Ensure the browser session is properly closed to free up server memory
    driver.quit()

# ==========================================
# 4. UPSERT LOGIC & DATA LOAD
# ==========================================
print("\n[3] WRITING TO DATABASE...")

if not daily_scraped_data:
    print("❌ Failure: No data collected today (Possible IP block by target server).")
else:
    df_new = pd.DataFrame(daily_scraped_data)
    
    if os.path.exists(FILE_NAME):
        # Merge new data with historical data
        df_old = pd.read_csv(FILE_NAME)
        df_combined = pd.concat([df_old, df_new])
        
        # Upsert: Remove duplicate IDs, keeping the most recent (last) entry to capture price updates
        df_final = df_combined.drop_duplicates(subset=['Property_ID'], keep='last')
        df_final.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"✅ Upsert successful. Total properties in warehouse: {len(df_final)}")
    else:
        # First-time execution logic
        df_new.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
        print(f"✅ Initial CSV created. Total properties: {len(df_new)}")