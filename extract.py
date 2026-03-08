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
chrome_options.page_load_strategy = 'eager'

chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")

chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

print("🚀 INITIALIZING HYBRID SCRAPER: GRID BOUNDING-BOX + LIST VIEW PAGINATION 🚀")

# ==========================================
# 1. CHROME CONFIGURATION (TURBO MODE)
# ==========================================
FILE_NAME = 'melbourne_full_hybrid_data.csv'

chrome_options = Options()
chrome_options.page_load_strategy = 'eager' # Don't wait for images/css
prefs = {
    "profile.managed_default_content_settings.images": 2,
    "profile.managed_default_content_settings.stylesheets": 2,
    "profile.default_content_setting_values.notifications": 2
}
chrome_options.add_experimental_option("prefs", prefs)

# Anti-Bot
chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def extract_numeric_price(price_str):
    if not price_str or "contact" in price_str.lower() or "auction" in price_str.lower():
        return None
    numbers = re.findall(r'\d+', str(price_str).replace(',', ''))
    if len(numbers) >= 2:
        return (float(numbers[0]) + float(numbers[1])) / 2 
    elif len(numbers) == 1:
        return float(numbers[0])
    return None

# ==========================================
# 3. GRID GENERATION (MELBOURNE BOUNDING BOX)
# ==========================================
# Melbourne coordinates roughly covering the entire metropolitan area
LAT_NORTH = -37.4000
LAT_SOUTH = -38.2000
LNG_WEST = 144.6000
LNG_EAST = 145.4000

GRID_SIZE = 30 # 30x30 = 900 grid cells

lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE

daily_scraped_data = []
seen_ids = set()

# ==========================================
# 4. EXTRACTION EXECUTION
# ==========================================
print("\n[i] Commencing Grid Search. Press CTRL+C to stop safely and save data.\n")

try:
    current_cell = 0
    total_cells = GRID_SIZE * GRID_SIZE
    
    for i in range(GRID_SIZE):
        for j in range(GRID_SIZE):
            current_cell += 1
            
            # Calculate coordinates for the current small cell
            top_lat = LAT_NORTH - (i * lat_step)
            bottom_lat = LAT_NORTH - ((i + 1) * lat_step)
            left_lng = LNG_WEST + (j * lng_step)
            right_lng = LNG_WEST + ((j + 1) * lng_step)
            
            print(f"==================================================")
            print(f"📍 Grid Cell [{current_cell}/{total_cells}]")
            
            # THE MAGIC URL: Using startloc & endloc but NO displaymap=1 (List View Mode)
            base_grid_url = f"https://www.domain.com.au/sale/?excludeunderoffer=1&startloc={top_lat}%2C{left_lng}&endloc={bottom_lat}%2C{right_lng}"
            
            # Visit Page 1 of this specific grid cell to find total pages
            driver.get(f"{base_grid_url}&page=1")
            time.sleep(2) # Eager loading is fast
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            script_tag = soup.find('script', id='__NEXT_DATA__')
            
            total_pages = 1
            if script_tag:
                try:
                    json_data = json.loads(script_tag.string)
                    # Get pagination data
                    total_pages = json_data['props']['pageProps']['componentProps'].get('totalPages', 1)
                    total_results = json_data['props']['pageProps']['componentProps'].get('totalListings', 0)
                    print(f"    -> Found {total_results} properties spread across {total_pages} pages.")
                    
                    if total_pages >= 50:
                        print("    ⚠️ WARNING: Hit 50-page limit. Some properties in this cell might be hidden.")
                        
                except KeyError:
                    print("    -> No pagination found. Assuming 1 page.")
            else:
                print("    -> No properties or blocked.")
                continue
                
            # Iterate through all pages of this specific grid cell
            for page in range(1, total_pages + 1):
                if page > 1:
                    driver.get(f"{base_grid_url}&page={page}")
                    time.sleep(1.5)
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    script_tag = soup.find('script', id='__NEXT_DATA__')
                
                if script_tag:
                    json_data = json.loads(script_tag.string)
                    try:
                        listings_map = json_data['props']['pageProps']['componentProps']['listingsMap']
                        
                        for prop_id, item in listings_map.items():
                            if prop_id in seen_ids:
                                continue # Skip already processed properties
                                
                            model = item.get('listingModel', {})
                            address_info = model.get('address', {})
                            features = model.get('features', {})
                            
                            street = address_info.get('street', 'N/A')
                            if street == 'N/A': continue
                            
                            suburb = address_info.get('suburb', 'Map Area')
                            postcode = address_info.get('postcode', '')
                            full_address = f"{street}, {suburb} VIC {postcode}"
                            
                            raw_price = model.get('price', 'N/A')
                            url_path = model.get('url', '')

                            house = {
                                'Property_ID': prop_id,
                                'Full_Address': full_address,
                                'Suburb': suburb,
                                'Postcode': postcode,
                                'Property_Type': features.get('propertyTypeFormatted', 'N/A'),
                                'Beds': features.get('beds', 0),
                                'Baths': features.get('baths', 0),
                                'Car_Spaces': features.get('parking', 0),
                                'Land_Size_sqm': features.get('landSize', 0),
                                'Raw_Price': raw_price,
                                'Numeric_Price': extract_numeric_price(raw_price),
                                'Latitude': address_info.get('lat', None),
                                'Longitude': address_info.get('lng', None),
                                'URL': f"https://www.domain.com.au{url_path}" if url_path else "N/A",
                                'Last_Updated': pd.Timestamp.now().strftime('%Y-%m-%d')
                            }
                            
                            daily_scraped_data.append(house)
                            seen_ids.add(prop_id)
                            
                    except KeyError:
                        pass

except KeyboardInterrupt:
    print("\n⚠️ [WARNING] Process interrupted by user. Proceeding to save collected data...")

except Exception as e:
    print(f"\n❌ [ERROR] Encountered an error: {e}. Proceeding to save collected data...")

finally:
    driver.quit()

# ==========================================
# 5. DATA LOAD & UPSERT
# ==========================================
print("\n[3] WRITING TO DATABASE...")
if daily_scraped_data:
    df_new = pd.DataFrame(daily_scraped_data)
    
    if os.path.exists(FILE_NAME):
        print(f"  -> Historical database '{FILE_NAME}' found. Performing UPSERT...")
        df_old = pd.read_csv(FILE_NAME)
        
        df_combined = pd.concat([df_old, df_new])
        df_final = df_combined.drop_duplicates(subset=['Property_ID'], keep='last')
        
        new_records = len(df_final) - len(df_old)
        print(f"  -> Updated existing prices and added {max(0, new_records)} newly listed properties.")
    else:
        print(f"  -> Creating new master database '{FILE_NAME}'.")
        df_final = df_new.drop_duplicates(subset=['Property_ID'])

    df_final.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
    print(f"✅ [SUCCESS] Pipeline completed! Total properties extracted: {len(df_new)}. Warehouse size: {len(df_final)}.")
else:
    print("❌ No data was extracted.")