import os
import json
import time
import random
import re
import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


print("🚀 STARTING MELBOURNE PROPERTY SCRAPER 🚀")


# ==========================================
# CONFIG
# ==========================================

FILE_NAME = "melbourne_full_data.csv"

LAT_NORTH = -37.4000
LAT_SOUTH = -38.2000
LNG_WEST = 144.6000
LNG_EAST = 145.4000

GRID_SIZE = 30   # 30x30 = 900 cells (safe for GitHub)

lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE

results = []
seen_ids = set()


# ==========================================
# CHROME OPTIONS (GITHUB SAFE)
# ==========================================

chrome_options = Options()

chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")

chrome_options.add_argument("--window-size=1920,1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

prefs = {
    "profile.managed_default_content_settings.images": 2,
    "profile.managed_default_content_settings.stylesheets": 2
}

chrome_options.add_experimental_option("prefs", prefs)


service = Service(ChromeDriverManager().install())

driver = webdriver.Chrome(service=service, options=chrome_options)


# ==========================================
# HELPERS
# ==========================================

def extract_numeric_price(price):

    if not price:
        return None

    if "auction" in price.lower():
        return None

    nums = re.findall(r"\d+", price.replace(",", ""))

    if len(nums) >= 2:
        return (float(nums[0]) + float(nums[1])) / 2

    if len(nums) == 1:
        return float(nums[0])

    return None


def get_json():

    soup = BeautifulSoup(driver.page_source, "html.parser")

    script = soup.find("script", id="__NEXT_DATA__")

    if not script:
        return None

    return json.loads(script.string)


# ==========================================
# SCRAPING
# ==========================================

try:

    cell_counter = 0
    total_cells = GRID_SIZE * GRID_SIZE

    for i in range(GRID_SIZE):

        for j in range(GRID_SIZE):

            cell_counter += 1

            top_lat = LAT_NORTH - (i * lat_step)
            bottom_lat = LAT_NORTH - ((i + 1) * lat_step)
            left_lng = LNG_WEST + (j * lng_step)
            right_lng = LNG_WEST + ((j + 1) * lng_step)

            base_url = (
                f"https://www.domain.com.au/sale/"
                f"?excludeunderoffer=1"
                f"&startloc={top_lat}%2C{left_lng}"
                f"&endloc={bottom_lat}%2C{right_lng}"
            )

            print(f"\n📍 Cell {cell_counter}/{total_cells}")

            driver.get(base_url + "&page=1")

            time.sleep(random.uniform(1.5, 2.5))

            data = get_json()

            if not data:
                print("No data found")
                continue

            comp = data["props"]["pageProps"]["componentProps"]

            total_pages = comp.get("totalPages", 1)

            print(f"Pages: {total_pages}")

            if total_pages >= 50:
                print("⚠️ Possible page limit")

            for page in range(1, total_pages + 1):

                if page > 1:

                    driver.get(base_url + f"&page={page}")

                    time.sleep(random.uniform(1.2, 2.0))

                    data = get_json()

                    if not data:
                        continue

                try:

                    listings = data["props"]["pageProps"]["componentProps"]["listingsMap"]

                except:

                    continue

                for pid, item in listings.items():

                    if pid in seen_ids:
                        continue

                    seen_ids.add(pid)

                    model = item.get("listingModel", {})
                    addr = model.get("address", {})
                    feat = model.get("features", {})

                    price = model.get("price")

                    results.append({

                        "Property_ID": pid,
                        "Street": addr.get("street"),
                        "Suburb": addr.get("suburb"),
                        "Postcode": addr.get("postcode"),

                        "Latitude": addr.get("lat"),
                        "Longitude": addr.get("lng"),

                        "Beds": feat.get("beds"),
                        "Baths": feat.get("baths"),
                        "Parking": feat.get("parking"),

                        "Land_Size": feat.get("landSize"),

                        "Raw_Price": price,
                        "Numeric_Price": extract_numeric_price(price),

                        "URL": "https://www.domain.com.au" + model.get("url", "")
                    })

except KeyboardInterrupt:

    print("⚠️ Interrupted")

finally:

    driver.quit()


# ==========================================
# SAVE DATA
# ==========================================

print("\n💾 SAVING DATA")

if results:

    df_new = pd.DataFrame(results)

    if os.path.exists(FILE_NAME):

        df_old = pd.read_csv(FILE_NAME)

        df = pd.concat([df_old, df_new])

        df = df.drop_duplicates(subset="Property_ID", keep="last")

    else:

        df = df_new.drop_duplicates(subset="Property_ID")

    df.to_csv(FILE_NAME, index=False)

    print("Saved", len(df), "total properties")

else:

    print("No data collected")