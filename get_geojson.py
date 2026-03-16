import os
import json
import time
import pandas as pd
import requests

print("🌍 STARTING SPATIAL BOUNDARY DATA DOWNLOAD (GEOJSON) - SAFE MODE 🌍")

INPUT_CSV = 'data/melbourne_price_data.csv'
OUTPUT_GEOJSON = 'data/melbourne_suburb_boundaries.geojson'

# 1. Check CSV file
if not os.path.exists(INPUT_CSV):
    print(f"❌ File not found: {INPUT_CSV}")
    exit()

df = pd.read_csv(INPUT_CSV)
if 'Suburb' not in df.columns:
    print("❌ CSV file does not have a 'Suburb' column. Cannot proceed.")
    exit()

suburbs = sorted([str(s).strip() for s in df['Suburb'].dropna().unique()])
print(f"📌 Found a total of {len(suburbs)} unique suburbs in the list.")

# 2. Read existing GeoJSON to RESUME PROGRESS (Avoid scraping from scratch)
geojson_features = []
completed_suburbs = set()

if os.path.exists(OUTPUT_GEOJSON):
    try:
        with open(OUTPUT_GEOJSON, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
            geojson_features = existing_data.get("features", [])
            # Record successfully scraped Suburbs
            for feat in geojson_features:
                if 'properties' in feat and 'Suburb' in feat['properties']:
                    completed_suburbs.add(feat['properties']['Suburb'])
        print(f"♻️ Recovered {len(completed_suburbs)} boundaries from the existing file. Skipping these areas.")
    except Exception as e:
        print(f"⚠️ Cannot read existing GeoJSON file (might be corrupted): {e}. Will create a new file.")

# 3. Prepare API Call
headers = {
    'User-Agent': 'MelbournePropertyDashboard/1.1 (DataAnalyticsProject - Contact: [YourEmail])',
    'Accept-Language': 'en-US,en;q=0.9'
}

print("\n🚀 STARTING TO FETCH MAP DATA FROM OPENSTREETMAP...")

# API call function with Retry capability
def fetch_boundary_with_retry(suburb_name, retries=3):
    query = f"{suburb_name.replace(' ', '+')},+Victoria,+Australia"
    url = f"https://nominatim.openstreetmap.org/search?q={query}&polygon_geojson=1&format=json"
    
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=headers, timeout=20)
            if res.status_code == 200:
                return res.json()
            elif res.status_code in [429, 503]: # Rate Limited
                print(f"\n   ⚠️ Rate limited (Code {res.status_code}). Waiting 10 seconds...")
                time.sleep(10)
            else:
                print(f"❌ API Error {res.status_code}. Retrying ({attempt+1}/{retries})...", end=" ")
        except requests.exceptions.RequestException as e:
            print(f"\n   ❌ Network Error (Retrying {attempt+1}/{retries}): {e}")
            time.sleep(5) # Wait 5 seconds if network drops/DNS error
    return None

# 4. Data fetching loop
for i, suburb in enumerate(suburbs):
    suburb_upper = suburb.upper()
    
    if suburb_upper in completed_suburbs:
        continue # Skip if boundary already exists
        
    print(f"[{i + 1}/{len(suburbs)}] Searching: {suburb_upper}...", end=" ")
    
    data = fetch_boundary_with_retry(suburb)
    
    if data and len(data) > 0:
        best_match = data[0]
        for r in data:
            if r.get('class') == 'boundary' or r.get('type') == 'administrative':
                best_match = r
                break
        
        if best_match.get('geojson'):
            geom_type = best_match['geojson'].get('type')
            if geom_type in ['Polygon', 'MultiPolygon']:
                geojson_features.append({
                    "type": "Feature",
                    "properties": { "Suburb": suburb_upper },
                    "geometry": best_match['geojson']
                })
                print("✅ Done")
                
                # SAVE FILE IMMEDIATELY AFTER EACH SUCCESS (Prevent data loss if crashed)
                with open(OUTPUT_GEOJSON, 'w', encoding='utf-8') as f:
                    json.dump({"type": "FeatureCollection", "features": geojson_features}, f, ensure_ascii=False)
            else:
                print(f"⚠️ Returned {geom_type}, not a Polygon")
        else:
            print("⚠️ No GeoJSON data found")
    elif data is not None and len(data) == 0:
        print("❌ Location not found on the map")
    else:
        print("❌ Failed after multiple attempts.")
        
    time.sleep(1.5) # Sleep 1.5s to comply with OSM guidelines

print(f"\nCOMPLETED! Successfully saved a total of {len(geojson_features)} boundaries into the GeoJSON file.")