import os
import json
import time
import pandas as pd
import requests

print("🌍 BẮT ĐẦU TẢI DỮ LIỆU RANH GIỚI KHÔNG GIAN (GEOJSON) - CHẾ ĐỘ AN TOÀN 🌍")

INPUT_CSV = 'data/melbourne_price_data.csv'
OUTPUT_GEOJSON = 'data/melbourne_suburb_boundaries.geojson'

# 1. Kiểm tra file CSV
if not os.path.exists(INPUT_CSV):
    print(f"❌ Không tìm thấy file: {INPUT_CSV}")
    exit()

df = pd.read_csv(INPUT_CSV)
if 'Suburb' not in df.columns:
    print("❌ File CSV không có cột 'Suburb'. Không thể tiếp tục.")
    exit()

suburbs = sorted([str(s).strip() for s in df['Suburb'].dropna().unique()])
print(f"📌 Có tổng cộng {len(suburbs)} khu vực trong danh sách.")

# 2. Đọc file GeoJSON cũ để KHÔI PHỤC TIẾN TRÌNH (Tránh cào lại từ đầu)
geojson_features = []
completed_suburbs = set()

if os.path.exists(OUTPUT_GEOJSON):
    try:
        with open(OUTPUT_GEOJSON, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
            geojson_features = existing_data.get("features", [])
            # Ghi nhận các Suburb đã cào thành công
            for feat in geojson_features:
                if 'properties' in feat and 'Suburb' in feat['properties']:
                    completed_suburbs.add(feat['properties']['Suburb'])
        print(f"♻️ Đã khôi phục {len(completed_suburbs)} ranh giới từ file cũ. Bỏ qua các khu vực này.")
    except Exception as e:
        print(f"⚠️ Không thể đọc file GeoJSON cũ (có thể file hỏng): {e}. Sẽ tạo file mới.")

# 3. Chuẩn bị gọi API
headers = {
    'User-Agent': 'MelbournePropertyDashboard/1.1 (DataAnalyticsProject - Contact: [YourEmail])',
    'Accept-Language': 'en-US,en;q=0.9'
}

print("\n🚀 BẮT ĐẦU KÉO DỮ LIỆU BẢN ĐỒ TỪ OPENSTREETMAP...")

# Hàm gọi API có tính năng Retry
def fetch_boundary_with_retry(suburb_name, retries=3):
    query = f"{suburb_name.replace(' ', '+')},+Victoria,+Australia"
    url = f"https://nominatim.openstreetmap.org/search?q={query}&polygon_geojson=1&format=json"
    
    for attempt in range(retries):
        try:
            res = requests.get(url, headers=headers, timeout=20)
            if res.status_code == 200:
                return res.json()
            elif res.status_code in [429, 503]: # Bị Rate Limit
                print(f"\n   ⚠️ Bị giới hạn tốc độ (Code {res.status_code}). Chờ 10 giây...")
                time.sleep(10)
            else:
                print(f"❌ Lỗi API {res.status_code}. Thử lại ({attempt+1}/{retries})...", end=" ")
        except requests.exceptions.RequestException as e:
            print(f"\n   ❌ Lỗi mạng (Thử lại {attempt+1}/{retries}): {e}")
            time.sleep(5) # Đợi 5 giây nếu rớt mạng/lỗi DNS
    return None

# 4. Vòng lặp lấy dữ liệu
for i, suburb in enumerate(suburbs):
    suburb_upper = suburb.upper()
    
    if suburb_upper in completed_suburbs:
        continue # Bỏ qua nếu đã có ranh giới
        
    print(f"[{i + 1}/{len(suburbs)}] Tìm: {suburb_upper}...", end=" ")
    
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
                print("✅ Xong")
                
                # LƯU FILE NGAY LẬP TỨC SAU MỖI LẦN THÀNH CÔNG (Chống mất data nếu Crash)
                with open(OUTPUT_GEOJSON, 'w', encoding='utf-8') as f:
                    json.dump({"type": "FeatureCollection", "features": geojson_features}, f, ensure_ascii=False)
            else:
                print(f"⚠️ Trả về {geom_type}, không phải Đa giác")
        else:
            print("⚠️ Không có dữ liệu GeoJSON")
    elif data is not None and len(data) == 0:
        print("❌ Không có vị trí này trên bản đồ")
    else:
        print("❌ Thất bại sau nhiều lần thử.")
        
    time.sleep(1.5) # Nghỉ 1.5s chuẩn quy định của OSM

print(f"\n🎉 HOÀN TẤT! Đã lưu tổng cộng {len(geojson_features)} ranh giới vào file GeoJSON.")