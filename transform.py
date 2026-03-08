import json
import pandas as pd
import re

print("Đang đọc file JSON...")
with open('domain_clayton_data.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("Đang trích xuất thông tin nhà đất...")

# Trỏ thẳng đến kho báu
try:
    listings_map = data['props']['pageProps']['componentProps']['listingsMap']
except KeyError as e:
    print(f"Lỗi: Không tìm thấy thư mục gốc. Chi tiết lỗi: {e}")
    listings_map = {}

clean_data = []

# Hàm phụ để làm sạch giá tiền (Biến đổi "$800k - $900k" thành 850000)
def clean_price(price_str):
    if not price_str or "Contact" in price_str or "offers" in price_str.lower():
        return None
    # Tìm tất cả các con số trong chuỗi giá
    numbers = re.findall(r'\d+', price_str.replace(',', ''))
    if len(numbers) >= 2:
        return (int(numbers[0]) + int(numbers[1])) / 2 # Lấy trung bình nếu có khoảng giá
    elif len(numbers) == 1:
        return int(numbers[0])
    return None

if listings_map:
    print(f"Tuyệt vời! Tìm thấy {len(listings_map)} căn nhà.\n")
    
    for property_id, item in listings_map.items():
        # Dữ liệu chính nằm trong 'listingModel'
        model = item.get('listingModel', {})
        
        # 1. Trích xuất Địa chỉ và Tọa độ
        address_info = model.get('address', {})
        street = address_info.get('street', 'N/A')
        suburb = address_info.get('suburb', 'N/A')
        postcode = address_info.get('postcode', 'N/A')
        lat = address_info.get('lat', None)
        lng = address_info.get('lng', None)
        
        full_address = f"{street}, {suburb} VIC {postcode}" if street != 'N/A' else "N/A"

        # 2. Trích xuất Đặc điểm nhà (Phòng ngủ, tắm, loại nhà)
        features = model.get('features', {})
        beds = features.get('beds', 0)
        baths = features.get('baths', 0)
        parking = features.get('parking', 0)
        property_type = features.get('propertyTypeFormatted', 'N/A')
        land_size = features.get('landSize', 0)

        # 3. Xử lý Giá tiền
        raw_price = model.get('price', 'N/A')
        numeric_price = clean_price(raw_price)

        # 4. Xử lý URL
        url_path = model.get('url', '')
        full_url = f"https://www.domain.com.au{url_path}" if url_path else "N/A"

        # Gom vào một Dictionary
        house = {
            'Property_ID': property_id,
            'Full_Address': full_address,
            'Suburb': suburb,
            'Property_Type': property_type,
            'Beds': beds,
            'Baths': baths,
            'Car_Spaces': parking,
            'Land_Size_sqm': land_size,
            'Raw_Price': raw_price,
            'Numeric_Price': numeric_price,
            'Latitude': lat,
            'Longitude': lng,
            'URL': full_url
        }
        
        # Lọc bỏ rác: Chỉ giữ lại nếu có địa chỉ hoặc có giá hợp lệ
        if house['Full_Address'] != 'N/A':
            clean_data.append(house)

    # Đẩy dữ liệu vào Pandas DataFrame
    df = pd.DataFrame(clean_data)
    
    print("--- PREVIEW BẢNG DỮ LIỆU (5 dòng đầu) ---")
    print(df[['Full_Address', 'Property_Type', 'Beds', 'Raw_Price', 'Numeric_Price']].head())
    
    # Xuất ra file CSV
    df.to_csv('clayton_properties_final.csv', index=False, encoding='utf-8-sig')
    print(f"\n[THÀNH CÔNG] Đã lưu {len(df)} bản ghi hoàn chỉnh ra file 'clayton_properties_final.csv'")
else:
    print("Không tìm thấy dữ liệu trong listingsMap.")