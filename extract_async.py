import asyncio
import aiohttp
import pandas as pd
import json
import re
from bs4 import BeautifulSoup

FILE_NAME = "melbourne_async_data.csv"

LAT_NORTH = -37.4000
LAT_SOUTH = -38.2000
LNG_WEST = 144.6000
LNG_EAST = 145.4000

GRID_SIZE = 40

lat_step = (LAT_NORTH - LAT_SOUTH) / GRID_SIZE
lng_step = (LNG_EAST - LNG_WEST) / GRID_SIZE

results = []
seen_ids = set()

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

def extract_numeric_price(price):
    if not price:
        return None

    nums = re.findall(r"\d+", price.replace(",", ""))

    if len(nums) >= 2:
        return (float(nums[0]) + float(nums[1])) / 2
    elif len(nums) == 1:
        return float(nums[0])

    return None


async def fetch(session, url):
    async with session.get(url) as resp:
        return await resp.text()


async def process_page(session, url):

    html = await fetch(session, url)

    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")

    if not script:
        return

    data = json.loads(script.string)

    try:
        listings = data["props"]["pageProps"]["componentProps"]["listingsMap"]
    except:
        return

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
            "URL": "https://www.domain.com.au" + model.get("url","")
        })


async def process_cell(session, i, j):

    top_lat = LAT_NORTH - (i * lat_step)
    bottom_lat = LAT_NORTH - ((i + 1) * lat_step)
    left_lng = LNG_WEST + (j * lng_step)
    right_lng = LNG_WEST + ((j + 1) * lng_step)

    base_url = f"https://www.domain.com.au/sale/?excludeunderoffer=1&startloc={top_lat}%2C{left_lng}&endloc={bottom_lat}%2C{right_lng}"

    html = await fetch(session, base_url + "&page=1")

    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")

    if not script:
        return

    data = json.loads(script.string)

    comp = data["props"]["pageProps"]["componentProps"]
    total_pages = comp.get("totalPages", 1)

    tasks = []

    for p in range(1, total_pages + 1):

        url = base_url + f"&page={p}"
        tasks.append(process_page(session, url))

    await asyncio.gather(*tasks)


async def main():

    connector = aiohttp.TCPConnector(limit=30)

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:

        tasks = []

        for i in range(GRID_SIZE):
            for j in range(GRID_SIZE):

                tasks.append(process_cell(session, i, j))

        await asyncio.gather(*tasks)

    df = pd.DataFrame(results)

    df.drop_duplicates(subset="Property_ID", inplace=True)

    df.to_csv(FILE_NAME, index=False)

    print("Saved", len(df), "properties")


if __name__ == "__main__":
    asyncio.run(main())