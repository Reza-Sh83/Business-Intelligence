import requests
import numpy as np
import pandas as pd


BASE_URL = "https://apigateway.okala.com/api/Lucifer/v1/StoreRanking/GetAllStores"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Origin": "https://www.okala.com",
    "Referer": "https://www.okala.com/",
}


def build_grid(center_lat, center_lon, N, delta):
    lat_points = np.linspace(center_lat- delta, center_lat + delta, N)
    lon_points = np.linspace(center_lon- delta, center_lon + delta, N)
    return [(lat, lon) for lat in lat_points for lon in lon_points]


def fetch_store_node(lat, lon):
    url = f"{BASE_URL}?latitude={lat}&longitude={lon}"
    response = requests.get(url, headers=HEADERS, timeout=10)
    try:
        data = response.json()
    except:
        return []
    stores = data.get("data", {}).get("stores", [])
    results = []
    for s in stores:
        results.append({
            "storeId": s.get("storeId"),
            "storeName": s.get("storeName"),
            "partnerName": s.get("partnerName"),
            "lat": lat, 
            "lon": lon
            })
    return results


def get_okala_stores(lat, lon, N=3, delta=0.01):
    grid = build_grid(lat, lon, N, delta)
    all_results = []
    for g_lat, g_lon in grid:
        stores = fetch_store_node(g_lat, g_lon)
        all_results.extend(stores)
    df = pd.DataFrame(all_results)
    if not df.empty:
        df = df.drop_duplicates("storeId")
    return df