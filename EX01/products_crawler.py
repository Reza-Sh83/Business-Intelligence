import time
import random
import requests
import pandas as pd

HEADERS = {
    "Accept": "application/json",
    "Referer": "https://www.okala.com/",
    "Origin": "https://www.okala.com/",
}
def fetch_store_products(store_id, sleep=1, max_retries=5):
    url = "https://apigateway.okala.com/api/Search/v1/Product/Search"
    for attempt in range(max_retries):
        try:
            time.sleep(sleep + random.random())
            response = requests.get(
                url,
                params={
                    "StoreIds": str(store_id),
                    "HasQuantity": "true",
                    },
                headers=HEADERS,
                timeout=15
            )
            if response.status_code == 429:
                print(f"Store {store_id}: rate limited, retrying...")
                time.sleep(sleep + random.random())
                continue
            if response.status_code != 200:
                print(f"Store {store_id}: HTTP {response.status_code}, retrying...")
                time.sleep(1)
                continue
            data = response.json()
            entities = data.get("entities", [])
            products = []
            for p in entities:
                products.append({
                    "storeid": store_id,
                    "id": p.get("id"),
                    "name": p.get("name"),
                    "c3id": p.get("c3Id"),
                    "c2id": p.get("c2Id"),
                    "okprice": p.get("okPrice"),
                    "brandname": p.get("brandName")
                })
            return products
        except Exception as e:
            print(f"Store {store_id}: error {e}, retrying...")
            time.sleep(1)
    print(f"Store {store_id} FAILED after {max_retries} retries.")
    return []
def fetch_okala_products(df_stores, sleep=1):
    store_ids = df_stores["storeId"].drop_duplicates().tolist()
    all_products = []
    failed_stores = []
    total = len(store_ids)
    print(f"Starting product crawl for {total} stores...\n")
    for i, store_id in enumerate(store_ids, start=1):
        print(f"[{i}/{total}] Fetching store {store_id}...")
        products = fetch_store_products(store_id, sleep=
        sleep)
        if not products:
            failed_stores.append(store_id)
        all_products.extend(products)
        print(f"-> Retrieved {len(products)} products\n")
    df = pd.DataFrame(all_products)
    if not df.empty:
        df = df.drop_duplicates(subset=["storeid", "id"])
    print("============================================")
    print("Crawling finished!")
    print(f"Failed stores: {len(failed_stores)}-{failed_stores}")
    print("============================================")
    return df
