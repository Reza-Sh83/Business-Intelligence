import time
import random
import requests
import logging
import pandas as pd

# Configure granular execution tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fetch_snapp_vendors(max_pages, size):
    HEADERS = {
        "Accept": "application/json",
        "Origin": "https://express.snapp.market",
        "Referer": "https://express.snapp.market/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    vendors = []
    logger.info("Initiating vendor extraction sequence.")
    
    for page in range(max_pages):
        url = f"https://api.snapp.express/express-vendor/general/vendors-list?page={page}&page_size={size}&is_home=true&lat=35.773643&long=51.418311"
        logger.info(f"Requesting vendors page {page}...")
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code != 200:
                logger.error(f"Vendor Page {page} terminated. HTTP Status: {response.status_code}. Response: {response.text[:200]}")
                break
                
            data = response.json()
            results = data.get("data", {}).get("finalResult", [])
            
            if not results:
                logger.warning(f"Vendor Page {page} returned empty 'finalResult' array. Halting vendor pagination.")
                break
                
            for v in results:
                v_data = v.get("data", {})
                vendors.append({
                    "storeId": v_data.get("id"),
                    "storeName": v_data.get("title"),
                    "lat": v_data.get("lat"),
                    "lon": v_data.get("long"),
                    "vendor_type": v_data.get("vendor_type")
                })
            
            logger.info(f"Extracted {len(results)} vendors from page {page}.")
            time.sleep(1 + random.random())
            
        except Exception as e:
            logger.error(f"Vendor extraction exception at page {page}: {str(e)}")
            break
            
    df_stores = pd.DataFrame(vendors)
    if not df_stores.empty:
        df_stores = df_stores.drop_duplicates(subset=["storeId"])
    
    logger.info(f"Vendor extraction complete. Unique stores acquired: {len(df_stores)}")
    return df_stores