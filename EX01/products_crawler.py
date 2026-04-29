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


def fetch_snapp_products(categories, max_pages_per_category, size):
    HEADERS = {
        "Accept": "application/json",
        "Origin": "https://express.snapp.market",
        "Referer": "https://express.snapp.market/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    
    products = []
    logger.info(f"Initiating product extraction for categories: {categories}")
    
    for category in categories:
        logger.info(f"Mining category: {category}")
        for page in range(max_pages_per_category):
            url = f"https://api.snapp.express/express-search/v1/pb/products?category_slug={category}&page={page}&size={size}&lat=35.773643&long=51.418311"
            
            try:
                response = requests.get(url, headers=HEADERS, timeout=15)
                if response.status_code != 200:
                    logger.error(f"Product extraction failed for {category} at page {page}. HTTP: {response.status_code}. Response: {response.text[:200]}")
                    break
                    
                data = response.json()
                items = data.get("items", [])
                
                if not items:
                    logger.warning(f"Category {category} returned no items at page {page}. Proceeding to next category.")
                    break
                    
                for p in items:
                    products.append({
                        "productId": p.get("id"),
                        "name": p.get("title"),
                        "price": p.get("price"),
                        "category": category
                    })
                
                logger.info(f"Extracted {len(items)} products from {category} page {page}.")
                time.sleep(1 + random.random())
                
            except Exception as e:
                logger.error(f"Product extraction exception [{category} - page {page}]: {str(e)}")
                break
                
    df_products = pd.DataFrame(products)
    if not df_products.empty:
        df_products = df_products.drop_duplicates(subset=["productId"]).dropna(subset=["productId", "price"])
        df_products["price"] = pd.to_numeric(df_products["price"], errors='coerce').fillna(0)
    
    logger.info(f"Product extraction complete. Unique products acquired: {len(df_products)}")
    return df_products