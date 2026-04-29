import logging
import numpy as np
import pandas as pd
from datetime import datetime
from products_crawler import fetch_snapp_products
from store_crawler import fetch_snapp_vendors

# Configure granular execution tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def simulate_transactions(df_stores, df_products, num_tickets, num_customers):
    if df_stores.empty or df_products.empty:
        logger.error("Simulation aborted: Input DataFrames possess null dimensions.")
        return pd.DataFrame(), pd.DataFrame()

    logger.info(f"Initiating vectorized transaction synthesis matrix: N={num_tickets}")
    
    ticket_ids = np.arange(1, num_tickets + 1)
    customer_ids = np.random.randint(1, num_customers + 1, size=num_tickets)
    
    store_indices = np.random.randint(0, len(df_stores), size=num_tickets)
    store_ids = df_stores['storeId'].values[store_indices]
    store_names = df_stores['storeName'].values[store_indices]
    lats = df_stores['lat'].values[store_indices]
    lons = df_stores['lon'].values[store_indices]
    
    base_date = datetime.now()
    random_minutes = np.random.randint(0, 60 * 24 * 60, size=num_tickets)
    timestamps = base_date - pd.to_timedelta(random_minutes, unit='m')
    
    num_products_per_ticket = np.random.randint(1, 11, size=num_tickets)
    total_items = num_products_per_ticket.sum()
    logger.info(f"Generating line items vector: N={total_items}")
    
    item_ticket_ids = np.repeat(ticket_ids, num_products_per_ticket)
    item_customer_ids = np.repeat(customer_ids, num_products_per_ticket)
    item_timestamps = np.repeat(timestamps, num_products_per_ticket)
    
    product_indices = np.random.randint(0, len(df_products), size=total_items)
    product_ids = df_products['productId'].values[product_indices]
    prices = df_products['price'].values[product_indices]
    
    quantities = np.random.randint(1, 6, size=total_items)
    line_amounts = prices * quantities
    
    logger.info("Constructing Ticket Items DataFrame.")
    df_ticket_items = pd.DataFrame({
        "ticketItemId": np.arange(1, total_items + 1),
        "ticketId": item_ticket_ids,
        "customerId": item_customer_ids,
        "datetime": item_timestamps,
        "productId": product_ids,
        "quantity": quantities,
        "price": prices,
        "lineAmount": line_amounts
    })
    
    logger.info("Executing groupby aggregation for Ticket Headers.")
    total_paid_series = df_ticket_items.groupby('ticketId')['lineAmount'].sum().values
    
    df_tickets = pd.DataFrame({
        "ticketId": ticket_ids,
        "customerId": customer_ids,
        "storeId": store_ids,
        "storeName": store_names,
        "lat": lats,
        "lon": lons,
        "datetime": timestamps,
        "numProducts": num_products_per_ticket,
        "total_paid": total_paid_series
    })
    
    logger.info("Transaction synthesis complete.")
    return df_tickets, df_ticket_items

# ==========================================
# PIPELINE EXECUTION
# ==========================================

if __name__ == "__main__":
    # Reduced max_pages for initial diagnostic run
    df_stores = fetch_snapp_vendors(max_pages=2, size=20)
    
    if df_stores.empty:
        logger.critical("Failed to acquire store dimension. Pipeline terminated.")
        exit(1)
        
    categories_to_mine = ['dairy', 'beverages', 'junk-food', 'groceries-bread', 'fruits-vegetables', 'health-beauty', 'meat-egg', 'dried-fruits-nuts']
    df_products = fetch_snapp_products(categories=categories_to_mine, max_pages_per_category=5)
    
    if df_products.empty:
        logger.critical("Failed to acquire product dimension. Pipeline terminated.")
        exit(1)
        
    df_tickets, df_ticket_items = simulate_transactions(df_stores, df_products, num_tickets=1000, num_customers=500)
    
    logger.info(f"Final output generated. df_tickets shape: {df_tickets.shape}, df_ticket_items shape: {df_ticket_items.shape}")
    
    logger.info("Enforcing ISO 8601 timestamp formatting.")
    
    df_tickets['datetime'] = df_tickets['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_ticket_items['datetime'] = df_ticket_items['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

    logger.info("Initiating I/O serialization to persistent storage.")
    
    df_stores.to_csv("dim_stores.csv", index=False, encoding="utf-8-sig")
    df_products.to_csv("dim_products.csv", index=False, encoding="utf-8-sig")
    df_tickets.to_csv("fact_tickets.csv", index=False, encoding="utf-8-sig")
    df_ticket_items.to_csv("fact_ticket_items.csv", index=False, encoding="utf-8-sig")
    
    
    logger.info("Initiating I/O serialization to persistent storage.")
    
    df_stores.to_csv("dim_stores.csv", index=False, encoding="utf-8-sig")
    df_products.to_csv("dim_products.csv", index=False, encoding="utf-8-sig")
    df_tickets.to_csv("fact_tickets.csv", index=False, encoding="utf-8-sig")
    df_ticket_items.to_csv("fact_ticket_items.csv", index=False, encoding="utf-8-sig")
    
    logger.info("Serialization complete. Data pipeline execution terminated.")