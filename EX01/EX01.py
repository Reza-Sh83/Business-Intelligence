import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from products_crawler import fetch_okala_products
from store_crawler import get_okala_stores


df_stores = get_okala_stores(
    lat=35.6892,
    lon=51.3890,
    N=4,
    delta=0.05
)

df_stores = df_stores[:50]
df_products = fetch_okala_products(df_stores, sleep=1)
df_products = df_products.drop_duplicates(subset=["id"])
df_products = df_products.rename(columns={"id": "productId"})
df_products.drop("storeid", axis=1, inplace=True)
print("Products:", len(df_products))

NUM_CUSTOMERS = 50000
customer_ids = np.arange(1, NUM_CUSTOMERS + 1)
NUM_TICKETS = 500_000
tickets = []
ticket_items = []
ticket_item_id_counter = 1
base_date = datetime.now()

for ticket_id in range(1, NUM_TICKETS + 1):
    customer_id = random.choice(customer_ids)
    store_row = df_stores.sample(1).iloc[0]
    store_id = store_row["storeId"]
    store_name = store_row["storeName"]
    lat = store_row["lat"]
    lon = store_row["lon"]
    num_products = random.randint(1, 10)
    timestamp = base_date- timedelta(
        days=random.randint(0, 60),
        minutes=random.randint(0, 1440)
        )
    total_paid = 0
    for _ in range(num_products):
        prod = df_products.sample(1).iloc[0]
        product_id = prod["productId"]
        price = prod["okprice"]
        qty = random.randint(1, 5)
        lineAmount = price * qty
        total_paid += lineAmount
        ticket_items.append({
            "ticketItemId": ticket_item_id_counter,
            "ticketId": ticket_id,
            "customerId": customer_id,
            "datetime": timestamp,
            "productId": product_id,
            "quantity": qty,
            "price": price,
            "lineAmount": lineAmount
            })
        ticket_item_id_counter += 1
    tickets.append({
        "ticketId": ticket_id,
        "customerId": customer_id,
        "storeId": store_id,
        "storeName": store_name,
        "lat": lat,
        "lon": lon,
        "datetime": timestamp,
        "numProducts": num_products,
        "total_paid": total_paid
        })
    print(f"Created Ticket {ticket_id}")
df_tickets = pd.DataFrame(tickets)
df_ticket_items = pd.DataFrame(ticket_items)