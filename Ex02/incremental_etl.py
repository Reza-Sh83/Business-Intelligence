import os
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ETL.INCREMENTAL")

DB_URL = os.getenv("DW_DB_URL", "postgresql://postgres:postgres@localhost:5432/dw_phase2")
CHUNK_SIZE = 5000
MAX_WORKERS = 10


def log_step_duration(start_time, step_name):
    elapsed = time.time() - start_time
    logger.info("✔ %s completed in %.2f seconds.", step_name, elapsed)

def insert_chunk_with_conflict_handling(chunk_df, table_name, conflict_column, connection_string):
    """Insert a single chunk with ON CONFLICT DO NOTHING. Thread‑safe."""
    engine = create_engine(connection_string)
    try:
        with engine.begin() as conn:
            metadata = MetaData()
            metadata.reflect(bind=engine, only=[table_name])
            table = Table(table_name, metadata, autoload_with=engine)
            data = chunk_df.to_dict(orient='records')
            if not data:
                return 0
            stmt = insert(table).values(data)
            stmt = stmt.on_conflict_do_nothing(index_elements=[conflict_column])
            result = conn.execute(stmt)
            return result.rowcount
    finally:
        engine.dispose()

def parallel_insert_fact(df, table_name, conflict_column, engine_url, chunk_size=CHUNK_SIZE, max_workers=MAX_WORKERS):
    """Parallel chunked insert of a DataFrame into a table."""
    logger.info("Starting parallel chunked insert into %s (chunk size=%d, workers=%d).",
                table_name, chunk_size, max_workers)
    chunks = np.array_split(df, max(1, len(df) // chunk_size + 1))
    logger.info("Data split into %d chunks.", len(chunks))
    total = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, chunk in enumerate(chunks):
            if chunk.empty:
                continue
            logger.debug("Submitting chunk %d (%d rows).", i, len(chunk))
            future = executor.submit(insert_chunk_with_conflict_handling,
                                     chunk, table_name, conflict_column, engine_url)
            futures[future] = i
        for future in as_completed(futures):
            idx = futures[future]
            try:
                rows = future.result()
                total += rows
                logger.info("Chunk %d inserted %d rows.", idx, rows)
            except Exception as e:
                logger.error("Error inserting chunk %d: %s", idx, e)
                raise
    return total


def scd2_dim_store(engine, new_stores: pd.DataFrame):
    """SCD2 for dim_store with logging."""
    logger.info("Processing dim_store SCD2...")
    t0 = time.time()
    new_stores = new_stores.copy()
    new_stores['store_bk'] = new_stores['storeId'].astype(str)
    new_stores.rename(columns={'storeName': 'store_name', 'lat': 'lat',
                               'lon': 'lon', 'vendor_type': 'vendor_type'}, inplace=True)
    cols = ['store_bk', 'store_name', 'lat', 'lon', 'vendor_type']
    new_stores = new_stores[cols].drop_duplicates(subset='store_bk')

    with engine.begin() as conn:
        current = pd.read_sql("SELECT * FROM dim_store WHERE is_current", conn)

    merged = new_stores.merge(current, on='store_bk', how='left', suffixes=('_new', '_cur'))
    now = datetime.now()
    changes = []
    for _, row in merged.iterrows():
        if pd.isna(row['store_sk']):
            changes.append(('insert', row['store_bk'], row['store_name_new'],
                            row['lat_new'], row['lon_new'], row['vendor_type_new']))
        else:
            if (row['store_name_new'] != row['store_name_cur'] or
                row['lat_new'] != row['lat_cur'] or
                row['lon_new'] != row['lon_cur'] or
                row['vendor_type_new'] != row['vendor_type_cur']):
                changes.append(('update', row['store_sk'], row['store_bk'],
                                row['store_name_new'], row['lat_new'],
                                row['lon_new'], row['vendor_type_new']))

    if changes:
        with engine.begin() as conn:
            for ch in changes:
                if ch[0] == 'insert':
                    conn.execute(text("""
                        INSERT INTO dim_store (store_bk, store_name, lat, lon, vendor_type,
                                                valid_from, valid_to, is_current)
                        VALUES (:bk, :name, :lat, :lon, :vtype, :now, NULL, TRUE)
                    """), {"bk": ch[1], "name": ch[2], "lat": ch[3], "lon": ch[4],
                           "vtype": ch[5], "now": now})
                else:
                    old_sk = ch[1]
                    conn.execute(text("""
                        UPDATE dim_store SET valid_to = :now, is_current = FALSE
                        WHERE store_sk = :sk AND is_current
                    """), {"now": now, "sk": old_sk})
                    conn.execute(text("""
                        INSERT INTO dim_store (store_bk, store_name, lat, lon, vendor_type,
                                                valid_from, valid_to, is_current)
                        VALUES (:bk, :name, :lat, :lon, :vtype, :now, NULL, TRUE)
                    """), {"bk": ch[2], "name": ch[3], "lat": ch[4], "lon": ch[5],
                           "vtype": ch[6], "now": now})
        logger.info("dim_store SCD2: processed %d changes.", len(changes))
    else:
        logger.info("dim_store SCD2: no changes detected.")
    log_step_duration(t0, "dim_store SCD2")

def scd2_dim_product(engine, new_products: pd.DataFrame):
    """SCD2 for dim_product with logging."""
    logger.info("Processing dim_product SCD2...")
    t0 = time.time()
    new_products = new_products.copy()
    new_products['product_bk'] = new_products['productId'].astype(str)
    new_products.rename(columns={'name': 'product_name', 'category': 'category',
                                 'price': 'price'}, inplace=True)
    cols = ['product_bk', 'product_name', 'category', 'price']
    new_products = new_products[cols].drop_duplicates(subset='product_bk')
    new_products['price'] = pd.to_numeric(new_products['price'], errors='coerce').fillna(0)

    with engine.begin() as conn:
        current = pd.read_sql("SELECT * FROM dim_product WHERE is_current", conn)

    merged = new_products.merge(current, on='product_bk', how='left', suffixes=('_new', '_cur'))
    now = datetime.now()
    changes = []
    for _, row in merged.iterrows():
        if pd.isna(row['product_sk']):
            changes.append(('insert', row['product_bk'], row['product_name_new'],
                            row['category_new'], row['price_new']))
        else:
            if (row['product_name_new'] != row['product_name_cur'] or
                row['category_new'] != row['category_cur'] or
                row['price_new'] != row['price_cur']):
                changes.append(('update', row['product_sk'], row['product_bk'],
                                row['product_name_new'], row['category_new'],
                                row['price_new']))

    if changes:
        with engine.begin() as conn:
            for ch in changes:
                if ch[0] == 'insert':
                    conn.execute(text("""
                        INSERT INTO dim_product (product_bk, product_name, category, price,
                                                 valid_from, valid_to, is_current)
                        VALUES (:bk, :name, :cat, :price, :now, NULL, TRUE)
                    """), {"bk": ch[1], "name": ch[2], "cat": ch[3],
                           "price": ch[4], "now": now})
                else:
                    old_sk = ch[1]
                    conn.execute(text("""
                        UPDATE dim_product SET valid_to = :now, is_current = FALSE
                        WHERE product_sk = :sk AND is_current
                    """), {"now": now, "sk": old_sk})
                    conn.execute(text("""
                        INSERT INTO dim_product (product_bk, product_name, category, price,
                                                 valid_from, valid_to, is_current)
                        VALUES (:bk, :name, :cat, :price, :now, NULL, TRUE)
                    """), {"bk": ch[2], "name": ch[3], "cat": ch[4],
                           "price": ch[5], "now": now})
        logger.info("dim_product SCD2: processed %d changes.", len(changes))
    else:
        logger.info("dim_product SCD2: no changes detected.")
    log_step_duration(t0, "dim_product SCD2")

def upsert_customer(engine, new_customers: pd.Series):
    """Type 1: insert new customers with logging."""
    t0 = time.time()
    new_bks = new_customers.unique()
    with engine.begin() as conn:
        existing = pd.read_sql(
            "SELECT customer_bk FROM dim_customer WHERE customer_bk IN %(bks)s",
            conn, params={"bks": tuple(new_bks)})
        missing = set(new_bks) - set(existing['customer_bk'])
        if missing:
            pd.DataFrame({'customer_bk': list(missing)}).to_sql(
                'dim_customer', conn, if_exists='append', index=False, method='multi')
            logger.info("Inserted %d new customers.", len(missing))
        else:
            logger.info("No new customers to insert.")
    log_step_duration(t0, "dim_customer upsert")

def main():
    overall_start = time.time()
    logger.info("============================================================")
    logger.info("INCREMENTAL ETL STARTED")
    logger.info("============================================================")

    engine = create_engine(DB_URL)
    logger.info("Connected to database: %s", DB_URL)

 
    t0 = time.time()
    logger.info("Reading source CSV files...")
    stores = pd.read_csv('dim_stores.csv')
    products = pd.read_csv('dim_products.csv')
    tickets = pd.read_csv('fact_tickets.csv')
    ticket_items = pd.read_csv('fact_ticket_items.csv')
    log_step_duration(t0, "CSV reading")
    logger.info("Stores: %d, Products: %d, Tickets: %d, Ticket Items: %d",
                len(stores), len(products), len(tickets), len(ticket_items))

     
    t0 = time.time()
    with engine.begin() as conn:
        watermark = conn.execute(
            text("SELECT last_load_ts FROM etl_control WHERE table_name = 'fact_ticket_items'")
        ).scalar()
        if watermark is None:
            watermark = pd.Timestamp('1970-01-01')
    logger.info("Current watermark: %s", watermark)

    # Vectorised datetime parsing
    logger.info("Parsing datetime column (vectorised)...")
    ticket_items['datetime_dt'] = pd.to_datetime(
        ticket_items['datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce'
    )
    bad_dates = ticket_items['datetime_dt'].isna().sum()
    if bad_dates:
        logger.warning("Dropping %d rows with unparseable datetime.", bad_dates)
        ticket_items = ticket_items.dropna(subset=['datetime_dt'])

    new_items = ticket_items[ticket_items['datetime_dt'] > watermark]
    logger.info("New ticket items to process: %d (out of %d total).",
                len(new_items), len(ticket_items))
    log_step_duration(t0, "Watermark filtering")
    if new_items.empty:
        logger.info("No new ticket items. Exiting.")
        return

     
    scd2_dim_store(engine, stores)
    scd2_dim_product(engine, products)
    upsert_customer(engine, new_items['customerId'])

    t0 = time.time()
    with engine.begin() as conn:
        existing_dates = set(pd.read_sql("SELECT date_sk FROM dim_date", conn)['date_sk'])
        existing_times = set(pd.read_sql("SELECT time_sk FROM dim_time", conn)['time_sk'])
    logger.info("Existing date keys: %d, time keys: %d.", len(existing_dates), len(existing_times))

    new_dates = new_items['datetime_dt'].dt.strftime('%Y%m%d').astype(int).unique()
    missing_dates = set(new_dates) - existing_dates
    if missing_dates:
        missing_dt = pd.to_datetime(list(missing_dates), format='%Y%m%d')
        df_date = pd.DataFrame({'full_date': missing_dt})
        df_date['date_sk'] = df_date['full_date'].dt.strftime('%Y%m%d').astype(int)
        df_date['year'] = df_date['full_date'].dt.year
        df_date['quarter'] = df_date['full_date'].dt.quarter
        df_date['month'] = df_date['full_date'].dt.month
        df_date['month_name'] = df_date['full_date'].dt.strftime('%B')
        df_date['day_of_week'] = (df_date['full_date'].dt.dayofweek + 1) % 7  # Sun=0
        df_date['is_weekend'] = df_date['day_of_week'].isin([0, 6])
        with engine.begin() as conn:
            df_date.to_sql('dim_date', conn, if_exists='append', index=False, method='multi')
        logger.info("Added %d new dates to dim_date.", len(df_date))
    else:
        logger.info("No new dates to add.")

    new_times = pd.DataFrame({'dt': new_items['datetime_dt']})
    new_times['hour'] = new_times['dt'].dt.hour
    new_times['minute'] = new_times['dt'].dt.minute
    new_times['time_sk'] = new_times['hour'] * 100 + new_times['minute']
    missing_times = new_times[~new_times['time_sk'].isin(existing_times)][['time_sk', 'hour', 'minute']].drop_duplicates('time_sk')
    if not missing_times.empty:
        missing_times['time_of_day'] = missing_times.apply(
            lambda r: dt_time(r['hour'], r['minute']), axis=1)
        missing_times['hour_minute'] = missing_times.apply(
            lambda r: f"{r['hour']:02d}:{r['minute']:02d}", axis=1)
        def part_of_day(h):
            if 6 <= h <= 11: return 'Morning'
            elif 12 <= h <= 17: return 'Afternoon'
            elif 18 <= h <= 21: return 'Evening'
            else: return 'Night'
        missing_times['part_of_day'] = missing_times['hour'].apply(part_of_day)
        with engine.begin() as conn:
            missing_times.to_sql('dim_time', conn, if_exists='append', index=False, method='multi')
        logger.info("Added %d new times to dim_time.", len(missing_times))
    else:
        logger.info("No new times to add.")
    log_step_duration(t0, "Date/time dimension updates")

    t0 = time.time()
    with engine.begin() as conn:
        store_sk_map = pd.read_sql("SELECT store_sk, store_bk FROM dim_store WHERE is_current", conn)
        prod_sk_map = pd.read_sql("SELECT product_sk, product_bk FROM dim_product WHERE is_current", conn)
        cust_sk_map = pd.read_sql("SELECT customer_sk, customer_bk FROM dim_customer", conn)
    logger.info("Loaded surrogate key maps: stores=%d, products=%d, customers=%d",
                len(store_sk_map), len(prod_sk_map), len(cust_sk_map))

    fact = new_items[['ticketItemId', 'ticketId', 'datetime_dt', 'productId', 'customerId',
                      'quantity', 'price', 'lineAmount']].copy()
    fact.rename(columns={
        'ticketItemId': 'ticket_item_id', 'ticketId': 'ticket_id',
        'productId': 'product_bk', 'customerId': 'customer_bk',
        'price': 'unit_price', 'lineAmount': 'line_amount'
    }, inplace=True)
    fact['product_bk'] = fact['product_bk'].astype(str)
    fact['customer_bk'] = fact['customer_bk'].astype(int)
    fact['date_sk'] = fact['datetime_dt'].dt.strftime('%Y%m%d').astype(int)
    fact['time_sk'] = fact['datetime_dt'].dt.hour * 100 + fact['datetime_dt'].dt.minute

    # Attach store_bk via ticket
    tickets_map = tickets[['ticketId', 'storeId']].copy()
    tickets_map['store_bk'] = tickets_map['storeId'].astype(str)
    fact = fact.merge(tickets_map[['ticketId', 'store_bk']], left_on='ticket_id',
                      right_on='ticketId', how='left')
    fact.drop(columns=['ticketId'], inplace=True)
    fact = fact.merge(store_sk_map, on='store_bk', how='left')
    fact = fact.merge(prod_sk_map, on='product_bk', how='left')
    fact = fact.merge(cust_sk_map, on='customer_bk', how='left')

    pre_drop = len(fact)
    fact = fact.dropna(subset=['store_sk', 'product_sk', 'customer_sk'])
    if len(fact) < pre_drop:
        logger.warning("Dropped %d fact rows due to missing dimension keys.", pre_drop - len(fact))

    fact['discount'] = 0.0
    fact_final = fact[['ticket_item_id', 'ticket_id', 'date_sk', 'time_sk', 'store_sk',
                       'product_sk', 'customer_sk', 'quantity', 'unit_price',
                       'line_amount', 'discount']]
    log_step_duration(t0, "Fact DataFrame assembly")
    logger.info("Final fact rows to insert: %d.", len(fact_final))

    t0 = time.time()
    try:
        total = parallel_insert_fact(
            df=fact_final,
            table_name='fact_sales',
            conflict_column='ticket_item_id',
            engine_url=DB_URL,
            chunk_size=CHUNK_SIZE,
            max_workers=MAX_WORKERS
        )
        logger.info("Total rows inserted/ignored: %d.", total)
    except Exception as e:
        logger.critical("Fact insertion failed: %s", e)
        raise
    log_step_duration(t0, "Parallel fact insertion")

    t0 = time.time()
    max_ts = new_items['datetime_dt'].max()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE etl_control SET last_load_ts = :ts WHERE table_name = 'fact_ticket_items'
        """), {"ts": max_ts})
    logger.info("Watermark advanced to %s", max_ts)
    log_step_duration(t0, "Watermark update")

    overall_elapsed = time.time() - overall_start
    logger.info("============================================================")
    logger.info("INCREMENTAL ETL SUCCESSFULLY COMPLETED IN %.2f SECONDS", overall_elapsed)
    logger.info("============================================================")

if __name__ == "__main__":
    main()