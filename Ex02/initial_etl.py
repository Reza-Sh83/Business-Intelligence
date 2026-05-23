import os
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, Table, MetaData, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("ETL.INITIAL")

DB_URL = os.getenv("DW_DB_URL", "postgresql://postgres:postgres@localhost:5432/dw_phase2")
CHUNK_SIZE = 5000       # rows per chunk for parallel insert
MAX_WORKERS = 10         # number of parallel threads

def parse_datetime(dt_str: str):
    """Robust ISO 8601 parsing."""
    try:
        return pd.to_datetime(dt_str, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    except Exception:
        return pd.NaT

def log_step_duration(start_time, step_name):
    """Utility to log elapsed time for a step."""
    elapsed = time.time() - start_time
    logger.info(f"✔ {step_name} completed in {elapsed:.2f} seconds.")

def load_date_dimension(dates: pd.DatetimeIndex, engine) -> pd.DataFrame:
    """Build and insert dim_date, return mapping DataFrame."""
    logger.info("Building dim_date from %d distinct dates.", len(dates))
    df_date = pd.DataFrame({'full_date': dates}).drop_duplicates()
    df_date['date_sk'] = df_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    df_date['year'] = df_date['full_date'].dt.year
    df_date['quarter'] = df_date['full_date'].dt.quarter
    df_date['month'] = df_date['full_date'].dt.month
    df_date['month_name'] = df_date['full_date'].dt.strftime('%B')
    df_date['day_of_week'] = (df_date['full_date'].dt.dayofweek + 1) % 7  # Sunday=0
    df_date['is_weekend'] = df_date['day_of_week'].isin([0, 6])

    logger.info("Inserting %d rows into dim_date.", len(df_date))
    df_date.to_sql('dim_date', engine, if_exists='append', index=False, method='multi')
    return df_date[['date_sk', 'full_date']]

def load_time_dimension(times: pd.DatetimeIndex, engine) -> pd.DataFrame:
    """Build and insert dim_time, return mapping DataFrame."""
    logger.info("Building dim_time from %d timestamps.", len(times))
    df_time = pd.DataFrame({'dt': times}).drop_duplicates()
    df_time['hour'] = df_time['dt'].dt.hour
    df_time['minute'] = df_time['dt'].dt.minute
    df_time['time_sk'] = df_time['hour'] * 100 + df_time['minute']
    df_time = df_time[['time_sk', 'hour', 'minute']].drop_duplicates('time_sk')

    df_time['time_of_day'] = df_time.apply(lambda r: dt_time(r['hour'], r['minute']), axis=1)
    df_time['hour_minute'] = df_time.apply(lambda r: f"{r['hour']:02d}:{r['minute']:02d}", axis=1)
    def part_of_day(h):
        if 6 <= h <= 11: return 'Morning'
        elif 12 <= h <= 17: return 'Afternoon'
        elif 18 <= h <= 21: return 'Evening'
        else: return 'Night'
    df_time['part_of_day'] = df_time['hour'].apply(part_of_day)

    logger.info("Inserting %d rows into dim_time.", len(df_time))
    df_time.to_sql('dim_time', engine, if_exists='append', index=False, method='multi')
    return df_time[['time_sk', 'hour', 'minute']]

def insert_chunk_with_conflict_handling(chunk_df, table_name, conflict_column, connection_string):
    """
    Insert a single chunk into the target table with ON CONFLICT DO NOTHING.
    Each worker uses its own engine/connection for thread safety.
    """
    # Each thread creates its own engine to avoid sharing connections across threads.
    engine = create_engine(connection_string)
    with engine.begin() as conn:
        metadata = MetaData()
        metadata.reflect(bind=engine, only=[table_name])
        table = Table(table_name, metadata, autoload_with=engine)

        # Convert DataFrame to list of dicts for bulk insert
        data = chunk_df.to_dict(orient='records')
        if not data:
            return 0

        stmt = insert(table).values(data)
        stmt = stmt.on_conflict_do_nothing(index_elements=[conflict_column])
        result = conn.execute(stmt)
        engine.dispose()  # close connections
        return result.rowcount

def parallel_insert_fact(df, table_name, conflict_column, engine_url, chunk_size=CHUNK_SIZE, max_workers=MAX_WORKERS):
    """
    Split dataframe into chunks and insert them in parallel threads.
    Returns total rows inserted.
    """
    logger.info("Starting parallel chunked insert into %s (chunk size=%d, workers=%d).",
                table_name, chunk_size, max_workers)
    chunks = np.array_split(df, max(1, len(df) // chunk_size + 1))
    logger.info("Data split into %d chunks.", len(chunks))

    total_inserted = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, chunk in enumerate(chunks):
            if chunk.empty:
                continue
            logger.debug("Submitting chunk %d with %d rows.", i, len(chunk))
            future = executor.submit(
                insert_chunk_with_conflict_handling,
                chunk, table_name, conflict_column, engine_url
            )
            futures[future] = i

        for future in as_completed(futures):
            idx = futures[future]
            try:
                rows = future.result()
                total_inserted += rows
                logger.info("Chunk %d inserted %d rows.", idx, rows)
            except Exception as e:
                logger.error("Error inserting chunk %d: %s", idx, e)
                raise  # re-raise to stop ETL on failure

    return total_inserted

def main():
    overall_start = time.time()
    logger.info("============================================================")
    logger.info("INITIAL FULL ETL STARTED")
    logger.info("============================================================")

    engine = create_engine(DB_URL)
    logger.info("Connected to database: %s", DB_URL)

    logger.info("Reading source CSV files...")
    t0 = time.time()
    stores = pd.read_csv('dim_stores.csv')
    products = pd.read_csv('dim_products.csv')
    tickets = pd.read_csv('fact_tickets.csv')
    ticket_items = pd.read_csv('fact_ticket_items.csv')
    log_step_duration(t0, "CSV reading")
    logger.info("Stores: %d rows, Products: %d, Tickets: %d, Ticket Items: %d",
                len(stores), len(products), len(tickets), len(ticket_items))

 
    t0 = time.time()
    logger.info("Performing data quality checks...")
    initial_counts = {
        "stores": len(stores),
        "products": len(products),
        "ticket_items": len(ticket_items)
    }

    stores.dropna(subset=['storeId'], inplace=True)
    products.dropna(subset=['productId'], inplace=True)
    ticket_items.dropna(subset=['ticketItemId', 'datetime', 'productId', 'customerId'], inplace=True)

    for name, initial in initial_counts.items():
        current_len = len(stores) if name == "stores" else len(products) if name == "products" else len(ticket_items)
        logger.info("  %s: %d rows retained (%d dropped due to missing keys).",
                    name, current_len, initial - current_len)

    # Vectorised datetime parsing (FAST)
    logger.info("Parsing datetime column...")
    ticket_items['datetime_dt'] = pd.to_datetime(
        ticket_items['datetime'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    )
    bad_dates = ticket_items['datetime_dt'].isna().sum()
    if bad_dates:
        logger.warning("Dropping %d rows with unparseable datetime.", bad_dates)
        ticket_items = ticket_items.dropna(subset=['datetime_dt'])
    logger.info("After datetime parsing, ticket items: %d rows.", len(ticket_items))
    log_step_duration(t0, "Data quality & datetime parsing")

     
    logger.info("--- Building Dimension Tables ---")

    # dim_date
    t0 = time.time()
    dates = ticket_items['datetime_dt'].dt.date.drop_duplicates()
    date_map = load_date_dimension(pd.to_datetime(dates), engine)
    log_step_duration(t0, "dim_date")

    # dim_time
    t0 = time.time()
    times = ticket_items['datetime_dt']
    time_map = load_time_dimension(times, engine)
    log_step_duration(t0, "dim_time")

    # dim_store (SCD2 initial snapshot)
    t0 = time.time()
    dim_store = stores.rename(columns={
        'storeId': 'store_bk', 'storeName': 'store_name',
        'lat': 'lat', 'lon': 'lon', 'vendor_type': 'vendor_type'
    })[['store_bk', 'store_name', 'lat', 'lon', 'vendor_type']].copy()
    dim_store['store_bk'] = dim_store['store_bk'].astype(str)
    dim_store['valid_from'] = datetime.now()
    dim_store['valid_to'] = None
    dim_store['is_current'] = True
    logger.info("Inserting %d stores into dim_store.", len(dim_store))
    dim_store.to_sql('dim_store', engine, if_exists='append', index=False, method='multi')
    log_step_duration(t0, "dim_store")

    # dim_product (SCD2 initial snapshot)
    t0 = time.time()
    dim_prod = products.rename(columns={
        'productId': 'product_bk', 'name': 'product_name',
        'category': 'category', 'price': 'price'
    })[['product_bk', 'product_name', 'category', 'price']].copy()
    dim_prod['product_bk'] = dim_prod['product_bk'].astype(str)
    dim_prod['price'] = pd.to_numeric(dim_prod['price'], errors='coerce').fillna(0)
    dim_prod['valid_from'] = datetime.now()
    dim_prod['valid_to'] = None
    dim_prod['is_current'] = True
    logger.info("Inserting %d products into dim_product.", len(dim_prod))
    dim_prod.to_sql('dim_product', engine, if_exists='append', index=False, method='multi')
    log_step_duration(t0, "dim_product")

    # dim_customer (Type 1)
    t0 = time.time()
    customers = pd.DataFrame({'customer_bk': ticket_items['customerId'].unique()})
    logger.info("Inserting %d customers into dim_customer.", len(customers))
    customers.to_sql('dim_customer', engine, if_exists='append', index=False, method='multi')
    log_step_duration(t0, "dim_customer")

    t0 = time.time()
    with engine.begin() as conn:
        store_map = pd.read_sql("SELECT store_sk, store_bk FROM dim_store WHERE is_current", conn)
        prod_map = pd.read_sql("SELECT product_sk, product_bk FROM dim_product WHERE is_current", conn)
        cust_map = pd.read_sql("SELECT customer_sk, customer_bk FROM dim_customer", conn)
    logger.info("Surrogate key mappings loaded: stores=%d, products=%d, customers=%d",
                len(store_map), len(prod_map), len(cust_map))
    log_step_duration(t0, "Surrogate key retrieval")

    t0 = time.time()
    fact = ticket_items[['ticketItemId', 'ticketId', 'datetime_dt', 'productId', 'customerId',
                         'quantity', 'price', 'lineAmount']].copy()
    fact.rename(columns={
        'ticketItemId': 'ticket_item_id', 'ticketId': 'ticket_id',
        'productId': 'product_bk', 'customerId': 'customer_bk',
        'price': 'unit_price', 'lineAmount': 'line_amount'
    }, inplace=True)
    fact['product_bk'] = fact['product_bk'].astype(str)
    fact['customer_bk'] = fact['customer_bk'].astype(int)

    # Date/Time keys
    fact['date_sk'] = fact['datetime_dt'].dt.strftime('%Y%m%d').astype(int)
    fact['time_sk'] = fact['datetime_dt'].dt.hour * 100 + fact['datetime_dt'].dt.minute

    # Store mapping through tickets
    tickets_map = tickets[['ticketId', 'storeId']].copy()
    tickets_map['store_bk'] = tickets_map['storeId'].astype(str)
    fact = fact.merge(tickets_map[['ticketId', 'store_bk']], left_on='ticket_id', right_on='ticketId', how='left')
    fact.drop(columns=['ticketId'], inplace=True)
    logger.info("After store merge, fact rows: %d.", len(fact))

    # Join surrogate keys
    fact = fact.merge(store_map, on='store_bk', how='left')
    fact = fact.merge(prod_map, on='product_bk', how='left')
    fact = fact.merge(cust_map, on='customer_bk', how='left')
    logger.info("After surrogate key joins, fact rows: %d.", len(fact))

    # Drop rows with missing dimension keys (should not happen if CSVs are consistent)
    pre_drop = len(fact)
    fact = fact.dropna(subset=['store_sk', 'product_sk', 'customer_sk'])
    if len(fact) < pre_drop:
        logger.warning("Dropped %d fact rows due to missing dimension references.", pre_drop - len(fact))

    fact['discount'] = 0.0
    fact_final = fact[['ticket_item_id', 'ticket_id', 'date_sk', 'time_sk', 'store_sk', 'product_sk',
                       'customer_sk', 'quantity', 'unit_price', 'line_amount', 'discount']]
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
        logger.info("Total rows inserted into fact_sales: %d (duplicates ignored).", total)
    except Exception as e:
        logger.critical("Fact table insertion failed: %s", e)
        raise
    log_step_duration(t0, "Parallel fact insertion")

    t0 = time.time()
    max_ts = ticket_items['datetime_dt'].max()
    if pd.notnull(max_ts):
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO etl_control (table_name, last_load_ts)
                VALUES ('fact_ticket_items', :ts)
                ON CONFLICT (table_name) DO UPDATE SET last_load_ts = EXCLUDED.last_load_ts
            """), {"ts": max_ts})
        logger.info("Watermark updated to %s", max_ts)
    log_step_duration(t0, "Watermark update")

    overall_elapsed = time.time() - overall_start
    logger.info("============================================================")
    logger.info("INITIAL FULL ETL SUCCESSFULLY COMPLETED IN %.2f SECONDS", overall_elapsed)
    logger.info("============================================================")

if __name__ == "__main__":
    main()