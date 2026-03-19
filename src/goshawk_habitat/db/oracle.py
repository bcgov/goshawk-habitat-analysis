import os
import time
import oracledb
from contextlib import contextmanager
from importlib.resources import files

def connect(
        
    host: str | None = None,
    port: int | None = None,
    service: str | None = None,
    username: str | None = None,
    password: str | None = None,  
):
    """
    Essentially just a wrapper for oracledb with some slightly enhanced 
    handling related to the BCGW.
        
    Create and return an Oracle database connection.

    Parameters may be passed explicitly or read from environment variables.

    Required env vars (if params not supplied):
      - BCGW_HOST
      - BCGW_PORT
      - BCGW_SERVICE
      - BCGW_USERNAME
      - BCGW_PASSWORD
    """

    host = host or os.environ.get("BCGW_HOST")
    port = port or int(os.environ.get("BCGW_PORT", "1521"))
    service = service or os.environ.get("BCGW_SERVICE")
    username = username or os.environ.get("BCGW_USERNAME")
    password = password or os.environ.get("BCGW_PASSWORD")

    missing = [k for k, v in {
        "host": host,
        "service": service,
        "username": username,
        "password": password,
    }.items() if not v]

    dsn = f"{host}:{port}/{service}"

    return oracledb.connect(
        user=username,
        password=password,
        dsn=dsn,
    )

@contextmanager
def oracle_cursor(connection):
    """
    Context manager guarantees that cleanup code runs no matter what happens 
    after the resource is opened.

    It will be closed if the query succeeds, if an exception is raised, if the
    code returns early or if something else goes wrong.
    """
    cursor = connection.cursor()
    try:
        yield cursor
    finally:
        cursor.close()

def get_db_info(connection):
    with oracle_cursor(connection) as cur:
        cur.execute("""
            SELECT
                sys_context('USERENV','DB_NAME'),
                sys_context('USERENV','CURRENT_USER'),
                sys_context('USERENV','CURRENT_SCHEMA')
            FROM dual
        """)
        return cur.fetchone()

def get_db_latency(connection):
    """
    Test 'round trip' time 
    """
    with oracle_cursor(connection) as cur:
        start = time.time()

        for _ in range(100):
            cur.execute("SELECT 1 FROM dual")
            cur.fetchone()
        
        end = time.time()
    
    print("100 round trips took:", round(end - start, 4), "seconds")
    print("Average per round trip:", round((end - start)/100, 6), "seconds")

def output_type_handler(cursor, metadata):
    # metadata is a sequence of column metadata objects
    if metadata.type_code == oracledb.DB_TYPE_BLOB:
        # Return a RAW buffer for BLOBs so the driver gives us bytes directly
        return cursor.var(oracledb.DB_TYPE_RAW, arraysize=cursor.arraysize)

def get_db_speed(connection, sample_rows=1000, arraysize=10000):
    """
    Test fetch speed + throughput for WKB geometries.
    - connection: an oracledb connection
    - sample_rows: how many rows to pull (SQL uses ROWNUM <= sample_rows)
    - arraysize: how many rows to fetch per round-trip (large => fewer round trips)
    Returns a dict of timings + rates.
    """
    # ensure output type handler is set BEFORE creating cursors
    connection.outputtypehandler = output_type_handler

    sql = f"""
    SELECT
        OBJECTID,
        SDO_UTIL.TO_WKBGEOMETRY(a.GEOMETRY) AS geom_wkb
    FROM WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY a
    WHERE ROWNUM <= {int(sample_rows)}
    """

    # Use the cursor directly (your oracle_cursor wrapper should behave similarly).
    cur = connection.cursor()
    try:
        cur.arraysize = int(arraysize)
        # prefetchrows can help in some drivers
        try:
            cur.prefetchrows = int(arraysize)
        except Exception:
            # some cx_Oracle/oracledb combinations may not expose prefetchrows; that's ok
            pass

        t0 = time.time()
        cur.execute(sql)
        t1 = time.time()

        rows = 0
        bytes_ = 0

        t2 = time.time()
        while True:
            batch = cur.fetchmany()   # will fetch up to cur.arraysize rows
            if not batch:
                break
            rows += len(batch)
            # Because of output_type_handler, geom_wkb is now bytes (or None)
            for _, geom_wkb in batch:
                if geom_wkb:
                    # geom_wkb is bytes; len() is cheap
                    bytes_ += len(geom_wkb)
        t3 = time.time()

        exec_s = t1 - t0
        fetch_s = t3 - t2
        mb = bytes_ / (1024.0 * 1024.0)

        result = {
            "exec_s": round(exec_s, 4),
            "fetch_s": round(fetch_s, 4),
            "rows": rows,
            "mb": round(mb, 2),
            "rows_per_s": round(rows / fetch_s, 2) if fetch_s > 0 else None,
            "mb_per_s": round(mb / fetch_s, 2) if fetch_s > 0 else None,
            "arraysize": cur.arraysize,
            "prefetchrows": getattr(cur, "prefetchrows", None),
            "thin_mode": oracledb.is_thin_mode(),
        }

        # print summary for quick inspection
        print("Exec:", result["exec_s"], "s")
        print("Fetch:", result["fetch_s"], "s")
        print("Rows:", result["rows"])
        print("MB:", result["mb"])
        print("MB/s:", result["mb_per_s"])

        return result

    finally:
        cur.close()

def load_sql(sql_filename: str) -> str:
    sql_path = files("goshawk_habitat.sql").joinpath(sql_filename)
    return sql_path.read_text(encoding="utf-8")

def run_sql(conn, sql_filename, params=None, arraysize=1000, max_rows=50000):
    sql_text = load_sql(sql_filename)
    conn.outputtypehandler = output_type_handler

    with conn.cursor() as cur:
        cur.arraysize = arraysize
        cur.execute(sql_text, params or {})
        cols = [c[0] for c in cur.description]

        rows = []
        while len(rows) < max_rows:
            batch = cur.fetchmany(min(arraysize, max_rows - len(rows)))
            if not batch:
                break
            rows.extend(batch)

    return cols, rows

def run_sql_file(connection, sql_filename: str, params: dict | None = None):
    sql = load_sql(sql_filename)
    return run_sql(connection, sql, params=params)