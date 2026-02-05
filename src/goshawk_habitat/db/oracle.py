import os
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
    
def load_sql(sql_filename: str) -> str:
    sql_path = files("goshawk_habitat.sql").joinpath(sql_filename)
    return sql_path.read_text(encoding="utf-8")

def run_sql(conn, sql_filename, params=None):
    sql_text = load_sql(sql_filename)  # however you're loading it

    with conn.cursor() as cur:
        cur.execute(sql_text, params or {})
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    return cols, rows

def run_sql_file(connection, sql_filename: str, params: dict | None = None):
    sql = load_sql(sql_filename)
    return run_sql(connection, sql, params=params)