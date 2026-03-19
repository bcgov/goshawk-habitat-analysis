"""
Stage 1: Oracle -> local GeoPackage
- Select a single TSA polygon (by FEATURE_ID)
- Pull VRI polygons that intersect the TSA (spatial index via SDO_FILTER + SDO_RELATE)
- Clip each VRI polygon to the TSA boundary (SDO_INTERSECTION)
- Stream output as WKB (fast) and write to a local GeoPackage (append in chunks)

Dependencies:
  pip install oracledb geopandas shapely pyogrio
(or fiona instead of pyogrio, but pyogrio is usually faster)
"""

import goshawk_habitat.db.oracle as bcgw
import os
import time
import oracledb
import pandas as pd
import geopandas as gpd
from shapely import wkb
from dotenv import load_dotenv
import tomllib
from pathlib import Path


# ---------- Oracle LOB handling (critical for performance) ----------
def output_type_handler(cursor, metadata):
    # Convert BLOB columns to RAW bytes so we don't get LOB objects (and avoid .read() per row)
    if metadata.type_code == oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_RAW, arraysize=cursor.arraysize)

# ---------- Core SQL (Stage 1 only: VRI clipped to TSA) ----------
STAGE1_SQL = """
WITH tsa AS (
  SELECT /*+ MATERIALIZE */ t.geometry AS tsa_geom
  FROM WHSE_ADMIN_BOUNDARIES.FADM_TSA t
  WHERE t.feature_id = :tsa_id
)
SELECT
  v.feature_id,
  v.proj_age_1,
  SDO_UTIL.TO_WKBGEOMETRY(
    SDO_GEOM.SDO_INTERSECTION(v.geometry, (SELECT tsa_geom FROM tsa), :tol)
  ) AS geom_wkb
FROM WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY v
WHERE v.proj_age_1 > :min_age
  AND SDO_FILTER(v.geometry, (SELECT tsa_geom FROM tsa)) = 'TRUE'
  AND SDO_RELATE(v.geometry, (SELECT tsa_geom FROM tsa), 'mask=ANYINTERACT') = 'TRUE'
"""

def stage1_clip_vri_to_tsa(
    dsn: str,
    user: str,
    password: str,
    tsa_id: int,
    min_age: int,
    tol: float,
    out_gpkg: str,
    layer: str = "vri_clip",
    arraysize: int = 10000,
    batch_rows: int = 10000,
    overwrite: bool = True,
    crs_epsg: int | None = None,   # set if you know it; otherwise leave None
):
    """
    Writes a GeoPackage containing VRI polygons clipped to the TSA.
    """
    # Overwrite output if requested
    if overwrite and os.path.exists(out_gpkg):
        os.remove(out_gpkg)

    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    conn.outputtypehandler = output_type_handler

    total_rows = 0
    total_bytes = 0

    t_start = time.time()

    with conn.cursor() as cur:
        cur.arraysize = arraysize
        try:
            cur.prefetchrows = arraysize
        except Exception:
            pass

        params = {"tsa_id": tsa_id, "min_age": min_age, "tol": tol}

        t0 = time.time()
        cur.execute(STAGE1_SQL, params)
        t1 = time.time()

        wrote = False

        while True:
            batch = cur.fetchmany(batch_rows)
            if not batch:
                break

            # batch rows: (feature_id, proj_age_1, geom_wkb_bytes)
            df = pd.DataFrame(batch, columns=["feature_id", "proj_age_1", "geom_wkb"])

            # Track size
            for b in df["geom_wkb"]:
                if b:
                    total_bytes += len(b)

            # Convert WKB -> shapely geometries
            geoms = [wkb.loads(b) if b else None for b in df["geom_wkb"]]
            gdf = gpd.GeoDataFrame(
                df.drop(columns=["geom_wkb"]),
                geometry=geoms,
                crs=(f"EPSG:{crs_epsg}" if crs_epsg else None),
            )

            # Drop null/empty geoms (intersection can return NULL)
            gdf = gdf[gdf.geometry.notnull()]
            gdf = gdf[~gdf.geometry.is_empty]

            if len(gdf) == 0:
                continue

            # Write / append
            mode = "w" if not wrote else "a"
            # geopandas will append to GPKG layer when mode="a"
            gdf.to_file(out_gpkg, layer=layer, driver="GPKG", mode=mode)

            wrote = True
            total_rows += len(gdf)

            elapsed = time.time() - t_start
            mb = total_bytes / (1024 * 1024)
            print(
                f"Wrote {total_rows:,} features | ~{mb:,.1f} MB WKB fetched | "
                f"{(total_rows/elapsed):,.0f} feat/s | elapsed {elapsed:,.1f}s"
            )

    conn.close()

    t_end = time.time()
    mb = total_bytes / (1024 * 1024)
    print("\n=== Stage 1 complete ===")
    print(f"TSA_ID: {tsa_id}")
    print(f"Oracle execute time: {t1 - t0:.2f} s")
    print(f"Total features written: {total_rows:,}")
    print(f"Total WKB fetched: {mb:,.2f} MB")
    print(f"Total wall time: {t_end - t_start:.2f} s")
    print(f"Output: {out_gpkg} (layer: {layer})")

# %%
# ---------------- Example usage ----------------
if __name__ == "__main__":
    # Fill these in
    ROOT = Path(__file__).resolve().parents[1]
    load_dotenv(ROOT / ".env")
    with open(ROOT / "config.toml", "rb") as f:
        cfg = tomllib.load(f)

    host = os.environ.get("BCGW_HOST")
    port = int(os.environ.get("BCGW_PORT", "1521"))
    service = os.environ.get("BCGW_SERVICE")
    dsn = f"{host}:{port}/{service}"


    # %%
    DSN = f"{host}:{port}/{service}"  # e.g. "myhost.gov.bc.ca:1521/BCGW"
    USER = os.environ.get("BCGW_USERNAME")
    PASS = os.environ.get("BCGW_PASSWORD")

    stage1_clip_vri_to_tsa(
        dsn=DSN,
        user=USER,
        password=PASS,
        tsa_id=333,          # your TSA FEATURE_ID
        min_age=0,           # adjust
        tol=0.005,           # adjust to your CRS/tolerance
        out_gpkg=r"data\\test.gpkg",
        layer="VRI_AreaOfInterest",
        arraysize=10000,
        batch_rows=10000,
        overwrite=True,
        crs_epsg=None,       # set if you know it (e.g., 3005)
    )

