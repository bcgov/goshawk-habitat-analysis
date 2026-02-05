# %% Import required Libraries / Modules
import goshawk_habitat.db.oracle as bcgw 
import goshawk_habitat.rast.raster as raster
from pathlib import Path
from dotenv import load_dotenv
import tomllib
import json
import oracledb
import logging
from datetime import datetime
import rasterio
import numpy as np

# Configure Root
ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
with open(ROOT / "config.toml", "rb") as f:
    cfg = tomllib.load(f)

# Configure Logging
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

timestamp = datetime.now().strftime("%d%m%Y_%H%M%S")
LOG_FILE = LOG_DIR / f"{timestamp}_run.log"

def log_event(message: str) -> None:
    """
    Docstring for log_event
    
    :param message: Description
    :type message: str
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"{timestamp} | {message}\n")

def generate_geojson(cols, rows, out_path):
    """
    Docstring for generate_geojson
    
    :param cols: Description
    :param rows: Description
    """

    # Find geom column
    geom_idx = None
    for i, c in enumerate(cols):
        if c.lower() == "geom_geojson":
            geom_idx = i
            break
    if geom_idx is None:
        raise ValueError(f"Expected 'geom_geojson' in columns, got: {cols}")

    # Attribute columns (everything except geom_geojson)
    prop_cols = [c for c in cols if c.lower() != "geom_geojson"]

    features = []
    for r in rows:
        geom_val = r[geom_idx]
        if isinstance(geom_val, oracledb.LOB):
            geom_val = geom_val.read()
        if not geom_val:
            continue

        geometry = json.loads(geom_val)

        # Build properties without the geom column
        props = {}
        for c in prop_cols:
            v = r[cols.index(c)]
            # Handle LOBs just in case
            if isinstance(v, oracledb.LOB):
                v = v.read()
            # If anything weird slips through, stringify it
            try:
                json.dumps(v)
            except TypeError:
                v = str(v)
            props[c] = v

        features.append({"type": "Feature", "geometry": geometry, "properties": props})

    fc = {"type": "FeatureCollection", "features": features}

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)

    log_event(f"Wrote {len(features):,} features to {out_path}")

def main():
    # Establish Connection to the BCGW and Confirm Successful Connection
    log_event("Creating Connection")
    conn = bcgw.connect()
    with bcgw.oracle_cursor(conn) as cur:
        cur.execute("SELECT * FROM v$version")
        for row in cur.fetchall():
            print(row[0])

    db_name, user, schema = bcgw.get_db_info(conn)

    log_event(f"Database Connection: {db_name}")
    log_event(f"User:     {user}")
    log_event(f"Schema:   {schema}")

    # Import Parameters from Config File
    log_event("Importing Configuration Parameters")
    nest_params = {
        "tsa_id": cfg["tsa"]["feature_id"],
        "min_age": cfg["nesting_vri_params"]["proj_age_1"],
        "min_height": cfg["nesting_vri_params"]["proj_height"],
        "min_crown_closure": cfg["nesting_vri_params"]["crown_closure"],
        "max_site_index": cfg["nesting_vri_params"]["site_index"],
        "tol":cfg["geoprocessing"]["tol"]
    }

    forage_params = {
         "tsa_id": cfg["tsa"]["feature_id"],
         "min_age": cfg["foraging_vri_params"]["proj_age_1"],
         "tol":cfg["geoprocessing"]["tol"],
         "min_disturbance_year":cfg["foraging_vri_params"]["min_disturbance_year"]
    }

    data_prep_params = {
        "tsa_id": cfg["tsa"]["feature_id"],
        "tol":cfg["geoprocessing"]["tol"], 
        "min_age": cfg["foraging_vri_params"]["proj_age_1"],
    }

#     # Nesting Geojson Generation
#     log_event(f"Running Nesting SQL Query for TSA {nest_params['tsa_id']}")
#     cols, rows = bcgw.run_sql(conn, "nesting.sql", params=nest_params)  

#     log_event(f"Creating Nesting GeoJson for TSA {nest_params['tsa_id']}")
#     geojson_out_path = ROOT / "data" / f"nesting_{cfg['tsa']['feature_id']}.geojson"
#     geojson_out_path.parent.mkdir(parents=True, exist_ok=True)
#     generate_geojson(cols, rows, geojson_out_path)

#     log_event(f"Creating Nesting TIF for TSA {nest_params['tsa_id']}")
#     nest_raster_out_path = ROOT / "data" / f"nest_raster_{cfg['tsa']['feature_id']}.tif"
#     nest_raster_out_path.parent.mkdir(parents=True, exist_ok=True)
#     raster.rasterize_geojson_to_30m(geojson_out_path, nest_raster_out_path)
#     with rasterio.open(nest_raster_out_path) as src:
#         arr = src.read(1)
#         print("dtype:", arr.dtype)
#         print("nodata:", src.nodata)
#         print("min/max:", arr.min(), arr.max())
#         vals, counts = np.unique(arr, return_counts=True)
#         unique_vals = dict(zip(vals.tolist(), counts.tolist()))
#         log_event(
#             f"Nesting TIF Created – unique values: "
#             f"{', '.join(f'{k}: {v:,}' for k, v in unique_vals.items())}"
# )

#     # Foraging Geojson Generation
#     log_event(f"Running Foraging SQL Query for TSA {forage_params['tsa_id']}")
#     cols, rows = bcgw.run_sql(conn, "foraging.sql", params=forage_params)  

#     log_event(f"Creating Foraging GeoJson for TSA {forage_params['tsa_id']}")
#     geojson_out_path = ROOT / "data" / f"foraging_{cfg['tsa']['feature_id']}.geojson"
#     geojson_out_path.parent.mkdir(parents=True, exist_ok=True)
#     generate_geojson(cols, rows, geojson_out_path)

#     log_event(f"Creating Foraging TIF for TSA {forage_params['tsa_id']}")
#     forage_raster_out_path = ROOT / "data" / f"forage_raster_{cfg['tsa']['feature_id']}.tif"
#     forage_raster_out_path.parent.mkdir(parents=True, exist_ok=True)
#     raster.rasterize_geojson_to_30m_age_classes(geojson_out_path, forage_raster_out_path)
#     with rasterio.open(forage_raster_out_path) as src:
#         arr = src.read(1)
#         print("dtype:", arr.dtype)
#         print("nodata:", src.nodata)
#         print("min/max:", arr.min(), arr.max())
#         vals, counts = np.unique(arr, return_counts=True)
#         unique_vals = dict(zip(vals.tolist(), counts.tolist()))
#         log_event(
#             f"Foraging TIF Created – unique values: "
#             f"{', '.join(f'{k}: {v:,}' for k, v in unique_vals.items())}"
# )

    # Nesting Geojson Generation
    log_event(f"Running Data Prep SQL Query for TSA {data_prep_params['tsa_id']}")
    cols, rows = bcgw.run_sql(conn, "data_prep.sql", params=data_prep_params)  

    log_event(f"Creating VRI GeoJson for TSA {nest_params['tsa_id']}")
    geojson_out_path = ROOT / "data" / f"vri_{cfg['tsa']['feature_id']}.geojson"
    geojson_out_path.parent.mkdir(parents=True, exist_ok=True)
    generate_geojson(cols, rows, geojson_out_path)


# %%
if __name__ == "__main__":
    main()


# %%