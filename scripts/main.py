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

def _log_raster_stats(path: Path, label: str) -> None:
    with rasterio.open(path) as src:
        arr = src.read(1)
        vals, counts = np.unique(arr, return_counts=True)
        unique_vals = dict(zip(vals.tolist(), counts.tolist()))
        log_event(
            f"{label} – dtype={arr.dtype}, nodata={src.nodata}, "
            f"min/max={arr.min()}/{arr.max()}, unique values: "
            f"{', '.join(f'{k}: {v:,}' for k, v in unique_vals.items())}"
        )

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
    
    tsa_params = {
        "tsa_id": cfg["tsa"]["feature_id"]
    }
    
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

    r_cfg = cfg["raster"]

    grid_cfg = raster.GridConfig(
        out_crs=r_cfg["out_crs"],
        pixel_size=float(r_cfg["pixel_size"]),
        all_touched=bool(r_cfg.get("all_touched", False)),
        nodata=int(r_cfg.get("nodata", 255)),
        dtype=str(r_cfg.get("dtype", "uint8")),
        compress=str(r_cfg.get("compress", "DEFLATE")),
        tiled=bool(r_cfg.get("tiled", True)),
        blockxsize=int(r_cfg.get("blockxsize", 256)),
        blockysize=int(r_cfg.get("blockysize", 256)),
        predictor=r_cfg.get("predictor", 2),
    )

    # -------------------------
    # TSA GeoJSON Creation and Grid Creation
    # -------------------------
    # ADD IN ITERATION TO TRY AND GENERATE ALL THE BLOCKS ONE AT A TIME
    log_event(f"Running TSA SQL Query for TSA {tsa_params['tsa_id']}")
    cols, rows = bcgw.run_sql(conn, "TSA.sql", params=tsa_params)

    log_event(f"Creating TSA GeoJSON for TSA {tsa_params['tsa_id']}")
    tsa_geojson_path = ROOT / "data" / f"tsa_{cfg['tsa']['feature_id']}.geojson"
    generate_geojson(cols, rows, tsa_geojson_path)

    # Create the canonical grid (this is what makes all rasters align)
    log_event("Creating canonical RasterGrid from TSA GeoJSON")
    grid = raster.RasterGrid.from_geojson_aoi(
        aoi_geojson_path=tsa_geojson_path,
        config=grid_cfg,  # <-- THIS MUST BE GridConfig, not dict
        aoi_crs_if_missing=r_cfg.get("geojson_crs_if_missing", "EPSG:4326"),
        pad_pixels=int(r_cfg.get("pad_pixels", 0)),
    )

    log_event(
        f"Grid created – CRS={grid.crs}, pixel={grid.pixel_size}, "
        f"origin=({grid.extent.xmin}, {grid.extent.ymax}), size={grid.width}x{grid.height}"
    )

    # # -------------------------
    # # Data Prep of Geojson
    # # -------------------------
    # # Get VRI Data
    # log_event(f"Running Data Prep SQL Query for TSA {data_prep_params['tsa_id']}")
    # cols, rows = bcgw.run_sql(conn, "data_prep.sql", params=data_prep_params)

    # log_event(f"Creating Data Prep GeoJSON for TSA {data_prep_params['tsa_id']}")
    # dataprep_geojson_path = ROOT / "data" / f"dataprep_{data_prep_params['tsa_id']}.geojson"
    # generate_geojson(cols, rows, dataprep_geojson_path)

    # # Get Fire Data (Historic)
    # log_event(f"Running Historic Fire SQL Query for TSA {data_prep_params['tsa_id']}")
    # cols, rows = bcgw.run_sql(conn, "Nesting_Historic_Fire.sql", params=data_prep_params)

    # log_event(f"Creating Historic Fire GeoJSON for TSA {data_prep_params['tsa_id']}")
    # NestingHistoricFire_geojson_path = ROOT / "data" / f"NestingHistoricFire_{data_prep_params['tsa_id']}.geojson"
    # generate_geojson(cols, rows, NestingHistoricFire_geojson_path)

    # # Get Fire Data (Current)
    # log_event(f"Running Current Fire SQL Query for TSA {data_prep_params['tsa_id']}")
    # cols, rows = bcgw.run_sql(conn, "Nesting_Current_Fire.sql", params=data_prep_params)

    # log_event(f"Creating Current Fire GeoJSON for TSA {data_prep_params['tsa_id']}")
    # NestingCurrentFire_geojson_path = ROOT / "data" / f"NestingCurrentFire_{data_prep_params['tsa_id']}.geojson"
    # generate_geojson(cols, rows, NestingCurrentFire_geojson_path)

    # # Get Cutblock Data (Current)
    # log_event(f"Running Cutblock SQL Query for TSA {data_prep_params['tsa_id']}")
    # cols, rows = bcgw.run_sql(conn, "Nesting_Consolidated_Cutblocks_Fire.sql", params=data_prep_params)

    # log_event(f"Creating Cutblock Fire GeoJSON for TSA {data_prep_params['tsa_id']}")
    # NestingCutblock_geojson_path = ROOT / "data" / f"NestingCutblock_{data_prep_params['tsa_id']}.geojson"
    # generate_geojson(cols, rows, NestingCutblock_geojson_path)

    # # -------------------------
    # # Nesting GeoJSON + raster (ALIGNED)
    # # -------------------------
    # log_event(f"Running Nesting SQL Query for TSA {nest_params['tsa_id']}")
    # cols, rows = bcgw.run_sql(conn, "nesting.sql", params=nest_params)

    # log_event(f"Creating Nesting GeoJSON for TSA {nest_params['tsa_id']}")
    # nesting_geojson_path = ROOT / "data" / f"nesting_{nest_params['tsa_id']}.geojson"
    # generate_geojson(cols, rows, nesting_geojson_path)

    nesting_geojson_path = ROOT / "data" / "robson_nesting_modelbuilder.geojson"

    log_event(f"Creating Nesting TIF for TSA {nest_params['tsa_id']} (aligned to canonical grid)")
    nest_raster_out_path = ROOT / "data" / f"nest_raster_{nest_params['tsa_id']}.tif"
    grid.rasterize_geojson_binary(
        geojson_path=nesting_geojson_path,
        out_tif_path=nest_raster_out_path,
        geojson_crs_if_missing=r_cfg.get("geojson_crs_if_missing", "EPSG:4326"),
        burn_value=1,
    )
    _log_raster_stats(nest_raster_out_path, "Nesting TIF Created")

    # # -------------------------
    # # Foraging GeoJSON + raster (ALIGNED)
    # # -------------------------
    # log_event(f"Running Foraging SQL Query for TSA {forage_params['tsa_id']}")
    # cols, rows = bcgw.run_sql(conn, "foraging_2.sql", params=forage_params)

    # log_event(f"Creating Foraging GeoJSON for TSA {forage_params['tsa_id']}")
    # foraging_geojson_path = ROOT / "data" / f"foraging_{forage_params['tsa_id']}.geojson"
    # generate_geojson(cols, rows, foraging_geojson_path)

    foraging_geojson_path = ROOT / "data" / "robson_foraging_modelbuilder.geojson"


    log_event(f"Creating Foraging TIF for TSA {forage_params['tsa_id']} (aligned to canonical grid)")
    forage_raster_out_path = ROOT / "data" / f"forage_raster_{forage_params['tsa_id']}.tif"
    grid.rasterize_geojson_age_classes(
        geojson_path=foraging_geojson_path,
        out_tif_path=forage_raster_out_path,
        age_field="PROJ_AGE_1",
        geojson_crs_if_missing=r_cfg.get("geojson_crs_if_missing", "EPSG:4326"),
        # If you're using nodata=255 (recommended for ArcGIS when 0 is a valid class),
        # keep it in config.toml as raster.nodata=255 and you can omit these:
        nodata=int(r_cfg.get("forage_nodata", grid_cfg.nodata)),
        dtype=str(r_cfg.get("forage_dtype", grid_cfg.dtype)),
    )
    _log_raster_stats(forage_raster_out_path, "Foraging TIF Created")



# %%
if __name__ == "__main__":
    main()


# %%