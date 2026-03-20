# %% Setup----------------------------------------------------------------------
import tomllib
from pathlib import Path
from sqlalchemy import create_engine, text, NullPool
import os 
from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
from shapely import wkt
import matplotlib.pyplot as plt
import logging
import time
import psutil
import numpy as np

PROJECT_ROOT = Path(__file__).parents[1]
CONFIG_PATH = PROJECT_ROOT / "config.toml"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------

# %% 1. Configuration -------------------------------------------------------------
def load_config(config_path: str = "config.toml") -> dict:
    """
    Load Parameters from a TOML config file. 
    """
    with open(config_path, "rb") as f:   # tomllib requires binary mode
        return tomllib.load(f)
    
def log_ram(label: str = ""):
    process = psutil.Process(os.getpid())
    ram_mb = process.memory_info().rss / 1024 ** 2
    logger.info(f"RAM {label}: {ram_mb:,.1f} MB")

# ------------------------------------------------------------------------------

# %% 2. Database Connection ----------------------------------------------------
def build_engine(project_root: Path):
    """
    Load Oracle credentials from the .env file at the project root
        and return a SQLAlchemy engine.

    expect required keys in .env:
        BCGW_HOST, BCGW_PORT, BCGW_SERVICE, BCGW_USER, BCGW_PASS
    """
    load_dotenv(project_root / ".env")
    
    host    = os.environ["BCGW_HOST"]
    port    = os.environ["BCGW_PORT"]
    service = os.environ["BCGW_SERVICE"]
    user    = os.environ["BCGW_USERNAME"]
    passwd  = os.environ["BCGW_PASSWORD"]
 
    url = f"oracle+oracledb://{user}:{passwd}@{host}:{port}/?service_name={service}"
    
    return create_engine(url)

def read_bcgw_table(engine, schema_table: str, where: str = None,
                    columns: list = None, geom_field: str = "SHAPE") -> gpd.GeoDataFrame:
    """
    Read a BCGW Oracle spatial table into a GeoDataFrame.

    Oracle stores geometry as SDO_GEOMETRY, which GeoPandas cannot parse
    directly. This function converts it to WKB inside the SQL query using
    SDO_UTIL.TO_WKBGEOMETRY(), then builds the GeoDataFrame from the result.

    Parameters
    ----------
    engine       : SQLAlchemy engine
    schema_table : e.g. "WHSE_ADMIN_BOUNDARIES.FADM_TSA"
    where        : optional SQL WHERE clause string (no 'WHERE' keyword)
    columns      : optional list of non-geometry columns to select;
                   geometry column is always included automatically
    geom_field   : name of the geometry column (default: "SHAPE";
                   some BCGW tables use "GEOMETRY" instead)
    """
    schema, table = schema_table.split(".")


    sql = f"""
        SELECT t.*, SDO_UTIL.TO_WKTGEOMETRY(t.{geom_field}) AS geom_wkt
        FROM {schema}.{table} t
    """
    if where:
        sql += f" WHERE {where}"

    logger.info(f"Querying {schema}.{table}...")
    t0 = time.perf_counter()
    df = pd.read_sql(text(sql), engine)
    logger.info(f"Fetched {len(df)} rows in {time.perf_counter() - t0:.1f}s")


    df = df.drop(columns=[geom_field.lower()])              # drop original SDO column first
    df["geometry"] = df["geom_wkt"].apply(lambda x: wkt.loads(x) if x is not None else None)
    df = df.drop(columns=["geom_wkt"])                      # then drop the wkt helper column

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:3005")
    return gdf

# ------------------------------------------------------------------------------

# %%
cfg = load_config(CONFIG_PATH)
engine = build_engine(PROJECT_ROOT)



# %% 3. Source Layers ----------------------------------------------------------
# 3a: Timber Supply Area & Buffer - Select Single TSA of Interest
log_ram("before AOI fetch")
tsa_id = str(cfg["tsa"]["feature_id"])
buffer_dist = int(cfg["tsa"]["buffer_dist"])
tsa_geom_field = str(cfg["tsa"]["geometry_f"])

tsa_all = read_bcgw_table(engine, "WHSE_ADMIN_BOUNDARIES.FADM_TSA", where=f"FEATURE_ID = '{tsa_id}'",geom_field=tsa_geom_field)
aoi_buffer = tsa_all.copy()
aoi_buffer["geometry"] = tsa_all.buffer(buffer_dist)
minx, miny, maxx, maxy = aoi_buffer.total_bounds
log_ram("after AOI fetch")

# 3b: VRI – large table; Filter applied server-side with the buffered AOI bbox
log_ram("before VRI fetch")
vri = read_bcgw_table(
    engine,
    "WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY",
    where=f"""SDO_FILTER(
        t.GEOMETRY,
        SDO_GEOMETRY(
            2003, 3005, NULL,
            SDO_ELEM_INFO_ARRAY(1, 1003, 3),
            SDO_ORDINATE_ARRAY({minx}, {miny}, {maxx}, {maxy})
        )
    ) = 'TRUE'""", columns=cfg["vri_params"]["vri_cols"],
    geom_field="GEOMETRY"
)
log_ram("after VRI fetch")

# Clip VRI to the Buffered TSA of interest
vri_clipped = gpd.clip(vri, aoi_buffer)
log_ram("after clip")

# Delete VRI Query to free up Memory
del vri
log_ram("after del vri")


# %% Dissolve and Consolidate VRI DATA -----------------------------------------
dissolve_fields = cfg["vri_params"]["vri_cols"]
dissolve_fields = [f.lower() for f in dissolve_fields if f.lower() in vri_clipped.columns]

logger.info("Dissolving VRI by stand attributes...")
t0 = time.perf_counter()
vri_dissolved = (
    vri_clipped
    .dissolve(by=dissolve_fields, as_index=False)
    .explode(index_parts=False)   # SINGLE_PART equivalent
    .reset_index(drop=True)
)
logger.info(f"Dissolved to {len(vri_dissolved)} features in {time.perf_counter() - t0:.1f}s")
log_ram("after VRI dissolve")

del vri_clipped
log_ram("after del vri_clipped")
# ------------------------------------------------------------------------------
# %% 3c Subtraction Layers------------------------------------------------------
logger.info("Loading additional Subtractions layers...")
cutblocks = read_bcgw_table(
    engine,
    "WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP",
    where=f"""SDO_FILTER(
        t.SHAPE,
        SDO_GEOMETRY(
            2003, 3005, NULL,
            SDO_ELEM_INFO_ARRAY(1, 1003, 3),
            SDO_ORDINATE_ARRAY({minx}, {miny}, {maxx}, {maxy})
        )
    ) = 'TRUE'""",
    columns=cfg["cutblock_params"]["cutblock_columns"],
    geom_field=cfg["cutblock_params"]["geometry_f"]
)

historic_fires = read_bcgw_table(
    engine,
    "WHSE_LAND_AND_NATURAL_RESOURCE.PROT_HISTORICAL_FIRE_POLYS_SP",
    where=f"""SDO_FILTER(
        t.SHAPE,
        SDO_GEOMETRY(
            2003, 3005, NULL,
            SDO_ELEM_INFO_ARRAY(1, 1003, 3),
            SDO_ORDINATE_ARRAY({minx}, {miny}, {maxx}, {maxy})
        )
    ) = 'TRUE'""",
    columns=["FIRE_YEAR"],
    geom_field="SHAPE"
)

current_fires = read_bcgw_table(
    engine,
    "WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP",
    where=f"""SDO_FILTER(
        t.SHAPE,
        SDO_GEOMETRY(
            2003, 3005, NULL,
            SDO_ELEM_INFO_ARRAY(1, 1003, 3),
            SDO_ORDINATE_ARRAY({minx}, {miny}, {maxx}, {maxy})
        )
    ) = 'TRUE'""",
    geom_field="SHAPE"
)
log_ram("after loading fire/cutblock layers")

# %% Nesting Workflow-----------------------------------------------------------
vri_nesting = vri_dissolved[
    (vri_dissolved["proj_age_1"]    >= int(cfg["nesting_vri_params"]["proj_age_1"])) &
    (vri_dissolved["proj_height_1"] >= int(cfg["nesting_vri_params"]["proj_height"])) &
    (vri_dissolved["crown_closure"] >= int(cfg["nesting_vri_params"]["crown_closure"])) &
    (vri_dissolved["site_index"]    >  int(cfg["nesting_vri_params"]["site_index"]))
].copy()
logger.info(f"Nesting candidates: {len(vri_nesting)} features")

# ESSF-mmp/wcp exclusion
essf_filter = vri_nesting[
    (vri_nesting["bec_zone_code"] == cfg["nesting_vri_params"]["bec_zone_code"]) &
    (vri_nesting["bec_subzone"].isin(cfg["nesting_vri_params"]["bec_subzone_codes"]))
].copy()
logger.info(f"ESSF exclusion filter: {len(essf_filter)} features")

# Cutblocks harvested since 1946
filter_cutblocks_nesting = cutblocks[
    cutblocks["harvest_start_year_calendar"] >= int(cfg["nesting_vri_params"]["cutblock_year"])
].copy()
logger.info(f"Cutblock filter (>=1946): {len(filter_cutblocks_nesting)} features")

# Historic fires since 1946
filter_fires_nesting = historic_fires[
    historic_fires["fire_year"] >= int(cfg["nesting_vri_params"]["fire_year"])
].copy()
logger.info(f"Historic fire filter (>=1946): {len(filter_fires_nesting)} features")

# Merge all exclusion layers
filter_nesting = gpd.GeoDataFrame(
    pd.concat(
        [current_fires, essf_filter, filter_cutblocks_nesting, filter_fires_nesting],
        ignore_index=True
    ),
    geometry="geometry",
    crs="EPSG:3005"
)
logger.info(f"Combined nesting exclusion filter: {len(filter_nesting)} features")

# Erase exclusion areas from nesting candidates
logger.info("Erasing exclusion areas from nesting polygons...")
t0 = time.perf_counter()
nesting_polygons = gpd.overlay(vri_nesting, filter_nesting,
                               how="difference", keep_geom_type=True)
nesting_polygons = nesting_polygons.explode(index_parts=False).reset_index(drop=True)
logger.info(f"Nesting polygons after erase: {len(nesting_polygons)} in {time.perf_counter() - t0:.1f}s")
log_ram("after nesting erase")

# Dissolve remaining nesting polygons
nesting_dissolved = (
    nesting_polygons
    .dissolve(as_index=False)
    .explode(index_parts=False)
    .reset_index(drop=True)
)
logger.info(f"Nesting dissolved: {len(nesting_dissolved)} features")




# %% Mapping VRI Data - TEST
# Export nesting GeoJSON (WGS84)
nesting_out = PROJECT_ROOT / f"Nesting_Model_{tsa_id}.geojson"
nesting_dissolved.to_crs("EPSG:4326").to_file(nesting_out, driver="GeoJSON")
logger.info(f"Nesting GeoJSON exported → {nesting_out}")

# Define 50-year bins
bins = [0, 50, 100, 150, 200, 250, 300, np.inf]
labels = ["0–50", "51–100", "101–150", "151–200", "201–250", "251–300", "300+"]

vri_dissolved["AGE_CLASS"] = pd.cut(
    vri_dissolved["proj_age_1"],
    bins=bins,
    labels=labels,
    right=True
)

fig, ax = plt.subplots(figsize=(12, 10))
vri_dissolved.plot(
    ax=ax,
    column="AGE_CLASS",
    cmap="YlGn",
    legend=True,
    legend_kwds={"title": "Stand Age (years)", "loc": "lower left"},
    missing_kwds={"color": "#d3d3d3", "label": "Non-forested / No Data"}
)
ax.set_title("VRI — Projected Stand Age Class (50-year intervals)", fontsize=13, fontweight="bold")
ax.set_xlabel("Easting (m) — BC Albers EPSG:3005")
ax.set_ylabel("Northing (m) — BC Albers EPSG:3005")
plt.tight_layout()
plt.show()
# %%
