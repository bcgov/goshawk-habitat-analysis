from __future__ import annotations
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from contextlib import contextmanager
from importlib.resources import files

def rasterize_geojson_to_30m(
    geojson_path: str | Path,
    out_tif_path: str | Path,
    out_crs: str = "EPSG:3005",  # BC Albers (meters). Change if you prefer.
    pixel_size: float = 30.0,
    all_touched: bool = False,
    nodata: int = 0,
) -> None:
    """
    Rasterize polygon features into a 0/1 raster GeoTIFF.

    Parameters
    ----------
    geojson_path : path to input GeoJSON
    out_tif_path : path to output GeoTIFF
    out_crs      : projected CRS in meters (required for 30m pixels)
    pixel_size   : pixel size in CRS units (meters)
    all_touched  : if True, any pixel touched by polygon becomes 1 (fatter)
                  if False, only pixels whose center is inside polygon become 1
    nodata       : background value (0 for non-suitable)
    """
    geojson_path = Path(geojson_path)
    out_tif_path = Path(out_tif_path)

    # 1) Read
    gdf = gpd.read_file(geojson_path)

    # Basic cleanup: keep polygonal geometry, drop empties
    gdf = gdf[gdf.geometry.notnull()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    gdf = gdf[gdf.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    if gdf.empty:
        raise ValueError("No Polygon/MultiPolygon geometries found after filtering.")

    # 2) Ensure input CRS is known; GeoJSON is often EPSG:4326.
    # If your file already has CRS, geopandas will keep it.
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    # 3) Reproject to a metric CRS for true 30m pixels
    gdf = gdf.to_crs(out_crs)

    # Optional: fix invalid geometries (common in large polygon sets)
    # Shapely 2.x: buffer(0) is still a decent "repair" for many cases.
    gdf["geometry"] = gdf.geometry.buffer(0)

    # 4) Compute raster bounds, snapped to pixel grid
    minx, miny, maxx, maxy = gdf.total_bounds

    # Snap bounds to pixel grid so output aligns nicely
    minx = math.floor(minx / pixel_size) * pixel_size
    miny = math.floor(miny / pixel_size) * pixel_size
    maxx = math.ceil(maxx / pixel_size) * pixel_size
    maxy = math.ceil(maxy / pixel_size) * pixel_size

    width = int((maxx - minx) / pixel_size)
    height = int((maxy - miny) / pixel_size)

    if width <= 0 or height <= 0:
        raise ValueError("Computed raster dimensions are not valid.")

    # 5) Affine transform: top-left origin
    transform = from_origin(minx, maxy, pixel_size, pixel_size)

    # 6) Rasterize to uint8 0/1
    shapes = ((geom, 1) for geom in gdf.geometry if geom is not None and not geom.is_empty)
    raster = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        fill=nodata,
        transform=transform,
        all_touched=all_touched,
        dtype="uint8",
    )

    # 7) Write GeoTIFF
    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": "uint8",
        "crs": out_crs,
        "transform": transform,
        "nodata": nodata,
        "compress": "DEFLATE",   # good default compression
        "predictor": 2,          # helps compression for integer rasters
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
    }

    out_tif_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(out_tif_path, "w", **profile) as dst:
        dst.write(raster, 1)

    print(f"Wrote {out_tif_path} ({width}x{height}, {pixel_size}m pixels, CRS={out_crs})")

def rasterize_geojson_to_30m_age_classes(
    geojson_path,
    out_tif_path,
    age_field="PROJ_AGE_1",
    out_crs="EPSG:3005",
    pixel_size=30.0,
    all_touched=False,
    nodata=255,  # keep nodata distinct from 0/1/2
    dtype=rasterio.uint8,
):
    gdf = gpd.read_file(geojson_path)

    # Reproject to raster CRS
    gdf = gdf.to_crs(out_crs)

    # Clean + coerce age to numeric
    gdf[age_field] = gdf[age_field].replace("", np.nan)
    gdf[age_field] = gdf[age_field].astype(float)

    # Build class column: 0/2/1
    # (default 0; set 2; then set 1 last so it wins for >=80)
    gdf["forage_cls"] = 0
    gdf.loc[(gdf[age_field] >= 40) & (gdf[age_field] < 80), "forage_cls"] = 2
    gdf.loc[(gdf[age_field] >= 80), "forage_cls"] = 1

    # Optional: drop null/empty geometries
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notnull()].copy()

    # Compute raster extent from features
    minx, miny, maxx, maxy = gdf.total_bounds
    width = int(np.ceil((maxx - minx) / pixel_size))
    height = int(np.ceil((maxy - miny) / pixel_size))

    transform = from_origin(minx, maxy, pixel_size, pixel_size)

    # IMPORTANT: ordering matters where polygons overlap.
    # Sort so that higher-quality overwrites lower-quality:
    # burn 0 first, then 2, then 1.
    gdf = gdf.sort_values("forage_cls", ascending=True)

    shapes = ((geom, int(val)) for geom, val in zip(gdf.geometry, gdf["forage_cls"]))

    out = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=nodata,          # background nodata
        all_touched=all_touched,
        dtype=dtype,
    )

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": dtype,
        "crs": out_crs,
        "transform": transform,
        "nodata": nodata,
        "compress": "lzw",
        "tiled": True,
        "blockxsize": 256,
        "blockysize": 256,
    }

    with rasterio.open(out_tif_path, "w", **profile) as dst:
        dst.write(out, 1)
