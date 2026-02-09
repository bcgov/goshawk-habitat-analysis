"""
raster_tools.py

Refactor of the original free functions into a small, reusable module that:

- Makes a canonical snapped raster grid based on:
    * target CRS (config)
    * resolution (config)
    * origin + extent inferred from a GeoJSON AOI (any CRS), snapped to the grid
- Provides methods to rasterize:
    * binary 0/1 masks
    * 3-class age-based rasters (0/2/1) with deterministic overwrite priority
- Designed to be called by an orchestration script: all inputs are passed in.

Dependencies:
    geopandas
    shapely
    numpy
    rasterio
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import math
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin, Affine
from rasterio.crs import CRS


PathLike = Union[str, Path]


# ----------------------------
# Config + helpers
# ----------------------------

@dataclass(frozen=True)
class GridConfig:
    """Stable raster settings you want consistent across outputs."""
    out_crs: Union[str, CRS] = "EPSG:3005"  # BC Albers (meters)
    pixel_size: float = 30.0
    all_touched: bool = False
    nodata: int = 0
    dtype: str = "uint8"
    compress: str = "DEFLATE"
    tiled: bool = True
    blockxsize: int = 256
    blockysize: int = 256
    predictor: Optional[int] = 2  # good for integer rasters; set None to omit


@dataclass(frozen=True)
class GridExtent:
    xmin: float
    ymin: float
    xmax: float
    ymax: float


def _ensure_crs(crs: Union[str, CRS]) -> CRS:
    return crs if isinstance(crs, CRS) else CRS.from_user_input(crs)


def _clean_polygons(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Keep only polygonal geometries, drop empties/nulls, and attempt simple repairs."""
    gdf = gdf[gdf.geometry.notnull()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    gdf = gdf[gdf.geom_type.isin(["Polygon", "MultiPolygon"])].copy()
    if gdf.empty:
        raise ValueError("No Polygon/MultiPolygon geometries found after filtering.")
    # Simple repair for common invalids
    gdf["geometry"] = gdf.geometry.buffer(0)
    # Drop any that became empty after repair
    gdf = gdf[gdf.geometry.notnull() & ~gdf.geometry.is_empty].copy()
    if gdf.empty:
        raise ValueError("All geometries became empty/invalid after repair.")
    return gdf


def _snap_bounds(bounds, pixel_size: float) -> GridExtent:
    """Snap bounds to the pixel grid so outputs align nicely."""
    minx, miny, maxx, maxy = bounds
    minx = math.floor(minx / pixel_size) * pixel_size
    miny = math.floor(miny / pixel_size) * pixel_size
    maxx = math.ceil(maxx / pixel_size) * pixel_size
    maxy = math.ceil(maxy / pixel_size) * pixel_size

    # FIX: use xmin/ymin field names
    return GridExtent(xmin=minx, ymin=miny, xmax=maxx, ymax=maxy)



# ----------------------------
# Canonical raster grid object
# ----------------------------

class RasterGrid:
    """
    Canonical, snapped grid definition built from an AOI GeoJSON.

    - Provide target CRS + pixel size via GridConfig.
    - Provide an AOI GeoJSON (any CRS) and optionally its CRS if missing.
    - The grid's origin and extent are computed from AOI bounds in target CRS,
      then snapped to the pixel grid.
    """

    def __init__(self, config: GridConfig, extent: GridExtent):
        self.config = config
        self.crs: CRS = _ensure_crs(config.out_crs)
        self.pixel_size: float = float(config.pixel_size)
        self.extent = extent

        width = (extent.xmax - extent.xmin) / self.pixel_size
        height = (extent.ymax - extent.ymin) / self.pixel_size

        self.width = int(round(width))
        self.height = int(round(height))
        if self.width <= 0 or self.height <= 0:
            raise ValueError(f"Invalid grid dimensions: width={self.width}, height={self.height}")

        # top-left origin (xmin, ymax)
        self.transform: Affine = from_origin(extent.xmin, extent.ymax, self.pixel_size, self.pixel_size)

    @classmethod
    def from_geojson_aoi(
        cls,
        aoi_geojson_path: PathLike,
        config: GridConfig,
        aoi_crs_if_missing: Union[str, CRS] = "EPSG:4326",
        pad_pixels: int = 0,
    ) -> "RasterGrid":
        """
        Create a grid from an AOI GeoJSON by:
        - reading AOI,
        - ensuring its CRS,
        - reprojecting to config.out_crs,
        - taking bounds and snapping to pixel grid,
        - optional padding by N pixels.
        """
        aoi_geojson_path = Path(aoi_geojson_path)
        if not aoi_geojson_path.exists():
            raise FileNotFoundError(f"AOI GeoJSON not found: {aoi_geojson_path}")

        gdf = gpd.read_file(aoi_geojson_path)
        if gdf.crs is None:
            gdf = gdf.set_crs(aoi_crs_if_missing)

        gdf = _clean_polygons(gdf)
        gdf = gdf.to_crs(_ensure_crs(config.out_crs))

        extent = _snap_bounds(gdf.total_bounds, config.pixel_size)

        if pad_pixels > 0:
            pad = pad_pixels * config.pixel_size
            extent = GridExtent(
                xmin=extent.xmin - pad,
                ymin=extent.ymin - pad,
                xmax=extent.xmax + pad,
                ymax=extent.ymax + pad,
            )

        return cls(config=config, extent=extent)

    def profile(self) -> dict:
        prof = {
            "driver": "GTiff",
            "height": self.height,
            "width": self.width,
            "count": 1,
            "dtype": self.config.dtype,
            "crs": self.crs,
            "transform": self.transform,
            "nodata": self.config.nodata,
            "compress": self.config.compress,
            "tiled": self.config.tiled,
            "blockxsize": self.config.blockxsize,
            "blockysize": self.config.blockysize,
        }
        if self.config.predictor is not None:
            # only meaningful for some compressions; safe to include for DEFLATE/LZW in many cases
            prof["predictor"] = self.config.predictor
        return prof

    # ----------------------------
    # Method 1: binary rasterize (0/1)
    # ----------------------------

    def rasterize_geojson_binary(
        self,
        geojson_path: PathLike,
        out_tif_path: PathLike,
        geojson_crs_if_missing: Union[str, CRS] = "EPSG:4326",
        burn_value: int = 1,
    ) -> Path:
        """
        Rasterize polygon features into a 0/1 raster on THIS grid.
        Background = config.nodata.
        """
        geojson_path = Path(geojson_path)
        out_tif_path = Path(out_tif_path)

        gdf = gpd.read_file(geojson_path)
        if gdf.crs is None:
            gdf = gdf.set_crs(geojson_crs_if_missing)

        gdf = _clean_polygons(gdf)
        gdf = gdf.to_crs(self.crs)

        shapes = (
            (geom, burn_value)
            for geom in gdf.geometry
            if geom is not None and not geom.is_empty
        )

        arr = rasterize(
            shapes=shapes,
            out_shape=(self.height, self.width),
            fill=self.config.nodata,
            transform=self.transform,
            all_touched=self.config.all_touched,
            dtype=self.config.dtype,
        )

        out_tif_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_tif_path, "w", **self.profile()) as dst:
            dst.write(arr, 1)

        return out_tif_path

    # ----------------------------
    # Method 2: age-class rasterize (0/2/1)
    # ----------------------------

    def rasterize_geojson_age_classes(
        self,
        geojson_path: PathLike,
        out_tif_path: PathLike,
        age_field: str = "PROJ_AGE_1",
        geojson_crs_if_missing: Union[str, CRS] = "EPSG:4326",
        nodata: Optional[int] = None,
        dtype: Optional[str] = None,
        # class rules (defaults match your current logic)
        not_forage_value: int = 0,        # <40
        functional_value: int = 2,        # 40-79
        high_quality_value: int = 1,      # >=80
        min_functional: float = 40.0,
        min_high_quality: float = 80.0,
    ) -> Path:
        """
        Rasterize polygons to age-based classes:
            <40   -> 0
            40-79 -> 2
            >=80  -> 1

        Overlap priority is deterministic:
            burns 0 first, then 2, then 1 so "better" overwrites "worse".
        """
        geojson_path = Path(geojson_path)
        out_tif_path = Path(out_tif_path)

        gdf = gpd.read_file(geojson_path)
        if gdf.crs is None:
            gdf = gdf.set_crs(geojson_crs_if_missing)

        gdf = _clean_polygons(gdf)
        gdf = gdf.to_crs(self.crs)

        # coerce age to numeric
        gdf[age_field] = gdf[age_field].replace("", np.nan)
        gdf[age_field] = gdf[age_field].astype(float)

        # build class column
        cls_col = "forage_cls"
        gdf[cls_col] = not_forage_value
        gdf.loc[(gdf[age_field] >= min_functional) & (gdf[age_field] < min_high_quality), cls_col] = functional_value
        gdf.loc[(gdf[age_field] >= min_high_quality), cls_col] = high_quality_value

        # overlap priority: low->high so high overwrites
        gdf = gdf.sort_values(cls_col, ascending=True)

        shapes = (
            (geom, int(val))
            for geom, val in zip(gdf.geometry, gdf[cls_col])
            if geom is not None and not geom.is_empty
        )

        out_dtype = dtype if dtype is not None else self.config.dtype
        out_nodata = nodata if nodata is not None else self.config.nodata

        arr = rasterize(
            shapes=shapes,
            out_shape=(self.height, self.width),
            fill=out_nodata,
            transform=self.transform,
            all_touched=self.config.all_touched,
            dtype=out_dtype,
        )

        profile = self.profile()
        profile["dtype"] = out_dtype
        profile["nodata"] = out_nodata

        out_tif_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_tif_path, "w", **profile) as dst:
            dst.write(arr, 1)

        return out_tif_path
