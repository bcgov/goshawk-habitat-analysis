# %%
import subprocess
from pathlib import Path
import shutil

import rasterio
from rasterio.merge import merge

raster_1 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_363.tif"
raster_2 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_364.tif"
raster_3 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_365.tif"
raster_4 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_366.tif"
raster_5 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_367.tif"
raster_6 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_368.tif"
raster_7 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_369.tif"
raster_8 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_370.tif"
raster_9 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_371.tif"
raster_10 = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/nest_raster_372.tif"


out_tif  = r"W:/gss/projects/gr_2025_1452_Goshawk_Occupancy_Analysis/work/goshawk-occupancy-analysis/data/forage_mosaic.tif"

# Put your 10 raster paths here (strings)
rasters = [
    raster_1,
    raster_2,
    raster_3,
    raster_4,
    raster_5,
    raster_6,
    raster_7,
    raster_8,
    raster_9,
    raster_10,
]

# Open all sources and merge
srcs = [rasterio.open(p) for p in rasters]
try:
    mosaic, transform = merge(srcs)

    meta = srcs[0].meta.copy()
    meta.update(
        {
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": transform,
            "compress": "deflate",
            "tiled": True,
            "bigtiff": "IF_SAFER",
        }
    )

    with rasterio.open(out_tif, "w", **meta) as dst:
        dst.write(mosaic)

    print("Done:", out_tif)

finally:
    for s in srcs:
        s.close()


# %%
