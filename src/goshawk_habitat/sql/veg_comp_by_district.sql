-- veg_comp_by_district.sql
-- Purpose:
--   Efficiently extract VRI veg comp polygons that intersect a single NR district,
--   using spatial index prefiltering + attribute filters, returning only needed columns.

WITH district AS (
    SELECT
        d.shape AS district_geom
    FROM
        WHSE_ADMIN_BOUNDARIES.ADM_NR_DISTRICTS_SP d
    WHERE
        d.org_unit = :district_code
)
SELECT
    v.feature_id,
    v.proj_age_1,
    v.proj_height_1,
    v.crown_closure,
    v.site_index,
    SDO_UTIL.TO_GEOJSON(SDO_CS.TRANSFORM(v.geometry, 4326)) AS geom_geojson
FROM
    WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY v
    CROSS JOIN district d
WHERE
    v.proj_age_1 > :min_age
    AND v.proj_height_1 > :min_height
    AND v.crown_closure > :min_crown_closure
    AND v.site_index < :max_site_index
    AND SDO_FILTER(v.geometry, d.district_geom) = 'TRUE'
    AND SDO_RELATE(v.geometry, d.district_geom, 'mask=ANYINTERACT') = 'TRUE'