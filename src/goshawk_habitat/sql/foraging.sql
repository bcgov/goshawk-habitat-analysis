WITH
tsa AS (
    SELECT /*+ MATERIALIZE */ t.geometry AS tsa_geom
    FROM WHSE_ADMIN_BOUNDARIES.FADM_TSA t
    WHERE t.feature_id = :tsa_id
),

-- Build erase geometry in 3 smaller unions, then union those.
fires_hist AS (
    SELECT /*+ MATERIALIZE */
        SDO_AGGR_UNION(SDOAGGRTYPE(h.shape, :tol)) AS geom
    FROM WHSE_LAND_AND_NATURAL_RESOURCE.PROT_HISTORICAL_FIRE_POLYS_SP h
    CROSS JOIN tsa t
    WHERE
        h.fire_year >= :min_disturbance_year
        AND SDO_FILTER(h.shape, t.tsa_geom) = 'TRUE'
        AND SDO_RELATE(h.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
),
fires_curr AS (
    SELECT /*+ MATERIALIZE */
        SDO_AGGR_UNION(SDOAGGRTYPE(c.shape, :tol)) AS geom
    FROM WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP c
    CROSS JOIN tsa t
    WHERE
        SDO_FILTER(c.shape, t.tsa_geom) = 'TRUE'
        AND SDO_RELATE(c.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
),
harvest AS (
    SELECT /*+ MATERIALIZE */
        SDO_AGGR_UNION(SDOAGGRTYPE(cb.shape, :tol)) AS geom
    FROM WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP cb
    CROSS JOIN tsa t
    WHERE
        cb.harvest_start_year_calendar >= :min_disturbance_year
        AND SDO_FILTER(cb.shape, t.tsa_geom) = 'TRUE'
        AND SDO_RELATE(cb.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
),

erase_union AS (
    SELECT /*+ MATERIALIZE */
        SDO_AGGR_UNION(SDOAGGRTYPE(x.geom, :tol)) AS erase_geom
    FROM (
        SELECT geom FROM fires_hist WHERE geom IS NOT NULL
        UNION ALL
        SELECT geom FROM fires_curr WHERE geom IS NOT NULL
        UNION ALL
        SELECT geom FROM harvest    WHERE geom IS NOT NULL
    ) x
),

base_vri AS (
    SELECT /*+ MATERIALIZE */
        v.feature_id,
        v.proj_age_1,
        v.proj_height_1,
        v.crown_closure,
        v.site_index,
        v.bec_zone_code,
        v.bec_subzone,
        v.geometry AS vri_geom,
        t.tsa_geom
    FROM WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY v
    CROSS JOIN tsa t
    WHERE
        v.proj_age_1 >= :min_age
        -- keep the spatial prefilter (index-friendly)
        AND SDO_FILTER(v.geometry, t.tsa_geom) = 'TRUE'
        AND SDO_RELATE(v.geometry, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
),

diffed AS (
    SELECT
        b.*,
        eu.erase_geom,
        CASE
            WHEN eu.erase_geom IS NULL THEN b.vri_geom

            -- Only do expensive DIFFERENCE if it actually intersects erase_geom
            WHEN SDO_RELATE(b.vri_geom, eu.erase_geom, 'mask=ANYINTERACT') = 'TRUE'
                THEN SDO_GEOM.SDO_DIFFERENCE(b.vri_geom, eu.erase_geom, :tol)

            ELSE b.vri_geom
        END AS diff_geom
    FROM base_vri b
    CROSS JOIN erase_union eu
),

clipped AS (
    SELECT
        d.feature_id,
        d.proj_age_1,
        d.proj_height_1,
        d.crown_closure,
        d.site_index,
        d.bec_zone_code,
        d.bec_subzone,
        SDO_GEOM.SDO_INTERSECTION(d.diff_geom, d.tsa_geom, :tol) AS final_geom
    FROM diffed d
    WHERE d.diff_geom IS NOT NULL
)

SELECT
    c.feature_id,
    c.proj_age_1,
    c.proj_height_1,
    c.crown_closure,
    c.site_index,
    c.bec_zone_code,
    c.bec_subzone,
    SDO_UTIL.TO_GEOJSON(
        SDO_CS.TRANSFORM(c.final_geom, 4326)
    ) AS geom_geojson
FROM clipped c
WHERE c.final_geom IS NOT NULL
