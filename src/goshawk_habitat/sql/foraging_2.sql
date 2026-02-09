WITH
tsa AS (
    SELECT t.geometry AS tsa_geom
    FROM WHSE_ADMIN_BOUNDARIES.FADM_TSA t
    WHERE t.feature_id = :tsa_id
),
erase_union AS (
    SELECT
        SDO_AGGR_UNION(SDOAGGRTYPE(x.erase_geom, :tol)) AS erase_geom
    FROM (
        -- 1) historical fires (pre-1986)
        SELECT h.shape AS erase_geom
        FROM WHSE_LAND_AND_NATURAL_RESOURCE.PROT_HISTORICAL_FIRE_POLYS_SP h
        CROSS JOIN tsa t
        WHERE
            h.fire_year > :min_disturbance_year
            AND SDO_FILTER(h.shape, t.tsa_geom) = 'TRUE'
            AND SDO_RELATE(h.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'

        UNION ALL

        -- 2) current fires (all)
        SELECT c.shape AS erase_geom
        FROM WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP c
        CROSS JOIN tsa t
        WHERE
            SDO_FILTER(c.shape, t.tsa_geom) = 'TRUE'
            AND SDO_RELATE(c.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'

        UNION ALL

        -- 3) consolidated cut blocks (pre-1986)
        SELECT cb.shape AS erase_geom
        FROM WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP cb
        CROSS JOIN tsa t
        WHERE
            cb.harvest_start_year_calendar > :min_disturbance_year
            AND SDO_FILTER(cb.shape, t.tsa_geom) = 'TRUE'
            AND SDO_RELATE(cb.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
    ) x
),
base_vri AS (
    SELECT
        v.feature_id,
        v.proj_age_1,
        v.geometry AS vri_geom,
        t.tsa_geom,
        eu.erase_geom
    FROM WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY v
    CROSS JOIN tsa t
    CROSS JOIN erase_union eu
    WHERE
        v.proj_age_1 > :min_age

        -- pre-filter to TSA using spatial index
        AND SDO_FILTER(v.geometry, t.tsa_geom) = 'TRUE'
        AND SDO_RELATE(v.geometry, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
),
diffed AS (
    -- erase fire/harvest areas from VRI geometry
    SELECT
        b.*,
        CASE
            WHEN b.erase_geom IS NULL THEN b.vri_geom
            ELSE SDO_GEOM.SDO_DIFFERENCE(b.vri_geom, b.erase_geom, :tol)
        END AS diff_geom
    FROM base_vri b
),
clipped AS (
    -- NOW clip the erased result to the TSA so you don't output geometry outside the TSA
    SELECT
        d.feature_id,
        d.proj_age_1,
        SDO_GEOM.SDO_INTERSECTION(d.diff_geom, d.tsa_geom, :tol) AS final_geom
    FROM diffed d
    WHERE d.diff_geom IS NOT NULL
)
SELECT
    c.feature_id,
    c.proj_age_1,
    SDO_UTIL.TO_GEOJSON(
        SDO_CS.TRANSFORM(c.final_geom, 4326)
    ) AS geom_geojson
FROM clipped c
WHERE c.final_geom IS NOT NULL
