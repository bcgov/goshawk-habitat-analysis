WITH tsa AS (
    SELECT
        t.geometry AS tsa_geom
    FROM
        WHSE_ADMIN_BOUNDARIES.FADM_TSA t
    WHERE
        t.feature_id = :tsa_id
)
SELECT
    v.feature_id,
    v.proj_age_1,
    v.proj_height_1,
    v.crown_closure,
    v.site_index,
    v.bec_zone_code,
    v.bec_subzone,
    SDO_UTIL.TO_GEOJSON(SDO_CS.TRANSFORM(v.geometry, 4326)) AS geom_geojson
FROM
    WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY v
    CROSS JOIN tsa t
WHERE
    v.proj_age_1 > :min_age
    AND v.proj_height_1 > :min_height
    AND v.crown_closure > :min_crown_closure
    AND v.site_index < :max_site_index
    AND NOT (
        UPPER(v.bec_zone_code) = 'ESSF'
        AND LOWER(v.bec_subzone) IN ('mmp', 'wcp')
    )
    AND SDO_FILTER(v.geometry, t.tsa_geom) = 'TRUE'
    AND SDO_RELATE(v.geometry, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'