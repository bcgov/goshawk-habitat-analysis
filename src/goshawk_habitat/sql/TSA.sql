SELECT /*+ MATERIALIZE */
    SDO_UTIL.TO_GEOJSON(
        SDO_CS.TRANSFORM(t.geometry, 4326)
    ) AS geom_geojson,
    t.feature_id
FROM WHSE_ADMIN_BOUNDARIES.FADM_TSA t
WHERE t.feature_id = :tsa_id
