WITH
tsa AS (
  SELECT /*+ MATERIALIZE */ t.geometry AS tsa_geom
  FROM WHSE_ADMIN_BOUNDARIES.FADM_TSA t
  WHERE t.feature_id = :tsa_id
),
erase_union AS (
  SELECT /*+ MATERIALIZE */
    SDO_AGGR_UNION(SDOAGGRTYPE(x.erase_geom, :tol)) AS erase_geom
  FROM (
    SELECT SDO_GEOM.SDO_INTERSECTION(h.shape, t.tsa_geom, :tol) AS erase_geom
    FROM WHSE_LAND_AND_NATURAL_RESOURCE.PROT_HISTORICAL_FIRE_POLYS_SP h
    CROSS JOIN tsa t
    WHERE h.fire_year > :min_disturbance_year
      AND SDO_FILTER(h.shape, t.tsa_geom) = 'TRUE'
      AND SDO_RELATE(h.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'

    UNION ALL

    SELECT SDO_GEOM.SDO_INTERSECTION(c.shape, t.tsa_geom, :tol) AS erase_geom
    FROM WHSE_LAND_AND_NATURAL_RESOURCE.PROT_CURRENT_FIRE_POLYS_SP c
    CROSS JOIN tsa t
    WHERE SDO_FILTER(c.shape, t.tsa_geom) = 'TRUE'
      AND SDO_RELATE(c.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'

    UNION ALL

    SELECT SDO_GEOM.SDO_INTERSECTION(cb.shape, t.tsa_geom, :tol) AS erase_geom
    FROM WHSE_FOREST_VEGETATION.VEG_CONSOLIDATED_CUT_BLOCKS_SP cb
    CROSS JOIN tsa t
    WHERE cb.harvest_start_year_calendar > :min_disturbance_year
      AND SDO_FILTER(cb.shape, t.tsa_geom) = 'TRUE'
      AND SDO_RELATE(cb.shape, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
  ) x
  WHERE x.erase_geom IS NOT NULL
),
vri_in_tsa AS (
  SELECT
    v.feature_id,
    v.proj_age_1,
    SDO_GEOM.SDO_INTERSECTION(v.geometry, t.tsa_geom, :tol) AS vri_geom,
    eu.erase_geom
  FROM WHSE_FOREST_VEGETATION.VEG_COMP_LYR_R1_POLY v
  CROSS JOIN tsa t
  CROSS JOIN erase_union eu
  WHERE v.proj_age_1 > :min_age
    AND SDO_FILTER(v.geometry, t.tsa_geom) = 'TRUE'
    AND SDO_RELATE(v.geometry, t.tsa_geom, 'mask=ANYINTERACT') = 'TRUE'
),
final AS (
  SELECT
    v.feature_id,
    v.proj_age_1,
    CASE
      WHEN v.erase_geom IS NULL THEN v.vri_geom
      WHEN SDO_FILTER(v.vri_geom, v.erase_geom) <> 'TRUE' THEN v.vri_geom
      ELSE SDO_GEOM.SDO_DIFFERENCE(v.vri_geom, v.erase_geom, :tol)
    END AS final_geom
  FROM vri_in_tsa v
)
SELECT
  f.feature_id,
  f.proj_age_1,
  SDO_UTIL.TO_WKBGEOMETRY(f.final_geom) AS geom_wkb
FROM final f
WHERE f.final_geom IS NOT NULL