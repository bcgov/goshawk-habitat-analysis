# 
# %% Import required Libraries / Modules
import goshawk_habitat.db.oracle as bcgw 
from pathlib import Path
from dotenv import load_dotenv
import tomllib
import json
import oracledb

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
with open(ROOT / "config.toml", "rb") as f:
    cfg = tomllib.load(f)


def main():
    conn = bcgw.connect()
    with bcgw.oracle_cursor(conn) as cur:
        cur.execute("SELECT * FROM v$version")
        for row in cur.fetchall():
            print(row[0])

    db_name, user, schema = bcgw.get_db_info(conn)

    print(f"Database: {db_name}")
    print(f"User:     {user}")
    print(f"Schema:   {schema}")

    # Test Function to verify connection to Database
    params = {
        "tsa_id": cfg["tsa"]["feature_id"],
        "min_age": cfg["vri_params"]["proj_age_1"],
        "min_height": cfg["vri_params"]["proj_height"],
        "min_crown_closure": cfg["vri_params"]["crown_closure"],
        "max_site_index": cfg["vri_params"]["site_index"],
    }

    cols, rows = bcgw.run_sql(conn, "veg_comp_by_tsa.sql", params=params)


    out_path = ROOT / "data" / f"veg_comp_{cfg['tsa']['feature_id']}.geojson"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Find geom column
    geom_idx = None
    for i, c in enumerate(cols):
        if c.lower() == "geom_geojson":
            geom_idx = i
            break
    if geom_idx is None:
        raise ValueError(f"Expected 'geom_geojson' in columns, got: {cols}")

    # Attribute columns (everything except geom_geojson)
    prop_cols = [c for c in cols if c.lower() != "geom_geojson"]

    features = []
    for r in rows:
        geom_val = r[geom_idx]
        if isinstance(geom_val, oracledb.LOB):
            geom_val = geom_val.read()
        if not geom_val:
            continue

        geometry = json.loads(geom_val)

        # Build properties without the geom column
        props = {}
        for c in prop_cols:
            v = r[cols.index(c)]
            # Handle LOBs just in case
            if isinstance(v, oracledb.LOB):
                v = v.read()
            # If anything weird slips through, stringify it
            try:
                json.dumps(v)
            except TypeError:
                v = str(v)
            props[c] = v

        features.append({"type": "Feature", "geometry": geometry, "properties": props})

    fc = {"type": "FeatureCollection", "features": features}

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False)

    print(f"Wrote {len(features):,} features to {out_path}")
# %%
if __name__ == "__main__":
    main()


# %%