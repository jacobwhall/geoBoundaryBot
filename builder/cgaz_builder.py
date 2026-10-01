"""CGAZ: Comprehensive Global Administrative Zones.

Stitches gbOpen ADM0/ADM1/ADM2 into gap-free global layers whose
international borders come from the US Department of State's LSIB
(dta/usDoSLSIB_Mar2020.geojson) rather than from geoBoundaries itself.

The work is split three ways so the parallel part runs on Dask alongside
the boundary builds:

  prepare_lsib        Dask task, once per run.  Downloads LSIB, folds
                      territories into their parent country, assigns ISO
                      codes and dissolves to one shape per country (and per
                      disputed area) in `cgaz_lsib`.
  build_cgaz_country  Dask task, one per gbOpen ISO.  Queued by the driver
                      once LSIB and that country's gbOpen ADM0-2 builds are
                      done; simplifies its ADM1/ADM2 and clips them to the
                      LSIB country outline into `cgaz_parts`.
  merge (__main__)    Single-pod Kubernetes Job.  Assembles each global layer
                      from PostGIS, closes small gaps, writes GeoJSON,
                      GeoPackage and zipped Shapefile, uploads them, and
                      records the upload manifest in `cgaz_outputs`.

Everything moves through the build's ephemeral PostGIS; the only bucket
traffic is the one LSIB download and the final upload.
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine, make_url, text

from builder.paths import ISO_CSV, TMP_DIR
from builder.s3 import s3_config_from_env, upload_to_s3

log = logging.getLogger(__name__)

LEVELS = ["ADM0", "ADM1", "ADM2"]

# Node heap for mapshaper, in mapshaper-xl's "<n>gb" syntax.  The global
# merges hold a whole layer in memory; per-country clips need far less.
MERGE_HEAP = os.environ.get("GB_CGAZ_MERGE_HEAP", "32gb")
COUNTRY_HEAP = os.environ.get("GB_CGAZ_COUNTRY_HEAP", "8gb")

# Share of removable vertices kept when simplifying each country's ADM1/ADM2
# before it's clipped to LSIB.  Same as the 2021 build.
SIMPLIFY_PERCENTAGE = "0.10"

# Slivers between countries (where gbOpen and LSIB disagree) up to this size
# are absorbed into a neighbouring shape during the merge.
GAP_FILL_AREA = "10000km2"

OUTPUT_DIR = TMP_DIR / "CGAZ"

# ---------------------------------------------------------------------------
# PostGIS schema
# ---------------------------------------------------------------------------

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS cgaz_lsib (
        id SERIAL PRIMARY KEY,
        iso TEXT,
        name TEXT,
        disputed BOOLEAN NOT NULL,
        geom geometry(Geometry, 4326)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_cgaz_lsib_iso ON cgaz_lsib (iso)",
    """
    CREATE TABLE IF NOT EXISTS cgaz_parts (
        id SERIAL PRIMARY KEY,
        iso TEXT NOT NULL,
        adm_level TEXT NOT NULL,
        shape_name TEXT,
        shape_id TEXT,
        shape_group TEXT,
        shape_type TEXT,
        geom geometry(Geometry, 4326)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_cgaz_parts_lookup ON cgaz_parts (adm_level, iso)",
    """
    CREATE TABLE IF NOT EXISTS cgaz_outputs (
        key TEXT PRIMARY KEY,
        size BIGINT NOT NULL
    )
    """,
]


def init_schema(conn):
    """Create the CGAZ tables.  `conn` is a SQLAlchemy connection in a transaction."""
    for statement in SCHEMA:
        conn.execute(text(statement))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _heap_mb(heap):
    """Convert mapshaper-xl's "<n>gb" heap syntax to megabytes."""
    return int(float(heap.lower().removesuffix("b").removesuffix("g")) * 1024)


def mapshaper(*args, heap=None):
    """Run mapshaper, raising if it fails.

    Calls plain `mapshaper` with the heap set through NODE_OPTIONS rather
    than going through `mapshaper-xl`, which always exits 0 (it never
    forwards its child's exit code) and isn't the real script in the image.
    """

    env = None
    if heap:
        env = {**os.environ, "NODE_OPTIONS": f"--max-old-space-size={_heap_mb(heap)}"}
    command = ["mapshaper", *map(str, args)]
    log.debug("Running %s", " ".join(command))
    r = subprocess.run(command, capture_output=True, text=True, env=env)
    if r.returncode != 0:
        raise RuntimeError(
            f"mapshaper failed (rc={r.returncode}): {r.stderr.strip()[-2000:]}"
        )
    return r


def _run(command):
    """Run a non-mapshaper command (ogr2ogr), raising if it fails."""
    log.debug("Running %s", " ".join(map(str, command)))
    r = subprocess.run(list(map(str, command)), capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(
            f"{command[0]} failed (rc={r.returncode}): {r.stderr.strip()[-2000:]}"
        )
    return r


def _to_parts_frame(gdf):
    """Normalise a frame read from GeoJSON to the PostGIS column layout."""
    if gdf.geometry.name != "geom":
        gdf = gdf.rename_geometry("geom")
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    return gdf


def _download(url, dest):
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)


# ---------------------------------------------------------------------------
# LSIB base layer (Dask task, once per run)
# ---------------------------------------------------------------------------

# LSIB names territories after their administering country.  CGAZ folds
# them into that country; the first marker found in the name wins.
TERRITORY_PARENTS = {
    "(US)": "United States",
    "(UK)": "United Kingdom",
    "(Aus)": "Australia",
    "Greenland (Den)": "Greenland",
    "(Den)": "Denmark",
    "(Fr)": "France",
    "(Ch)": "China",
    "(Nor)": "Norway",
    "(NZ)": "New Zealand",
    "Netherlands [Caribbean]": "Netherlands",
    "(Neth)": "Netherlands",
    "Portugal [": "Portugal",
    "Spain [": "Spain",
}

# LSIB country names that don't match a name in dta/iso_3166_1_alpha_3.csv.
LSIB_ISO_OVERRIDES = {
    "Antigua & Barbuda": "ATG",
    "Bahamas, The": "BHS",
    "Bosnia & Herzegovina": "BIH",
    "Congo, Dem Rep of the": "COD",
    "Congo, Rep of the": "COG",
    "Cabo Verde": "CPV",
    "Cote d'Ivoire": "CIV",
    "Central African Rep": "CAF",
    "Czechia": "CZE",
    "Gambia, The": "GMB",
    "Iran": "IRN",
    "Korea, North": "PRK",
    "Korea, South": "KOR",
    "Laos": "LAO",
    "Macedonia": "MKD",
    "Marshall Is": "MHL",
    "Micronesia, Fed States of": "FSM",
    "Moldova": "MDA",
    "Sao Tome & Principe": "STP",
    "Solomon Is": "SLB",
    "St Kitts & Nevis": "KNA",
    "St Lucia": "LCA",
    "St Vincent & the Grenadines": "VCT",
    "Syria": "SYR",
    "Tanzania": "TZA",
    "Vatican City": "VAT",
    "United States": "USA",
    "Antarctica": "ATA",
    "Bolivia": "BOL",
    "Brunei": "BRN",
    "Russia": "RUS",
    "Trinidad & Tobago": "TTO",
    "Swaziland": "SWZ",
    "Venezuela": "VEN",
    "Vietnam": "VNM",
    "Burma": "MMR",
}


def parent_country(name):
    """Map an LSIB territory name to the country CGAZ folds it into."""
    for marker, parent in TERRITORY_PARENTS.items():
        if marker in name:
            return parent
    return name


def iso_tables():
    """Return ({CSV name: ISO code}, {ISO code: CSV name}).

    Names that appear more than once in the CSV are left out of the first
    table, so they fall through to LSIB_ISO_OVERRIDES.
    """
    iso_df = pd.read_csv(ISO_CSV)
    counts = iso_df["Name"].value_counts()
    unique = iso_df[iso_df["Name"].map(counts) == 1]
    codes = dict(zip(unique["Name"], unique["Alpha-3code"]))
    names = dict(zip(iso_df["Alpha-3code"], iso_df["Name"]))
    return codes, names


def split_lsib(lsib, codes):
    """Split raw LSIB features into countries and disputed areas.

    Both frames come back with `name`, `iso` and `key` (what to dissolve
    on) columns.  Territories take their parent country's name.  Disputed
    areas keep their own name minus " (disp)", and get an ISO code only
    when that name is itself a country (e.g. Western Sahara).
    """

    is_disputed = lsib["COUNTRY_NA"].str.contains("(disp)", regex=False)

    countries = lsib[~is_disputed].copy()
    countries["name"] = countries["COUNTRY_NA"].map(parent_country)
    disputed = lsib[is_disputed].copy()
    disputed["name"] = disputed["COUNTRY_NA"].str.replace(" (disp)", "", regex=False)

    for gdf in (countries, disputed):
        gdf["iso"] = gdf["name"].map(
            lambda n: codes.get(n) or LSIB_ISO_OVERRIDES.get(n)
        )

    unmatched = sorted(countries.loc[countries["iso"].isna(), "name"].unique())
    if unmatched:
        # Kept, so they still cover their land in every layer, just without
        # an ISO code.  Add them to LSIB_ISO_OVERRIDES to fix.
        log.warning("LSIB countries with no ISO code: %s", ", ".join(unmatched))

    # Countries without a code dissolve by name so they don't merge together.
    countries["key"] = countries["iso"].fillna(countries["name"])
    disputed["key"] = disputed["name"]

    columns = ["key", "name", "iso", "geometry"]
    return countries[columns], disputed[columns]


def prepare_lsib(lsib_url, db_url):
    """Dask task: load the LSIB base layer into `cgaz_lsib`.

    Returns a result dict with "status" ("ok" or "error").  No
    build_cgaz_country task runs until this one has succeeded.
    """

    t0 = time.perf_counter()
    result = {"task": "lsib"}
    tmpdir = Path(tempfile.mkdtemp(prefix="gb-cgaz-lsib-"))
    engine = create_engine(db_url)
    try:
        raw = tmpdir / "lsib.geojson"
        _download(lsib_url, raw)
        codes, names = iso_tables()
        countries, disputed = split_lsib(gpd.read_file(raw), codes)
        raw.unlink()

        frames = []
        for kind, gdf in (("countries", countries), ("disputed", disputed)):
            src = tmpdir / f"{kind}.geojson"
            out = tmpdir / f"{kind}-dissolved.geojson"
            gdf.to_file(src, driver="GeoJSON")
            mapshaper(
                src,
                "-dissolve",
                "fields=key",
                "copy-fields=name,iso",
                "multipart",
                "-o",
                "format=geojson",
                out,
                heap=COUNTRY_HEAP,
            )
            dissolved = gpd.read_file(out)
            dissolved["disputed"] = kind == "disputed"
            frames.append(dissolved)

        lsib = _to_parts_frame(gpd.GeoDataFrame(pd.concat(frames, ignore_index=True)))
        # Countries are named as in the ISO CSV, like the rest of geoBoundaries.
        is_country = ~lsib["disputed"] & lsib["iso"].notna()
        lsib.loc[is_country, "name"] = (
            lsib.loc[is_country, "iso"].map(names).fillna(lsib.loc[is_country, "name"])
        )
        lsib = lsib[["iso", "name", "disputed", "geom"]]

        # Replace rather than append, so a retried task doesn't double up.
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM cgaz_lsib"))
            lsib.to_postgis("cgaz_lsib", conn, if_exists="append", index=False)

        result.update(
            status="ok",
            countries=int((~lsib["disputed"]).sum()),
            disputed=int(lsib["disputed"].sum()),
        )
        log.info(
            "Loaded LSIB: %d countries, %d disputed areas",
            result["countries"],
            result["disputed"],
        )
    except Exception as e:
        log.exception("LSIB preparation failed")
        result.update(status="error", error=str(e))
    finally:
        engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)
        result["timings"] = {"total": round(time.perf_counter() - t0, 1)}
    return result


# ---------------------------------------------------------------------------
# Per-country parts (Dask task, one per gbOpen ISO)
# ---------------------------------------------------------------------------


def cgaz_sources(ok_levels):
    """Pick the gbOpen level each CGAZ level of a country is cut from.

    Missing levels fall back up the hierarchy, ADM2 -> ADM1 -> ADM0, so the
    world has no holes.  None means the country has no usable gbOpen
    boundary, and the merge uses its LSIB outline instead.
    """
    adm1 = next((level for level in ("ADM1", "ADM0") if level in ok_levels), None)
    adm2 = "ADM2" if "ADM2" in ok_levels else adm1
    return {"ADM1": adm1, "ADM2": adm2}


def _clip_boundary(engine, iso, adm, outline_path, workdir):
    """Simplify one gbOpen boundary and clip it to the LSIB country outline."""

    gdf = gpd.read_postgis(
        text(
            "SELECT shape_name, shape_id, shape_group, shape_type, geom "
            "FROM boundaries "
            "WHERE product = 'gbOpen' AND iso = :iso AND adm_level = :adm"
        ),
        engine,
        geom_col="geom",
        params={"iso": iso, "adm": adm},
    )
    if gdf.empty:
        return gdf

    src = workdir / f"{adm}.geojson"
    out = workdir / f"{adm}-clipped.geojson"
    gdf.to_file(src, driver="GeoJSON")
    mapshaper(
        src,
        "-simplify",
        "keep-shapes",
        f"percentage={SIMPLIFY_PERCENTAGE}",
        "-clip",
        outline_path,
        "-o",
        "format=geojson",
        out,
        heap=COUNTRY_HEAP,
    )
    return _to_parts_frame(gpd.read_file(out))


def build_cgaz_country(iso, db_url, builds):
    """Dask task: write one country's clipped ADM1/ADM2 to `cgaz_parts`.

    The driver only runs this once LSIB is loaded and this country's
    gbOpen ADM0-2 builds have finished.  `builds` holds their
    {"adm", "status"}; only levels that built successfully are used.
    """

    t0 = time.perf_counter()
    result = {"task": "country", "iso": iso}
    sources = cgaz_sources({b["adm"] for b in builds if b.get("status") == "ok"})
    result["sources"] = sources
    tmpdir = Path(tempfile.mkdtemp(prefix=f"gb-cgaz-{iso}-"))
    engine = create_engine(db_url)
    try:
        outline = gpd.read_postgis(
            text("SELECT geom FROM cgaz_lsib WHERE iso = :iso AND NOT disputed"),
            engine,
            geom_col="geom",
            params={"iso": iso},
        )
        if outline.empty:
            # CGAZ is laid out on LSIB countries, so there's nowhere to put it.
            result.update(status="skipped", reason="not an LSIB country")
            return result
        outline_path = tmpdir / "outline.geojson"
        outline.to_file(outline_path, driver="GeoJSON")

        clipped = {}
        parts = []
        empty = []
        for level, source in sources.items():
            if source is None:
                continue
            if source not in clipped:
                clipped[source] = _clip_boundary(
                    engine, iso, source, outline_path, tmpdir
                )
            if clipped[source].empty:
                empty.append(level)
                continue
            part = clipped[source].copy()
            part["iso"] = iso
            part["adm_level"] = level
            parts.append(part)

        # Replace rather than append, so a retried task doesn't double up.
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM cgaz_parts WHERE iso = :iso"), {"iso": iso})
            for part in parts:
                part.to_postgis("cgaz_parts", conn, if_exists="append", index=False)

        if empty:
            # The merge falls back to the LSIB outline for these levels.
            result["empty_levels"] = empty
            log.warning(
                "CGAZ/%s: %s came out empty after clipping to LSIB",
                iso,
                ", ".join(empty),
            )
        result["status"] = "ok"
    except Exception as e:
        log.exception("CGAZ/%s failed", iso)
        result.update(status="error", error=str(e))
    finally:
        engine.dispose()
        shutil.rmtree(tmpdir, ignore_errors=True)
        result["timings"] = {"total": round(time.perf_counter() - t0, 1)}
    return result


# ---------------------------------------------------------------------------
# Global merge (Kubernetes Job)
# ---------------------------------------------------------------------------


def level_sql(level):
    """SQL for every shape in one global CGAZ layer.

    That's the country parts cut for this level, then the LSIB outline of
    every country without parts (no gbOpen data, a failed build, or ADM0),
    then the disputed areas.
    """
    if level not in LEVELS:
        raise ValueError(f"Unknown CGAZ level {level!r}")
    return f"""
        SELECT shape_name AS "shapeName", shape_id AS "shapeID",
               shape_group AS "shapeGroup", shape_type AS "shapeType", geom
          FROM cgaz_parts
         WHERE adm_level = '{level}'
        UNION ALL
        SELECT name, iso, iso, 'ADM0', geom
          FROM cgaz_lsib l
         WHERE NOT disputed
           AND NOT EXISTS (
               SELECT 1 FROM cgaz_parts p
                WHERE p.adm_level = '{level}' AND p.iso = l.iso
           )
        UNION ALL
        SELECT name, NULL, iso, 'Disputed', geom
          FROM cgaz_lsib
         WHERE disputed
    """


def pg_conninfo(db_url):
    """Turn a SQLAlchemy URL into the libpq conninfo string GDAL expects."""
    url = make_url(db_url)
    parts = {
        "dbname": url.database,
        "host": url.host,
        "port": url.port,
        "user": url.username,
        "password": url.password,
    }
    return "PG:" + " ".join(f"{k}='{v}'" for k, v in parts.items() if v is not None)


def merge_level(db_url, level, workdir, out_dir):
    """Build the GeoJSON, GeoPackage and zipped Shapefile for one level."""

    name = f"geoBoundariesCGAZ_{level}"
    raw = workdir / f"{name}-raw.geojson"
    geojson = out_dir / f"{name}.geojson"

    # ogr2ogr streams the layer out of PostGIS instead of holding it in Python.
    log.info("%s: exporting from PostGIS", level)
    _run(
        ["ogr2ogr", "-f", "GeoJSON", raw, pg_conninfo(db_url), "-sql", level_sql(level)]
    )

    log.info("%s: merging with mapshaper", level)
    mapshaper(
        raw,
        "-clean",
        f"gap-fill-area={GAP_FILL_AREA}",
        "-o",
        "format=geojson",
        geojson,
        heap=MERGE_HEAP,
    )
    raw.unlink()

    _run(["ogr2ogr", "-f", "GPKG", out_dir / f"{name}.gpkg", geojson])

    shp_dir = workdir / f"{name}-shp"
    shp_dir.mkdir()
    _run(
        [
            "ogr2ogr",
            "-f",
            "ESRI Shapefile",
            "-lco",
            "ENCODING=UTF-8",
            shp_dir / f"{name}.shp",
            geojson,
        ]
    )
    with zipfile.ZipFile(out_dir / f"{name}.zip", "w", zipfile.ZIP_DEFLATED) as zf:
        for part in sorted(shp_dir.iterdir()):
            zf.write(part, part.name)
    shutil.rmtree(shp_dir)
    log.info("%s: done", level)


def merge(db_url, s3_config):
    """Build every global layer and upload it.  Returns the upload manifest."""

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            countries = conn.execute(
                text("SELECT count(*) FROM cgaz_lsib WHERE NOT disputed")
            ).scalar()
            parts = dict(
                conn.execute(
                    text(
                        "SELECT adm_level, count(DISTINCT iso) "
                        "FROM cgaz_parts GROUP BY adm_level"
                    )
                ).all()
            )
        if not countries:
            raise RuntimeError("cgaz_lsib is empty; the LSIB task didn't load it")
        for level in LEVELS[1:]:
            log.info(
                "%s: %d of %d countries have gbOpen parts; the rest use LSIB outlines",
                level,
                parts.get(level, 0),
                countries,
            )

        # Start clean: everything in OUTPUT_DIR gets uploaded.
        shutil.rmtree(OUTPUT_DIR, ignore_errors=True)
        out_dir = OUTPUT_DIR / "out"
        workdir = OUTPUT_DIR / "work"
        out_dir.mkdir(parents=True)
        workdir.mkdir()
        for level in LEVELS:
            merge_level(db_url, level, workdir, out_dir)

        if s3_config is None:
            log.warning("S3 not configured; CGAZ outputs left in %s", out_dir)
            return []

        manifest = upload_to_s3(out_dir, "CGAZ", s3_config)
        with engine.begin() as conn:
            conn.execute(text("DELETE FROM cgaz_outputs"))
            conn.execute(
                text("INSERT INTO cgaz_outputs (key, size) VALUES (:key, :size)"),
                manifest,
            )
        return manifest
    finally:
        engine.dispose()


def main():
    parser = argparse.ArgumentParser(
        description="Merge the per-country CGAZ parts in PostGIS into global layers"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity"
    )
    args = parser.parse_args()
    level = [logging.WARNING, logging.INFO, logging.DEBUG][min(args.verbose, 2)]
    logging.basicConfig(
        level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    version = os.environ.get("GB_RELEASE_VERSION", "nightly")
    try:
        merge(os.environ["DATABASE_URL"], s3_config_from_env(version))
    except Exception:
        log.exception("CGAZ merge failed")
        sys.exit(1)
    log.info("CGAZ merge complete")


if __name__ == "__main__":
    main()
