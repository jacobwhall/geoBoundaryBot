"""Canonical filesystem paths for the builder pipeline.

All paths are derived from two environment variables:

    GB_REPO_DIR   Path to the geoBoundaries data repository
                  (contains sourceData/, releaseData/)

    GB_BOT_DIR    Path to the geoBoundaryBot repository
                  (contains dta/, builder/, utils/)
"""

import os
from pathlib import Path

# Root directories

REPO_DIR = Path(os.environ.get("GB_REPO_DIR", "."))
BOT_DIR = Path(os.environ.get("GB_BOT_DIR", "."))
TMP_DIR = Path(os.environ.get("GB_TMP_DIR", "/tmp/geoboundaries"))

# geoBoundaries repo paths

SOURCE_DATA = REPO_DIR / "sourceData"
RELEASE_DATA = REPO_DIR / "releaseData"

# geoBoundaryBot repo paths

DTA_DIR = BOT_DIR / "dta"
ISO_CSV = DTA_DIR / "iso_3166_1_alpha_3.csv"
LICENSES_CSV = DTA_DIR / "gbLicenses.csv"
LSIB_GEOJSON = DTA_DIR / "usDoSLSIB_Mar2020.geojson"
