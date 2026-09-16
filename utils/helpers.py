import json
import os
import shutil
import stat
import subprocess
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

BOT_ROOT = Path(__file__).resolve().parents[1]
VALIDATION_CHECKS = {"fileChecks", "metaChecks", "geometryDataChecks"}


def _is_within(path, root):
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _validation_results_root():
    raw = os.environ.get("RESULTS_DIR")
    if not raw:
        raise RuntimeError("RESULTS_DIR is required for pull-request validation")

    root = Path(raw)
    if not root.is_absolute():
        raise ValueError("RESULTS_DIR must be an absolute path")
    root = root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _check_output_dir(check):
    if check not in VALIDATION_CHECKS:
        return Path.home() / "tmp"

    check_dir = _validation_results_root() / check
    if check_dir.is_symlink():
        raise ValueError(f"Result directory must not be a symlink: {check_dir}")
    check_dir.mkdir(parents=True, exist_ok=True)
    return check_dir


def _parse_changed_files(raw):
    try:
        changed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("changes must be a valid JSON array") from exc

    if not isinstance(changed, list) or not all(
        isinstance(item, str) for item in changed
    ):
        raise ValueError("changes must be a JSON array of filenames")

    validated = []
    for item in changed:
        if not item or "\x00" in item:
            raise ValueError("changes contains an empty filename or NUL byte")
        path = PurePosixPath(item)
        if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
            raise ValueError(f"changes contains a non-relative filename: {item!r}")
        validated.append(item)
    return validated


def initiateWorkspace(check, build=None):
    ws = {}
    if build != None:
        ws["working"] = os.environ["GITHUB_WORKSPACE"]
        ws["logPath"] = str(Path.home() / "tmp" / f"{check}_buildStatus.csv")

        print("Python WD: " + ws["working"])
        print("Logging Path: " + str(ws["logPath"]))
        ws["zips"] = []

    else:
        if check not in VALIDATION_CHECKS:
            raise ValueError(f"Unknown validation check: {check}")

        working = Path(os.environ["GITHUB_WORKSPACE"])
        if not working.is_absolute() or not working.is_dir():
            raise ValueError("GITHUB_WORKSPACE must be an existing absolute directory")
        working = working.resolve()

        results_root = _validation_results_root()
        if _is_within(results_root, working) or _is_within(results_root, BOT_ROOT):
            raise ValueError(
                "RESULTS_DIR must be outside the PR checkout and installed action"
            )

        changed_files = _parse_changed_files(os.environ["changes"])
        check_dir = _check_output_dir(check)
        ws["working"] = str(working)
        ws["changedFiles"] = changed_files
        ws["logPath"] = str(check_dir / f"{check}.txt")
        ws["resultPath"] = str(check_dir / "RESULT.txt")
        ws["previewPath"] = str(check_dir / "preview.png")
        ws["zips"] = [name for name in changed_files if name.endswith(".zip")]

        print("Python WD: " + ws["working"])
        print("Python changedFiles: " + str(ws["changedFiles"]))
        print("Logging Path: " + str(ws["logPath"]))
        print("Changed Zips Detected: " + str(ws["zips"]))

    ws["zipFailures"] = 0
    ws["zipSuccess"] = 0
    ws["zipTotal"] = 0
    ws["checkType"] = check
    return ws


def logWrite(check, line):
    print(line)
    output_dir = _check_output_dir(check)
    with (output_dir / f"{check}.txt").open("a", encoding="utf-8") as f:
        f.write(line + "\n")


LFS_POINTER_PREFIX = b"version https://git-lfs.github.com/spec/v1"


def _failLFSRetrieval(relative, detail):
    message = (
        "I was not able to retrieve "
        + relative.as_posix()
        + " from Git LFS.  Submissions larger than 25mb are stored in Git LFS, and "
        "the data behind this one was not available to the validation runner, so I "
        "could not open it at all.  This is a problem on our side rather than "
        "something wrong with your boundaries - please let the geoBoundaries team "
        "know on this pull request so we can retrieve your submission manually."
    )
    gbEnvVars("RESULT", message, "w")
    raise RuntimeError(f"Git LFS retrieval failed for {relative.as_posix()}: {detail}")


def checkRetrieveLFSFiles(z, workingDir="./"):
    working = Path(workingDir).resolve()
    relative = PurePosixPath(z)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Invalid repository-relative LFS path: {z!r}")

    print("")
    print("--------------------------------")
    print("Retrieving submitted file from Git LFS when applicable: " + str(relative))
    try:
        subprocess.run(
            [
                "git",
                "-C",
                str(working),
                "lfs",
                "pull",
                f"--include={relative.as_posix()}",
                "--exclude=",
            ],
            check=True,
        )
    except (subprocess.CalledProcessError, OSError) as exc:
        _failLFSRetrieval(relative, exc)

    # A successful pull can still leave a pointer file behind when the object
    # itself is unreachable, so confirm we have the real submission.
    candidate = (working / relative).resolve()
    if _is_within(candidate, working) and candidate.is_file():
        with candidate.open("rb") as f:
            if f.read(len(LFS_POINTER_PREFIX)) == LFS_POINTER_PREFIX:
                _failLFSRetrieval(relative, "file is still a Git LFS pointer")


def submissionPath(workingDir, relative_name):
    working = Path(workingDir).resolve()
    candidate = (working / relative_name).resolve()
    if not _is_within(candidate, working) or not candidate.is_file():
        raise ValueError(
            f"Submission is not a regular file in the checkout: {relative_name!r}"
        )
    return candidate


def gbEnvVars(varName, content, mode):
    if varName != "RESULT":
        raise ValueError(f"Unsupported validation variable: {varName}")
    check = os.environ.get("CHECK_TYPE")
    if check not in VALIDATION_CHECKS:
        # Full builds retain their historical scratch-file behavior.
        result_path = Path.home() / "tmp" / f"{varName}.txt"
    else:
        result_path = _check_output_dir(check) / "RESULT.txt"

    if mode == "w":
        result_path.parent.mkdir(parents=True, exist_ok=True)
        with result_path.open("w", encoding="utf-8") as f:
            f.write(content)
        print("Set variable " + str(varName) + " to " + str(content))
    if mode == "r":
        with result_path.open("r", encoding="utf-8") as f:
            return f.read()
    if mode not in ("r", "w"):
        raise ValueError(f"Unsupported mode: {mode}")


def unzipGB(zipObj):
    runner_temp = os.environ.get("RUNNER_TEMP")
    temp_parent = (
        Path(runner_temp).resolve() if runner_temp else Path(tempfile.gettempdir())
    )
    working = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    if (
        not temp_parent.is_absolute()
        or _is_within(temp_parent, working)
        or _is_within(temp_parent, BOT_ROOT)
    ):
        raise ValueError(
            "ZIP extraction directory must be outside the PR checkout and installed action"
        )
    temp_parent.mkdir(parents=True, exist_ok=True)
    destination = Path(
        tempfile.mkdtemp(prefix="geoboundary-validation-", dir=temp_parent)
    )

    for member in zipObj.infolist():
        member_path = PurePosixPath(member.filename)
        if (
            member_path.is_absolute()
            or ".." in member_path.parts
            or "\\" in member.filename
            or stat.S_ISLNK(member.external_attr >> 16)
        ):
            raise zipfile.BadZipFile(f"Unsafe ZIP member path: {member.filename!r}")
        target = (destination / member_path.as_posix()).resolve()
        if not _is_within(target, destination):
            raise zipfile.BadZipFile(
                f"ZIP member escapes extraction directory: {member.filename!r}"
            )

    zipObj.extractall(destination)
    macos_metadata = destination / "__MACOSX"
    if macos_metadata.exists():
        shutil.rmtree(macos_metadata)
    return destination


def citationUse(releaseType):
    citUse = "====================================================\n"
    citUse = citUse + "Citation of the geoBoundaries Data Product\n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "www.geoboundaries.org \n"
    citUse = citUse + "geolab.wm.edu \n"
    citUse = citUse + "The geoBoundaries database is made available in a \n"
    citUse = citUse + "variety of software formats to support GIS software programs.\n"

    if releaseType == "gbOpen":
        citUse = citUse + "This file is a part of the geoBoundaries Open Database \n"
        citUse = citUse + "(gbOpen).  All boundaries in this database are open and \n"
        citUse = (
            citUse + "redistributable, and are released alongside extensive metadata \n"
        )
        citUse = citUse + "and licence information to help inform end users. \n"

    else:
        citUse = citUse + "This file is a part of a geoBoundaries Mixed Database. \n"
        citUse = citUse + "All boundaries in this database are \n"
        citUse = (
            citUse + "redistributable, and are released alongside extensive metadata \n"
        )
        citUse = citUse + "and licence information to help inform end users. \n"
        citUse = citUse + "Unlike data provided in the geoBoundaries Open Database, \n"
        citUse = (
            citUse
            + "information in this database may have restrictions on (for example) \n"
        )
        citUse = (
            citUse
            + "commercial use.  Users should carefully read each license to ensure they are \n"
        )
        citUse = (
            citUse
            + "not violating the terms of an individual layer for any non-private uses. \n"
        )

    citUse = citUse + "We update geoBoundaries on a yearly cycle, \n"
    citUse = citUse + "with new versions in or around August of each calendar \n"
    citUse = (
        citUse + "year; old versions remain accessible at www.geoboundaries.org. \n"
    )
    citUse = (
        citUse + "The only requirement to use this data is to, with any use, provide\n"
    )
    citUse = (
        citUse + "information on the authors (us), a link to geoboundaries.org or \n"
    )
    citUse = citUse + "our academic citation, and the version of geoBoundaries used. \n"
    citUse = citUse + "Example citations for GeoBoundaries are:  \n"
    citUse = citUse + " \n"
    citUse = citUse + "+++++ General Use Citation +++++\n"
    citUse = citUse + "Please include the term 'geoBoundaries' with a link to \n"
    citUse = citUse + "https://www.geoboundaries.org\n"
    citUse = citUse + " \n"
    citUse = citUse + "+++++ Academic Use Citation +++++++++++\n"
    citUse = (
        citUse
        + "Runfola D, Anderson A, Baier H, Crittenden M, Dowker E, Fuhrig S, et al. (2020) \n"
    )
    citUse = (
        citUse
        + "geoBoundaries: A global database of political administrative boundaries. \n"
    )
    citUse = (
        citUse
        + "PLoS ONE 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866. \n"
    )
    citUse = citUse + "\n"
    citUse = (
        citUse
        + "Users using individual boundary files from geoBoundaries should additionally\n"
    )
    citUse = (
        citUse
        + "ensure that they are citing the sources provided in the metadata for each file.\n"
    )
    citUse = citUse + " \n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "Column Definitions\n"
    citUse = citUse + "====================================================\n"
    citUse = (
        citUse
        + "boundaryID - A unique ID created for every boundary in the geoBoundaries database by concatenating ISO 3166-1 3 letter country code, boundary level, geoBoundaries version, and an incrementing ID.\n"
    )
    citUse = (
        citUse
        + "boundaryISO -  The ISO 3166-1 3-letter country codes for each boundary.\n"
    )
    citUse = (
        citUse + "boundaryYear - The year for which a boundary is representative.\n"
    )
    citUse = (
        citUse
        + "boundaryType - The type of boundary defined (i.e., ADM0 is equivalent to a country border; ADM1 a state.  Levels below ADM1 can vary in definition by country.)\n"
    )
    citUse = (
        citUse
        + "boundarySource-K - The name of the Kth source for the boundary definition used (with most boundaries having two identified sources).\n"
    )
    citUse = (
        citUse + "boundaryLicense - The specific license the data is released under.\n"
    )
    citUse = (
        citUse
        + "licenseDetail - Any details necessary for the interpretation or use of the license noted.\n"
    )
    citUse = (
        citUse
        + "licenseSource - A resolvable URL (checked at the time of data release) declaring the license under which a data product is made available.\n"
    )
    citUse = (
        citUse
        + "boundarySourceURL -  A resolvable URL (checked at the time of data release) from which source data was retrieved.\n"
    )
    citUse = (
        citUse
        + "boundaryUpdate - A date encoded following ISO 8601 (Year-Month-Date) describing the last date this boundary was updated, for use in programmatic updating based on new releases.\n"
    )
    citUse = (
        citUse + "downloadURL - A URL from which the geoBoundary can be downloaded.\n"
    )
    citUse = (
        citUse
        + "shapeID - The boundary ID, followed by the letter `B' and a unique integer for each shape which is a member of that boundary.\n"
    )
    citUse = (
        citUse
        + "shapeName - The identified name for a given shape.  'None' if not identified.\n"
    )
    citUse = (
        citUse
        + "shapeGroup - The country or similar organizational group that a shape belongs to, in ISO 3166-1 where relevant.\n"
    )
    citUse = citUse + "shapeType - The type of boundary represented by the shape.\n"
    citUse = (
        citUse
        + "shapeISO - ISO codes for individual administrative districts, where available.  Where possible, these conform to ISO 3166-2, but this is not guaranteed in all cases. 'None' if not identified.\n"
    )
    citUse = (
        citUse
        + "boundaryCanonical - Canonical name(s) for the administrative hierarchy represented.  Present where available."
    )
    citUse = citUse + " \n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "Reporting Issues or Errors\n"
    citUse = citUse + "====================================================\n"
    citUse = (
        citUse
        + "We track issues associated with the geoBoundaries dataset publically,\n"
    )
    citUse = (
        citUse
        + "and any individual can contribute comments through our github repository:\n"
    )
    citUse = citUse + "https://github.com/wmgeolab/geoBoundaries\n"
    citUse = citUse + " \n"
    citUse = citUse + " \n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "Disclaimer"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "With respect to the works on or made available\n"
    citUse = citUse + "through download from www.geoboundaries.org,\n"
    citUse = (
        citUse
        + "we make no representations or warranties—express, implied, or statutory—as\n"
    )
    citUse = (
        citUse
        + "to the validity, accuracy, completeness, or fitness for a particular purpose;\n"
    )
    citUse = (
        citUse
        + "nor represent that use of such works would not infringe privately owned rights;\n"
    )
    citUse = (
        citUse
        + "nor assume any liability resulting from use of such works; and shall in no way\n"
    )
    citUse = (
        citUse
        + "be liable for any costs, expenses, claims, or demands arising out of use of such works.\n"
    )
    citUse = citUse + "====================================================\n"
    citUse = citUse + " \n"
    citUse = citUse + " \n"
    citUse = (
        citUse
        + "Thank you for citing your use of geoBoundaries and reporting any issues you find -\n"
    )
    citUse = (
        citUse
        + "as a non-profit academic project, your citations are what keeps geoBoundaries alive.\n"
    )
    citUse = citUse + "-Dan Runfola (github.com/DanRunfola ; danr@wm.edu)"

    return citUse
