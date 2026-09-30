import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from utils import meta_check

META_TXT = """\
Boundary Representative of Year: 2020
ISO-3166-1 (Alpha-3): KEN
Boundary Type: ADM1
Source 1: RCMRD GeoPortal
Release Type: gbOpen
License: Public Domain
License Source: https://rcmrd.africageoportal.com/datasets/africageoportal
Link to Source Data: https://example.org:8443/data?t=12:30
"""


class MetaCheckParsingTests(unittest.TestCase):
    def test_logged_values_keep_their_colons(self):
        with tempfile.TemporaryDirectory() as working:
            relative = "sourceData/gbOpen/KEN_ADM1.zip"
            path = Path(working) / relative
            path.parent.mkdir(parents=True)
            with zipfile.ZipFile(path, "w") as zf:
                zf.writestr("meta.txt", META_TXT)

            ws = {
                "zips": [relative],
                "checkType": "metaChecks",
                "working": working,
                "zipTotal": 0,
                "zipSuccess": 0,
                "zipFailures": 0,
            }
            logged = []
            with (
                mock.patch.object(
                    meta_check.gbHelpers,
                    "logWrite",
                    side_effect=lambda _check, line: logged.append(line),
                ),
                mock.patch.object(meta_check.gbHelpers, "checkRetrieveLFSFiles"),
                mock.patch.object(meta_check.gbHelpers, "gbEnvVars"),
                mock.patch("builtins.print"),
            ):
                meta_check.metaCheck(ws)

        self.assertIn(
            "Data Source Found: https://example.org:8443/data?t=12:30", logged
        )
        self.assertTrue(
            any(
                "https://rcmrd.africageoportal.com/datasets/africageoportal" in line
                for line in logged
            ),
            logged,
        )
        self.assertFalse(any("https//" in line for line in logged), logged)


if __name__ == "__main__":
    unittest.main()
