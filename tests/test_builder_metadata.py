import math
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from builder import builder_class
from builder.builder_class import (
    COUNTRY_FIELDS,
    builder,
    country_details,
    parse_meta_line,
)

META_TXT = """\
Boundary Representative of Year: 2020
ISO-3166-1 (Alpha-3): KEN
Boundary Type: ADM1
Canonical Boundary Type Name: Counties
Source 1: RCMRD GeoPortal
Source 2: Africa GeoPortal
Release Type: gbOpen
License: Public Domain
License Notes:
License Source: https://rcmrd.africageoportal.com/datasets/africageoportal
Link to Source Data: https://example.org:8443/data?t=12:30
Other Notes:
"""


class ParseMetaLineTests(unittest.TestCase):
    def test_splits_at_first_colon_only(self):
        self.assertEqual(
            parse_meta_line("License Source: https://example.org/a"),
            ("License Source", "https://example.org/a"),
        )
        self.assertEqual(
            parse_meta_line("Link to Source Data: https://example.org:8443/x?t=12:30"),
            ("Link to Source Data", "https://example.org:8443/x?t=12:30"),
        )

    def test_empty_value_is_allowed(self):
        self.assertEqual(parse_meta_line("License Notes:"), ("License Notes", ""))

    def test_line_without_colon_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_meta_line("no separator here")


class CountryDetailsTests(unittest.TestCase):
    def test_known_iso(self):
        self.assertEqual(
            country_details("KEN"),
            {
                "boundaryName": "Kenya",
                "Continent": "Africa",
                "UNSDG-region": "Sub-Saharan Africa",
                "UNSDG-subregion": "Eastern Africa",
                "worldBankIncomeGroup": "Lower-middle-income Countries",
            },
        )

    def test_unknown_iso_is_blank(self):
        details = country_details("ZZZ")
        self.assertEqual(set(details), {"boundaryName", *COUNTRY_FIELDS})
        self.assertTrue(all(value == "" for value in details.values()))

    def test_no_nan_values(self):
        for iso in builder_class._country_table().index:
            for value in country_details(iso).values():
                self.assertIsInstance(value, str, iso)


class TabularMetadataTests(unittest.TestCase):
    def build_metadata(self, meta_txt):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "gbOpen" / "KEN_ADM1.zip"
            source.parent.mkdir()
            with zipfile.ZipFile(source, "w") as zf:
                zf.writestr("meta.txt", meta_txt)

            b = builder("KEN", "ADM1", "gbOpen", ["KEN"], ["Public Domain"], tmpdir=tmp)
            b.sourcePath = source
            b.sourceFolder = source.parent

            def fake_hash():
                b.metaHash = "12345678"

            git_log = MagicMock(stdout="Thu Jan 19 07:31:04 2023 -0500\n")
            with (
                patch.object(b, "hashCalc", side_effect=fake_hash),
                patch.object(builder_class.subprocess, "run", return_value=git_log),
            ):
                message = b.checkBuildTabularMetaData()

        self.assertNotIn("ERROR", message.upper(), message)
        return b.metaDataLib

    def test_urls_keep_their_colons(self):
        meta = self.build_metadata(META_TXT)
        self.assertEqual(
            meta["licenseSource"],
            "https://rcmrd.africageoportal.com/datasets/africageoportal",
        )
        self.assertEqual(
            meta["boundarySourceURL"], "https://example.org:8443/data?t=12:30"
        )

    def test_legacy_api_fields_are_present(self):
        meta = self.build_metadata(META_TXT)
        self.assertEqual(meta["boundaryName"], "Kenya")
        self.assertEqual(meta["boundaryYearRepresented"], "2020")
        self.assertEqual(meta["boundaryYear"], "2020")
        self.assertEqual(meta["Continent"], "Africa")
        self.assertEqual(meta["UNSDG-region"], "Sub-Saharan Africa")
        self.assertEqual(meta["UNSDG-subregion"], "Eastern Africa")
        self.assertEqual(meta["worldBankIncomeGroup"], "Lower-middle-income Countries")
        for key, value in meta.items():
            self.assertFalse(isinstance(value, float) and math.isnan(value), key)


if __name__ == "__main__":
    unittest.main()
