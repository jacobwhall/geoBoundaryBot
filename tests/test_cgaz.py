import os
import unittest
from unittest.mock import MagicMock, patch

import geopandas as gpd
from shapely.geometry import box

from builder import cgaz_builder, run


def rows(df, columns):
    """DataFrame rows as lists, with missing values as None."""
    df = df[columns].astype(object)
    return df.where(df.notna(), None).values.tolist()


class LsibTests(unittest.TestCase):
    def test_territories_fold_into_parent_country(self):
        self.assertEqual(cgaz_builder.parent_country("Guam (US)"), "United States")
        self.assertEqual(cgaz_builder.parent_country("Greenland (Den)"), "Greenland")
        self.assertEqual(cgaz_builder.parent_country("Faroe Is (Den)"), "Denmark")
        self.assertEqual(cgaz_builder.parent_country("Kenya"), "Kenya")

    def test_split_lsib(self):
        lsib = gpd.GeoDataFrame(
            {
                "COUNTRY_NA": [
                    "Kenya",
                    "Burma",
                    "Guam (US)",
                    "Nowhere Land",
                    "Western Sahara (disp)",
                    "Some Area (disp)",
                ]
            },
            geometry=[box(i, 0, i + 1, 1) for i in range(6)],
            crs="EPSG:4326",
        )
        codes = {"Kenya": "KEN", "Western Sahara": "ESH"}

        with self.assertLogs(cgaz_builder.log, "WARNING") as logs:
            countries, disputed = cgaz_builder.split_lsib(lsib, codes)

        self.assertEqual(
            rows(countries, ["name", "iso", "key"]),
            [
                ["Kenya", "KEN", "KEN"],
                ["Burma", "MMR", "MMR"],
                ["United States", "USA", "USA"],
                # Kept so its land is still covered, dissolved on its own name.
                ["Nowhere Land", None, "Nowhere Land"],
            ],
        )
        self.assertIn("Nowhere Land", logs.output[0])
        self.assertEqual(
            rows(disputed, ["name", "iso", "key"]),
            [
                ["Western Sahara", "ESH", "Western Sahara"],
                ["Some Area", None, "Some Area"],
            ],
        )

    def test_iso_tables_drop_ambiguous_names(self):
        codes, names = cgaz_builder.iso_tables()
        self.assertEqual(codes["Kenya"], "KEN")
        self.assertEqual(names["KEN"], "Kenya")


class SourceTests(unittest.TestCase):
    def test_levels_fall_back_up_the_hierarchy(self):
        sources = cgaz_builder.cgaz_sources
        self.assertEqual(
            sources({"ADM0", "ADM1", "ADM2"}), {"ADM1": "ADM1", "ADM2": "ADM2"}
        )
        self.assertEqual(sources({"ADM0", "ADM1"}), {"ADM1": "ADM1", "ADM2": "ADM1"})
        self.assertEqual(sources({"ADM0", "ADM2"}), {"ADM1": "ADM0", "ADM2": "ADM2"})
        self.assertEqual(sources({"ADM0"}), {"ADM1": "ADM0", "ADM2": "ADM0"})
        self.assertEqual(sources(set()), {"ADM1": None, "ADM2": None})

    def test_country_task_waits_out_a_failed_lsib(self):
        result = cgaz_builder.build_cgaz_country(
            "KEN", "postgresql://unused", {"status": "error"}
        )
        self.assertEqual(result["status"], "error")
        self.assertIn("LSIB", result["error"])


class MergeTests(unittest.TestCase):
    def test_level_sql_only_takes_known_levels(self):
        self.assertIn("adm_level = 'ADM2'", cgaz_builder.level_sql("ADM2"))
        with self.assertRaises(ValueError):
            cgaz_builder.level_sql("ADM2'; DROP TABLE cgaz_parts; --")

    def test_pg_conninfo(self):
        self.assertEqual(
            cgaz_builder.pg_conninfo(
                "postgresql://gb:builddb@db-host:5432/geoboundaries"
            ),
            "PG:dbname='geoboundaries' host='db-host' port='5432' "
            "user='gb' password='builddb'",
        )

    def test_heap_syntax(self):
        self.assertEqual(cgaz_builder._heap_mb("32gb"), 32768)
        self.assertEqual(cgaz_builder._heap_mb("8"), 8192)

    @patch("builder.cgaz_builder.subprocess.run")
    def test_mapshaper_raises_on_failure(self, run_):
        run_.return_value = MagicMock(returncode=1, stderr="Error: [i] File not found")
        with self.assertRaisesRegex(RuntimeError, "File not found"):
            cgaz_builder.mapshaper("in.geojson", "-o", "out.geojson", heap="2gb")
        command = run_.call_args.args[0]
        self.assertEqual(command[0], "mapshaper")
        self.assertEqual(
            run_.call_args.kwargs["env"]["NODE_OPTIONS"], "--max-old-space-size=2048"
        )


class FakeFuture:
    def __init__(self, key, status="finished"):
        self.key = key
        self.status = status

    def __repr__(self):
        return f"<FakeFuture {self.key}>"


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.client = MagicMock()
        self.client.submit.side_effect = lambda fn, *args, key, priority, **kw: (
            FakeFuture(key)
        )

    def test_country_tasks_wait_on_their_gbopen_admin_levels(self):
        builds = {
            FakeFuture("ken0"): ("gbOpen", "KEN", "ADM0", 10),
            FakeFuture("ken1"): ("gbOpen", "KEN", "ADM1", 20),
            FakeFuture("ken3"): ("gbOpen", "KEN", "ADM3", 900),
            FakeFuture("kenh"): ("gbHumanitarian", "KEN", "ADM1", 50),
            FakeFuture("uga2"): ("gbOpen", "UGA", "ADM2", 5),
        }

        tasks = run.submit_cgaz_tasks(
            self.client, builds, "postgresql://db", "https://lsib", "v7"
        )

        self.assertEqual(sorted(tasks.values()), ["CGAZ/KEN", "CGAZ/LSIB", "CGAZ/UGA"])
        calls = {c.kwargs["key"]: c for c in self.client.submit.call_args_list}
        lsib = calls["v7-cgaz-lsib"]
        self.assertIs(lsib.args[0], cgaz_builder.prepare_lsib)
        # Ahead of the biggest build.
        self.assertEqual(lsib.kwargs["priority"], 901)

        ken = calls["v7-cgaz-KEN"]
        self.assertIs(ken.args[0], cgaz_builder.build_cgaz_country)
        lsib_future = next(f for f, tag in tasks.items() if tag == "CGAZ/LSIB")
        self.assertIs(ken.args[3], lsib_future)
        self.assertEqual([f.key for f in ken.args[4:]], ["ken0", "ken1"])
        self.assertEqual(ken.kwargs["priority"], 30)

    @patch("builder.run.discover_boundaries")
    @patch("builder.run.as_completed")
    @patch("builder.run.Client")
    def test_strict_failure_cancels_outstanding_cgaz(
        self, client_cls, as_completed, discover
    ):
        client = client_cls.return_value
        futures = {}

        def submit(fn, *args, key, priority, **kwargs):
            futures[key] = FakeFuture(key)
            return futures[key]

        client.submit.side_effect = submit
        discover.return_value = [
            ("gbOpen", "KEN", "ADM1", 10),
            ("gbOpen", "UGA", "ADM1", 20),
        ]

        def completed(fs, with_results, raise_errors):
            self.assertFalse(raise_errors)
            ok = {"product": "gbOpen", "iso": "KEN", "adm": "ADM1", "status": "ok"}
            lsib = {"status": "ok"}
            futures["v7-gbOpen-UGA-ADM1"].status = "error"
            yield futures["v7-cgaz-lsib"], lsib
            yield futures["v7-gbOpen-KEN-ADM1"], ok
            yield futures["v7-gbOpen-UGA-ADM1"], (MemoryError, MemoryError(), None)
            self.fail("kept waiting on CGAZ after a strict failure")

        as_completed.side_effect = completed

        successes, failures, cgaz = run.run_boundary_builds(
            "tcp://scheduler",
            "postgresql://db",
            "https://lsib",
            version="v7",
            strict=True,
        )

        self.assertEqual(len(successes), 1)
        self.assertEqual(failures[0]["iso"], "UGA")
        self.assertEqual(failures[0]["failed_stage"], "dask")
        self.assertIn("MemoryError", failures[0]["error"])
        self.assertEqual(cgaz, [{"status": "ok", "tag": "CGAZ/LSIB"}])
        cancelled = {f.key for f in client.cancel.call_args.args[0]}
        self.assertEqual(cancelled, {"v7-cgaz-lsib", "v7-cgaz-KEN", "v7-cgaz-UGA"})


class PipelineCheckTests(unittest.TestCase):
    LSIB_OK = {"tag": "CGAZ/LSIB", "status": "ok"}
    KEN_FAILED = {"tag": "CGAZ/KEN", "status": "error", "error": "boom"}

    def test_lsib_failure_is_always_fatal(self):
        with self.assertLogs(run.log, "ERROR"):
            self.assertFalse(run.check_cgaz_results([], strict=False))
            self.assertFalse(
                run.check_cgaz_results(
                    [{"tag": "CGAZ/LSIB", "status": "error"}], strict=False
                )
            )

    def test_country_failure_is_fatal_only_in_strict_mode(self):
        results = [self.LSIB_OK, self.KEN_FAILED]
        with self.assertLogs(run.log, "ERROR"):
            self.assertTrue(run.check_cgaz_results(results, strict=False))
            self.assertFalse(run.check_cgaz_results(results, strict=True))
        self.assertTrue(run.check_cgaz_results([self.LSIB_OK], strict=True))

    @patch("builder.run._s3_client")
    def test_verification_covers_cgaz(self, client_factory):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "v7/gbOpen/KEN/ADM1/file.json", "Size": 10},
                    {"Key": "v7/CGAZ/geoBoundariesCGAZ_ADM0.geojson", "Size": 99},
                ]
            }
        ]
        client_factory.return_value.get_paginator.return_value = paginator
        config = {"bucket": "geoboundaries", "prefix": "v7"}
        successes = [
            {
                "product": "gbOpen",
                "iso": "KEN",
                "adm": "ADM1",
                "uploaded": [{"key": "v7/gbOpen/KEN/ADM1/file.json", "size": 10}],
            }
        ]
        cgaz = [{"key": "v7/CGAZ/geoBoundariesCGAZ_ADM0.geojson", "size": 99}]

        run.verify_release_objects(successes, config, [], cgaz_objects=cgaz)
        for bad in ([], [{"key": "v7/CGAZ/geoBoundariesCGAZ_ADM1.zip", "size": 1}]):
            with self.assertRaisesRegex(RuntimeError, "missing or mismatched"):
                run.verify_release_objects(successes, config, [], cgaz_objects=bad)

    @patch.dict(os.environ, {"GB_S3_CREDENTIALS_SECRET": "gb-s3-credentials"})
    def test_merge_job_gets_credentials_by_reference(self):
        config = {
            "endpoint": "https://r2.example.test",
            "bucket": "geoboundaries",
            "prefix": "v7",
            "access_key_id": "AKIA-secret",
            "secret_access_key": "very-secret",
        }
        env = run._cgaz_job_env("postgresql://db", "v7", config)

        self.assertNotIn("very-secret", repr(env))
        self.assertNotIn("AKIA-secret", repr(env))
        by_name = {e["name"]: e for e in env}
        self.assertEqual(by_name["GB_RELEASE_VERSION"]["value"], "v7")
        self.assertEqual(
            by_name["AWS_SECRET_ACCESS_KEY"]["valueFrom"]["secretKeyRef"],
            {"name": "gb-s3-credentials", "key": "secret-access-key"},
        )
        self.assertEqual(
            [e["name"] for e in run._cgaz_job_env("postgresql://db", "v7", None)],
            ["DATABASE_URL", "GB_RELEASE_VERSION"],
        )


if __name__ == "__main__":
    unittest.main()
