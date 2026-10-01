import os
import time
import unittest
from unittest.mock import MagicMock, patch

import geopandas as gpd
from dask.distributed import LocalCluster
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


STARTED = []
SLEEP = {"CAN": 0.5, "USA": 0.4}


def fake_build(product, iso, adm, db_url, s3_config=None):
    STARTED.append(f"{product}/{iso}_{adm}")
    if iso == "BOOM":
        raise MemoryError("worker ran out of memory")
    time.sleep(SLEEP.get(iso, 0.05))
    status = "error" if iso == "BAD" else "ok"
    return {"product": product, "iso": iso, "adm": adm, "status": status}


def fake_lsib(lsib_url, db_url):
    STARTED.append("CGAZ/LSIB")
    return {"status": "ok"}


def fake_country(iso, db_url, builds):
    STARTED.append(f"CGAZ/{iso}")
    time.sleep(0.3)
    return {"status": "ok", "iso": iso, "builds": builds}


class BuildQueueTests(unittest.TestCase):
    BOUNDARIES = [
        ("gbOpen", "KEN", "ADM1", 20),
        ("gbOpen", "CAN", "ADM2", 900),
        ("gbOpen", "KEN", "ADM0", 10),
        ("gbHumanitarian", "KEN", "ADM1", 50),
        ("gbOpen", "KEN", "ADM3", 5),
    ]

    def setUp(self):
        self.queue = run.BuildQueue(self.BOUNDARIES, "postgresql://db", "https://lsib")

    def drain(self):
        tasks = []
        while self.queue:
            tasks.append(self.queue.pop())
        return tasks

    def test_lsib_then_builds_biggest_first(self):
        self.assertEqual(
            [t["tag"] for t in self.drain()],
            [
                "CGAZ/LSIB",
                "gbOpen/CAN_ADM2",
                "gbHumanitarian/KEN_ADM1",
                "gbOpen/KEN_ADM1",
                "gbOpen/KEN_ADM0",
                "gbOpen/KEN_ADM3",
            ],
        )
        self.assertEqual(self.queue.cgaz_countries, 2)

    def test_country_waits_for_lsib_and_its_gbopen_levels(self):
        tasks = {t["tag"]: t for t in self.drain()}
        done = self.queue.finished

        done(tasks["gbOpen/KEN_ADM1"], {"status": "ok"})
        done(tasks["gbOpen/KEN_ADM3"], {"status": "ok"})
        done(tasks["gbHumanitarian/KEN_ADM1"], {"status": "ok"})
        done(tasks["CGAZ/LSIB"], {"status": "ok"})
        self.assertEqual(len(self.queue), 0)  # still waiting on KEN ADM0

        done(tasks["gbOpen/KEN_ADM0"], {"status": "error"})
        cgaz = self.queue.pop()
        self.assertEqual(cgaz["tag"], "CGAZ/KEN")
        self.assertIs(cgaz["fn"], cgaz_builder.build_cgaz_country)
        self.assertEqual(
            cgaz["args"],
            (
                "KEN",
                "postgresql://db",
                [{"adm": "ADM1", "status": "ok"}, {"adm": "ADM0", "status": "error"}],
            ),
        )

        # A country whose builds finish before LSIB is queued with LSIB.
        queue = run.BuildQueue(self.BOUNDARIES, "postgresql://db", "https://lsib")
        tasks = {
            t["tag"]: t for t in iter(lambda: queue.pop() if queue else None, None)
        }
        queue.finished(tasks["gbOpen/CAN_ADM2"], {"status": "ok"})
        self.assertEqual(len(queue), 0)
        queue.finished(tasks["CGAZ/LSIB"], {"status": "ok"})
        self.assertEqual(queue.pop()["tag"], "CGAZ/CAN")

    def test_cgaz_waits_behind_every_queued_build(self):
        lsib = self.queue.pop()
        can = self.queue.pop()
        self.queue.finished(lsib, {"status": "ok"})
        self.queue.finished(can, {"status": "ok"})
        # CGAZ/CAN is ready, but the remaining builds still go first.
        self.assertEqual(
            [t["tag"] for t in self.drain()],
            [
                "gbHumanitarian/KEN_ADM1",
                "gbOpen/KEN_ADM1",
                "gbOpen/KEN_ADM0",
                "gbOpen/KEN_ADM3",
                "CGAZ/CAN",
            ],
        )

    def test_failed_lsib_queues_no_countries(self):
        tasks = {t["tag"]: t for t in self.drain()}
        self.queue.finished(tasks["CGAZ/LSIB"], {"status": "error"})
        for tag, task in tasks.items():
            if tag != "CGAZ/LSIB":
                self.queue.finished(task, {"status": "ok"})
        self.assertEqual(len(self.queue), 0)


@patch("builder.cgaz_builder.build_cgaz_country", fake_country)
@patch("builder.cgaz_builder.prepare_lsib", fake_lsib)
@patch("builder.run.build_boundary", fake_build)
class DaskDispatchTests(unittest.TestCase):
    """The driver's dispatch loop against a real (in-process) Dask cluster."""

    @classmethod
    def setUpClass(cls):
        cls.cluster = LocalCluster(
            n_workers=2, threads_per_worker=1, processes=False, dashboard_address=None
        )

    @classmethod
    def tearDownClass(cls):
        cls.cluster.close()

    def setUp(self):
        STARTED.clear()

    def build(self, boundaries, strict=False):
        with patch("builder.run.discover_boundaries", return_value=boundaries):
            return run.run_boundary_builds(
                self.cluster.scheduler_address,
                "postgresql://db",
                "https://lsib",
                version="t",
                strict=strict,
            )

    def test_big_builds_start_first_and_cgaz_fills_the_tail(self):
        successes, failures, cgaz = self.build(
            [
                ("gbOpen", "AAA", "ADM1", 1),
                ("gbOpen", "BOOM", "ADM1", 3),
                ("gbOpen", "CAN", "ADM2", 100),
                ("gbOpen", "BBB", "ADM1", 2),
                ("gbOpen", "USA", "ADM1", 90),
            ]
        )

        self.assertEqual(set(STARTED[:2]), {"CGAZ/LSIB", "gbOpen/CAN_ADM2"})
        self.assertEqual(STARTED[2], "gbOpen/USA_ADM1")
        last_build = max(i for i, t in enumerate(STARTED) if t.startswith("gbOpen"))
        first_cgaz = min(
            i
            for i, t in enumerate(STARTED)
            if t.startswith("CGAZ/") and t != "CGAZ/LSIB"
        )
        self.assertLess(last_build, first_cgaz)

        self.assertEqual(len(successes), 4)
        self.assertEqual(failures[0]["iso"], "BOOM")
        self.assertEqual(failures[0]["failed_stage"], "dask")
        self.assertIn("MemoryError", failures[0]["error"])
        by_tag = {r["tag"]: r for r in cgaz}
        self.assertEqual(
            sorted(by_tag),
            ["CGAZ/AAA", "CGAZ/BBB", "CGAZ/BOOM", "CGAZ/CAN", "CGAZ/LSIB", "CGAZ/USA"],
        )
        # BOOM's CGAZ still runs, without the level that failed.
        self.assertEqual(
            by_tag["CGAZ/BOOM"]["builds"], [{"adm": "ADM1", "status": "error"}]
        )

    def test_strict_failure_abandons_cgaz(self):
        successes, failures, cgaz = self.build(
            [("gbOpen", "AAA", "ADM1", 2), ("gbOpen", "BAD", "ADM1", 1)], strict=True
        )

        self.assertEqual([f["iso"] for f in failures], ["BAD"])
        self.assertNotIn("CGAZ/BAD", STARTED)
        self.assertNotIn("CGAZ/BAD", [r["tag"] for r in cgaz])


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
