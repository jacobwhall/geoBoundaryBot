import json
import os
import stat
import subprocess
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from utils import helpers
from utils import response

SHA = "a" * 40
RUN_URL = "https://github.com/wmgeolab/geoBoundaries/actions/runs/123"


class Environment:
    def __init__(self, **updates):
        self.updates = updates
        self.original = None

    def __enter__(self):
        self.original = os.environ.copy()
        os.environ.update(self.updates)

    def __exit__(self, *_):
        os.environ.clear()
        os.environ.update(self.original)


class WorkspaceSecurityTests(unittest.TestCase):
    def test_changes_requires_json_array_and_preserves_special_filenames(self):
        names = [
            "sourceData/name with spaces.zip",
            'sourceData/quote" and $() ;.zip',
            "sourceData/unicode-ö.zip",
        ]
        with (
            tempfile.TemporaryDirectory() as checkout,
            tempfile.TemporaryDirectory() as results,
        ):
            with Environment(
                GITHUB_WORKSPACE=checkout,
                RESULTS_DIR=results,
                changes=json.dumps(names),
            ):
                workspace = helpers.initiateWorkspace("fileChecks")
        self.assertEqual(workspace["zips"], names)

    def test_changes_rejects_malformed_or_traversing_input(self):
        for changes in (
            "not json",
            '"sourceData/a.zip"',
            '["../outside.zip"]',
            '["/absolute.zip"]',
        ):
            with self.subTest(changes=changes):
                with (
                    tempfile.TemporaryDirectory() as checkout,
                    tempfile.TemporaryDirectory() as results,
                ):
                    with Environment(
                        GITHUB_WORKSPACE=checkout,
                        RESULTS_DIR=results,
                        changes=changes,
                    ):
                        with self.assertRaises((ValueError, json.JSONDecodeError)):
                            helpers.initiateWorkspace("fileChecks")

    @mock.patch("utils.helpers.subprocess.run")
    def test_lfs_uses_argument_array_and_explicit_checkout(self, run):
        with tempfile.TemporaryDirectory() as checkout:
            helpers.checkRetrieveLFSFiles("sourceData/a;$(touch nope).zip", checkout)
            args = run.call_args.args[0]
        self.assertEqual(args[:3], ["git", "-C", str(Path(checkout).resolve())])
        self.assertIn("--include=sourceData/a;$(touch nope).zip", args)
        self.assertTrue(run.call_args.kwargs["check"])

    @mock.patch("utils.helpers.subprocess.run")
    def test_lfs_failure_writes_readable_result_and_stops(self, run):
        failures = [
            subprocess.CalledProcessError(2, ["git", "lfs", "pull"]),
            FileNotFoundError("git"),
        ]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                run.side_effect = failure
                with (
                    tempfile.TemporaryDirectory() as checkout,
                    tempfile.TemporaryDirectory() as results,
                ):
                    with Environment(
                        CHECK_TYPE="fileChecks",
                        GITHUB_WORKSPACE=checkout,
                        RESULTS_DIR=results,
                    ):
                        with self.assertRaises(RuntimeError):
                            helpers.checkRetrieveLFSFiles(
                                "sourceData/PCN_ADM0.zip", checkout
                            )
                        result = (
                            Path(results) / "fileChecks" / "RESULT.txt"
                        ).read_text(encoding="utf-8")
                self.assertIn("sourceData/PCN_ADM0.zip", result)
                self.assertIn("Git LFS", result)
                self.assertNotEqual(result, "PASSED")

    @mock.patch("utils.helpers.subprocess.run")
    def test_lfs_pointer_left_behind_is_reported_as_failure(self, run):
        with (
            tempfile.TemporaryDirectory() as checkout,
            tempfile.TemporaryDirectory() as results,
        ):
            submission = Path(checkout) / "sourceData"
            submission.mkdir()
            (submission / "PCN_ADM0.zip").write_bytes(
                helpers.LFS_POINTER_PREFIX + b"\noid sha256:" + b"0" * 64 + b"\n"
            )
            with Environment(
                CHECK_TYPE="fileChecks",
                GITHUB_WORKSPACE=checkout,
                RESULTS_DIR=results,
            ):
                with self.assertRaises(RuntimeError):
                    helpers.checkRetrieveLFSFiles("sourceData/PCN_ADM0.zip", checkout)
                result = (Path(results) / "fileChecks" / "RESULT.txt").read_text(
                    encoding="utf-8"
                )
        self.assertTrue(run.called)
        self.assertIn("sourceData/PCN_ADM0.zip", result)

    @mock.patch("utils.helpers.subprocess.run")
    def test_lfs_success_leaves_real_file_untouched(self, run):
        with (
            tempfile.TemporaryDirectory() as checkout,
            tempfile.TemporaryDirectory() as results,
        ):
            submission = Path(checkout) / "sourceData"
            submission.mkdir()
            (submission / "PCN_ADM0.zip").write_bytes(b"PK\x03\x04 not a pointer")
            with Environment(
                CHECK_TYPE="fileChecks",
                GITHUB_WORKSPACE=checkout,
                RESULTS_DIR=results,
            ):
                helpers.checkRetrieveLFSFiles("sourceData/PCN_ADM0.zip", checkout)
            self.assertFalse((Path(results) / "fileChecks" / "RESULT.txt").exists())
        self.assertTrue(run.called)

    def test_zip_extraction_is_unique_and_rejects_traversal_and_symlinks(self):
        with (
            tempfile.TemporaryDirectory() as runner_temp,
            tempfile.TemporaryDirectory() as checkout,
        ):
            safe_zip = Path(runner_temp) / "safe.zip"
            with zipfile.ZipFile(safe_zip, "w") as archive:
                archive.writestr("shape/data.geojson", "{}")
            with Environment(RUNNER_TEMP=runner_temp, GITHUB_WORKSPACE=checkout):
                with zipfile.ZipFile(safe_zip) as archive:
                    first = helpers.unzipGB(archive)
                with zipfile.ZipFile(safe_zip) as archive:
                    second = helpers.unzipGB(archive)
            self.assertNotEqual(first, second)
            self.assertTrue((first / "shape" / "data.geojson").is_file())

            bad_zip = Path(runner_temp) / "bad.zip"
            with zipfile.ZipFile(bad_zip, "w") as archive:
                archive.writestr("../escape", "bad")
            with Environment(RUNNER_TEMP=runner_temp, GITHUB_WORKSPACE=checkout):
                with zipfile.ZipFile(bad_zip) as archive:
                    with self.assertRaises(zipfile.BadZipFile):
                        helpers.unzipGB(archive)

            symlink_zip = Path(runner_temp) / "symlink.zip"
            info = zipfile.ZipInfo("link")
            info.create_system = 3
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            with zipfile.ZipFile(symlink_zip, "w") as archive:
                archive.writestr(info, "target")
            with Environment(RUNNER_TEMP=runner_temp, GITHUB_WORKSPACE=checkout):
                with zipfile.ZipFile(symlink_zip) as archive:
                    with self.assertRaises(zipfile.BadZipFile):
                        helpers.unzipGB(archive)


class ResponseSecurityTests(unittest.TestCase):
    def _results(self, root, values):
        for check, value in values.items():
            directory = Path(root) / check
            directory.mkdir(parents=True)
            (directory / "RESULT.txt").write_bytes(value)

    def test_success_requires_all_results_and_successful_run(self):
        with tempfile.TemporaryDirectory() as root:
            self._results(root, {name: b"PASSED" for name, _ in response.CHECKS})
            body = response.build_response(root, RUN_URL, SHA, "success")
            self.assertIn("All checks passed", body)
            body = response.build_response(root, RUN_URL, SHA, "failure")
            self.assertIn("Validation is incomplete", body)
            self.assertNotIn("OVERALL STATUS: All checks passed", body)

    def test_untrusted_result_is_literal_and_comment_is_bounded(self):
        attack = b"**bold** <script>alert(1)</script> $() GHEOF\n" + b"&" * 65000
        with tempfile.TemporaryDirectory() as root:
            self._results(
                root,
                {
                    "fileChecks": attack,
                    "metaChecks": b"PASSED",
                    "geometryDataChecks": b"PASSED",
                },
            )
            body = response.build_response(root, RUN_URL, SHA, "failure")
        self.assertIn("&lt;script&gt;", body)
        self.assertNotIn("<script>", body)
        self.assertLessEqual(len(body), response.MAX_COMMENT_CHARS)

    def test_missing_oversized_invalid_utf8_and_symlink_results_are_incomplete(self):
        cases = (b"", b"x" * (response.MAX_INPUT_BYTES + 1), b"\xff")
        for value in cases:
            with self.subTest(size=len(value)), tempfile.TemporaryDirectory() as root:
                self._results(root, {"fileChecks": value})
                body = response.build_response(root, RUN_URL, SHA, "failure")
                self.assertIn("Validation is incomplete", body)

        with (
            tempfile.TemporaryDirectory() as root,
            tempfile.TemporaryDirectory() as outside,
        ):
            check_dir = Path(root) / "fileChecks"
            check_dir.mkdir()
            target = Path(outside) / "RESULT.txt"
            target.write_text("PASSED", encoding="utf-8")
            (check_dir / "RESULT.txt").symlink_to(target)
            body = response.build_response(root, RUN_URL, SHA, "success")
            self.assertIn("Validation is incomplete", body)

    def test_context_must_match_independently_verified_identity(self):
        with tempfile.TemporaryDirectory() as context_dir:
            path = Path(context_dir) / "context.json"
            path.write_text(
                json.dumps({"pr_number": 4, "head_sha": SHA}), encoding="utf-8"
            )
            response.verify_context(context_dir, 4, SHA)
            with self.assertRaises(response.UnsafeReportInput):
                response.verify_context(context_dir, 5, SHA)
            path.write_text("{}", encoding="utf-8")
            with self.assertRaises(response.UnsafeReportInput):
                response.verify_context(context_dir, 4, SHA)


if __name__ == "__main__":
    unittest.main()
