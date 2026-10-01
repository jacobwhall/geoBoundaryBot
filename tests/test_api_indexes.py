import json
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from builder import run


class ApiIndexTests(unittest.TestCase):
    def setUp(self):
        self.s3_config = {
            "endpoint": "https://r2.example.test",
            "bucket": "geoboundaries",
            "prefix": "v7",
            "access_key_id": "test",
            "secret_access_key": "test",
        }

    @staticmethod
    def success(product, iso, adm):
        return {
            "status": "ok",
            "product": product,
            "iso": iso,
            "adm": adm,
            "metadata": {
                "boundaryID": f"{iso}-{adm}-test",
                "boundaryISO": iso,
                "boundaryType": adm,
            },
            "uploaded": [],
        }

    @patch("builder.run._s3_client")
    def test_uploads_sorted_index_for_every_product(self, client_factory):
        client = client_factory.return_value
        successes = [
            self.success("gbOpen", "USA", "ADM10"),
            self.success("gbHumanitarian", "UKR", "ADM1"),
            self.success("gbOpen", "CAN", "ADM1"),
            self.success("gbOpen", "USA", "ADM2"),
        ]

        manifest = run.upload_api_indexes(successes, self.s3_config)

        self.assertEqual(
            [item["key"] for item in manifest],
            [
                "v7/gbOpen/index.json",
                "v7/gbHumanitarian/index.json",
                "v7/gbAuthoritative/index.json",
            ],
        )
        calls = {
            call.kwargs["Key"]: call.kwargs for call in client.put_object.call_args_list
        }
        open_index = json.loads(calls["v7/gbOpen/index.json"]["Body"])
        self.assertEqual(
            [
                f"{record['boundaryISO']}/{record['boundaryType']}"
                for record in open_index
            ],
            ["CAN/ADM1", "USA/ADM2", "USA/ADM10"],
        )
        self.assertEqual(json.loads(calls["v7/gbAuthoritative/index.json"]["Body"]), [])
        for kwargs in calls.values():
            self.assertEqual(kwargs["ContentType"], "application/json")
            self.assertEqual(kwargs["CacheControl"], "public, max-age=300")
        self.assertEqual(
            [item["size"] for item in manifest],
            [len(calls[item["key"]]["Body"]) for item in manifest],
        )

    @patch("builder.run._s3_client")
    def test_rejects_success_without_matching_metadata(self, client_factory):
        success = self.success("gbOpen", "USA", "ADM1")
        success["metadata"]["boundaryISO"] = "CAN"

        with self.assertRaisesRegex(ValueError, "Metadata ISO mismatch"):
            run.upload_api_indexes([success], self.s3_config)

        client_factory.assert_not_called()

    def test_index_upload_failure_stops_pipeline_before_cgaz_and_promotion(self):
        success = self.success("gbOpen", "USA", "ADM1")
        with ExitStack() as stack:
            stack.enter_context(patch("builder.run.sync_data_repo"))
            stack.enter_context(patch("builder.run.stage_lsib"))
            stack.enter_context(
                patch("builder.run.create_build_db", return_value="postgresql://test")
            )
            stack.enter_context(patch("builder.run.scale_dask_workers"))
            stack.enter_context(
                patch("builder.run.lsib_url", return_value="https://lsib.test")
            )
            stack.enter_context(
                patch(
                    "builder.run.run_boundary_builds",
                    return_value=(
                        [success],
                        [],
                        [{"tag": "CGAZ/LSIB", "status": "ok"}],
                    ),
                )
            )
            upload_indexes = stack.enter_context(
                patch(
                    "builder.run.upload_api_indexes",
                    side_effect=RuntimeError("index upload failed"),
                )
            )
            run_cgaz = stack.enter_context(patch("builder.run.run_cgaz_job"))
            promote = stack.enter_context(patch("builder.run.promote_to_current"))
            stack.enter_context(patch("builder.run.teardown_build_db"))

            with self.assertRaisesRegex(RuntimeError, "index upload failed"):
                run._run_pipeline(
                    "v7", True, True, 2, "tcp://scheduler:8786", self.s3_config
                )

        upload_indexes.assert_called_once_with([success], self.s3_config)
        run_cgaz.assert_not_called()
        promote.assert_not_called()


class ReleaseVerificationTests(unittest.TestCase):
    @patch("builder.run._s3_client")
    def test_verifies_api_indexes_with_boundary_objects(self, client_factory):
        paginator = MagicMock()
        paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "v7/gbOpen/USA/ADM0/file.json", "Size": 10},
                    {"Key": "v7/gbOpen/index.json", "Size": 20},
                ]
            }
        ]
        client_factory.return_value.get_paginator.return_value = paginator
        config = {"bucket": "geoboundaries", "prefix": "v7"}
        successes = [
            {
                "product": "gbOpen",
                "iso": "USA",
                "adm": "ADM0",
                "uploaded": [{"key": "v7/gbOpen/USA/ADM0/file.json", "size": 10}],
            }
        ]
        indexes = [{"key": "v7/gbOpen/index.json", "size": 20}]

        run.verify_release_objects(successes, config, indexes)

        with self.assertRaisesRegex(RuntimeError, "missing or mismatched"):
            run.verify_release_objects(
                successes,
                config,
                [{"key": "v7/gbHumanitarian/index.json", "size": 2}],
            )


if __name__ == "__main__":
    unittest.main()
