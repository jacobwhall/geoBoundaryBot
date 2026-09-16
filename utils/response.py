import argparse
import html
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

MAX_INPUT_BYTES = 64 * 1024
MAX_COMMENT_CHARS = 20_000
MAX_RENDERED_RESULT_CHARS = 3_500
CHECKS = (
    ("fileChecks", "File checks"),
    ("metaChecks", "Metadata checks"),
    ("geometryDataChecks", "Geometry and data checks"),
)


class UnsafeReportInput(ValueError):
    pass


@dataclass(frozen=True)
class CheckResult:
    state: str
    text: str


def _safe_regular_file(root, relative_path):
    root = Path(root)
    if not root.is_absolute():
        raise UnsafeReportInput("Artifact directory must be absolute")
    if root.is_symlink() or not root.is_dir():
        raise UnsafeReportInput(f"Artifact directory is missing or symlinked: {root}")

    root_resolved = root.resolve(strict=True)
    candidate = root
    for part in Path(relative_path).parts:
        candidate = candidate / part
        try:
            mode = candidate.lstat().st_mode
        except FileNotFoundError:
            raise
        if stat.S_ISLNK(mode):
            raise UnsafeReportInput(f"Artifact path contains a symlink: {candidate}")

    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise UnsafeReportInput("Artifact file escapes its download directory") from exc
    if not stat.S_ISREG(resolved.stat().st_mode):
        raise UnsafeReportInput(f"Artifact input is not a regular file: {candidate}")
    return resolved


def _read_bounded_utf8(path):
    with path.open("rb") as artifact_input:
        size = os.fstat(artifact_input.fileno()).st_size
        if size == 0:
            raise UnsafeReportInput(f"Artifact input is empty: {path.name}")
        if size > MAX_INPUT_BYTES:
            raise UnsafeReportInput(f"Artifact input exceeds 64 KiB: {path.name}")
        raw = artifact_input.read(MAX_INPUT_BYTES)
        if len(raw) != size:
            raise UnsafeReportInput(
                f"Artifact input changed while being read: {path.name}"
            )
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnsafeReportInput(
            f"Artifact input is not valid UTF-8: {path.name}"
        ) from exc


def read_check_result(results_root, check_name):
    artifact_dir = Path(results_root) / check_name
    try:
        result_path = _safe_regular_file(artifact_dir, "RESULT.txt")
        text = _read_bounded_utf8(result_path)
    except (FileNotFoundError, OSError, UnsafeReportInput) as exc:
        return CheckResult("incomplete", str(exc))

    if text == "PASSED":
        return CheckResult("passed", text)
    return CheckResult("failed", text)


def verify_context(context_dir, expected_pr_number, expected_head_sha):
    if not context_dir:
        return
    path = _safe_regular_file(Path(context_dir), "context.json")
    raw = _read_bounded_utf8(path)
    try:
        context = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise UnsafeReportInput("context.json is not valid JSON") from exc
    if not isinstance(context, dict):
        raise UnsafeReportInput("context.json must contain an object")
    if context.get("pr_number") != expected_pr_number:
        raise UnsafeReportInput("context.json PR number does not match GitHub metadata")
    if context.get("head_sha") != expected_head_sha:
        raise UnsafeReportInput("context.json head SHA does not match GitHub metadata")


def _validate_run_url(run_url):
    parsed = urlparse(run_url)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "github.com"
        or not re.fullmatch(r"/[^/]+/[^/]+/actions/runs/[0-9]+", parsed.path)
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("run-url must be a GitHub Actions run URL")


def _literal_result(text):
    escaped = html.escape(text, quote=False)
    if len(escaped) > MAX_RENDERED_RESULT_CHARS:
        escaped = (
            escaped[:MAX_RENDERED_RESULT_CHARS]
            + "\n… result truncated; see the run artifacts"
        )
    return f"<pre>{escaped}</pre>"


def build_response(results_root, run_url, validated_sha, run_conclusion):
    _validate_run_url(run_url)
    if not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", validated_sha):
        raise ValueError("validated-sha must be a full Git commit SHA")

    results = {name: read_check_result(results_root, name) for name, _ in CHECKS}
    failures = sum(result.state == "failed" for result in results.values())
    incomplete = sum(result.state == "incomplete" for result in results.values())
    all_passed = not failures and not incomplete

    lines = [
        "**Hello! I am the geoBoundary Bot.** I completed the automated checks for this submission.",
        "",
    ]
    if all_passed:
        if run_conclusion == "success":
            lines.append(
                "**OVERALL STATUS: All checks passed.** The submission is ready for manual review."
            )
        else:
            lines.append(
                "**OVERALL STATUS: Validation is incomplete.** The validation workflow did not succeed."
            )
    elif incomplete:
        lines.append(
            "**OVERALL STATUS: Validation is incomplete.** Some results were unavailable or rejected."
        )
        if failures:
            lines.extend(
                ["", f"The available results also contain {failures} failed check(s)."]
            )
    else:
        lines.append(f"**OVERALL STATUS: {failures} check(s) failed.**")

    if all_passed and run_conclusion != "success":
        lines.extend(
            [
                "",
                "All three result files say `PASSED`, but the validation workflow did not succeed, so this report is incomplete.",
            ]
        )

    for check_name, label in CHECKS:
        result = results[check_name]
        lines.extend(["", f"**{label}: {result.state.upper()}**"])
        if result.state != "passed":
            lines.extend(["", _literal_result(result.text)])

    lines.extend(
        [
            "",
            f"Validated commit: `{validated_sha.lower()}`",
            "",
            f"[Open the validation run and download its logs or preview artifacts]({run_url})",
        ]
    )
    response = "\n".join(lines)
    if len(response) > MAX_COMMENT_CHARS:
        raise RuntimeError("Generated response exceeds the 20,000 character limit")
    return response


def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", required=True)
    parser.add_argument("--run-url", required=True)
    parser.add_argument("--validated-sha", required=True)
    parser.add_argument("--run-conclusion", required=True)
    parser.add_argument("--expected-pr-number", required=True, type=int)
    parser.add_argument("--context-dir", default="")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    results_root = Path(args.results_dir)
    if (
        not results_root.is_absolute()
        or results_root.is_symlink()
        or not results_root.is_dir()
    ):
        raise UnsafeReportInput(
            "results-dir must be an existing absolute, non-symlink directory"
        )

    verify_context(
        args.context_dir,
        args.expected_pr_number,
        args.validated_sha,
    )
    response = build_response(
        results_root,
        args.run_url,
        args.validated_sha,
        args.run_conclusion,
    )

    output_parent = Path(os.environ.get("RUNNER_TEMP", tempfile.gettempdir())).resolve()
    output_parent.mkdir(parents=True, exist_ok=True)
    descriptor, output_name = tempfile.mkstemp(
        prefix="geoboundary-comment-", suffix=".md", dir=output_parent
    )
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
        output.write(response)
    print(output_name)


if __name__ == "__main__":
    main()
