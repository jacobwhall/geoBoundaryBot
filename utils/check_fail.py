import os
import sys

from . import helpers as gbHelpers

MAX_RESULT_BYTES = 64 * 1024


def main():
    check_type = os.environ.get("CHECK_TYPE")
    if check_type not in gbHelpers.VALIDATION_CHECKS:
        raise ValueError("CHECK_TYPE must identify a validation result directory")

    result_path = gbHelpers._check_output_dir(check_type) / "RESULT.txt"
    try:
        raw = result_path.read_bytes()
    except (FileNotFoundError, OSError):
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_path.write_text(
            "Validation crashed before producing a result. See the job log for details.",
            encoding="utf-8",
        )
        print("Validation did not produce RESULT.txt", file=sys.stderr)
        return 1

    if not raw or len(raw) > MAX_RESULT_BYTES:
        print("RESULT.txt is empty or exceeds 64 KiB", file=sys.stderr)
        return 1
    try:
        result = raw.decode("utf-8")
    except UnicodeDecodeError:
        print("RESULT.txt is not valid UTF-8", file=sys.stderr)
        return 1

    print(result)
    return 0 if result == "PASSED" else 1


if __name__ == "__main__":
    sys.exit(main())
