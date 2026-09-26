"""Apply a self-contained measurement patch to a clean, explicit baseline revision."""

import argparse
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("--expected-ref", required=True)
    parser.add_argument(
        "--patch",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "patches/baseline.patch",
    )
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    root = args.baseline.resolve()

    def git(*arguments):
        return subprocess.check_output(
            ["git", "-C", str(root), *arguments], text=True
        ).strip()

    if Path(git("rev-parse", "--show-toplevel")).resolve() != root:
        parser.error("baseline must name a checkout root")
    expected = git("rev-parse", "--verify", args.expected_ref + "^{commit}")
    if git("rev-parse", "HEAD") != expected:
        parser.error("baseline HEAD does not match --expected-ref")
    if git("status", "--porcelain"):
        parser.error("baseline must be clean, including untracked files and submodules")
    patch = args.patch.resolve()
    subprocess.run(["git", "-C", str(root), "apply", "--check", str(patch)], check=True)
    if not args.check_only:
        subprocess.run(["git", "-C", str(root), "apply", str(patch)], check=True)
    print(("Checked" if args.check_only else "Prepared") + " baseline " + expected)


if __name__ == "__main__":
    main()
