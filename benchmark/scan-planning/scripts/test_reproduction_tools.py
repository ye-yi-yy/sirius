"""Offline checks for reproduction tools; no CUDA build or SQL execution."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent
PHASES = [
    "optimizer_hook",
    "capture_logical",
    "finalize",
    "capture_physical",
    "candidate_build",
    "comparison",
    "publish",
    "execute_rebuild",
    "lifecycle_lock_wait",
    "native_walk",
]


class ReproductionTools(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def call(self, tool, *args, success=True):
        result = subprocess.run(
            [sys.executable, str(SCRIPTS / tool), *map(str, args)],
            capture_output=True,
            text=True,
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result

    def repo(self, name):
        root = self.root / name
        root.mkdir()
        subprocess.run(["git", "init", "-q", str(root)], check=True)
        (root / "anchor").write_text("old\n")
        subprocess.run(["git", "-C", str(root), "add", "anchor"], check=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "-qm",
                "fixture",
            ],
            check=True,
        )
        return root

    def test_baseline_preflight_and_application(self):
        root = self.repo("baseline")
        patch = self.root / "adapter.patch"
        patch.write_text(
            "diff --git a/anchor b/anchor\n--- a/anchor\n+++ b/anchor\n@@ -1 +1 @@\n-old\n+new\n"
        )
        args = [root, "--expected-ref", "HEAD", "--patch", patch]
        self.call("prepare_baseline.py", *args, "--check-only")
        self.assertEqual((root / "anchor").read_text(), "old\n")
        self.call(
            "prepare_baseline.py",
            root,
            "--expected-ref",
            "missing-ref",
            "--patch",
            patch,
            success=False,
        )
        (root / "untracked").write_text("keep")
        self.call("prepare_baseline.py", *args, success=False)
        (root / "untracked").unlink()
        self.call("prepare_baseline.py", *args)
        self.assertEqual((root / "anchor").read_text(), "new\n")

    def test_collection_resume_integrity_and_report(self):
        baseline, candidate = self.repo("baseline"), self.repo("candidate")
        binary_source = """#!/usr/bin/env python3
import json,os
phases = PHASE_LIST
for i in range(int(os.environ['SCAN_PLANNING_ATTEMPTS'])):
 print(json.dumps({'record':'planning_attempt','attempt':i,'mode':'latency','operation':'rebind','timing_valid':True,'api_total_us':1000,'phases':{p:{'inclusive_us':0,'calls':1} for p in phases}}))
""".replace(
            "PHASE_LIST", repr(PHASES)
        )
        for root in [baseline, candidate]:
            build = root / "build/release"
            build.mkdir(parents=True)
            (build / "CMakeCache.txt").write_text(
                "SIRIUS_ENABLE_PLANNING_MEASUREMENTS:BOOL=ON\n"
            )
            binary = root / "fake-binary"
            binary.write_text(binary_source)
            binary.chmod(0o755)
        pixi = self.root / "fake-pixi"
        pixi.write_text(
            "#!/usr/bin/env python3\nimport os,sys\na=sys.argv[4:]\nif a[0]=='nvidia-smi': print('name, driver_version\\nFixture GPU, 0')\nelif a[0]=='c++': print('Fixture compiler')\nelse: os.execv(a[0],a)\n"
        )
        pixi.chmod(0o755)
        fixture = self.root / "fixture"
        glob = fixture / "glob"
        glob.mkdir(parents=True)
        source = fixture / "source.parquet"
        source.write_bytes(b"fixture")
        width = 96 - len(os.fsencode(glob)) - 1 - len(".parquet")
        for i in range(10000):
            os.link(source, glob / (str(i).zfill(width) + ".parquet"))
        output = self.root / "collection"
        args = [
            "--baseline",
            baseline,
            "--candidate",
            candidate,
            "--fixture",
            fixture,
            "--output",
            output,
            "--pixi",
            pixi,
            "--binary-relative",
            "fake-binary",
            "--stage",
            "rebind-repeat",
            "--workloads",
            "small4",
            "--rounds",
            "1",
            "--attempts",
            "2",
        ]
        self.call("run_measurements.py", *args)
        self.call("run_measurements.py", *args, success=False)
        self.call("run_measurements.py", *args, "--resume")
        self.call("validate_results.py", "--input", output)
        selection = self.root / "selection.json"
        selection.write_text(
            json.dumps(
                {
                    "small4-rebind": {
                        "directory": "results/rebind-repeat",
                        "workload": "small4",
                    }
                }
            )
        )
        report = self.root / "report"
        self.call(
            "summarize.py",
            "--input",
            output,
            "--output",
            report,
            "--report-inputs",
            selection,
        )
        data = json.loads((report / "summary.json").read_text())
        self.assertEqual(
            data["latency"]["small4-rebind-candidate"]["statistics"]["api_total_us"][
                "n"
            ],
            2,
        )
        self.assertIn("Collection in progress", (report / "report.md").read_text())
        raw = output / "results/rebind-repeat/round1-small4-rebind-candidate.jsonl"
        rows = [json.loads(line) for line in raw.read_text().splitlines()]
        for row in rows:
            row["api_total_us"] = 2000
        raw.write_text("".join(json.dumps(row) + "\n" for row in rows))
        self.call("validate_results.py", "--input", output, success=False)
        self.call("run_measurements.py", *args, "--resume", success=False)
        self.call(
            "summarize.py",
            "--input",
            output,
            "--output",
            report,
            "--report-inputs",
            selection,
        )
        self.assertIn("P95 FAIL", (report / "report.md").read_text())
        (candidate / "fake-binary").write_text(binary_source + "\n# changed\n")
        self.call("run_measurements.py", *args, "--resume", success=False)


if __name__ == "__main__":
    unittest.main()
