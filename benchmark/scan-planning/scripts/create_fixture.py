"""Create 10,000 hard-linked Parquet files with 96-byte absolute paths."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

p = argparse.ArgumentParser()
p.add_argument("directory", type=Path)
p.add_argument("--duckdb", type=Path, required=True)
a = p.parse_args()
root = a.directory.resolve()
root.mkdir(parents=True, exist_ok=False)
source = root / "source.parquet"
subprocess.run(
    [
        str(a.duckdb.resolve()),
        "-c",
        "COPY (SELECT 1::INTEGER AS id) TO '"
        + str(source).replace("'", "''")
        + "' (FORMAT PARQUET)",
    ],
    env={**os.environ, "SIRIUS_DISABLE": "1"},
    check=True,
)
glob = root / "glob"
glob.mkdir()
width = 96 - len(os.fsencode(glob)) - 1 - len(".parquet")
if width < 5:
    raise ValueError("Dataset parent path is too long for fixed 96-byte names")
for i in range(10000):
    os.link(source, glob / (str(i).zfill(width) + ".parquet"))
files = sorted(glob.glob("*.parquet"))
assert len(files) == 10000 and all(len(os.fsencode(x)) == 96 for x in files)
(root / "fixture.json").write_text(
    json.dumps(
        {
            "file_count": len(files),
            "path_bytes": 960000,
            "absolute_path_length": 96,
            "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "glob": str(glob / "*.parquet"),
            "added_path": str(glob / (str(10000).zfill(width) + ".parquet")),
        },
        indent=2,
    )
    + "\n"
)
