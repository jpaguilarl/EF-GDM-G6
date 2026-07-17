#!/usr/bin/env python3
"""Copy metadata JSON files from data/gold/models/ into a folder grouped by model type."""

import shutil
from pathlib import Path

SRC = Path("data/gold/models")
DST = Path("data/gold/models_metadata")


def main():
    for model_type_dir in sorted(SRC.iterdir()):
        if not model_type_dir.is_dir():
            continue
        json_files = list(model_type_dir.rglob("*.json"))
        if not json_files:
            continue
        for src_file in json_files:
            rel = src_file.relative_to(model_type_dir)
            dst_file = DST / model_type_dir.name / rel
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dst_file)
            print(f"  {src_file} -> {dst_file}")

    print(f"\nDone. Metadata copied to {DST}/")


if __name__ == "__main__":
    main()
