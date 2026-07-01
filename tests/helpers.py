from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq


def assert_parquet_schema(path: Path, required_cols: set[str]) -> None:
    """Assert a parquet file (or directory) contains all ``required_cols``."""
    resolved = path
    if path.is_dir():
        files = list(path.glob("*.parquet")) or list(path.glob("part-*.parquet"))
        if not files:
            files = list(path.rglob("*.parquet"))
        if files:
            resolved = files[0]
    schema = pq.read_schema(resolved)
    col_names = {f.name for f in schema}
    missing = required_cols - col_names
    assert not missing, f"Missing columns in {path}: {missing}"


def assert_audit_fk(
    child_audit_path: Path,
    parent_audit_path: Path,
    fk_col: str,
    pk_col: str = "audit_id",
) -> None:
    """Assert referential integrity: every FK in child has a matching PK in parent."""
    import polars as pl

    child = pl.read_parquet(str(child_audit_path))
    parent = pl.read_parquet(str(parent_audit_path))
    orphan_ids = child.select(fk_col).join(
        parent.select(pk_col), left_on=fk_col, right_on=pk_col, how="anti"
    )
    assert len(orphan_ids) == 0, (
        f"Found {len(orphan_ids)} orphan {fk_col}s in {child_audit_path.name}"
    )
