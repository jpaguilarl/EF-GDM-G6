from __future__ import annotations

import time
from pathlib import Path

import polars as pl

from app.utils.logger import Logger


class PolarsQueryEngine:
    MARTS_DIR: Path
    _cache: dict[str, tuple[pl.LazyFrame, float]]

    def __init__(self, marts_dir: Path, cache_ttl: int = 60):
        self.MARTS_DIR = marts_dir
        self._cache_ttl = cache_ttl
        self._cache = {}
        self.logger = Logger()

    def get_mart(self, name: str) -> pl.LazyFrame | None:
        now = time.time()
        if name in self._cache:
            scan, ts = self._cache[name]
            if now - ts < self._cache_ttl:
                return scan
        mart_dir = self.MARTS_DIR / name
        if not mart_dir.exists():
            self.logger.warning(
                f"Directorio del mart '{name}' no existe: {mart_dir}"
            )
            self._cache.pop(name, None)
            return None
        scan = pl.scan_parquet(str(mart_dir))
        self._cache[name] = (scan, now)
        return scan

    def query(
        self,
        mart: str,
        filters: dict[str, list | tuple | None] | None = None,
        group_by: list[str] | None = None,
        agg: dict[str, str] | None = None,
        order_by: list[tuple[str, str]] | None = None,
        limit: int | None = None,
    ) -> pl.DataFrame:
        lf = self.get_mart(mart)
        if lf is None:
            self.logger.warning(
                f"Consulta a mart '{mart}' devolvió vacío: directorio no encontrado"
            )
            return pl.DataFrame()

        if filters:
            predicates = []
            for col_name, values in filters.items():
                if values is None:
                    continue
                if isinstance(values, (list, tuple)):
                    predicates.append(pl.col(col_name).is_in(values))
                else:
                    predicates.append(pl.col(col_name) == values)
            if predicates:
                combined = predicates[0]
                for p in predicates[1:]:
                    combined = combined & p
                lf = lf.filter(combined)

        if group_by and agg:
            agg_exprs = [
                getattr(pl.col(col_name), aggr)().alias(col_name)
                for col_name, aggr in agg.items()
            ]
            lf = lf.group_by(group_by).agg(agg_exprs)

        if order_by:
            by = [col for col, _ in order_by]
            descending = [direction == "desc" for _, direction in order_by]
            lf = lf.sort(by, descending=descending)

        if limit is not None:
            lf = lf.limit(limit)

        return lf.collect()
