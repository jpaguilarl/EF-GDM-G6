from pathlib import Path

import polars as pl


class ZoneLookup:
    def __init__(self):
        self._zones: dict[int, dict[str, str]] = {}

    def load(self, path: Path) -> None:
        df = pl.read_parquet(path)
        for row in df.to_dicts():
            self._zones[row["LocationID"]] = {
                "borough": row["Borough"],
                "zone": row["Zone"],
                "borough_es": row.get("borough_name_es", row["Borough"]),
            }

    def lookup(self, location_id: int | None) -> dict[str, str] | None:
        if location_id is None:
            return None
        return self._zones.get(location_id)
