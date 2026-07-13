import asyncio
import uuid
from datetime import datetime

from app.client.download_client import DownloadClient
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils.globals import globals
from app.utils.logger import Logger

# 8 descargas simultaneas: buen balance entre throughput y no saturar
# CloudFront / la red. Con chunks de 4 MiB el pico de RAM es ~32 MiB.
MAX_CONCURRENT_DOWNLOADS = 8


class BronzePipeline:
    def __init__(self) -> None:
        audit_id = str(uuid.uuid4())
        self.client = DownloadClient(audit_id=audit_id)
        self.logger = Logger()

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _expand_tasks(
        year_span: DatasetsConfig,
    ) -> list[tuple[str, int, int]]:
        """Expand config into (category, year, month) triples, grouped by
        year+category so logs follow a logical order."""
        tasks: list[tuple[str, int, int]] = []
        for year in year_span.years:
            if isinstance(year, int):
                for cat in globals.tlc_categories:
                    for m in range(1, 13):
                        tasks.append((cat, year, m))
            elif isinstance(year, Module):
                for m in range(1, 13):
                    tasks.append((year.category, year.year, m))
        return tasks

    # ------------------------------------------------------------------
    # run
    # ------------------------------------------------------------------
    async def run(self, year_span: DatasetsConfig):
        await self.client.get_lookup_table()

        tasks = self._expand_tasks(year_span)
        total = len(tasks)
        if total == 0:
            self.logger.info("Sin archivos para descargar")
            return

        sem = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
        completed = 0          # safe: asyncio is single-threaded
        errors: list[str] = []

        self.logger.info(
            f"Iniciando descarga concurrente: {total} archivos, "
            f"max {MAX_CONCURRENT_DOWNLOADS} simultáneos"
        )

        async def _download(cat: str, year: int, month: int) -> None:
            nonlocal completed
            async with sem:
                start = datetime.now()
                try:
                    await self.client.get_trip_data(year, month, cat)
                except Exception as e:
                    tag = f"{cat}_{year}-{month:02d}"
                    self.logger.error(f"Fallo en descarga {tag}: {e}")
                    errors.append(f"{tag}: {e}")
                    return
                elapsed = (datetime.now() - start).total_seconds()
                completed += 1
                self.logger.info(
                    f"[{completed}/{total}] {cat}_{year}-{month:02d} ✓ "
                    f"({elapsed:.0f}s)"
                )

        coros = [_download(cat, year, m) for cat, year, m in tasks]
        await asyncio.gather(*coros)

        if errors:
            msg = (
                f"{len(errors)}/{total} descargas fallaron:\n"
                + "\n".join(errors)
            )
            self.logger.error(msg)
            raise RuntimeError(msg)
