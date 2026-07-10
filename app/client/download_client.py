import asyncio
import io
from datetime import datetime
from pathlib import Path

import httpx
import polars as pl

from app.utils import storage
from app.utils.logger import Logger


class DownloadClient:
    """Client for downloading TLC trip data and lookup tables from the NY Taxi dataset."""

    def __init__(
        self,
        audit_id: str | None = None,
        base_url: str = "https://d37ci6vzurychx.cloudfront.net",
    ):
        self.audit_id = audit_id
        self.base_url = base_url.rstrip("/")
        self.logger = Logger()
        self._client: httpx.AsyncClient | None = None
        self._audit_lock = asyncio.Lock()

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(300.0),
                limits=httpx.Limits(
                    max_connections=16,
                    max_keepalive_connections=10,
                ),
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get_trip_data(
        self,
        year: int,
        month: int,
        category: str,
        path: str | None = None,
    ) -> None:
        start_ts = datetime.now()
        name = f"{category}_{year}-{month:02d}"
        url = (
            f"{self.base_url}/trip-data/{category}_tripdata_{year}-{month:02d}.parquet"
        )

        self.logger.info(f"Iniciando descarga de datos de viajes: {url}")
        self.logger.info(f"Marca de tiempo de inicio: {start_ts.isoformat()}")

        file_path = (
            Path(path) if path else storage.data_path("bronze", category, f"{year}-{month:02d}.parquet")
        )

        # Idempotencia: si el archivo ya existe y es un parquet legible (footer
        # valido), no re-descargar. Un archivo truncado por una descarga
        # interrumpida falla la lectura del footer y se re-descarga.
        if file_path.exists():
            if storage.parquet_footer_readable(file_path):
                self.logger.info(f"Ya descargado, se omite: {file_path}")
                return
            self.logger.warning(
                f"Archivo existente ilegible, se re-descarga: {file_path}"
            )

        try:
            client = await self._get_client()
            # Streaming por chunks de 4 MiB. Los writes van a
            # asyncio.to_thread: s3fs flush (multipart upload cada ~5 MB)
            # es bloqueante y congelaria el event loop impidiendo que las
            # demas descargas concurrentes avancen.
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                f = storage.open_writable(file_path)
                try:
                    async for chunk in response.aiter_bytes(chunk_size=4 * 1024 * 1024):
                        await asyncio.to_thread(f.write, chunk)
                finally:
                    await asyncio.to_thread(f.close)
        except httpx.HTTPStatusError as e:
            self.logger.error(f"Error HTTP {e.response.status_code} al descargar {url}")
            return
        except httpx.RequestError as e:
            self.logger.error(f"Error de conexión al descargar {url}: {e}")
            return

        try:
            pf = storage.parquet_file(file_path)
            rowcount = pf.metadata.num_rows
            bytecount = file_path.stat().st_size
            source_file = str(file_path)

            end_ts = datetime.now()
            await self._write_audit(name, start_ts, end_ts, rowcount, source_file, bytecount)

            self.logger.info(
                f"Descarga completada: {name} — {rowcount} registros procesados"
            )

        except Exception as e:
            self.logger.critical(
                f"Error crítico al procesar el archivo descargado de {url}: {e}"
            )
            raise

    async def get_lookup_table(self, path: str | None = None) -> None:
        start_ts = datetime.now()
        url = f"{self.base_url}/misc/taxi_zone_lookup.csv"

        self.logger.info(f"Iniciando descarga de tabla de búsqueda: {url}")
        self.logger.info(f"Marca de tiempo de inicio: {start_ts.isoformat()}")

        try:
            client = await self._get_client()
            response = await client.get(url)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            self.logger.error(f"Error HTTP {e.response.status_code} al descargar {url}")
            return
        except httpx.RequestError as e:
            self.logger.error(f"Error de conexión al descargar {url}: {e}")
            return

        try:
            df = pl.read_csv(io.StringIO(response.text))
            rowcount = len(df)

            file_path = (
                Path(path)
                if path
                else storage.data_path("bronze", "zone-lookup", "zone-lookup-table.parquet")
            )
            file_path.parent.mkdir(parents=True, exist_ok=True)
            df.write_parquet(str(file_path))

            bytecount = file_path.stat().st_size
            source_file = str(file_path)

            end_ts = datetime.now()
            await self._write_audit("zone-lookup-table", start_ts, end_ts, rowcount, source_file, bytecount)

            self.logger.info(
                f"Descarga completada: zone-lookup-table — {rowcount} registros procesados"
            )

        except Exception as e:
            self.logger.critical(
                f"Error crítico al procesar el archivo descargado de {url}: {e}"
            )
            raise

    async def _write_audit(
        self,
        name: str,
        start_ts: datetime,
        end_ts: datetime,
        rowcount: int,
        source_file: str | None = None,
        bytecount: int | None = None,
    ) -> None:
        async with self._audit_lock:
            audit_path = storage.data_path("bronze", "audit.parquet")
            audit_path.parent.mkdir(parents=True, exist_ok=True)

            start_str = start_ts.isoformat()
            end_str = end_ts.isoformat()

            self.logger.info(
                f"Auditoría registrada: {name} | Filas: {rowcount} | "
                f"Inicio: {start_str} | Fin: {end_str}"
            )

            df_new = pl.DataFrame(
                [
                    {
                        "audit_id": self.audit_id,
                        "name": name,
                        "source_file": source_file,
                        "bytecount": bytecount,
                        "rowcount": rowcount,
                        "start_timestamp": start_str,
                        "end_timestamp": end_str,
                    }
                ]
            )
            if audit_path.exists():
                df_existing = pl.read_parquet(str(audit_path))
                df = pl.concat([df_existing, df_new])
            else:
                df = df_new
            df.write_parquet(str(audit_path))

    async def __aenter__(self) -> "DownloadClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
