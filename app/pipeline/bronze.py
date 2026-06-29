import uuid

from app.client.download_client import DownloadClient
from app.schemas.settings_schema import DatasetsConfig, Module
from app.utils.globals import globals
from app.utils.logger import Logger


class BronzePipeline:
    def __init__(self) -> None:
        audit_id = str(uuid.uuid4())
        self.client = DownloadClient(audit_id=audit_id)
        self.logger = Logger()

    async def run(self, year_span: DatasetsConfig):
        await self.client.get_lookup_table()
        for year in year_span.years:
            if isinstance(year, int):
                for cat in globals.tlc_categories:
                    for m in range(1, 13):
                        await self.client.get_trip_data(year, m, cat)
            elif isinstance(year, Module):
                for m in range(1, 13):
                    await self.client.get_trip_data(year.year, m, year.category)
