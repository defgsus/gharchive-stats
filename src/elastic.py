import hashlib
import datetime
from pathlib import Path
from typing import Union

from tqdm import tqdm
from elastipy import Exporter

from .file_iter import iter_file, iter_lines


class ElasticExporterBase(Exporter):

    BASE_MAPPING = {
        "timestamp": {"type": "date"},
        "timestamp_weekday": {"type": "keyword"},
        "timestamp_hour": {"type": "integer"},
    }

    def get_document_index(self, es_data: dict) -> str:
        return self.index_name().replace("*", es_data["timestamp"][:4])

    def get_document_id(self, es_data: dict) -> str:
        return hashlib.md5(str(es_data).encode("utf-8")).hexdigest()

    def transform_document(self, data: dict) -> dict:
        timestamp = datetime.datetime.strptime(
            data.pop("date"),
            "%Y-%m-%dT%H:%M:%SZ"
        )
        return {
            "timestamp": timestamp.isoformat(),
            "timestamp_hour": timestamp.hour,
            "timestamp_weekday": timestamp.strftime("%w %A"),
            **data,
        }


class ElasticDeleteExporter(ElasticExporterBase):

    INDEX_NAME = "gharchive-delete-*"

    MAPPINGS = {
        "properties": {
            **ElasticExporterBase.BASE_MAPPING,
            "user": {"type": "keyword"},
            "org": {"type": "keyword"},
            "repo": {"type": "keyword"},
            "ref": {"type": "keyword"},
            "ref_type": {"type": "keyword"},
            "pusher_type": {"type": "keyword"},
            "events": {"type": "integer"},
        }
    }


class ElasticWatchExporter(ElasticExporterBase):

    INDEX_NAME = "gharchive-watch-*"

    MAPPINGS = {
        "properties": {
            **ElasticExporterBase.BASE_MAPPING,
            "user": {"type": "keyword"},
            # "org": {"type": "keyword"},
            "repo": {"type": "keyword"},
            "action": {"type": "keyword"},
            "events": {"type": "integer"},
        }
    }


ELASTIC_EXPORTERS = {
    "delete": ElasticDeleteExporter,
    "watch": ElasticWatchExporter,
}


def export_elastic(filename: Union[str, Path], chuck_size: int = 500, with_count: bool = True):
    export_type = Path(filename).name.split("_")[0]
    if export_type not in ELASTIC_EXPORTERS:
        raise ValueError(
            f"type '{export_type}' not supported by elasticsearch exporters"
        )

    exporter = ELASTIC_EXPORTERS[export_type]()

    total = None
    if with_count:
        total = sum(1 for _ in tqdm(iter_lines(filename), desc=f"counting lines in '{filename}'"))

    bulk_rows = []
    for row in tqdm(iter_file(filename), desc=f"exporting '{filename}'", total=total):
        bulk_rows.append(row)
        if len(bulk_rows) >= chuck_size:
            exporter.export_list(bulk_rows, chunk_size=500)
            bulk_rows = []

    if bulk_rows:
        exporter.export_list(bulk_rows, chunk_size=500)
