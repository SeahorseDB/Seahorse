import pyarrow as pa
import redis
from typing import Iterable, Optional
import struct
from lib.utils import convert_data_to_bytes, print_arrow_table, repartition_data_by_segment_id_columns
import os

class SeahorseDB:
    def __init__(self, host='localhost', port=6379):
        host=os.environ.get("SEAHORSEDB_HOST", host)
        self.r = redis.Redis(host=host, port=port)

    def create_table(self, schema: pa.Schema, table_name: str | None = None, metadata: dict | None = None):
        if metadata is None:
            if schema.metadata is not None:
                metadata = schema.metadata
            else:
                metadata = {}

        table_name_in_metadata = metadata.get('table name')
        if table_name_in_metadata is None:
            metadata['table name'] = table_name

        if metadata is not None:
            schema = schema.with_metadata(metadata)

        self.r.execute_command("table", "create", schema.serialize().to_pybytes())

    def drop_table(self, table_name: str, ignore_if_not_exists: bool = False):
        try:
            self.r.execute_command("table", "drop", table_name)
        except redis.exceptions.ResponseError as e:
            if not ignore_if_not_exists:
                raise

    def get_segment_id_columns(self, table_name: str):
        result = self.r.execute_command("table", "describe", table_name)
        msg = pa.ipc.read_message(result)
        schema = pa.ipc.read_schema(msg)
        segment_id_info = schema.metadata.get(b'segment_id_info')
        if segment_id_info is None:
            return []
        segment_id_column_indexes = struct.unpack_from("<" + "I" * int((len(segment_id_info)/4)), segment_id_info)
        return [schema[col].name for col in segment_id_column_indexes]

    def insert_data(self, table_name: str, data: pa.Table | pa.RecordBatch | Iterable[pa.RecordBatch | pa.Table], segment_id_columns: list[str] | None = None):
        if segment_id_columns is None:
            segment_id_columns = self.get_segment_id_columns(table_name)
        repartitioned_record_batches = repartition_data_by_segment_id_columns(data, segment_id_columns)
        for record_batch in repartitioned_record_batches:
            bytes = convert_data_to_bytes(record_batch, record_batch.schema)
            self.r.execute_command("table", "batchinsert", table_name, bytes)

    def scan_table_to_arrow_table(self, table_name: str, projection: str = "*", filter: str = ""):
        result = self.r.execute_command("table", "scan", table_name, projection, filter)
        reader = pa.ipc.open_stream(result)
        table = reader.read_all()
        return table

    def scan_table_to_record_batches(self, table_name: str, projection: str = "*", filter: str = ""):
        table = self.scan_table_to_arrow_table(table_name, projection, filter)
        return table.to_batches()

    def ann_table_to_arrow_table(self, table_name: str, k: int, query: bytes, ef_search: Optional[int] = None, filter: str = ""):
        if ef_search is None:
            ef_search = k
        result = self.r.execute_command("table", "ann", table_name, k, query, ef_search, filter)
        reader = pa.ipc.open_stream(result)
        table = reader.read_all()
        return table

    def ann_table_to_record_batches(self, table_name: str, k: int, query: bytes, ef_search: Optional[int] = None, filter: str = ""):
        table = self.ann_table_to_arrow_table(table_name, k, query, ef_search, filter)
        return table.to_batches()

    def parse_vector_string_to_float_list(self, vector_string: str):
        return [float(token.strip()) for token in vector_string.split(',')]

    def convert_vector_list_to_bytes(self, vector_list: list[float]):
        return struct.pack("<" + "f" * len(vector_list), *vector_list)
