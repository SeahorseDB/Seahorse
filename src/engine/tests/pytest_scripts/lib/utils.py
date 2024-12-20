import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as papq
from prettytable import PrettyTable
import polars as pl
from typing import Iterable

def read_csv(csv_file_path: str):
    return pacsv.open_csv(csv_file_path).read_all()

def read_parquet(parquet_file_path: str):
    return papq.read_table(parquet_file_path)

def read_parquet_to_arrow_schema(parquet_file_path: str):
    return papq.read_schema(parquet_file_path)

def convert_data_to_bytes(data: pa.Table | pa.RecordBatch | Iterable[pa.RecordBatch | pa.Table], schema: pa.Schema):
    output_stream = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(output_stream, schema)
    writer.write(data)
    writer.close()
    return output_stream.getvalue().to_pybytes()

def repartition_data_by_segment_id_columns(data: pa.Table | pa.RecordBatch | Iterable[pa.RecordBatch | pa.Table], segment_id_columns: list[str] | None):
    if segment_id_columns is None or len(segment_id_columns) == 0:
        return data.to_batches()

    df = pl.from_arrow(data)
    batches = []
    for (_, data) in df.group_by(*segment_id_columns):
        batches.append(data.to_arrow())
    return batches

def print_arrow_table(table: pa.Table):
    pretty_table = PrettyTable()

    # Add columns to the table
    for name, col in zip(table.schema.names, table.columns):
        pretty_table.add_column(name, col.to_pylist())

    pretty_table.align = 'l'  # Left align all columns
    pretty_table.border = True
    pretty_table.header = True
    pretty_table.padding_width = 1
    print(pretty_table)
