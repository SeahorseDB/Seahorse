import pytest
import pyarrow as pa
from lib.seahorsedb import SeahorseDB

# REF: https://docs.pytest.org/en/stable/index.html

#============================================================
# Fixtures
#============================================================

@pytest.fixture
def seahorsedb():
    return SeahorseDB()

@pytest.fixture
def regression_table():
    table = pa.table({
        "id": pa.array([1, 2, 3, 4, 5], type=pa.int32()),
        "name": pa.array(["Alice", "Bob", "Charlie", "David", "Eve"], type=pa.string()),
        "desc": pa.array(["Student", "Engineer", "Teacher", "Artist", "Doctor"], type=pa.string())
    })
    return table

@pytest.fixture
def seahorsedb_with_empty_table(seahorsedb, regression_table):
    seahorsedb.drop_table("test_table", ignore_if_not_exists=True)
    seahorsedb.create_table(regression_table.schema, "test_table")
    return seahorsedb


#============================================================
# Test cases
#============================================================

def test_insert_table(seahorsedb_with_empty_table, regression_table):
    seahorsedb = seahorsedb_with_empty_table
    seahorsedb.insert_data("test_table", regression_table)
    assert seahorsedb.scan_table_to_arrow_table("test_table") == regression_table

def test_scan_table_to_record_batches(seahorsedb_with_empty_table, regression_table):
    seahorsedb = seahorsedb_with_empty_table
    seahorsedb.insert_data("test_table", regression_table)
    record_batches = seahorsedb.scan_table_to_record_batches("test_table")
    assert len(record_batches) == 1
    assert record_batches[0] == regression_table.to_batches()[0]
