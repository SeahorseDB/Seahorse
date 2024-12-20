#!python3
import pyarrow as pa
import redis
import typer
import os
import struct
import pandas as pd
from typing import Optional, Annotated
from prettytable import PrettyTable, MARKDOWN

"""
Moohn: This is a python client for the vdb. This is currently for testing only.
"""

# TODO: This should be customizable with config later.
r = redis.Redis(host='127.0.0.1', port=6379)
app = typer.Typer()

@app.command()
def Ping():
    """
    redis-cli ping
    """
    if r.ping():
        print("PONG")
    else:
        print("....")

# Schema Parsing
def ParseField(field: str):
    if field == "bool":
        return pa.bool_()
    elif field == "int8":
        return pa.int8()
    elif field == "int16":
        return pa.int16()
    elif field == "int":
        return pa.int32()
    elif field == "int32":
        return pa.int32()
    elif field == "long":
        return pa.int64()
    elif field == "int64":
        return pa.int64()
    elif field == "uint8":
        return pa.uint8()
    elif field == "uint16":
        return pa.uint16()
    elif field == "uint32":
        return pa.uint32()
    elif field == "uint":
        return pa.uint32()
    elif field == "ulong":
        return pa.uint64()
    elif field == "uint64":
        return pa.uint64()
    elif field == "float":
        return pa.float32()
    elif field == "float32":
        return pa.float32()
    elif field == "float64":
        return pa.float64()
    elif field == "double":
        return pa.float64()
    elif field == "string":
        return pa.string()
    elif field == "varchar":
        return pa.string()
    elif field[:15] == "fixed_size_list":
        return pa.list_(ParseField(field[field.find('[')+1:field.rfind(',')].strip()), int(field[field.find(',')+1:field.rfind(']')]))
    elif field[:4] == "list" and ',' in field:
        return pa.list_(ParseField(field[field.find('[')+1:field.rfind(',')].strip()), int(field[field.find(',')+1:field.rfind(']')]))
    elif field[:4] == "list":
        return pa.list_(ParseField(field[field.find('[')+1:field.rfind(']')].strip()))
    else:
        print(field)
        return pa.NA

def TokenizeSchemaString(schema_str: str):
    schema_str = schema_str.strip().lower()
    tokens = []
    ignore_comma = False
    token = False
    intermediate_str = ""
    for c in schema_str:
        if c == ']':
            ignore_comma = False
        if c == '[':
            ignore_comma = True
        if not ignore_comma and c == ',':
            token = False
            tokens.append(intermediate_str)
            intermediate_str = ""
            continue
        if not c.isspace():
            token = True
            intermediate_str += c
        if token and c.isspace():
            token = False
            intermediate_str += ' '
    if len(intermediate_str) > 0:
        tokens.append(intermediate_str)
    return tokens

def ParseSchema(schema_str: str, metadata: dict):
    segment_id_type = [
        pa.bool_(),
        pa.int8(),
        pa.int16(),
        pa.int32(),
        pa.int64(),
        pa.uint8(),
        pa.uint16(),
        pa.uint32(),
        pa.uint64(),
        pa.string()]
    tokens = TokenizeSchemaString(schema_str)
    fields = []
    segment_id_columns = metadata["segment_id_info"].split('\u001d')
    segment_id_indexes = []
    for token in tokens:
        key_, _, type_ = token.partition(' ')
        fields.append((key_, ParseField(type_)))
    for col in segment_id_columns:
        for i, tup in enumerate(fields):
            if col == tup[0]:
                if not tup[1] in segment_id_type:
                    raise ValueError(f"Segment id column({tup[0]}, {str(tup[1])}) is not a segment id type.")
                segment_id_indexes.append(int(i))
    binary_indexes = struct.pack("<" + "I"*len(segment_id_indexes), *segment_id_indexes)
    metadata["segment_id_info"] = binary_indexes
    return pa.schema(fields, metadata)

def TokenizeMetadataString(metadata_str: str):
    #metadata__str = metadata_str.strip().lower()
    tokens = []
    ignore_comma = False
    token = False
    intermediate_str = ""
    for c in metadata_str:
        if c == ':':
            token = False
            intermediate_str = intermediate_str.strip()
            intermediate_str += ':'
            continue
        if c == ']':
            ignore_comma = False
        if c == '[':
            ignore_comma = True
        if not ignore_comma and c == ',':
            token = False
            tokens.append(intermediate_str)
            intermediate_str = ""
            continue
        if not c.isspace():
            token = True
            intermediate_str += c
        if token and c.isspace():
            token = False
            intermediate_str += ' '
    if len(intermediate_str) > 0:
        tokens.append(intermediate_str)
    return tokens

def ParseMetadataList(list_str: str):
    result = ''
    tokens = list_str[1:-1].strip().split(',')
    for token in tokens:
        result += token.strip()
        result += '\u001d'
    result = result[:-1]
    return result

def ParseMetadata(metadata_str: str):
    metadata = {}
    tokens = TokenizeMetadataString(metadata_str)
    for token in tokens:
        key, _, value = token.partition(':')
        if value[0] == '[':
            metadata[key] = ParseMetadataList(value)
        else:
            metadata[key] = value
    return metadata

@app.command()
def Create(table_name: str, schema: str, metadata: str):
    """
    Create table with `table_name` and `schema`. Schema should provide segment column information if possible.

    e.g.) client.py create table_name "id int32, name string, attributes list[string], feature fixed_size_list[128, float32]"
    """
    meta = ParseMetadata(metadata)
    meta["table name"] = table_name.lower()
    aschema = ParseSchema(schema, meta)
    schema_msg = aschema.serialize().to_pybytes()
    try:
        result = r.execute_command("table", "create", schema_msg)
        if result == b'OK':
            print("Successful.")
        else:
            print("Failed.")
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")

@app.command()
def List():
    """
    `SQL: SHOW TABLES` equivalent.
    """
    tables = r.execute_command("table", "list")
    if tables == None:
        print("Empty.")
        return
    for i, table in enumerate(tables):
        print(f"{i+1}:\t{table.decode('utf-8')}")

@app.command()
def CheckIndexing(table_name: str):
    """
    Check whether the table is indexing or not.
    """
    isIndexing = r.execute_command("table", "checkindexing", table_name)
    if isIndexing:
        print(f"Table `{table_name}` is indexing.")
    else:
        print(f"Table `{table_name}` is not indexing.")

@app.command()
def Describe(table_name: str):
    """
    Retrieve table information such as schema, ...
    """
    try:
        result = r.execute_command("table", "describe", table_name)
        msg = pa.ipc.read_message(result)
        schema = pa.ipc.read_schema(msg)
        print(schema)
    except redis.exceptions.ResponseError as e:
        print(e)

@app.command()
def Insert(table_name: str, arg: str):
    """
    Insert `row` into table with `table_name`

    e.g.) client.py insert table_name "0, John, [Strong, Tall, Intelligent]"
    (with schema "id int32, name string, attributes list[string]")
    """
    data = ""
    list_flag = False
    for item in arg.split(','):
        to_append = item.strip()
        if to_append[0] == '[':
            list_flag = True
            to_append = to_append[1:].strip()
        if to_append[-1] == ']':
            data += to_append[:-1].strip()
            data += '\u001e'
            list_flag = False
            continue
        if list_flag:
            data += to_append
            data += '\u001d'
        else:
            data += to_append
            data += '\u001e'
    data = data[:-1]
    try:
        result = r.execute_command("table", "insert", table_name, data)
        if result == b'OK':
            print("Successful.")
        else:
            print("Failed.")
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")

@app.command()
def InsertCSV(table_name: str, path: str):
    """
    Insert data from csv file at `path` into table with `table_name`

    e.g.) client.py InsertCSV table_name ~/test_data.csv
    (with schema "id int32, name string, attributes list[string]")
    """
    if not os.path.exists(path):
        print(f"File({path}) does not exists.")
        return
    df = pd.read_csv(path)
    try:
        result = r.execute_command("table", "describe", table_name)
        msg = pa.ipc.read_message(result)
        schema = pa.ipc.read_schema(msg)
        if len(df.columns) != len(schema):
            print(f"File schema does not match with the table schema.")
            return
        # pandas to_csv() format
        for f in schema:
            if "list" in str(f.type):
                df[f.name] = df[f.name].fillna("()").apply(lambda x: eval(x))

        segment_id_info = schema.metadata.get(b'segment_id_info')
        segment_id_column_indexes = struct.unpack_from("<" + "I" * int((len(segment_id_info)/4)), segment_id_info)
        columns = [schema[col].name for col in segment_id_column_indexes]
        failed = False
        for segment_id, values_with_same_id in df.groupby(columns):
            rb = pa.RecordBatch.from_pandas(values_with_same_id, schema=schema)
            output_stream = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(output_stream, schema)
            writer.write_batch(rb)
            writer.close()
            result = r.execute_command("table", "batchinsert", table_name, output_stream.getvalue().to_pybytes())
            if result != b'OK':
                print(f"Failed.({segment_id})")
                failed = True
                break
        if not failed:
            print("Successful.")
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")


# Filter parsing
def ParseParentheses(filter: str):
    stack = []
    stack.append(-1)
    for i, c in enumerate(filter):
        if c == '(':
            stack.append(i)
        elif c == ')' and stack:
            start = stack.pop()
            yield(len(stack), filter[start + 1: i])
    start = stack.pop()
    yield(len(stack), filter[start + 1: ])

def ParseCondition(cond: str):
    RS = '\u001e'  # Record Separator
    if cond.strip().startswith("not "):
        return ParseCondition(cond[4:]) + RS + "not"
    if " is null" in cond:
        return cond.split()[0].strip() + RS + "isnull"
    elif " is not null" in cond:
        return cond.split()[0].strip() + RS + "isnotnull"
    elif " not like " in cond:
        operands = cond.split(" not like ")
        return operands[0].strip() + RS + operands[1].strip() + RS + "like" + RS + "not"
    elif " like " in cond:
        operands = cond.split(" like ")
        return operands[0].strip() + RS + operands[1].strip() + RS + "like"
    elif " not in " in cond:
        operands = cond.split(" not in ")
        return operands[0].strip() + RS + operands[1].strip() + RS + "in" + RS + "not"
    elif " in " in cond:
        operands = cond.split(" in ")
        return operands[0].strip() + RS + operands[1].strip() + RS + "in"
    elif "<=" in cond:
        tokens = cond.partition("<=")  # (op1, <=, op2)
        return tokens[0].strip() + RS + tokens[2].strip() + RS + tokens[1]
    elif "<" in cond:
        tokens = cond.partition("<")
        return tokens[0].strip() + RS + tokens[2].strip() + RS + tokens[1]
    elif ">=" in cond:
        tokens = cond.partition(">=")
        return tokens[0].strip() + RS + tokens[2].strip() + RS + tokens[1]
    elif ">" in cond:
        tokens = cond.partition(">")
        return tokens[0].strip() + RS + tokens[2].strip() + RS + tokens[1]
    elif "=" in cond:
        tokens = cond.partition("=")
        return tokens[0].strip() + RS + tokens[2].strip() + RS + tokens[1]
    else:
        raise ValueError(f"Filter condition({cond}) cannot be parsed.")

def HandleFilterConditions(cond, parsed):
    stack = []
    tokens = []
    intermediate_token = ""
    _in = False
    level = 0
    if cond[0] != "(":
        stack.append(-1)
    for i, c in enumerate(cond):
        if c == '(':
            _in = True
            stack.append(i)
            level += 1
        if not _in:
            if c == ' ':
                if len(stack) == 0:
                    stack.append(i)
                else:
                    start = stack.pop()
                    stack.append(i)
                    token = cond[start+1:i]
                    if '(' in token:
                        continue
                    if token == 'or' or token == 'and':
                        if len(intermediate_token) > 0:
                            tokens.append((False, intermediate_token))
                        tokens.append((False, token))
                        #print(tokens)
                        #print("1 " + intermediate_token)
                        intermediate_token = ""
                    else:
                        if len(intermediate_token) != 0:
                            intermediate_token += " "
                        intermediate_token += token
                        #print("2 " + intermediate_token)
            if i == len(cond)-1:
                start = stack.pop()
                token = cond[start+1:]
                if len(intermediate_token) != 0:
                    intermediate_token += " "
                intermediate_token += token
                #print("3 "+intermediate_token)
        if c == ')':
            start = stack.pop()
            level -= 1
            if level == 0:
                token = cond[start+1:i]
                #print(token)
                tokens.append((True, parsed[token]))
                #print(tokens)
                _in = False
    if len(intermediate_token) != 0:
        tokens.append((False, intermediate_token))

    intermediate_tokens = []
    intermediate_result = ""
    and_ = False
    for already_parsed, token in tokens:
        if not already_parsed:
            if token == 'and':
                and_ = True
                continue
            if token == 'or':
                intermediate_tokens.append(intermediate_result)
                intermediate_result = ""
                continue
        if len(intermediate_result) > 0:
            intermediate_result += '\u001e'
        if already_parsed:
            intermediate_result += token
        else:
            intermediate_result += ParseCondition(token)
        if and_:
            intermediate_result += '\u001e'
            intermediate_result += 'and'
            and_ = False
    if len(intermediate_result) > 0:
        intermediate_tokens.append(intermediate_result)
    result = intermediate_tokens.pop(0)
    while len(intermediate_tokens) > 0:
        result += '\u001e'
        result += intermediate_tokens.pop(0)
        result += '\u001e'
        result += 'or'
    return result

def ParseFilter(filter: str):
    level = 0
    for c in filter:
        if c == '(':
            level += 1
        elif c == ')':
            level -= 1
            if level < 0:
                print("err: ')' is more than '('.")
                return None
    if level != 0:
        print("err: Parentheses is not matched.")
        return None
    target = list(ParseParentheses(filter))
    dict_ = {}
    for item in target:
        dict_[item[1]] = HandleFilterConditions(item[1], dict_)
    return dict_[target[-1][1]]

def FilterSchemaByColumns(schema, column_names):
    # Create a dictionary mapping column names to their fields
    field_dict = {field.name: field for field in schema}

    # Filter and order fields based on the column_names list
    filtered_fields = []
    for name in column_names:
        if name in field_dict:
            filtered_fields.append(field_dict[name])
        else:
            print(f"Warning: Column '{name}' not found in the schema.")

    # Create a new schema with the filtered fields
    return pa.schema(filtered_fields)

def PrintTable(arrow_table, format):
    print(f"Returned Record Count: {arrow_table.num_rows}")

    if format != "pretty" and format != "markdown":
        print(f"Wrong table printing format: {format}. Please use pretty or markdown")
        return

    # Create PrettyTable instance
    table = PrettyTable()

    if format == "markdown":
        table.set_style(MARKDOWN)

    # Add columns to the table
    for name, col in zip(arrow_table.schema.names, arrow_table.columns):
        table.add_column(name, col.to_pylist())

    # Customize the table
    table.align = 'l'  # Left align all columns
    table.border = True
    table.header = True
    table.padding_width = 1

    # Print the table
    print(table)

@app.command()
def Fscan(table_name: str, projection: Annotated[str, typer.Argument(help="Projection List")] = "*", filter: Annotated[str, typer.Option(help="SQL-like filter")] = "", format: Annotated[str, typer.Option(help="Table printing format. 'pretty' or 'markdown'")] = "pretty"):
    """
    Scan table with `table_name` to find data filtered by `filter`.

    e.g.) client.py fscan table_name "*" --filter="id is not null and name like J%"

    e.g.) client.py fscan table_name "col1, col2" --filter="id is not null and name like J%"

    e.g.) client.py fscan table_name "*" --filter="id is not null and name like J%" --format=markdown

    e.g.) client.py fscan table_name "*" --filter="id is not null and name like J%" --format=pretty

    TODO: filter should be easier to write.
    """

    # pre-process filter string
    filter_str = ""
    if filter != "":
        filter_str = ParseFilter(filter)
    if filter_str == None:
        print("Failed to parse filter.")
        return

    try:
        result = r.execute_command("table", "scan", table_name, projection, filter_str)
        reader = pa.ipc.open_stream(result)
        table = reader.read_all()
        PrintTable(table, format)
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")

@app.command()
def Dscan(table_name: str, proj: str, filter: Annotated[str, typer.Option(help="SQL-like filter")] = ""):
    """
    Scan table with `table_name` to find data filtered by `filter`. Table printing format is same as arrow::Table:::ToString.

    e.g.) client.py dscan table_name "col1, col2" --filter="id is not null and name like J%"

    e.g.) client.py dscan table_name "*" --filter="id is not null and name like J%"
    """

    # pre-process projection string
    proj_list = None
    proj_str = "*"
    if proj != "*" and proj != "":
        proj_list = [token.strip() for token in proj.split(',')]
        proj_str = ",".join(proj_list)

    # pre-process filter string
    filter_str = ""
    if filter != "":
        filter_str = ParseFilter(filter)
    if filter_str == None:
        print("Failed to parse filter.")
        return

    try:
        result = r.execute_command("table", "debugscan", table_name, proj_str, filter_str)
        if len(result) >= 1:
            print(result.decode('utf-8'))
        else:
            print("Result set is empty.")
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")

def ParseVector(vector_str: str):
    result = []
    tokens = vector_str.split(',')
    for token in tokens:
        result.append(float(token.strip()))
    return result

def ConvertVector(vector_list: list):
    return struct.pack("<" + "f" * len(vector_list), *vector_list)

@app.command()
def Ann(table_name: str, k: int, query: str, ef_search: Optional[int] = None, projection: Optional[str] = "*", filter: Optional[str] = "", format: Annotated[str, typer.Option(help="table printing format. 'pretty' or 'markdown'")] = "pretty"):
    """
    Approximate `k` nearest neighbor search with `query`. `ef_search`, `projection`, and `filter` is optional.
    """
    if filter != "":
        filter = ParseFilter(filter)
    try:
        query = ConvertVector(ParseVector(query))
        if ef_search is None:
            ef_search = k
        #print(struct.unpack_from("!"+"f"*2, query, 4))
        result = r.execute_command("table", "ann", table_name, k, query, ef_search, projection, filter)
        reader = pa.ipc.open_stream(result)
        table = reader.read_all()
        PrintTable(table, format)
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")

@app.command()
def BatchAnn(table_name: str, k: int, queries: str, ef_search: Optional[int] = None, projection: Optional[str] = "*", filter: Optional[str] = "", format: Annotated[str, typer.Option(help="table printing format. 'pretty' or 'markdown'")] = "pretty"):
    """
    Approximate `k` nearest neighbor search with multiple `query`. `ef_search`, `projection`, and `filter` is optional.
    """
    if filter != "":
        filter = ParseFilter(filter)
    try:
        # Parse the JSON string to a Python list of lists
        import json
        queries_list = json.loads(queries)

        # Validate that we have a list of lists of floats
        if not all(isinstance(q, list) and all(isinstance(x, (int, float)) for x in q) for q in queries_list):
            raise ValueError("Invalid query format. Expected a JSON list of float lists.")

        # Convert the list of lists to a list of numpy arrays
        import numpy as np
        vectors = [np.array(q, dtype=np.float32) for q in queries_list]

        # Ensure all vectors have the same length
        if len(set(len(v) for v in vectors)) != 1:
            raise ValueError("All query vectors must have the same length.")

        size = len(vectors[0])
        concatenated_vectors = np.concatenate(vectors)

        # Convert float vectors to Arrow RecordBatch
        fixed_size_list_array = pa.FixedSizeListArray.from_arrays(concatenated_vectors, size)

        batch = pa.RecordBatch.from_arrays(
            [fixed_size_list_array],
            ['query']
        )

        # Serialize the RecordBatch to bytes
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, batch.schema)
        writer.write_batch(batch)
        writer.close()
        query_record_batch = sink.getvalue().to_pybytes()

        if ef_search is None:
            ef_search = k
        result = r.execute_command("table", "batchann", table_name, k, query_record_batch, ef_search, projection, filter)
        for item in result:
            reader = pa.ipc.open_stream(item)
            table = reader.read_all()
            PrintTable(table, format)
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")

@app.command()
def Drop(table_name: str):
    try:
        result = r.execute_command("table", "drop", table_name)
        if result == b'OK':
            print("Successful.")
        else:
            print("Failed.")
    except redis.exceptions.ResponseError as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    app()
