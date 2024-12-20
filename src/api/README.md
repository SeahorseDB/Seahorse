# Coral: API server for SeahorseDB

Coral is the RESTful API server for [SeahorseDB](https://github.com/dnload/SeahorseDB). It manages the catalog of the database and orchestrates distributed vector similarity search across multiple SeahorseDB nodes.

## 1. Prerequiste for development
### Installing Rust

Reference: https://www.rust-lang.org/learn/get-started

#### Rustup: the Rust installer and version management tool

The primary way that folks install Rust is through a tool called Rustup, which is a Rust installer and version management tool.

It looks like you're running macOS, Linux, or another Unix-like OS. To download Rustup and install Rust, run the following in your terminal, then follow the on-screen instructions. See "[Other Installation Methods](https://forge.rust-lang.org/infra/other-installation-methods.html)" if you are on Windows.

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```


#### Is Rust up to date?
Rust updates very frequently. If you have installed Rustup some time ago, chances are your Rust version is out of date. Get the latest version of Rust by running `rustup update`

```
rustup update
```

## 2. Development

### Clone repository
You can clone this repository by using following commands.

```
git clone git@github.com:dnload/Coral.git
```

or

```
git clone https://github.com/dnload/Coral.git
```

### Build and Run

When you install Rustup you'll also get the latest stable version of the Rust build tool and package manager, also known as Cargo. You can build the project using

- build your project with `cargo build`
- run your project with `cargo run`
- format your rust source code with `cargo fmt`
  - Prerequiste: need to install rustfmt `rustup component add rustfmt`

## 3. REST APIs v0

The port number is figured as '3000'.

Based on the provided Rust `actix-web` routing and handler setup, here is the updated REST API documentation:

### REST API Documentation

#### **Health Check**
   - **URL:** `/health`
   - **Method:** `GET`
   - **Description:** A simple health check endpoint to verify that the service is running.
   - **Responses:**
     - `200 OK`: Service is running.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/health
     ```

#### **Get Log Level**
   - **URL:** `/log/level`
   - **Method:** `GET`
   - **Description:** Retrieves the current log level of the service. 'error', 'warn', 'info', 'debug', 'trace' are valid log levels.
   - **Responses:**
     - `200 OK`: Successfully retrieved the log level.
     - `500 Internal Server Error`: Error occurred while retrieving the log level.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/log/level
     ```

#### **Set Log Level**
   - **URL:** `/log/level`
   - **Method:** `POST`
   - **Description:** Sets the log level for the service. 'error', 'warn', 'info', 'debug', 'trace' are valid log levels.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object containing the new log level.
   - **Responses:**
     - `200 OK`: Log level successfully set.
     - `400 Bad Request`: Invalid log level provided.

   - **Curl Example:**
     ```bash
     curl -X POST http://localhost:3000/log/level \
          -H "Content-Type: application/json" \
          -d '{"level": "info"}'
     ```

#### **Get Log Print Record Batch Setting**
   - **URL:** `/log/print_record_batch`
   - **Method:** `GET`
   - **Description:** Retrieves the current log print record batch setting of the service. 'true' means print the record batch in logs.
   - **Responses:**
     - `200 OK`: Successfully retrieved the log print record batch setting.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/log/print_record_batch
     ```

#### **Set Log Print Record Batch Setting**
   - **URL:** `/log/print_record_batch`
   - **Method:** `POST`
   - **Description:** Sets the log print record batch setting for the service. 'true' means print the record batch in logs.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object containing the new log print record batch setting.
   - **Responses:**
     - `200 OK`: Log print record batch setting successfully set.

   - **Curl Example:**
     ```bash
     curl -X POST http://localhost:3000/log/print_record_batch \
          -H "Content-Type: application/json" \
          -d '{"enabled": true}'
     ```

#### **Get VDB Nodes**
   - **URL:** `/v0/nodes`
   - **Method:** `GET`
   - **Description:** Retrieves a list of available VDB nodes.
   - **Responses:**
     - `200 OK`: Successfully retrieved the list of nodes.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes
     ```

#### **Add VDB Nodes**
   - **URL:** `/v0/nodes`
   - **Method:** `POST`
   - **Description:** Registers new nodes in the VDB service.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object containing the details of the nodes to be registered.
   - **Responses:**
     - `200 OK`: Nodes successfully registered.
     - `400 Bad Request`: Invalid input provided.

   - **Curl Example:**
     ```bash
     curl -X POST http://localhost:3000/v0/nodes \
          -H "Content-Type: application/json" \
          -d '{"nodes" : [{"name": "node1", "endpoint": "redis://127.0.0.1:6379"}] }'
     ```

#### **Ping VDB Node**
   - **URL:** `/v0/nodes/{node_name}/ping`
   - **Method:** `GET`
   - **Description:** Pings a specific VDB node to check if it is responsive.
   - **Path Parameters:**
     - `node_name` (string): The name of the node to ping.
   - **Responses:**
     - `200 OK`: Node is responsive.
     - `404 Not Found`: Node not found.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes/node1/ping
     ```

#### **Get VDB Tables**
   - **URL:** `/v0/nodes/{node_name}/tables`
   - **Method:** `GET`
   - **Description:** Retrieves a list of tables available in the specified VDB node.
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
   - **Responses:**
     - `200 OK`: Successfully retrieved the list of tables.
     - `404 Not Found`: Node not found.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes/node1/tables
     ```

#### **Create VDB Table**
   - **URL:** `/v0/nodes/{node_name}/tables`
   - **Method:** `POST`
   - **Description:** Creates a new table in the specified VDB node.
   - **Headers:**
     - `Content-Type: application/json`
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
   - **Request Body:**
     - JSON object containing the table definition (DDL).
   - **Responses:**
     - `200 OK`: Table successfully created.
     - `400 Bad Request`: Invalid input provided.
     - `404 Not Found`: Node not found.

   - **Curl Example:**
     ```bash
     curl -X POST http://localhost:3000/v0/nodes/node1/tables \
          -H "Content-Type: application/json" \
          -d @create.json

      # create.json content:
      {
        "ddl": "CREATE TABLE my_table
                (
                  col1 INT,
                  col2 ARRAY<INT>,
                  col3 INT[4],
                  col4 VARCHAR
                )
                WITH
                (
                  segment_id_info = '[col1, col4]',
                  ann_column_id = '3',
                  index_space = 'L2Space',
                  ef_construction = '50',
                  active_set_size_limit = '10',
                  index_type = 'HNSW'
                )"
      }
     ```

#### **schema VDB Table**
   - **URL:** `/v0/nodes/{node_name}/tables/{table_name}/schema`
   - **Method:** `GET`
   - **Description:** Provides detailed information about the structure and metadata of the specified table in the specified VDB node.
   - **Headers:**
     - `Content-Type: application/json`
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
     - `table_name` (string): The name of the table to get schema.
   - **Responses:**
     - `200 OK`: Successfully retrieved the table description in Apache Arrow schema format.
     - `404 Not Found`: Node or table not found.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes/node1/tables/my_table/schema
     ```

#### **Drop VDB Table**
   - **URL:** `/v0/nodes/{node_name}/tables/{table_name}`
   - **Method:** `DELETE`
   - **Description:** Deletes the specified table from the specified VDB node.
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
     - `table_name` (string): The name of the table to be deleted.
   - **Responses:**
     - `200 OK`: Table successfully deleted.
     - `404 Not Found`: Node or table not found.
     - `500 Internal Server Error`: Error occurred while trying to delete the table.

   - **Curl Example:**
     ```bash
     curl -X DELETE http://localhost:3000/v0/nodes/node1/tables/my_table
     ```

#### **Generate Create Table Statement from a File**
   - **URL:** `/utils/ddl`
   - **Method:** `POST`
   - **Description:** Create a DDL statement from file schema. The request body should be a file that has schema (e.g., Parquet).
   - **Headers:**
     - `Content-Type: application/octet-stream`
   - **Parameters:**
     - `format`: The file format. Currently supports only parquet file.
   - **Request Body:**
     - Parquet file as bytes.
   - **Responses:**
     - `200 OK`: Successfully returned the DDL statement.
     - `400 Bad Request`: Unsupported file format.
     - `500 Internal Server Error`: Error occurred while producing the DDL statement.

   - **Curl Example:**
     ```bash
     curl -X POST http://localhost:3000/v0/utils/ddl?format=parquet \
          -H "Content-Type: application/octet-stream" \
          --data-binary @example.parquet
     ```

#### **Insert Data into VDB Table from a Parquet File**
   - **URL:** `/v0/nodes/{node_name}/tables/{table_name}`
   - **Method:** `PUT`
   - **Description:** Inserts data into a VDB table from a Parquet file. This endpoint can also create the table if it doesn't exist.
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
     - `table_name` (string): The name of the table to insert data into.
   - **Headers:**
     - `Content-Type: multipart/form-data`
   - **Request Body:**
     - Multipart form data with two parts:
       1. `json`: A JSON file containing metadata and options for the insert operation.
       2. `file`: The Parquet file containing the data to be inserted.
   - **JSON File Structure:**
     ```json
     {
       "format": "Parquet",
       "with_create": true,
       "metadata": {
         "segment_id_info": "[video_id]",
         "ann_column_id": "2",
         "index_space": "ipspace",
         "ef_construction": "200",
         "M": "16",
         "index_type": "hnsw"
       }
     }
     ```
     - `format`: The format of the input file (currently only "Parquet" is supported).
     - `with_create` (optional): If true, the table will be created if it doesn't exist.
     - `metadata` (optional): Additional metadata to be associated with the table:
       - `segment_id_info`: Specifies the column(s) to be used as segment IDs.
       - `ann_column_id`: The ID of the column to be used for approximate nearest neighbor (ANN) search.
       - `index_space`: The type of index space to be used (e.g., "ipspace").
       - `ef_construction`: A parameter for the HNSW index construction.
       - `M`: A parameter for the HNSW index construction.
       - `index_type`: The type of index to be used (e.g., "hnsw").
   - **Responses:**
     - `200 OK`: Data successfully inserted.
     - `400 Bad Request`: Invalid input provided.
     - `404 Not Found`: Node or table not found.
     - `500 Internal Server Error`: Error occurred while inserting data.

   - **Curl Example:**
     ```bash
     curl -X PUT http://localhost:3000/v0/nodes/node1/tables/parquet_test \
          -H "Content-Type:multipart/form-data" \
          -F "json=@/path/to/with_create.json;type=application/json" \
          -F "file=@/path/to/youtube_snapshot_embedding_fixed_size_list.parquet"
     ```

   - **Notes:**
     - The `json` file should be a valid JSON file containing the insert options and metadata.
     - The `file` should be a valid Parquet file containing the data to be inserted.
     - If `with_create` is true in the JSON file, the table will be created if it doesn't exist, using the schema from the Parquet file.
     - The `metadata` in the JSON file provides important information for table creation and indexing, such as segment ID columns, ANN search configuration, and index parameters.

#### **Scan VDB Table**
   - **URL:** `/v0/scan`
   - **Method:** `POST`
   - **Description:** Performs a scan operation across all VDB nodes, returning relevant data from the specified table.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object specifying the scan criteria.
   - **Responses:**
     - `200 OK`: Successfully retrieved the scan results.
     - `400 Bad Request`: Invalid scan criteria provided.

   - **Curl Example:**
     ```bash
     # default result format is Json
     curl -X POST http://localhost:3000/v0/scan \
          -H "Content-Type: application/json" \
          -d '{"table_name":"my_table", "projection":"col1, col2", "filter":"col1=1"}'
     ```
     ```bash
     # result format arrow ipc, filter string need to use U+001E ('1E') as delimiter for token instead of space currently
     curl -X POST http://localhost:3000/v0/scan \
          -H "Content-Type: application/json" \
          -d '{"table_name":"my_table", "projection":"col1, col2", "filter":"col1\u{1e}=\u{1e}1", "result_format": "ArrowIpc"}'
     ```

#### **ANN Operation on VDB Table**
   - **URL:** `/v0/ann`
   - **Method:** `POST`
   - **Description:** Executes an Approximate Nearest Neighbors (ANN) operation across all VDB nodes on the specified table.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object specifying the ANN criteria.
   - **Responses:**
     - `200 OK`: Successfully retrieved the ANN results.
     - `400 Bad Request`: Invalid ANN criteria provided.

   - **Curl Example:**
     ```bash
     # default result format is Json
     curl -X POST http://localhost:3000/v0/ann \
          -H "Content-Type: application/json" \
          -d '{"table_name": "my_table", "topk": 10, "vector": "0.001,0.02324,-0.9052"}'
     ```
     ```bash
     # result format arrow ipc
     curl -X POST http://localhost:3000/v0/ann \
          -H "Content-Type: application/json" \
          -d '{"table_name": "my_table", "topk": 10, "vector": "0.001,0.02324,-0.9052", "result_format": "ArrowIpc"}'
     ```