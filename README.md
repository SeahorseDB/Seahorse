# SeahorseDB

The seahorse, a small marine fish, shares its name with the hippocampus, a vital brain region responsible for transferring short-term memory to long-term memory. Similarly, SeahorseDB, an in-memory database built on Redis, is designed for fast and scalable similarity search, acting as the computer's equivalent of the hippocampus for efficient data consolidation and retrieval. To ensure optimal performance, SeahorseDB incorporates various optimizations, making it a robust and high-performance solution for managing complex data operations.

# Based on Redis 7.2.6
This project is built upon Redis 7.2.6 (https://redis.io), an open-source in-memory data structure store.  
Redis is licensed under the BSD 3-Clause License, and its copyright is held by Salvatore Sanfilippo.


# Quick Start

We recommend using the Docker images and the provided script to get started.

```bash
./run.sh
```

Once the service is running, you can use the RESTful APIs to perform operations.

```bash
curl -X POST "http://127.0.0.1:3000/health"
```


# API Documentation

### **Nodes**
API for managing nodes in SeahorseDB.

#### **Get List of SeahorseDB Nodes**
   - **URL:** `/v0/nodes`
   - **Method:** `GET`
   - **Description:** Retrieves a list of available SeahorseDB nodes.
   - **Responses:**
     - `200 OK`: Successfully retrieved the list of nodes.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes
     ```

#### **Add SeahorseDB Nodes**
   - **URL:** `/v0/nodes`
   - **Method:** `POST`
   - **Description:** Registers new SeahorseDB nodes in the service.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object containing the details of the nodes to be registered.
   - **Responses:**
     - `200 OK`: Nodes are successfully registered.
     - `400 Bad Request`: Invalid input provided.

   - **Curl Example:**
     ```bash
     curl -X POST http://localhost:3000/v0/nodes \
          -H "Content-Type: application/json" \
          -d '{"nodes" : [{"name": "node1", "endpoint": "redis://127.0.0.1:6379"}] }'
     ```

#### **Delete SeahorseDB Node**
   - **URL:** `/v0/nodes/{node_name}`
   - **Method:** `DELETE`
   - **Description:** Deletes the existing SeahorseDB node from the service.
   - **Responses:**
     - `200 OK`: The node is successfully deleted.
     - `400 Bad Request`: Invalid input provided.

   - **Curl Example:**
     ```bash
     curl -X DELETE http://localhost:3000/v0/nodes/node1
     ```

#### **Ping SeahorseDB Node**
   - **URL:** `/v0/nodes/{node_name}/ping`
   - **Method:** `GET`
   - **Description:** Pings a specific SeahorseDB node to check if it is responsive.
   - **Path Parameters:**
     - `node_name` (string): The name of the node to ping.
   - **Responses:**
     - `200 OK`: Node is responsive.
     - `404 Not Found`: Node not found.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes/node1/ping
     ```

### **Tables**
API for managing tables within SeahorseDB nodes.

#### **Get Tables in SeahorseDB Node**
   - **URL:** `/v0/nodes/{node_name}/tables`
   - **Method:** `GET`
   - **Description:** Retrieves a list of tables available in the given SeahorseDB node.
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
   - **Responses:**
     - `200 OK`: Successfully retrieved the list of tables.
     - `404 Not Found`: Node not found.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes/node1/tables
     ```

#### **Create a Table**
   - **URL:** `/v0/nodes/{node_name}/tables`
   - **Method:** `POST`
   - **Description:** Creates a new table in the given SeahorseDB node.
   - **Headers:**
     - `Content-Type: application/json`
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
   - **Request Body:**
     - JSON object containing the table definition (DDL).
   - **Responses:**
     - `200 OK`: Table is successfully created.
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
                  segment_id_info = '[col1, col4]',  # list of keys for sharding
                  ann_column_id = '3',
                  index_space = 'L2Space',
                  ef_construction = '50',
                  active_set_size_limit = '10',
                  index_type = 'HNSW'
                )"
      }
     ```

#### **Drop a Table in SeahorseDB Node**
   - **URL:** `/v0/nodes/{node_name}/tables/{table_name}`
   - **Method:** `DELETE`
   - **Description:** Drop the table in the given SeahorseDB node.
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
     - `table_name` (string): The name of the table to be deleted.
   - **Responses:**
     - `200 OK`: Table is successfully deleted.
     - `404 Not Found`: Node or table not found.
	- `500 Internal Server Error`: Error occurred while trying to delete the table.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/v0/nodes/node1/tables/my_table
     ```

#### **Import data into table from the file path**
   - **URL:** `/v0/nodes/{node_name}/tables/{table_name}/import`
   - **Method:** `POST`
   - **Description:** Insert data from the given file path. The service will read the file in the specified path.
   - **Headers:**
     - `Content-Type: application/json`
   - **Path Parameters:**
     - `node_name` (string): The name of the node.
     - `table_name` (string): The name of the table to get schema.
   - **Responses:**
     - `200 OK`: Data successfully inserted.
     - `400 Bad Request`: Invalid input provided.
     - `404 Not Found`: Node or table not found.
     - `500 Internal Server Error`: Error occurred while inserting data.

   - **Curl Example:**
     ```bash
	curl -X POST "http://localhost:3000/v0/nodes/node1/tables/my_table/import" 
		-H  "Content-Type: application/json" 
		-d "\"format\":\"Parquet\",\"file_path\":\"/path/to/data.parquet\"}"
     ```

#### **Describe Table**
   - **URL:** `/v0/nodes/{node_name}/tables/{table_name}/schema`
   - **Method:** `GET`
   - **Description:** Provides detailed information about the structure and metadata of the specified table in the specified SeahorseDB node.
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

### **Search**
API for querying data in SeahorseDB.

#### **Scan SeahorseDB Table**
   - **URL:** `/v0/scan`
   - **Method:** `POST`
   - **Description:** Performs a scan operation across all SeahorseDB nodes, returning relevant data from the specified table. It is almost same as normal SQL "SELECT" execution of normal RDBMSs.
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
          -d '{"table_name":"my_table", \
	          "projection":"*", \
	          "result_format": "Json"}'
     ```

#### **ANN request to SeahorseDB Table**
   - **URL:** `/v0/ann`
   - **Method:** `POST`
   - **Description:** Executes an Approximate Nearest Neighbors (ANN) operation across all SeahorseDB nodes on the specified table.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object specifying the ANN criteria.
	     - **table_name**: name of the target table for ANN
		- **topk**: specify the number of ANN results
		- **ef_search**: The size of the dynamic list to store candidates used during ANN searches. Higher number leads to more accurate, but slower searches.
		- **vector**: Query vector embedding to search. List of floating-point number string delimited by comma (,)
		- **projection**: List of the columns to be retrieved.
		- **result_format**: format of the resultset. Default is Json.
   - **Responses:**
     - `200 OK`: Successfully retrieved the ANN results.
     - `400 Bad Request`: Invalid ANN criteria provided.

   - **Curl Example:**
     ```bash
     # default result format is Json
     curl -X POST http://localhost:3000/v0/ann \
          -H "Content-Type: application/json" \
          -d '{"table_name": "my_table", \
	          "topk": 10, \
			  "vector": "0.001,0.02324,-0.9052" \
			  "filter": "id < 10", \
			  "result_format": "Json"}'
     ```

#### **Batch ANN request to SeahorseDB Table**
   - **URL:** `/v0/batch-ann`
   - **Method:** `POST`
   - **Description:** Executes batch Approximate Nearest Neighbors (ANN) operations across all SeahorseDB nodes on the specified table.
   - **Headers:**
     - `Content-Type: application/json`
   - **Request Body:**
     - JSON object specifying the ANN criteria.
	     - **table_name**: name of the target table for ANN
		- **topk**: specify the number of ANN results
		- **ef_search**: The size of the dynamic list to store candidates used during ANN searches. Higher number leads to more accurate, but slower searches.
		- **vectors**: Target batch query vectors to search. List of floating-point number string delimited by comma (,)
		- **projection**: List of the columns to be retrieved.
		- **result_format**: format of the resultset. Default is Json.
   - **Responses:**
     - `200 OK`: Successfully retrieved the ANN results.
     - `400 Bad Request`: Invalid ANN criteria provided.

   - **Curl Example:**
     ```bash
     # default result format is Json
     curl -X POST http://localhost:3000/v0/ann \
          -H "Content-Type: application/json" \
          -d '{"table_name": "my_table", \
	          "topk": 10, \
			  "vector": [[0.001,0.02324,-0.9052], \
						  [0.3,0.823,0.001]] \
				"projection": "id, title, timestamp", \
			  "result_format": "Json"}'
     ```


### **Log**
API for log handling

#### **Get Log Level**
   - **URL:** `/log/level`
   - **Method:** `GET`
   - **Description:** Retrieves the current log level of the service. `error`, `warn`, `info`, `debug`, and `trace` are valid log levels.
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
   - **Description:** Sets the log level for the service. `error`, `warn`, `info`, `debug`, and `trace` are valid log levels.
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

### **Health**
API for monitoring the health of SeahorseDB.

#### **Health Check**
   - **URL:** `/health`
   - **Method:** `GET`
   - **Description:** Performs a health check to ensure the service is operational.
   - **Responses:**
     - `200 OK`: Service is healthy.

   - **Curl Example:**
     ```bash
     curl -X GET http://localhost:3000/health
     ```
# **Contributors**
- Sehyeon Oh(sehyeon@dnotitia.com)
- Moohyeon Nam(mhnam@dnotitia.com)
- Hyunsoo Cho(hyunsoo_cho@dnotitia.com)
- Juhyeon Jeong(juhyeon.jeong@dnotitia.com)
- Jooho Kim(joohok@dnotitia.com)
- Seongil Park(athjk3@dnotitia.com)
