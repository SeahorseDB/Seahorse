#include <memory>
#include <random>

#include <arrow/api.h>
#include <arrow/acero/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/type_fwd.h>
#include <string>

#include "vdb/common/memory_allocator.hh"
#include "vdb/data/table.hh"
#include "vdb/tests/util_for_test.hh"

std::string empty_string = "";
/* to string */
std::string VecToStr(const float *vec, size_t dim) {
  std::stringstream ss;

  ss << "[ ";
  for (size_t i = 0; i < dim; i++) {
    ss << vec[i];
    if (i != dim - 1) ss << ", ";
  }

  ss << " ]";
  return ss.str();
}
/* comparison */
bool EmbeddingEquals(const float *lhs, const float *rhs, size_t dimension) {
  for (size_t i = 0; i < dimension; i++) {
    if (lhs[i] != rhs[i]) {
      std::cout << i << "th Value in Embedding is NOT MATCHED!" << std::endl;
      std::cout << lhs[i] << std::endl;
      std::cout << rhs[i] << std::endl;
      return false;
    }
  }
  return true;
}

bool IndexEquals(std::shared_ptr<vdb::VectorIndex> &lhs,
                 std::shared_ptr<vdb::VectorIndex> &rhs) {
  /* compare metadata */
  if (lhs->Size() != rhs->Size()) {
    std::cout << "Index Size Compare is failed" << std::endl;
    std::cout << lhs->Size() << std::endl;
    std::cout << rhs->Size() << std::endl;
    return false;
  }

  if (lhs->Dimension() != rhs->Dimension()) {
    std::cout << "Index Dimension is NOT MATCHED!" << std::endl;
    std::cout << lhs->Dimension() << std::endl;
    std::cout << rhs->Dimension() << std::endl;
    return false;
  }

  if (lhs->ToString().compare(rhs->ToString()) != 0) {
    std::cout << "Index ToString is NOT MATCHED!" << std::endl;
    std::cout << lhs->ToString() << std::endl;
    std::cout << rhs->ToString() << std::endl;
    return false;
  }

  /* compare embeddings */
  for (size_t i = 0; i < lhs->Size(); i++) {
    auto embedding_1 = lhs->GetEmbedding(i);
    auto embedding_2 = rhs->GetEmbedding(i);
    if (!EmbeddingEquals(embedding_1, embedding_2, lhs->Dimension())) {
      std::cout << i << "th embedding is NOT MATCHED!" << std::endl;
      return false;
    }
  }
  /* TODO compare edges */
  return true;
}

bool IndexHandlerEquals(std::shared_ptr<vdb::IndexHandler> &lhs,
                        std::shared_ptr<vdb::IndexHandler> &rhs) {
  /* compare index count */
  if (lhs->Size() != rhs->Size()) {
    std::cout << "Index Count is NOT MATCHED!" << std::endl;
    std::cout << lhs->Size() << std::endl;
    std::cout << rhs->Size() << std::endl;
    return false;
  }
  /* compare indexes */
  for (size_t i = 0; i < lhs->Size(); i++) {
    auto index_1 = lhs->Index(i);
    auto index_2 = rhs->Index(i);
    if (!IndexEquals(index_1, index_2)) {
      std::cout << i << "th Index is NOT MATCHED!" << std::endl;
      return false;
    }
  }
  return true;
}

bool SegmentEquals(std::shared_ptr<vdb::Segment> &lhs,
                   std::shared_ptr<vdb::Segment> &rhs) {
  /* compare inactive batches */
  size_t lhs_inactive_set_count = lhs->InactiveSetCount();
  size_t rhs_inactive_set_count = rhs->InactiveSetCount();
  if (lhs_inactive_set_count != rhs_inactive_set_count) {
    std::cout << "Segment Inactive Set Count is NOT MATCHED! "
              << lhs_inactive_set_count << " " << rhs_inactive_set_count
              << std::endl;
    return false;
  }
  auto lhs_segment_id = lhs->GetId();
  auto rhs_segment_id = rhs->GetId();
  if (lhs_segment_id.compare(rhs_segment_id) != 0) {
    std::cout << "Segment Id is NOT MATCHED! " << lhs_segment_id << " "
              << rhs_segment_id << std::endl;
    return false;
  }
  for (size_t i = 0; i < lhs_inactive_set_count; i++) {
    auto lhs_inactive_rb = lhs->InactiveSetRecordBatch(i);
    auto rhs_inactive_rb = rhs->InactiveSetRecordBatch(i);
    if (!lhs_inactive_rb->Equals(*rhs_inactive_rb)) {
      std::cout << i << "th Inactive Rb is NOT MATCHED!" << std::endl;
      std::cout << lhs_inactive_rb->ToString() << std::endl;
      std::cout << rhs_inactive_rb->ToString() << std::endl;
      return false;
    }
  }
  /* compare active batches */
  auto lhs_active_rb = lhs->ActiveSetRecordBatch();
  auto rhs_active_rb = rhs->ActiveSetRecordBatch();
  if (!lhs_active_rb->Equals(*rhs_active_rb)) {
    std::cout << "Active rb is NOT MATCHED!" << std::endl;
    std::cout << lhs_active_rb->ToString() << std::endl;
    std::cout << rhs_active_rb->ToString() << std::endl;
    return false;
  }

  if (lhs->HasIndex() || rhs->HasIndex()) {
    if (lhs->HasIndex() != rhs->HasIndex()) {
      return false;
    }
    auto lhs_handler = lhs->IndexHandler();
    auto rhs_handler = rhs->IndexHandler();
    if (!IndexHandlerEquals(lhs_handler, rhs_handler)) {
      return false;
    }
  }

  return true;
}

bool TableEquals(std::shared_ptr<vdb::Table> &lhs,
                 std::shared_ptr<vdb::Table> &rhs) {
  /* compare schema */
  if (!lhs->GetSchema()->Equals(rhs->GetSchema(), true)) {
    std::cout << "Table Schema is NOT MATCHED!" << std::endl;
    std::cout << lhs->GetSchema()->ToString() << std::endl;
    std::cout << rhs->GetSchema()->ToString() << std::endl;
    return false;
  }
  /* compare segments */
  for (auto [lhs_id, lhs_segment] : lhs->GetSegments()) {
    auto rhs_segment = rhs->GetSegment(lhs_id.data());
    if (rhs_segment == nullptr) {
      std::cout << lhs_id
                << "id: segment in " + rhs->GetTableName() + " is not found"
                << std::endl;
      return false;
    }
    if (!SegmentEquals(lhs_segment, rhs_segment)) {
      std::cout << lhs_id
                << " id: segment in " + rhs->GetTableName() + " is not found"
                << std::endl;
      return false;
    }
  }
  return true;
}

/* check index build complete */
bool TableIndexFullBuildCheck(std::shared_ptr<vdb::Table> &table,
                              const bool print_log) {
  auto segments = table->GetSegments();
  for (auto &[segment_id, segment] : segments) {
    std::shared_ptr<vdb::IndexHandler> index_handler = segment->IndexHandler();
    size_t num_index = index_handler->Size();

    for (size_t i = 0; i < num_index; i++) {
      auto index = index_handler->Index(i);
      auto index_element_count = index->Size();
      auto rb = segment->GetRecordbatch(i);
      size_t row_count = (size_t)rb->num_rows();
      if (index_element_count != row_count) {
        if (print_log) {
          std::cout << "Index is not completely built in "
                    << "segment id " << segment_id << ", set number " << i
                    << ". (index element count: " << index_element_count
                    << ", row count: " << row_count << ")" << std::endl;
        }
        return false;
      }
    }
  }
  return true;
}

/* generate sample data */
std::vector<std::vector<float>> generateSequentialFloatArray(int rows, int dim,
                                                             int start) {
  int cols = dim;
  float start_val = static_cast<float>(start);
  std::vector<std::vector<float>> result(rows, std::vector<float>(cols));

  for (int i = 0; i < rows; ++i) {
    for (int j = 0; j < cols; ++j) {
      result[i][j] = start_val;
    }
    start_val++;
  }

  return result;
}

std::vector<std::vector<float>> generateRandomFloatArray(int rows, int dim,
                                                         bool xy_same) {
  int cols = dim;
  std::vector<std::vector<float>> result(rows, std::vector<float>(cols));

  srand(static_cast<unsigned int>(time(0)));
  for (int i = 0; i < rows; ++i) {
    float value = static_cast<float>(rand()) * 10 / RAND_MAX;
    for (int j = 0; j < cols; ++j) {
      /* random float number is assigned */
      if (xy_same) {
        result[i][j] = value;
      } else {
        result[i][j] = static_cast<float>(rand()) * 10 / RAND_MAX;
      }
    }
  }

  return result;
}

arrow::Result<std::shared_ptr<arrow::Array>> GenerateRandomInt32Array(
    int64_t length) {
  arrow::Int32Builder builder(&vdb::arrow_pool);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 100);

  for (int64_t i = 0; i < length; ++i) {
    ARROW_RETURN_NOT_OK(builder.Append(dis(gen)));
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Array>> GenerateRandomFloat32Array(
    int64_t length) {
  arrow::FloatBuilder builder(&vdb::arrow_pool);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dis(0.0, 100.0);

  for (int64_t i = 0; i < length; ++i) {
    ARROW_RETURN_NOT_OK(builder.Append(dis(gen)));
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Array>> GenerateRandomStringArray(
    int64_t length) {
  arrow::StringBuilder builder(&vdb::arrow_pool);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 100);

  for (int64_t i = 0; i < length; ++i) {
    ARROW_RETURN_NOT_OK(builder.Append("str" + std::to_string(dis(gen))));
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::Array>>
GenerateRandomFloatFixedSizeListArray(int64_t length, int32_t list_size) {
  auto value_builder = std::make_shared<arrow::FloatBuilder>();
  arrow::FixedSizeListBuilder builder(&vdb::arrow_pool, value_builder,
                                      list_size);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dis(0.0, 100.0);

  for (int64_t i = 0; i < length; ++i) {
    ARROW_RETURN_NOT_OK(builder.Append());
    for (int32_t j = 0; j < list_size; ++j) {
      ARROW_RETURN_NOT_OK(value_builder->Append(dis(gen)));
    }
  }

  std::shared_ptr<arrow::Array> array;
  ARROW_RETURN_NOT_OK(builder.Finish(&array));
  return array;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> GenerateRecordBatch(
    std::shared_ptr<arrow::Schema> schema, int64_t length, int32_t dimension) {
  auto maybe_int_array = GenerateRandomInt32Array(length);
  if (!maybe_int_array.ok()) {
    return maybe_int_array.status();
  }
  auto int_array = maybe_int_array.ValueUnsafe();

  auto maybe_float_array = GenerateRandomFloat32Array(length);
  if (!maybe_float_array.ok()) {
    return maybe_float_array.status();
  }
  auto float_array = maybe_float_array.ValueUnsafe();
  auto maybe_string_array = GenerateRandomStringArray(length);
  if (!maybe_string_array.ok()) {
    return maybe_string_array.status();
  }
  auto string_array = maybe_string_array.ValueUnsafe();
  auto maybe_fixed_size_list_array =
      GenerateRandomFloatFixedSizeListArray(length, dimension);
  if (!maybe_fixed_size_list_array.ok()) {
    return maybe_fixed_size_list_array.status();
  }
  auto fixed_size_list_array = maybe_fixed_size_list_array.ValueUnsafe();
  std::shared_ptr<arrow::RecordBatch> record_batch = arrow::RecordBatch::Make(
      schema, length,
      std::vector{int_array, string_array, float_array, fixed_size_list_array});
  return record_batch;
}

/* exact knn */
float L2Sqr(const void *pVect1v, const void *pVect2v, const void *qty_ptr) {
  float *pVect1 = (float *)pVect1v;
  float *pVect2 = (float *)pVect2v;
  size_t qty = *((size_t *)qty_ptr);

  float res = 0;
  for (size_t i = 0; i < qty; i++) {
    float t = *pVect1 - *pVect2;
    pVect1++;
    pVect2++;
    res += t * t;
  }
  return (res);
}

std::vector<std::pair<float, const float *>> SearchExactKnn(
    const float *query, const std::vector<std::vector<float>> &points,
    size_t k) {
  size_t dim = points[0].size();
  std::vector<std::pair<float, const float *>> distances;

  for (const auto &point_vec : points) {
    auto point = point_vec.data();
    float dist = L2Sqr(query, point, &dim);
    distances.push_back(std::make_pair(dist, point));
  }

  std::sort(distances.begin(), distances.end());

  distances.resize(k);
  return distances;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeRecordBatches(
    std::shared_ptr<arrow::Schema> schema,
    std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches) {
  // Create an in-memory output stream
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::BufferOutputStream::Create());

  // Create an IPC stream writer
  ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeStreamWriter(out, schema));

  for (auto &record_batch : record_batches) {
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*record_batch));
  }

  ARROW_RETURN_NOT_OK(writer->Close());
  return out->Finish();
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTable(
    std::shared_ptr<arrow::Table> table) {
  // Create an in-memory output stream
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::BufferOutputStream::Create());

  // Create an IPC stream writer
  ARROW_ASSIGN_OR_RAISE(auto writer,
                        arrow::ipc::MakeStreamWriter(out, table->schema()));

  ARROW_RETURN_NOT_OK(writer->WriteTable(*table));

  ARROW_RETURN_NOT_OK(writer->Close());
  return out->Finish();
}

arrow::Result<std::shared_ptr<arrow::Table>> DeserializeToTableFrom(
    const std::shared_ptr<arrow::Buffer> &serialized_rb) {
  ARROW_ASSIGN_OR_RAISE(
      auto reader,
      arrow::ipc::RecordBatchStreamReader::Open(
          std::make_shared<arrow::io::BufferReader>(serialized_rb)));
  return reader->ToTable();
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
DeserializeToRecordBatchesFrom(
    const std::shared_ptr<arrow::Buffer> &serialized_rb) {
  ARROW_ASSIGN_OR_RAISE(
      auto reader,
      arrow::ipc::RecordBatchStreamReader::Open(
          std::make_shared<arrow::io::BufferReader>(serialized_rb)));
  return reader->ToRecordBatches();
}

arrow::Result<std::shared_ptr<arrow::Table>> SortTable(
    const std::shared_ptr<arrow::Table> &table) {
  std::vector<arrow::compute::SortKey> sort_keys;
  for (const auto &field : table->schema()->fields()) {
    if (field->type()->id() != arrow::Type::LIST) {
      sort_keys.emplace_back(field->name());
    }
  }

  std::vector<arrow::acero::Declaration> decls;
  std::shared_ptr<arrow::Table> output_table;
  decls.emplace_back(arrow::acero::Declaration(
      "table_source", arrow::acero::TableSourceNodeOptions(table)));
  decls.emplace_back(arrow::acero::Declaration{
      "order_by", arrow::acero::OrderByNodeOptions(sort_keys)});
  decls.emplace_back(arrow::acero::Declaration{
      "table_sink", arrow::acero::TableSinkNodeOptions(&output_table)});
  arrow::acero::Declaration decl = arrow::acero::Declaration::Sequence(decls);

  auto maybe_plan = arrow::acero::ExecPlan::Make();
  ARROW_ASSIGN_OR_RAISE(auto plan, maybe_plan);
  ARROW_RETURN_NOT_OK(decl.AddToPlan(plan.get()).status());
  ARROW_RETURN_NOT_OK(plan->Validate());

  /* The plan involves using one or more threads for pipelined execution
   * processing. */
  plan->StartProducing();
  auto finished = plan->finished();
  ARROW_RETURN_NOT_OK(finished.status());

  return output_table;
}

/* snapshot test names */
std::string GetBaseSnapshotDirectoryName(long long sequence) {
  std::stringstream ss;
  ss << server.aof_filename << "." << sequence;
  ss << ".base.vdb";

  return ss.str();
}

std::string GetTempSnapshotDirectoryName(pid_t pid) {
  /* vdb snapshot is always base of aof file. */
  std::stringstream ss;
  ss << "temp-rewriteaof-vdb-" << pid;
  ss << ".aof";

  return ss.str();
}

/* etc */
std::vector<std::pair<float, const float *>> ResultToDistEmbeddingPairs(
    std::shared_ptr<vdb::Hnsw> &index,
    std::priority_queue<std::pair<float, size_t>> &pq) {
  std::vector<std::pair<float, const float *>> result;

  while (!pq.empty()) {
    auto index_pair = pq.top();
    auto index_dist = index_pair.first;
    auto index_point = index->GetEmbedding(index_pair.second);
    result.emplace(result.begin(), index_dist, index_point);
    pq.pop();
  }
  return result;
}

std::vector<std::pair<float, const float *>> ResultToDistEmbeddingPairs(
    std::shared_ptr<IndexHandlerForTest> &index_handler,
    std::vector<std::pair<float, size_t>> &vec) {
  std::vector<std::pair<float, const float *>> result;

  for (auto index_pair : vec) {
    auto index_dist = index_pair.first;
    auto index_point = index_handler->GetEmbedding(index_pair.second);
    result.emplace_back(index_dist, index_point);
  }
  return result;
}

float CalculateKnnAccuracy(
    std::vector<std::pair<float, const float *>> &index_result,
    std::vector<std::pair<float, const float *>> &knn_result) {
  float accept_count = 0.0;
  float farthest = knn_result.back().first;
  for (size_t i = 0; i < knn_result.size(); i++) {
    auto index_pair = index_result[i];
    auto index_dist = index_pair.first;
    if (index_dist <= farthest) {
      accept_count++;
    }
  }
  return accept_count * 100.0 / static_cast<float>(knn_result.size());
}

vdb::Status CreateTableForTest(std::string table_name,
                               std::string schema_string) {
  auto table_dictionary = vdb::GetTableDictionary();

  if (std::any_of(table_name.begin(), table_name.end(),
                  [](char c) { return std::isspace(c); }))
    return vdb::Status(vdb::Status::kInvalidArgument,
                       "CreateTableForTestCommand: Invalid table name.");

  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    auto schema = vdb::ParseSchemaFrom(schema_string);
    if (schema != nullptr) {
      uint32_t idx = 0;
      uint32_t *idx_ptr = &idx;
      std::string segment_id_info(reinterpret_cast<char *>(idx_ptr),
                                  sizeof(uint32_t));
      auto metadata = std::make_shared<arrow::KeyValueMetadata>(
          std::unordered_map<std::string, std::string>(
              {{"segment_id_info", segment_id_info},
               {"table name", table_name},
               {"active_set_size_limit",
                std::to_string(server.vdb_active_set_size_limit)}}));
      schema = schema->WithMetadata(metadata);
      vdb::TableBuilderOptions options;
      vdb::TableBuilder builder{
          std::move(options.SetTableName(table_name).SetSchema(schema))};
      ARROW_ASSIGN_OR_RAISE(auto table, builder.Build());
      table_dictionary->insert({table_name, table});
    } else {
      return vdb::Status(vdb::Status::kInvalidArgument,
                         "CreateTableForTestCommand: Invalid schema.");
    }
    return vdb::Status::Ok();
  }

  return vdb::Status(vdb::Status::kAlreadyExists,
                     "CreateTableForTestCommand: Table already exists.");
}
