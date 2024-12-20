#include <algorithm>
#include <cctype>
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
#include <random>
#include <string>
#include <string_view>

#include <arrow/api.h>
#include <arrow/compare.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include "hnswlib/hnswlib.h"

#include "vdb/data/expression.hh"
#include "vdb/vdb.hh"
#include "vdb/vdb_api.hh"
#include "vdb/data/checker.hh"
#include "vdb/common/defs.hh"
#include "vdb/common/system_log.hh"
#include "vdb/common/memory_allocator.hh"
#include "vdb/common/util.hh"
#include "vdb/compute/execution.hh"
#include "vdb/data/expression.hh"
#include "vdb/data/filter.hh"
#include "vdb/data/index_handler.hh"
#include "vdb/data/label_info.hh"
#include "vdb/data/table.hh"

#include "server.h"

// TODO - This macro should be replaced with the proper error handling in the
// future.
#define ARROW_IS_OK(ARROW_FUNC)                                 \
  {                                                             \
    auto status = ARROW_FUNC;                                   \
    if (!status.ok())                                           \
      std::cerr << "- Err) " << status.ToString() << std::endl; \
  }

namespace vdb {

// Test VDB module integration (will be removed or used as a simple unit test?)
void _TestCommand() {
  std::cout << "Arrow Array Test - buliding array with {32, 16, 128}."
            << std::endl;
  std::shared_ptr<arrow::Int32Array> int32arr;
  arrow::Int32Builder builder;
  ARROW_IS_OK(builder.Append(32));
  ARROW_IS_OK(builder.Append(16));
  ARROW_IS_OK(builder.Append(128));
  ARROW_IS_OK(builder.Finish(&int32arr));
  std::cout << int32arr->ToString() << std::endl;

  std::cout << "HSNWLIB test - buildling index with 1000 random points with 16 "
               "dimensions."
            << std::endl;
  hnswlib::L2Space space(4);
  auto alg_hnsw =
      std::make_shared<hnswlib::HierarchicalNSW<float>>(&space, 1000, 16, 128);

  std::mt19937 rng;
  rng.seed(42);
  std::uniform_real_distribution<> distrib;
  float* data = new float[16 * 1000];
  for (int i = 0; i < 16 * 1000; i++) {
    data[i] = distrib(rng);
  }

  for (int i = 0; i < 1000; i++) {
    alg_hnsw->addPoint(data + i * 16, i);
  }

  float correct = 0;
  for (size_t i = 0; i < 1000; i++) {
    std::priority_queue<std::pair<float, hnswlib::labeltype>> result =
        alg_hnsw->searchKnn(data + i * 16, 1);
    hnswlib::labeltype label = result.top().second;
    if (label == i) correct++;
  }
  float recall = correct / 1000;
  std::cout << "Recall: " << recall << std::endl;

  std::string test_schema_string = "ID INT32, NAME STRING, HEIGHT FLOAT32";
  std::cout << "Parsing Schema Test - parsing: " << test_schema_string
            << std::endl;
  auto schema = ParseSchemaFrom(test_schema_string);
  std::cout << schema->ToString(false) << std::endl;

  std::string test_schema_string2 =
      "ID String, Vector Fixed_Size_List[ 1024, Float32 ], Attributes List[  "
      "Int16 ]";
  std::cout << "Parsing Schema Test2 - parsing: " << test_schema_string2
            << std::endl;
  auto schema2 = ParseSchemaFrom(test_schema_string2);
  if (schema2 == nullptr)
    std::cout << "Parse Failed." << std::endl;
  else
    std::cout << schema2->ToString(false) << std::endl;

  std::cout << "VDB Test Success." << std::endl;
}

Status _CreateTableCommand(sds serialized_schema) {
  auto schema_buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<uint8_t*>(serialized_schema), sdslen(serialized_schema));
  auto buf_reader = std::make_shared<arrow::io::BufferReader>(schema_buffer);
  auto maybe_schema = arrow::ipc::ReadSchema(buf_reader.get(), nullptr);
  if (!maybe_schema.ok()) {
    return Status(Status::kInvalidArgument,
                  "CreateTableCommand is Failed: Schema is incorrect. error=" +
                      maybe_schema.status().ToString());
  }

  auto schema = maybe_schema.ValueUnsafe();
  auto table_dictionary = GetTableDictionary();
  auto metadata = schema->metadata();
  auto maybe_table_name = metadata->Get("table name");

  if (!maybe_table_name.ok()) {
    return Status(Status::kInvalidArgument,
                  "CreateTableCommand is Failed: Table name is not provided.");
  }

  auto table_name = maybe_table_name.ValueUnsafe();
  if (std::any_of(table_name.begin(), table_name.end(),
                  [](char c) { return std::isspace(c); }))
    return Status(Status::kInvalidArgument,
                  "CreateTableCommand is Failed: Invalid table name.");

  auto iter = table_dictionary->find(table_name);

  if (iter != table_dictionary->end()) {
    return Status(Status::kAlreadyExists,
                  "CreateTableCommand is Failed: Table already exists.");
  }

  TableBuilderOptions options;
  options.SetTableName(table_name).SetSchema(schema);
  TableBuilder builder{options};
  ARROW_ASSIGN_OR_RAISE(auto table, builder.Build());
  table_dictionary->insert({table_name, table});
  return Status::Ok();
}

Status _DropTableCommand(const std::string& table_name) {
  auto table_dictionary = GetTableDictionary();
  auto iter = table_dictionary->find(table_name);
  if (iter != table_dictionary->end()) {
    table_dictionary->erase(iter);
    SYSTEM_LOG(vdb::LogLevel::kLogVerbose, "Table (%s) Dropping is Done.",
               table_name.data());
    return Status::Ok();
  } else {
    return Status(Status::kInvalidArgument,
                  "DropTableCommand is Failed: Table does not exists.");
  }
}

Status _InsertCommand(const std::string& table_name,
                      const std::string_view record_view) {
  auto table_dictionary = GetTableDictionary();

  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return Status(Status::kInvalidArgument,
                  "InsertCommand is Failed: Invalid table name.");
  }

  auto table = iter->second;
  auto schema = table->GetSchema();
  auto metadata = schema->metadata();
  auto segment_id_info = metadata->Get("segment_id_info").ValueOr("");
  auto segment_id_info_count = segment_id_info.size() / sizeof(uint32_t);

  std::string segment_id = "";
  auto tokens = Tokenize(record_view, kRS);

  for (uint32_t i = 0; i < segment_id_info_count; i++) {
    uint32_t pid = *(reinterpret_cast<uint32_t*>(segment_id_info.data()) + i);
    segment_id += std::string{tokens[pid]};
    segment_id += kRS;
  }
  segment_id.resize(segment_id.size() - 1);
  auto segment = table->GetSegment(segment_id);
  if (!segment) {
    segment = table->AddSegment(table, segment_id);
    if (segment == nullptr)
      return Status(Status::kInvalidArgument,
                    "InsertCommand is Failed: failed to create segment.");
  }

  return segment->AppendRecord(record_view);
}

std::string _GetFirstValueStringFrom(
    std::shared_ptr<arrow::Array>& arr,
    const std::shared_ptr<arrow::DataType>& type) {
  switch (type->id()) {
#define __CASE(ARROW_TYPE, ARR_TYPE, RET_VAL)                    \
  {                                                              \
    case ARROW_TYPE: {                                           \
      auto casted_arr = std::static_pointer_cast<ARR_TYPE>(arr); \
      auto val = casted_arr->GetView(0);                         \
      return RET_VAL;                                            \
    } break;                                                     \
  }
    __CASE(arrow::Type::BOOL, arrow::BooleanArray, val ? "t" : "0");
    __CASE(arrow::Type::INT8, arrow::Int8Array, std::to_string(val));
    __CASE(arrow::Type::INT16, arrow::Int16Array, std::to_string(val));
    __CASE(arrow::Type::INT32, arrow::Int32Array, std::to_string(val));
    __CASE(arrow::Type::INT64, arrow::Int64Array, std::to_string(val));
    __CASE(arrow::Type::UINT8, arrow::UInt8Array, std::to_string(val));
    __CASE(arrow::Type::UINT16, arrow::UInt16Array, std::to_string(val));
    __CASE(arrow::Type::UINT32, arrow::UInt32Array, std::to_string(val));
    __CASE(arrow::Type::UINT64, arrow::UInt64Array, std::to_string(val));
    __CASE(arrow::Type::STRING, arrow::StringArray, std::string(val));
    __CASE(arrow::Type::LARGE_STRING, arrow::LargeStringArray,
           std::string(val));
#undef __CASE
    default:
      return "";
  }
}

arrow::Result<std::string> _GetSegmentIdFrom(
    std::vector<std::shared_ptr<arrow::RecordBatch>>& rbs,
    std::vector<uint32_t>& column_indexes) {
  /* all records in recordbatch have same value in segment id columns. */
  std::string segment_id = "";
  if (!rbs.empty()) {
    auto& rb = rbs[0];
    auto& schema = rb->schema();
    for (size_t i = 0; i < column_indexes.size(); i++) {
      auto column_index = column_indexes[i];
      auto field = schema->field(column_index);
      auto column = rb->column(column_index);
      segment_id += _GetFirstValueStringFrom(column, field->type());
      if (i != column_indexes.size() - 1) {
        segment_id += kRS;
      }
    }
  } else {
    return arrow::Status::Invalid("Empty record batch vector");
  }
  return segment_id;
}

Status _BatchInsertCommand(const std::string& table_name, sds serialized_rbs) {
  auto table_dictionary = GetTableDictionary();

  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return Status(Status::kInvalidArgument,
                  "TableBatchInsertCommand is Failed: Invalid table name. ");
  }

  auto table = iter->second;
  auto schema = table->GetSchema();
  auto rbs_buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<uint8_t*>(serialized_rbs), sdslen(serialized_rbs));
  auto maybe_reader = arrow::ipc::RecordBatchStreamReader::Open(
      std::make_shared<arrow::io::BufferReader>(rbs_buffer));
  if (!maybe_reader.ok()) {
    return Status(Status::kInvalidArgument,
                  "TableBatchInsertCommand is Failed: " +
                      maybe_reader.status().ToString());
  }
  auto reader = maybe_reader.ValueUnsafe();
  auto rbs_schema = reader->schema();
  auto status = CheckRecordBatchIsInsertable(rbs_schema, schema);
  if (!status.ok()) {
    return arrow::Status::Invalid("TableBatchInsertCommand is Failed: " +
                                  status.ToString());
  }

  auto maybe_rbs = reader->ToRecordBatches();
  if (!maybe_rbs.ok()) {
    return Status(
        Status::kInvalidArgument,
        "TableBatchInsertCommand is Failed: " + maybe_rbs.status().ToString());
  }
  auto rbs = maybe_rbs.ValueUnsafe();

  /* segment id check */
  auto segment_id_column_indexes = table->GetSegmentIdColumnIndexes();

  /* build segment id */
  auto maybe_segment_id = _GetSegmentIdFrom(rbs, segment_id_column_indexes);

  if (!maybe_segment_id.ok()) {
    return Status(Status::kUnknown, "TableBatchInsertCommand is Failed: " +
                                        maybe_segment_id.status().ToString());
  }
  auto segment_id = maybe_segment_id.ValueUnsafe();

  /* find or create segment */
  auto segment = table->GetSegment(segment_id);

  if (!segment) {
    segment = table->AddSegment(table, segment_id);
    if (segment == nullptr) {
      return Status(
          Status::kInvalidArgument,
          "TableBatchInsertCommand is Failed: Failed to create a segment.");
    }
  }

  return segment->AppendRecords(rbs);
}

arrow::Result<std::shared_ptr<arrow::Table>> ScanImpl(
    const std::string& table_name, std::string_view projection,
    std::string_view filter) {
  auto table_dictionary = GetTableDictionary();

  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return arrow::Status::Invalid("ScanCommand is Failed: Invalid table name.");
  }

  auto table = iter->second;
  auto schema = table->GetSchema();
  auto whole_filter = vdb::Filter::Parse(filter, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);

  auto segments = table->GetSegments();
  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Scanning %lu segments from %lu segments for table %s",
             filtered_segments.size(), segments.size(), table_name.data());

  auto maybe_filter = whole_filter
                          ? std::optional{GetExpression(whole_filter, schema)}
                          : std::nullopt;

  return _ExecutePlan(schema, SegmentGenerator(filtered_segments),
                      std::optional{projection}, maybe_filter);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> _ScanCommand(
    const std::string& table_name, std::string_view projection,
    std::string_view filter) {
  ARROW_ASSIGN_OR_RAISE(auto filtered_table,
                        ScanImpl(table_name, projection, filter));
  auto maybe_serialized = SerializeTable(filtered_table);
  ARROW_RETURN_NOT_OK(maybe_serialized);
  return maybe_serialized.ValueUnsafe();
}

arrow::Result<std::string> _DebugScanCommand(const std::string& table_name,
                                             std::string_view projection,
                                             std::string_view filter) {
  ARROW_ASSIGN_OR_RAISE(auto filtered_table,
                        ScanImpl(table_name, projection, filter));
  return filtered_table->ToString();
}

std::vector<std::string_view> _ListTableCommand() {
  std::vector<std::string_view> table_list;
  auto table_dictionary = GetTableDictionary();
  for (auto it = table_dictionary->begin(); it != table_dictionary->end();
       it++) {
    table_list.emplace_back(it->first);
  }
  return table_list;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> _DescribeTableCommand(
    const std::string& table_name) {
  auto table_dictionary = GetTableDictionary();
  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return arrow::Status::Invalid(
        "DescribeTableCommand is Failed: Table does not exists.");
  }
  return arrow::ipc::SerializeSchema(*iter->second->GetSchema(), &arrow_pool);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> _AnnCommand(
    const std::string& table_name, size_t k, const sds query, size_t ef_search,
    std::string_view projection, std::string_view filter) {
  auto table_dictionary = GetTableDictionary();
  arrow::Status status;

  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return arrow::Status::Invalid("AnnCommand is Failed: Invalid table name.");
  }

  auto table = iter->second;
  auto schema = table->GetSchema();
  auto ann_column_id =
      std::stoi(schema->metadata()->Get("ann_column_id").ValueOr("-1"));

  /* if no ann_column is set, ann search is not supported. */
  if (ann_column_id == -1) {
    return arrow::Status::Invalid(
        "AnnCommand is Failed: Table does not support ann.");
  }

  auto dimension = std::static_pointer_cast<arrow::FixedSizeListType>(
                       schema->field(ann_column_id)->type())
                       ->list_size();

  if (dimension != sdslen(query) / sizeof(float)) {
    return arrow::Status::Invalid(
        "AnnCommand is Failed: Query dimension is not matched.");
  }
  auto raw_query = reinterpret_cast<float*>(query);

  auto segments = table->GetSegments();

  auto whole_filter = vdb::Filter::Parse(filter, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);
  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);
  auto dist_builder = std::make_unique<arrow::FloatBuilder>();
  std::shared_ptr<arrow::Array> dist_column;

  /* set k as max(k, ef_search). in hnsw algorithm, k is used for resizing final
   * result. so, set k as max(k, ef_Search) and resizing it to k in caller
   * function. */
  auto search_size = max(k, ef_search);

  // TODO: This implementation *WILL BE and SHOULD BE* replaced with
  // the ThreadPool version of the implementation.
  const size_t kFilteredSegmentSize = filtered_segments.size();
  std::vector<std::tuple<float, size_t, Segment*>> ann_result(
      kFilteredSegmentSize * search_size);
  std::vector<std::thread> threads;
  for (size_t i = 0; i < kFilteredSegmentSize; i++) {
    threads.emplace_back([&, i]() {
      SYSTEM_LOG(LogLevel::kLogDebug, "Start of scan thread. %lu", i);
      const size_t thread_offset = i * search_size;
      auto index_handler = filtered_segments[i]->IndexHandler();
      auto sub_result = index_handler->SearchKnn(raw_query, search_size);
      for (size_t j = 0; j < sub_result->size(); j++) {
        auto& [distance, label] = (*sub_result)[j];
        ann_result[thread_offset + j] = {distance, label,
                                         filtered_segments[i].get()};
      }
      SYSTEM_LOG(LogLevel::kLogDebug, "End of scan thread. %lu", i);
    });
  }

  for (auto& thread : threads) thread.join();

  std::sort(ann_result.begin(), ann_result.end());

  size_t ann_result_cnt = 0;
  std::map<std::shared_ptr<arrow::RecordBatch>, std::vector<uint32_t>>
      record_ids_per_rb;
  std::map<std::shared_ptr<arrow::RecordBatch>, std::vector<float>>
      dists_per_rb;

  // grouping records and distances with set unit
  for (auto& [dist, label, segment] : ann_result) {
    if (segment == nullptr) continue;  // this is empty slot
    if (ann_result_cnt == k) break;
    uint32_t set_number = vdb::LabelInfo::GetSetNumber(label);
    size_t record_number = vdb::LabelInfo::GetRecordNumber(label);
    auto rb = segment->GetRecordbatch(set_number);
    record_ids_per_rb[rb].push_back(record_number);
    dists_per_rb[rb].push_back(dist);

    ann_result_cnt++;
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> filtered_batches;
  for (auto& [rb, vec] : record_ids_per_rb) {
    ARROW_ASSIGN_OR_RAISE(
        auto result, arrow::compute::Take(
                         rb, arrow::UInt32Array(
                                 vec.size(), arrow::Buffer::FromVector(vec))));
    filtered_batches.push_back(result.record_batch());
    ARROW_RETURN_NOT_OK(dist_builder->AppendValues(dists_per_rb[rb]));
  }
  ARROW_RETURN_NOT_OK(dist_builder->Finish(&dist_column));

  ARROW_ASSIGN_OR_RAISE(auto schema_with_dist,
                        schema->AddField(schema->fields().size(),
                                         std::make_shared<arrow::Field>(
                                             "distance", arrow::float32())));

  ARROW_ASSIGN_OR_RAISE(auto filtered_table,
                        arrow::Table::FromRecordBatches(filtered_batches));

  ARROW_ASSIGN_OR_RAISE(
      auto filtered_table_with_dist,
      filtered_table->AddColumn(
          schema->fields().size(),
          std::make_shared<arrow::Field>("distance", arrow::float32()),
          std::make_shared<arrow::ChunkedArray>(dist_column)));

  ARROW_ASSIGN_OR_RAISE(auto rb_with_dist,
                        filtered_table_with_dist->CombineChunksToBatch());

  ARROW_ASSIGN_OR_RAISE(
      auto result_table,
      _ExecutePlan(schema_with_dist, RecordBatchGenerator(rb_with_dist),
                   std::make_optional(projection), std::nullopt));

  auto maybe_serialized_rb = SerializeTable(result_table);
  ARROW_RETURN_NOT_OK(maybe_serialized_rb);

  return maybe_serialized_rb.ValueUnsafe();
}

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> _BatchAnnCommand(
    std::string_view table_name, size_t k, const sds query_vectors,
    size_t ef_search, std::string_view projection, std::string_view filter) {
  ARROW_ASSIGN_OR_RAISE(auto table, GetTable(std::string(table_name)));
  auto schema = table->GetSchema();
  ARROW_ASSIGN_OR_RAISE(auto dimension, table->GetAnnDimension());
  ARROW_ASSIGN_OR_RAISE(auto rbs, MakeRecordBatchesFromSds(query_vectors));
  ARROW_RETURN_NOT_OK(vdb::CheckQueryVectors(rbs, dimension));

  SYSTEM_LOG(LogLevel::kLogDebug, "BatchAnnCommand is called for table %s",
             table_name.data());

  auto segments = table->GetSegments();
  auto whole_filter = vdb::Filter::Parse(filter, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);
  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);

  SYSTEM_LOG(LogLevel::kLogDebug, "Filtered %lu segments from %lu segments.",
             filtered_segments.size(), segments.size());

  /* set k as max(k, ef_search). in hnsw algorithm, k is used for resizing final
   * result. so, set k as max(k, ef_Search) and resizing it to k in caller
   * function. */
  auto search_size = max(k, ef_search);

  std::vector<std::shared_ptr<arrow::Table>> result_tables;
  for (auto& rb : rbs) {
    auto query_vectors = std::static_pointer_cast<arrow::FixedSizeListArray>(
        rb->GetColumnByName("query"));
    for (size_t i = 0; i < static_cast<size_t>(query_vectors->length()); i++) {
      ARROW_ASSIGN_OR_RAISE(auto query, query_vectors->GetScalar(i));
      SYSTEM_LOG(LogLevel::kLogDebug, "Processing query vector %lu, %s", i,
                 query->ToString().c_str());
      auto raw_query_array =
          std::static_pointer_cast<arrow::FixedSizeListScalar>(query)->value;
      auto* raw_query =
          const_cast<float*>(raw_query_array->data()->GetValues<float>(
              1));  // I don't understand why it starts from 1 not 0

      std::vector<std::tuple<float, size_t, Segment*>> ann_result(
          filtered_segments.size() * search_size);
      std::vector<std::thread> threads;
      for (size_t i = 0; i < filtered_segments.size(); i++) {
        threads.emplace_back([&, i]() {
          SYSTEM_LOG(LogLevel::kLogDebug, "Start of ann thread. %lu", i);
          const size_t thread_offset = i * search_size;
          auto index_handler = filtered_segments[i]->IndexHandler();
          auto sub_result = index_handler->SearchKnn(raw_query, search_size);
          for (size_t j = 0; j < sub_result->size(); j++) {
            auto& [distance, label] = (*sub_result)[j];
            ann_result[thread_offset + j] = {distance, label,
                                             filtered_segments[i].get()};
          }
          SYSTEM_LOG(LogLevel::kLogDebug, "End of ann thread. %lu", i);
        });
      }
      std::for_each(threads.begin(), threads.end(),
                    [](std::thread& t) { t.join(); });

      std::sort(ann_result.begin(), ann_result.end());

      ARROW_ASSIGN_OR_RAISE(
          auto result_table,
          _BuildAnnResultTable(ann_result, schema, k, projection));
      result_tables.push_back(result_table);
    }
  }

  std::vector<std::shared_ptr<arrow::Buffer>> result_buffers;
  for (auto& table : result_tables) {
    ARROW_ASSIGN_OR_RAISE(auto serialized_table, SerializeTable(table));
    result_buffers.push_back(serialized_table);
  }

  return result_buffers;
}

// Returns whether the background thread of this table is indexing
// or not.
arrow::Result<bool> _CheckIndexingCommand(const std::string& table_name) {
  auto table_dictionary = GetTableDictionary();

  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return arrow::Status::Invalid(
        "CheckIndexingCommand is failed: Invalid table name.");
  }

  auto table = iter->second;
  return table->IsIndexing();
}

// Returns numbers of total elements and indexed elements in the table with
// `table_name`.
arrow::Result<std::pair<size_t, size_t>> _CountIndexedElementsCommand(
    const std::string& table_name) {
  auto table_dictionary = GetTableDictionary();

  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return arrow::Status::Invalid(
        "CountIndexedElementsCommand is failed: Invalid table name.");
  }

  auto table = iter->second;
  size_t indexed_count = 0;
  size_t total_count = 0;
  for (auto& [_, segment] : table->GetSegments()) {
    indexed_count += segment->IndexHandler()->CountIndexedElements();
    total_count += segment->Size();
  }
  return std::pair{total_count, indexed_count};
}

vdb::map<std::string, std::shared_ptr<vdb::Table>>* GetTableDictionary() {
  return static_cast<vdb::map<std::string, std::shared_ptr<vdb::Table>>*>(
      server.table_dictionary);
}

arrow::Result<std::shared_ptr<vdb::Table>> GetTable(
    const std::string& table_name) {
  auto table_dictionary = GetTableDictionary();
  auto iter = table_dictionary->find(table_name);
  if (iter == table_dictionary->end()) {
    return arrow::Status::Invalid("Could not find table: " + table_name);
  }
  return iter->second;
}

arrow::Result<std::shared_ptr<arrow::Table>> _ExecutePlan(
    std::shared_ptr<arrow::Schema> schema,
    arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> generator,
    std::optional<std::string_view> maybe_projection,
    std::optional<arrow::compute::Expression> maybe_filter) {
  std::vector<arrow::acero::Declaration> decls;
  decls.emplace_back(arrow::acero::Declaration(
      "source",
      arrow::acero::SourceNodeOptions(schema->RemoveMetadata(), generator)));

  if (maybe_filter) {
    SYSTEM_LOG(LogLevel::kLogDebug, "Filter is applied.");
    auto bind_result = maybe_filter->Bind(*schema);
    if (!bind_result.ok()) {
      return bind_result.status();
    }
    auto bound_filter = bind_result.ValueUnsafe();
    decls.emplace_back(arrow::acero::Declaration(
        "filter", arrow::acero::FilterNodeOptions(bound_filter)));
  } else {
    SYSTEM_LOG(LogLevel::kLogDebug, "No filter is applied.");
  }

  std::string_view projection = maybe_projection.value_or("*");
  ARROW_ASSIGN_OR_RAISE(auto projection_exprs,
                        vdb::expression::Expression::ParseSimpleProjectionList(
                            projection, schema));

  if (!projection_exprs.empty()) {
    std::vector<arrow::compute::Expression> exprs;
    for (const auto& proj : projection_exprs) {
      exprs.emplace_back(proj->BuildArrowExpression());
    }
    decls.emplace_back(arrow::acero::Declaration{
        "project", arrow::acero::ProjectNodeOptions(exprs)});
  }

  std::shared_ptr<arrow::Table> filtered_table;
  decls.emplace_back(arrow::acero::Declaration{
      "table_sink", arrow::acero::TableSinkNodeOptions(&filtered_table)});
  arrow::acero::Declaration decl = arrow::acero::Declaration::Sequence(decls);

  auto maybe_plan = arrow::acero::ExecPlan::Make();
  ARROW_ASSIGN_OR_RAISE(auto plan, maybe_plan);
  ARROW_RETURN_NOT_OK(decl.AddToPlan(plan.get()).status());
  ARROW_RETURN_NOT_OK(plan->Validate());

  /* The plan involves using one or more threads for pipelined execution
   * processing. */
  SYSTEM_LOG(LogLevel::kLogDebug, "Start executing plan.");
  plan->StartProducing();
  auto finished = plan->finished();
  ARROW_RETURN_NOT_OK(finished.status());
  SYSTEM_LOG(LogLevel::kLogDebug, "End executing plan.");
  return filtered_table;
}

arrow::Result<std::vector<std::unique_ptr<arrow::ArrayBuilder>>>
_MakeArrayBuildersForAnn(std::shared_ptr<arrow::Schema> schema) {
  std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
  for (auto& field : schema->fields()) {
    ARROW_ASSIGN_OR_RAISE(auto builder,
                          arrow::MakeBuilder(field->type(), &arrow_pool));
    builders.push_back(std::move(builder));
  }
  ARROW_ASSIGN_OR_RAISE(auto builder,
                        arrow::MakeBuilder(arrow::float32(), &arrow_pool));
  builders.push_back(std::move(builder));
  return std::move(builders);
}

arrow::Result<std::vector<std::shared_ptr<arrow::Array>>>
_MakeResultColumnsForAnn(
    const std::vector<std::unique_ptr<arrow::ArrayBuilder>>& builders) {
  std::vector<std::shared_ptr<arrow::Array>> result_columns;
  for (const auto& builder : builders) {
    ARROW_ASSIGN_OR_RAISE(auto array, builder->Finish());
    result_columns.push_back(std::move(array));
  }
  return std::move(result_columns);
}

arrow::Result<std::shared_ptr<arrow::Table>> _BuildAnnResultTable(
    const std::vector<std::tuple<float, size_t, Segment*>>& ann_result,
    std::shared_ptr<arrow::Schema> schema, size_t k,
    std::string_view projection) {
  ARROW_ASSIGN_OR_RAISE(auto builders, _MakeArrayBuildersForAnn(schema));

  size_t ann_result_cnt = 0;
  for (const auto& [dist, label, segment] : ann_result) {
    if (segment == nullptr) continue;  // this is empty slot
    if (ann_result_cnt == k) break;
    uint32_t set_number = vdb::LabelInfo::GetSetNumber(label);
    size_t record_number = vdb::LabelInfo::GetRecordNumber(label);
    auto rb = segment->GetRecordbatch(set_number);

    for (size_t i = 0; i < schema->fields().size(); i++) {
      ARROW_ASSIGN_OR_RAISE(auto scalar,
                            rb->column(i)->GetScalar(record_number));
      ARROW_RETURN_NOT_OK(builders[i]->AppendScalar(*scalar));
    }

    ARROW_RETURN_NOT_OK(
        reinterpret_cast<arrow::FloatBuilder*>(builders.back().get())
            ->Append(dist));
    ann_result_cnt++;
  }

  ARROW_ASSIGN_OR_RAISE(auto result_columns,
                        _MakeResultColumnsForAnn(builders));
  ARROW_ASSIGN_OR_RAISE(auto new_schema,
                        schema->AddField(schema->fields().size(),
                                         std::make_shared<arrow::Field>(
                                             "distance", arrow::float32())));

  auto rb = arrow::RecordBatch::Make(new_schema->RemoveMetadata(),
                                     ann_result_cnt, result_columns);

  ARROW_ASSIGN_OR_RAISE(
      auto result_table,
      _ExecutePlan(new_schema, RecordBatchGenerator(rb),
                   std::make_optional(projection), std::nullopt));

  return result_table;
}

}  // namespace vdb
