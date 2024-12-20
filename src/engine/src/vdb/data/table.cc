#include <sched.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>

#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/array/array_nested.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "vdb/vdb_api.hh"
#include "vdb/common/defs.hh"
#include "vdb/common/memory_allocator.hh"
#include "vdb/common/status.hh"
#include "vdb/common/system_log.hh"
#include "vdb/common/util.hh"
#include "vdb/data/index_handler.hh"
#include "vdb/data/label_info.hh"
#include "vdb/data/metadata.hh"
#include "vdb/data/mutable_array.hh"
#include "vdb/data/table.hh"

#ifdef __cplusplus
extern "C" {
#include "server.h"
}
#endif

namespace vdb {
Table::Table(const std::string& table_name,
             std::shared_ptr<arrow::Schema>& schema)
    : table_name_{table_name},
      table_version_{0},
      schema_{schema},
      segments_{},
      active_set_size_limit_{0} {}

Table::Table() : segments_{}, active_set_size_limit_{0} {}

Table::~Table() {
  if (indexing_thread_) StopIndexingThread();
}

Status Table::LoadSegments(std::shared_ptr<Table> table,
                           std::string& table_directory_path) {
  /* load segments */
  std::string segment_ids_file_path(table_directory_path);
  segment_ids_file_path.append("/ids.segment");
  size_t segment_ids_size = std::filesystem::file_size(segment_ids_file_path);
  auto segment_ids_buffer =
      static_cast<uint8_t*>(AllocateAligned(64, segment_ids_size));
  /* read segment count and segment ids */
  auto status =
      ReadFrom(segment_ids_file_path, segment_ids_buffer, segment_ids_size, 0);
  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Table (%s) Loading Segment Ids from (%s) is Failed: %s",
               table_name_.data(), segment_ids_file_path.data(),
               status.ToString().data());
    return status;
  }
  uint64_t offset = 0;
  uint64_t segment_count;
  /* get segment count */
  offset += GetLengthFrom(segment_ids_buffer, segment_count);
  for (uint64_t i = 0; i < segment_count; i++) {
    uint64_t segment_id_size;
    /* get segment id */
    std::string segment_id;
    offset += GetLengthFrom(segment_ids_buffer + offset, segment_id_size);
    if (segment_id_size > 0) {
      segment_id.resize(segment_id_size);
      memcpy(segment_id.data(), segment_ids_buffer + offset, segment_id_size);
      offset += segment_id_size;
    }
    std::string segment_directory_path(table_directory_path);
    segment_directory_path.append("/segment.");
    segment_directory_path.append(std::to_string(i));
    /* load segment */
    auto segment = std::make_shared<vdb::Segment>(table, segment_id,
                                                  segment_directory_path);
    segments_.emplace(segment->GetId(), segment);
  }
  DeallocateAligned(segment_ids_buffer, segment_ids_size);
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Table (%s) Loading Segments from (%s/segment#) is Done.",
             table_name_.data(), table_directory_path.data());
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Table (%s) Loading from (%s) is Done.", table_name_.data(),
             table_directory_path.data());
  return Status::Ok();
}

Status Table::LoadManifest(std::string& file_path) {
  size_t manifest_size = std::filesystem::file_size(file_path);
  auto buffered_manifest =
      static_cast<uint8_t*>(AllocateAligned(64, manifest_size));
  /* read table version and schema */
  auto status = ReadFrom(file_path, buffered_manifest, manifest_size, 0);
  if (!status.ok()) {
    return status;
  }
  uint64_t buffer_offset = 0;
  /* get table version */
  uint64_t table_version;
  buffer_offset += GetLengthFrom(buffered_manifest, table_version);
  table_version_ = static_cast<size_t>(table_version);
  /* get schema */
  uint64_t serialized_schema_size;
  buffer_offset +=
      GetLengthFrom(buffered_manifest + buffer_offset, serialized_schema_size);
  uint8_t* serialized_schema = (buffered_manifest + buffer_offset);
  buffer_offset += serialized_schema_size;
  auto serialized_schema_buffer = vdb::make_shared<arrow::Buffer>(
      serialized_schema, serialized_schema_size);
  auto buf_reader =
      vdb::make_shared<arrow::io::BufferReader>(serialized_schema_buffer);
  arrow::ipc::DictionaryMemo dictionary_memo;
  auto maybe_schema =
      arrow::ipc::ReadSchema(buf_reader.get(), &dictionary_memo);
  DeallocateAligned(buffered_manifest, manifest_size);
  if (!maybe_schema.ok()) {
    return Status(Status::kInvalidArgument,
                  "Schema is incorrect. - " + maybe_schema.status().ToString());
  }
  auto schema = maybe_schema.ValueUnsafe();
  status = SetSchema(schema);
  if (!status.ok()) {
    return status;
  }
  auto metadata = schema->metadata();
  auto maybe_table_name = metadata->Get("table name");
  if (!maybe_table_name.ok()) {
    return Status(Status::kInvalidArgument,
                  "Table name is not provided. - " +
                      maybe_table_name.status().ToString());
  }
  /* get table name */
  auto table_name = maybe_table_name.ValueUnsafe();
  table_name_ = table_name;

  return Status::Ok();
}

Status Table::Save(std::string_view& snapshot_directory_path) {
  std::error_code ec;
  std::string table_directory_path(snapshot_directory_path);
  table_directory_path.append("/");
  table_directory_path.append(table_name_);

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Table (%s) Saving to (%s) is Started.", table_name_.data(),
             table_directory_path.data());

  if (!std::filesystem::create_directory(table_directory_path, ec)) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Table (%s) Saving to (%s) is Failed: %s", table_name_.data(),
               table_directory_path.data(), ec.message().data());
    throw std::invalid_argument(ec.message());
  }

  /* save all segment id in one file */
  std::string segment_id_file_path(table_directory_path);
  segment_id_file_path.append("/ids.segment");
  auto status = SaveSegmentIds(segment_id_file_path);
  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Table (%s) Saving to (%s) is Failed: %s", table_name_.data(),
               table_directory_path.data(), status.ToString().data());
    return status;
  }

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Table (%s) Saving Segment Ids to (%s) is Done.",
             table_name_.data(), segment_id_file_path.data());
  /* save segments */
  uint64_t segment_index = 0;
  for (auto [key, segment] : GetSegments()) {
    std::string segment_directory_path(table_directory_path);
    segment_directory_path.append("/segment.");
    segment_directory_path.append(std::to_string(segment_index++));

    auto status = segment->Save(segment_directory_path);
    if (!status.ok()) {
      SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                 "Table (%s) Saving to (%s) is Failed: %s", table_name_.data(),
                 table_directory_path.data(), status.ToString().data());
      return status;
    }
  }

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Table (%s) Saving Segments to (%s) is Done.", table_name_.data(),
             table_directory_path.data());
  /* save manifest (version, schema) */
  std::string manifest_file_path(table_directory_path);
  manifest_file_path.append("/manifest");
  status = SaveManifest(manifest_file_path);

  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Table (%s) Saving to (%s) is Failed: %s", table_name_.data(),
               table_directory_path.data(), status.ToString().data());
    return status;
  }

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Table (%s) Saving Manifest to (%s) is Done.", table_name_.data(),
             manifest_file_path.data());
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose, "Table (%s) Saving to (%s) is Done.",
             table_name_.data(), table_directory_path.data());

  return Status::Ok();
}

Status Table::SaveSegmentIds(std::string& file_path) {
  size_t segment_count = segments_.size();
  vdb::vector<uint8_t> buffer;
  int buffer_offset = 0;
  /* save segment count */
  int32_t needed_bytes = ComputeBytesFor(segment_count);
  buffer.resize(needed_bytes);
  buffer_offset += PutLengthTo(buffer.data() + buffer_offset, segment_count);
  for (auto [segment_id, segment] : segments_) {
    /* save length of segment_id */
    needed_bytes = ComputeBytesFor(segment_id.length());
    buffer.resize(buffer.size() + needed_bytes + segment_id.length());
    buffer_offset +=
        PutLengthTo(buffer.data() + buffer_offset, segment_id.length());
    /* save segment id */
    memcpy(buffer.data() + buffer_offset, segment_id.data(),
           segment_id.length());
    buffer_offset += segment_id.length();
  }

  /* write byte_vector to file */
  return WriteTo(file_path, buffer.data(), buffer_offset, 0);
}

Status Table::SaveManifest(std::string& file_path) {
  vdb::vector<uint8_t> buffer;
  int buffer_offset = 0;
  /* save table version */
  int32_t needed_bytes = ComputeBytesFor(table_version_);
  buffer.resize(needed_bytes);
  buffer_offset += PutLengthTo(buffer.data() + buffer_offset, table_version_);
  /* save schema */
  auto maybe_serialized_schema_buffer =
      arrow::ipc::SerializeSchema(*GetSchema(), &arrow_pool);
  if (!maybe_serialized_schema_buffer.ok()) {
    return Status::InvalidArgument(
        maybe_serialized_schema_buffer.status().ToString());
  }
  auto serialized_schema_buffer = maybe_serialized_schema_buffer.ValueUnsafe();
  uint64_t serialized_schema_size =
      static_cast<uint64_t>(serialized_schema_buffer->size());
  needed_bytes = ComputeBytesFor(serialized_schema_size);
  buffer.resize(buffer.size() + needed_bytes + serialized_schema_size);
  buffer_offset +=
      PutLengthTo(buffer.data() + buffer_offset, serialized_schema_size);

  memcpy(buffer.data() + buffer_offset, serialized_schema_buffer->data(),
         serialized_schema_size);
  buffer_offset += serialized_schema_size;
  return WriteTo(file_path, buffer.data(), buffer_offset, 0);
}

std::shared_ptr<Segment> Table::AddSegment(const std::shared_ptr<Table>& table,
                                           const std::string& segment_id) {
  auto segment = segments_.find(segment_id);
  if (segment != segments_.end()) return segment->second;
  auto new_segment = vdb::make_shared<Segment>(table, segment_id);
  segment = segments_.insert({new_segment->GetId(), new_segment}).first;
  return segment->second;
}

std::shared_ptr<Segment> Table::GetSegment(
    const std::string& segment_id) const {
  auto iter = segments_.find(segment_id);
  if (iter != segments_.end()) {
    return iter->second;
  } else {
    return nullptr;
  }
}

std::vector<std::string_view> Table::GetSegmentIdColumnNames() const {
  auto metadata = GetSchema()->metadata();
  auto segment_id_columns = GetSegmentIdColumnIndexes();
  std::vector<std::string_view> segment_id_column_names;
  for (auto index : segment_id_columns) {
    segment_id_column_names.push_back(GetSchema()->field(index)->name());
  }
  return segment_id_column_names;
}

std::vector<uint32_t> Table::GetSegmentIdColumnIndexes() const {
  std::vector<int> indexes;
  auto metadata = GetSchema()->metadata();
  auto segment_id_info = metadata->Get("segment_id_info").ValueOr("");
  auto segment_id_info_count = segment_id_info.size() / sizeof(uint32_t);
  uint32_t* segment_id_column_indexes =
      reinterpret_cast<uint32_t*>(segment_id_info.data());
  return std::vector<uint32_t>(
      segment_id_column_indexes,
      segment_id_column_indexes + segment_id_info_count);
}

size_t Table::GetActiveSetSizeLimit() const { return active_set_size_limit_; }

Status Table::SetActiveSetSizeLimit() {
  auto active_set_size_limit = std::stoul(
      GetSchema()->metadata()->Get(kActiveSetSizeLimitKey).ValueOr("0"));
  active_set_size_limit = active_set_size_limit == 0
                              ? server.vdb_active_set_size_limit
                              : active_set_size_limit;

  return SetActiveSetSizeLimit(active_set_size_limit);
}

Status Table::SetActiveSetSizeLimit(size_t active_set_size_limit) {
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Set Active Set Size Limit of Table (%s) as %lu",
             table_name_.data(), active_set_size_limit);

  auto status = SetMetadata(kActiveSetSizeLimitKey,
                            std::to_string(active_set_size_limit));
  if (!status.ok()) {
    return status;
  }
  active_set_size_limit_ = active_set_size_limit;
  return Status::Ok();
}

Status Table::SetSchema(const std::shared_ptr<arrow::Schema>& schema) {
  if (schema == nullptr)
    return Status(Status::kInvalidArgument, "null schema is not available. ");
  schema_ = schema;
  return Status::Ok();
}

Status Table::SetMetadata(const char* key, std::string value) {
  auto maybe_active_set_size_limit_in_metadata = schema_->metadata()->Get(key);
  if (!maybe_active_set_size_limit_in_metadata.ok()) {
    auto metadata = std::make_shared<arrow::KeyValueMetadata>(
        std::unordered_map<std::string, std::string>{{key, value}});
    return AddMetadata(metadata);
  }

  if (IsImmutable(key)) {
    // check whether try to change existing value in metadata
    auto value_in_metadata =
        maybe_active_set_size_limit_in_metadata.ValueUnsafe();
    if (value != value_in_metadata) {
      std::ostringstream oss;
      oss << key
          << " is immutable metadata, value is different from the value in "
             "metadata. "
          << "value=" << value << ", value in metadata=" << value_in_metadata;
      return Status::InvalidArgument(oss.str());
    }
  }
  return Status::Ok();
}

Status Table::AddMetadata(
    const std::shared_ptr<arrow::KeyValueMetadata>& metadata) {
  auto schema = GetSchema();
  if (schema->metadata() != nullptr) {
    return SetSchema(
        schema->WithMetadata(schema->metadata()->Merge(*metadata)));
  } else {
    return SetSchema(schema->WithMetadata(metadata));
  }
}

arrow::Result<int> Table::GetAnnColumnId() const {
  auto schema = GetSchema();
  auto ann_column_id =
      std::stoi(schema->metadata()->Get("ann_column_id").ValueOr("-1"));
  if (ann_column_id == -1) {
    return arrow::Status::Invalid("ANN search not supported: " +
                                  GetTableName());
  }
  return ann_column_id;
}

arrow::Result<int32_t> Table::GetAnnDimension() const {
  ARROW_ASSIGN_OR_RAISE(auto ann_column_id, GetAnnColumnId());
  return std::static_pointer_cast<arrow::FixedSizeListType>(
             GetSchema()->field(ann_column_id)->type())
      ->list_size();
}

bool Table::HasIndex() const {
  auto schema = GetSchema();
  if (schema->metadata() != nullptr) {
    auto ann_column_id =
        std::stoi(schema->metadata()->Get("ann_column_id").ValueOr("-1"));
    if (ann_column_id != -1) {
      return true;
    }
  }
  return false;
}

void Table::StartIndexingThread() {
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose, "Start Indexing Thread");
  indexing_thread_ =
      std::make_shared<std::thread>(&Table::IndexingThreadJob, this);
}

bool Table::IsIndexing() const { return indexing_; }

void Table::StopIndexingThread() {
  terminate_thread_ = true;
  cond_.notify_all();
  if (indexing_thread_->joinable()) indexing_thread_->join();
}

void Table::IndexingThreadJob() {
  SYSTEM_LOG(LogLevel::kLogVerbose, "Indexing Thread Started.");
  auto schema = GetSchema();
  auto metadata = schema->metadata();

  const size_t kMaxThreads = std::stoi(
      metadata->Get("max_threads").ValueOr("16"));  // TODO: make config
  const int32_t ann_column_id =
      std::stoi(metadata->Get("ann_column_id").ValueOr("-1"));
  if (ann_column_id == -1) {
    SYSTEM_LOG(LogLevel::kLogNotice,
               "Table metadata does not have information about the feature "
               "embedding column.");
    return;
  }
  const size_t kDim = std::static_pointer_cast<arrow::FixedSizeListType>(
                          schema->field(ann_column_id)->type())
                          ->list_size();
  while (true) {
    EmbeddingJobInfo info;
    {  // job queue lock scope
      std::unique_lock<std::mutex> lock(job_queue_mtx_);
      if (job_queue_.empty()) {
        indexing_ = false;
        cond_.wait(lock,
                   [&] { return !job_queue_.empty() || terminate_thread_; });
      }
      if (terminate_thread_) return;
      info = job_queue_.front();
      job_queue_.pop_front();
    }  // end of job queue lock scope
    std::vector<std::thread> threads;
    auto [index, emb_ptr, starting_label, size] = info;
    SYSTEM_LOG(LogLevel::kLogDebug, "Found indexing job. (%llu, %llu)",
               starting_label, size);

    if (size < kMaxThreads) {
      for (size_t thread_id = 0; thread_id < size - 1; thread_id++) {
        threads.emplace_back([&, thread_id] {
          index->AddEmbedding(emb_ptr + kDim * thread_id,
                              starting_label + thread_id);
        });
      }
      index->AddEmbedding(emb_ptr + kDim * (size - 1),
                          starting_label + size - 1);
    } else {
      std::atomic<size_t> record_id = 0;
      threads.reserve(kMaxThreads - 1);
      for (size_t thread_id = 0; thread_id < kMaxThreads - 1; thread_id++) {
        threads.emplace_back([&] {
          while (1) {
            /* If the snapshot operation is in progress, wait until it is
             * completed */
            CheckVdbSnapshot();
            size_t cur_id = record_id.fetch_add(1);
            if (cur_id >= size) break;
            index->AddEmbedding(emb_ptr + kDim * cur_id,
                                starting_label + cur_id);
          }
        });
      }

      while (1) {
        /* If the snapshot operation is in progress, wait until it is
         * completed */
        CheckVdbSnapshot();
        size_t cur_id = record_id.fetch_add(1);
        if (cur_id >= size) break;
        index->AddEmbedding(emb_ptr + kDim * cur_id, starting_label + cur_id);
      }
    }
    // TODO: range loop makes pointing error
    for (size_t i = 0; i < threads.size(); i++) {
      threads[i].join();
    }
    /*for (auto& t : threads) {
      t.join();
    }*/
    SYSTEM_LOG(LogLevel::kLogDebug, "Indexing job done. (%llu, %llu)",
               starting_label, size);
  }
}

std::string Table::ToString(bool show_records, bool show_metadata) const {
  std::stringstream ss;
  ss << "Table Name: " << table_name_ << std::endl;
  ss << "Table Schema: " << schema_->ToString(show_metadata) << std::endl;
  ss << "Active Set Size Limit: " << active_set_size_limit_ << std::endl;
  for (auto& kv : segments_) {
    ss << "----- " << kv.first << " -----" << std::endl;
    ss << kv.second->ToString(show_records) << std::endl;
  }
  return ss.str();
}

Status BuildColumns(
    std::shared_ptr<arrow::Schema>& schema,
    vdb::vector<std::shared_ptr<vdb::MutableArray>>& column_vector,
    size_t max_count) {
  for (auto& field : schema->fields()) {
    switch (field->type()->id()) {
#define __CASE(ARROW_TYPE, ARR_TYPE)                                     \
  {                                                                      \
    case ARROW_TYPE: {                                                   \
      column_vector.emplace_back(vdb::make_shared<ARR_TYPE>(max_count)); \
    } break;                                                             \
  }
      __CASE(arrow::Type::BOOL, BooleanArray);
      __CASE(arrow::Type::INT8, Int8Array);
      __CASE(arrow::Type::INT16, Int16Array);
      __CASE(arrow::Type::INT32, Int32Array);
      __CASE(arrow::Type::INT64, Int64Array);
      __CASE(arrow::Type::UINT8, UInt8Array);
      __CASE(arrow::Type::UINT16, UInt16Array);
      __CASE(arrow::Type::UINT32, UInt32Array);
      __CASE(arrow::Type::UINT64, UInt64Array);
      __CASE(arrow::Type::FLOAT, FloatArray);
      __CASE(arrow::Type::DOUBLE, DoubleArray);
      __CASE(arrow::Type::STRING, StringArray);
      __CASE(arrow::Type::LARGE_STRING, LargeStringArray);
#undef __CASE
      case arrow::Type::LIST: {
        auto type = std::static_pointer_cast<arrow::ListType>(field->type())
                        ->value_type();
        switch (type->id()) {
#define __CASE(ARROW_TYPE, ARR_TYPE)                                     \
  {                                                                      \
    case ARROW_TYPE: {                                                   \
      column_vector.emplace_back(vdb::make_shared<ARR_TYPE>(max_count)); \
    } break;                                                             \
  }
          __CASE(arrow::Type::BOOL, BooleanListArray);
          __CASE(arrow::Type::INT8, Int8ListArray);
          __CASE(arrow::Type::INT16, Int16ListArray);
          __CASE(arrow::Type::INT32, Int32ListArray);
          __CASE(arrow::Type::INT64, Int64ListArray);
          __CASE(arrow::Type::UINT8, UInt8ListArray);
          __CASE(arrow::Type::UINT16, UInt16ListArray);
          __CASE(arrow::Type::UINT32, UInt32ListArray);
          __CASE(arrow::Type::UINT64, UInt64ListArray);
          __CASE(arrow::Type::FLOAT, FloatListArray);
          __CASE(arrow::Type::DOUBLE, DoubleListArray);
          __CASE(arrow::Type::STRING, StringListArray);
#undef __CASE
          default:
            return Status(Status::kInvalidArgument, "Wrong list schema.");
        }
      } break;
      case arrow::Type::FIXED_SIZE_LIST: {
        auto fixed_size_list_type =
            std::static_pointer_cast<arrow::FixedSizeListType>(field->type());
        auto type = fixed_size_list_type->value_type();
        auto list_size = fixed_size_list_type->list_size();
        switch (type->id()) {
#define __CASE(ARROW_TYPE, ARR_TYPE)                         \
  {                                                          \
    case ARROW_TYPE: {                                       \
      column_vector.emplace_back(                            \
          vdb::make_shared<ARR_TYPE>(list_size, max_count)); \
    } break;                                                 \
  }
          __CASE(arrow::Type::BOOL, BooleanFixedSizeListArray);
          __CASE(arrow::Type::INT8, Int8FixedSizeListArray);
          __CASE(arrow::Type::INT16, Int16FixedSizeListArray);
          __CASE(arrow::Type::INT32, Int32FixedSizeListArray);
          __CASE(arrow::Type::INT64, Int64FixedSizeListArray);
          __CASE(arrow::Type::UINT8, UInt8FixedSizeListArray);
          __CASE(arrow::Type::UINT16, UInt16FixedSizeListArray);
          __CASE(arrow::Type::UINT32, UInt32FixedSizeListArray);
          __CASE(arrow::Type::UINT64, UInt64FixedSizeListArray);
          __CASE(arrow::Type::FLOAT, FloatFixedSizeListArray);
          __CASE(arrow::Type::DOUBLE, DoubleFixedSizeListArray);
          __CASE(arrow::Type::STRING, StringFixedSizeListArray);
#undef __CASE
          default:
            return Status(Status::kInvalidArgument,
                          "Wrong fixed_size_list schema.");
        }
      } break;
      default:
        return Status(Status::kInvalidArgument, "Wrong Schema.");
    }
  }
  return Status::Ok();
}

arrow::Result<std::pair<std::shared_ptr<arrow::RecordBatch>,
                        std::shared_ptr<arrow::Buffer>>>
_LoadRecordBatchFrom(std::string& file_path,
                     std::shared_ptr<arrow::Schema>& schema) {
  ARROW_ASSIGN_OR_RAISE(auto infile,
                        arrow::io::ReadableFile::Open(file_path, &arrow_pool));

  ARROW_ASSIGN_OR_RAISE(int64_t file_size, infile->GetSize());

  ARROW_ASSIGN_OR_RAISE(auto serialized_buffer, infile->Read(file_size));

  auto buf_reader =
      std::make_shared<arrow::io::BufferReader>(serialized_buffer);

  arrow::ipc::DictionaryMemo dictionary_memo;

  auto options = arrow::ipc::IpcReadOptions::Defaults();
  options.memory_pool = &arrow_pool;
  ARROW_ASSIGN_OR_RAISE(auto record_batch,
                        arrow::ipc::ReadRecordBatch(schema, &dictionary_memo,
                                                    options, buf_reader.get()));

  return std::make_pair(record_batch, serialized_buffer);
}

Status _SaveRecordBatchTo(std::string& file_path,
                          std::shared_ptr<arrow::RecordBatch> rb) {
  auto options = arrow::ipc::IpcWriteOptions::Defaults();
  options.memory_pool = &arrow_pool;
  ARROW_ASSIGN_OR_RAISE(auto serialized_buffer,
                        arrow::ipc::SerializeRecordBatch(*rb, options));
  return WriteTo(file_path, serialized_buffer->data(),
                 serialized_buffer->size(), 0);
}

Segment::Segment(std::shared_ptr<Table> table, const std::string& segment_id)
    : table_{table}, segment_id_{segment_id}, size_{0} {
  auto schema = table->GetSchema();
  auto status =
      BuildColumns(schema, active_set_, table->GetActiveSetSizeLimit());
  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Segment (%s, %s) Creating is Failed: %s",
               table->GetTableName().data(), segment_id.data(),
               status.ToString().data());
    throw std::invalid_argument(status.ToString());
  }
  auto metadata = schema->metadata();
  if (metadata != nullptr && metadata->FindKey("ann_column_id") != -1) {
    index_handler_ = vdb::make_shared<vdb::IndexHandler>(table);
  } else {
    index_handler_ = nullptr;
  }
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Creating is Done: %s",
             table->GetTableName().data(), segment_id.data(),
             index_handler_ == nullptr ? "has index." : "has no index.");
}

Segment::Segment(std::shared_ptr<Table> table, const std::string& segment_id,
                 std::string& directory_path)
    : table_{table}, segment_id_{segment_id}, size_{0} {
  std::error_code ec;
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Loading from (%s) is Started.",
             table->GetTableName().data(), segment_id.data(),
             directory_path.data());
  /* load manifest */
  std::string manifest_file_path(directory_path);
  manifest_file_path.append("/manifest");
  /* existence of manifest shows completeness of saving snapshot */
  if (!std::filesystem::exists(manifest_file_path, ec)) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Segment (%s, %s) Loading Manifest from (%s) is Failed: %s",
               table->GetTableName().data(), segment_id.data(),
               manifest_file_path.data(), ec.message().data());
    throw std::invalid_argument("saving snapshot of segment is not completed.");
  }
  size_t manifest_size = std::filesystem::file_size(manifest_file_path);
  auto buffered_manifest =
      static_cast<uint8_t*>(AllocateAligned(64, manifest_size));
  /* read table version and schema */
  auto status =
      ReadFrom(manifest_file_path, buffered_manifest, manifest_size, 0);
  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Segment (%s, %s) Loading Manifest from (%s) is Failed: %s",
               table->GetTableName().data(), segment_id.data(),
               manifest_file_path.data(), status.ToString().data());
    throw std::invalid_argument(status.ToString());
  }
  uint64_t buffer_offset = 0;
  /* get inactive_set_count */
  uint64_t inactive_set_count;
  buffer_offset += GetLengthFrom(buffered_manifest, inactive_set_count);
  /* get index count */
  uint64_t index_count = 0;
  if (table->HasIndex()) {
    buffer_offset +=
        GetLengthFrom(buffered_manifest + buffer_offset, index_count);
  }
  DeallocateAligned(buffered_manifest, manifest_size);
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Loading Manifest from (%s) is Done.",
             table->GetTableName().data(), segment_id.data(),
             manifest_file_path.data());

  auto schema = table->GetSchema();
  {
    /* load active rb */
    std::string active_set_file_path(directory_path);
    active_set_file_path.append("/set.active");
    auto maybe_rb_buffer_pair =
        _LoadRecordBatchFrom(active_set_file_path, schema);
    if (!maybe_rb_buffer_pair.ok()) {
      SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                 "Segment (%s, %s) Loading Active Set from (%s) is Failed: %s",
                 table->GetTableName().data(), segment_id.data(),
                 active_set_file_path.data(),
                 maybe_rb_buffer_pair.status().ToString().data());
      throw std::invalid_argument(maybe_rb_buffer_pair.status().ToString());
    }
    auto [active_rb, buffer] = maybe_rb_buffer_pair.ValueUnsafe();
    /* append active rb into active set */
    status = BuildColumns(schema, active_set_, table->GetActiveSetSizeLimit());
    if (!status.ok()) {
      SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                 "Segment (%s, %s) Loading Active Set from (%s) is Failed: %s",
                 table->GetTableName().data(), segment_id.data(),
                 active_set_file_path.data(), status.ToString().data());
      throw std::invalid_argument(status.ToString());
    }
    if (active_rb->num_rows() > 0) {
      status = AddRecordBatch(active_rb);
      if (!status.ok()) {
        SYSTEM_LOG(
            vdb::LogLevel::kLogNotice,
            "Segment (%s, %s) Loading Active Set from (%s) is Failed: %s",
            table->GetTableName().data(), segment_id.data(),
            active_set_file_path.data(), status.ToString().data());
        throw std::invalid_argument(status.ToString());
      }
    }
    SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
               "Segment (%s, %s) Loading Active Set from (%s) is Done.",
               table->GetTableName().data(), segment_id.data(),
               active_set_file_path.data());
    /* active recordbatch and buffer of serialized recordbatch are freed */
  }

  /* load inactive sets */
  for (uint64_t i = 0; i < inactive_set_count; i++) {
    std::string inactive_set_file_path(directory_path);
    inactive_set_file_path.append("/sets.inactive/set.");
    inactive_set_file_path.append(std::to_string(i));
    auto maybe_rb_buffer_pair =
        _LoadRecordBatchFrom(inactive_set_file_path, schema);
    if (!maybe_rb_buffer_pair.ok()) {
      SYSTEM_LOG(
          vdb::LogLevel::kLogNotice,
          "Segment (%s, %s) Loading Inactive Set (%ld) from (%s) is Failed: %s",
          table->GetTableName().data(), segment_id.data(), (long int)i,
          inactive_set_file_path.data(),
          maybe_rb_buffer_pair.status().ToString().data());
      throw std::invalid_argument(maybe_rb_buffer_pair.status().ToString());
    }
    auto [inactive_rb, buffer] = maybe_rb_buffer_pair.ValueUnsafe();
    if (inactive_rb->num_rows() == 0) {
      SYSTEM_LOG(
          vdb::LogLevel::kLogNotice,
          "Segment (%s, %s) Loading Inactive Set (%ld) from (%s) is Failed: %s",
          table->GetTableName().data(), segment_id.data(), (long int)i,
          inactive_set_file_path.data(), "inactive set is empty.");
      throw std::invalid_argument("Inactive set is empty. ");
    }
    inactive_sets_.push_back(
        vdb::make_shared<InactiveSet>(inactive_rb, buffer));
    size_ += inactive_rb->num_rows();
  }

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Loading Inactive Sets from "
             "(%s/inactive_sets/set#) is Done.",
             table->GetTableName().data(), segment_id.data(),
             directory_path.data());
  /* load vector indexes */
  if (index_count > 0) {
    std::string index_directory_path(directory_path);
    index_directory_path.append("/index");
    index_handler_ = vdb::make_shared<vdb::IndexHandler>(
        table, index_directory_path, index_count);
    SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
               "Segment (%s, %s) Loading Vector Indexes from "
               "(%s/index/index#) is Done.",
               table->GetTableName().data(), segment_id.data(),
               directory_path.data());

    /* If the index was saved during the process of index building, resume it */
    auto index_count = index_handler_->Size();
    for (size_t set_id = 0; set_id < index_count; set_id++) {
      auto cur_index = index_handler_->Index(set_id);
      auto index_element_count = cur_index->Size();
      auto cur_rb = GetRecordbatch(set_id);
      size_t row_count = (size_t)cur_rb->num_rows();

      if (index_element_count != row_count) {
        auto ann_column_id = std::stoull(
            table->GetSchema()->metadata()->Get("ann_column_id").ValueUnsafe());
        auto dimension = cur_index->Dimension();
        arrow::Array* target_arr = cur_rb->column(ann_column_id).get();
        auto fixed_size_lists =
            static_cast<const arrow::FixedSizeListArray*>(target_arr);
        auto numeric_array =
            fixed_size_lists->values()->data()->GetValues<float>(1);
        auto numeric_pos = index_element_count * dimension;
        auto embedding_ptr = numeric_array + numeric_pos;

        /* Resuming the process of index building starts with the last element
         * of the index */
        status = AddEmbedding(embedding_ptr, set_id, index_element_count,
                              row_count - index_element_count);
        if (!status.ok()) {
          SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                     "Segment (%s, %s) Resuming Index Building with set (%lu) "
                     "is Failed: %s",
                     table->GetTableName().data(), segment_id.data(), set_id,
                     status.ToString().data());
          throw std::invalid_argument(status.ToString());
        }
        SYSTEM_LOG(
            vdb::LogLevel::kLogVerbose,
            "Segment (%s, %s) Resuming Index Building with set (%lu) is Done.",
            table->GetTableName().data(), segment_id.data(), set_id);
      }
    }
    SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
               "Segment (%s, %s) Resuming Index Building is All Done.",
               table->GetTableName().data(), segment_id.data());
  } else {
    index_handler_ = nullptr;
  }
  SYSTEM_LOG(
      vdb::LogLevel::kLogVerbose, "Segment (%s, %s) Loading from (%s) is Done.",
      table->GetTableName().data(), segment_id.data(), directory_path.data());
}

Status Segment::Save(std::string& directory_path) {
  auto table = table_.lock();
  std::error_code ec;
  SYSTEM_LOG(
      vdb::LogLevel::kLogVerbose, "Segment (%s, %s) Saving to (%s) is Started.",
      table->GetTableName().data(), segment_id_.data(), directory_path.data());
  if (!std::filesystem::create_directory(directory_path, ec)) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Segment (%s, %s) Saving to (%s) is Failed: %s",
               table->GetTableName().data(), directory_path.data(),
               segment_id_.data(), ec.message().data());
    throw std::invalid_argument(ec.message());
  }
  /* save active set */
  std::string active_set_file_path(directory_path);
  active_set_file_path.append("/set.active");
  auto active_rb = ActiveSetRecordBatch();
  auto status = _SaveRecordBatchTo(active_set_file_path, active_rb);
  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Segment (%s, %s) Saving Active Set to (%s) is Failed: %s",
               table->GetTableName().data(), segment_id_.data(),
               active_set_file_path.data(), status.ToString().data());
    return status;
  }

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Saving Active Set to (%s) is Done.",
             table->GetTableName().data(), segment_id_.data(),
             active_set_file_path.data());
  /* save inactive set recordbatches */
  std::string inactive_sets_directory_path(directory_path);
  inactive_sets_directory_path.append("/sets.inactive");
  std::filesystem::create_directory(inactive_sets_directory_path);
  for (size_t i = 0; i < InactiveSets().size(); i++) {
    std::string inactive_set_file_path(inactive_sets_directory_path);
    inactive_set_file_path.append("/set.");
    inactive_set_file_path.append(std::to_string(i));
    auto inactive_rb = InactiveSetRecordBatch(i);
    status = _SaveRecordBatchTo(inactive_set_file_path, inactive_rb);
    if (!status.ok()) {
      SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                 "Segment (%s, %s) Saving Inactive Set to (%s) is Failed: %s",
                 table->GetTableName().data(), segment_id_.data(),
                 inactive_set_file_path.data(), status.ToString().data());
      return status;
    }
  }
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Saving Inactive Sets to (%s/set#) is Done.",
             table->GetTableName().data(), segment_id_.data(),
             inactive_sets_directory_path.data());
  /* save vector indexes */
  if (HasIndex()) {
    auto index_directory_path(directory_path);
    index_directory_path.append("/index");
    auto status = IndexHandler()->Save(index_directory_path);
    if (!status.ok()) {
      SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                 "Segment (%s, %s) Saving Vector Indexes to (%s) is Failed: %s",
                 table->GetTableName().data(), segment_id_.data(),
                 index_directory_path.data(), status.ToString().data());
      return status;
    }
    SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
               "Segment (%s, %s) Saving Vector Indexes to (%s/index#) is Done.",
               table->GetTableName().data(), segment_id_.data(),
               index_directory_path.data());
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
               "Segment (%s, %s) Saving Vector Indexes is Skipped: No Index.",
               table->GetTableName().data(), segment_id_.data());
  }

  /* save manifest (# of inactive sets and # of indexes) */
  std::string manifest_file_path(directory_path);
  manifest_file_path.append("/manifest");
  status = SaveManifest(manifest_file_path);
  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Segment (%s, %s) Saving Manifest to (%s) is Failed: %s",
               table->GetTableName().data(), segment_id_.data(),
               manifest_file_path.data(), status.ToString().data());
    return status;
  }

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Saving Manifest to (%s) is Done.",
             table->GetTableName().data(), segment_id_.data(),
             manifest_file_path.data());

  SYSTEM_LOG(
      vdb::LogLevel::kLogVerbose, "Segment (%s, %s) Saving to (%s) is Done.",
      table->GetTableName().data(), segment_id_.data(), directory_path.data());
  return Status::Ok();
}

Status Segment::SaveManifest(std::string& file_path) const {
  vdb::vector<uint8_t> buffer;
  int buffer_offset = 0;
  /* save # of inactive sets */
  int32_t needed_bytes = ComputeBytesFor(InactiveSetCount());
  buffer.resize(needed_bytes);
  buffer_offset +=
      PutLengthTo(buffer.data() + buffer_offset, InactiveSetCount());
  if (HasIndex()) {
    /* save # of indexes */
    needed_bytes = ComputeBytesFor(IndexHandler()->Size());
    buffer.resize(buffer.size() + needed_bytes);
    buffer_offset +=
        PutLengthTo(buffer.data() + buffer_offset, IndexHandler()->Size());
  }
  return WriteTo(file_path, buffer.data(), buffer_offset, 0);
}

std::string Segment::ToString(bool show_records) const {
  auto table = table_.lock();
  auto fields = table->GetSchema()->fields();
  std::stringstream ss;
  ss << "SegmentId:  " << segment_id_ << std::endl;
  ss << "SegmentSize: " << size_ << std::endl;
  ss << "SegmentData: " << std::endl;
  ss << "---- Inactive sets (set count=" << InactiveSetCount() << ")----"
     << std::endl;
  if (show_records) {
    for (auto& set : inactive_sets_) {
      ss << set->ToString() << std::endl;
    }
  } else {
    for (auto& set : inactive_sets_) {
      ss << set->GetRb()->num_rows() << " records are not shown." << std::endl;
    }
  }
  ss << "---- Active set (record count=" << ActiveSetRecordCount() << ")----"
     << std::endl;
  if (show_records) {
    for (size_t i = 0; i < fields.size(); i++) {
      ss << fields[i]->name() << ": ";
      ss << active_set_[i]->ToString() << std::endl;
    }
  } else {
    ss << ActiveSetRecordCount() << " records are not shown." << std::endl;
  }
  if (HasIndex()) {
    ss << index_handler_->ToString(false) << std::endl;
  }
  return ss.str();
}

Status Segment::AddEmbedding(const float* embedding, const uint32_t set_number,
                             const size_t record_number) {
  /* there is no need to distinguish set by label. i th record is mapped to i th
   * embedding. */
  size_t label = vdb::LabelInfo::Build(set_number, record_number);
  return IndexHandler()->AddEmbedding(embedding, label);
}

Status Segment::AddEmbedding(const float* embedding, const uint32_t set_number,
                             const size_t record_number, const size_t size) {
  /* there is no need to distinguish set by label. i th record is mapped to i th
   * embedding. */
  size_t starting_label = vdb::LabelInfo::Build(set_number, record_number);
  return IndexHandler()->AddEmbedding(embedding, starting_label, size);
}

Status Segment::AppendRecord(const std::string_view& record_string) {
  std::shared_ptr<Table> table = table_.lock();
  auto schema = table->GetSchema();
  auto fields = schema->fields();
  size_t column_count = schema->fields().size();
  size_t column_id = 0;

  auto prev = 0;
  auto pos = record_string.find(kRS, 0);

  while (true) {
    auto token = record_string.substr(prev, pos - prev);

    switch (fields[column_id]->type()->id()) {
      /* Non-list type cases */
#define __CASE(ARROW_TYPE, ARR_TYPE, PARSED_VALUE)               \
  {                                                              \
    case ARROW_TYPE: {                                           \
      std::static_pointer_cast<ARR_TYPE>(active_set_[column_id]) \
          ->Append(PARSED_VALUE);                                \
    } break;                                                     \
  }
      __CASE(arrow::Type::BOOL, BooleanArray, token[0] != '0');
      __CASE(arrow::Type::INT8, Int8Array,
             static_cast<int8_t>(std::stoi(std::string(token))));
      __CASE(arrow::Type::INT16, Int16Array,
             static_cast<int16_t>(std::stoi(std::string(token))));
      __CASE(arrow::Type::INT32, Int32Array,
             static_cast<int32_t>(std::stol(std::string(token))));
      __CASE(arrow::Type::INT64, Int64Array,
             static_cast<int64_t>(std::stoll(std::string(token))));
      __CASE(arrow::Type::UINT8, UInt8Array,
             static_cast<uint8_t>(std::stoul(std::string(token))));
      __CASE(arrow::Type::UINT16, UInt16Array,
             static_cast<uint16_t>(std::stoul(std::string(token))));
      __CASE(arrow::Type::UINT32, UInt32Array,
             static_cast<uint32_t>(std::stoul(std::string(token))));
      __CASE(arrow::Type::UINT64, UInt64Array,
             static_cast<uint64_t>(std::stoull(std::string(token))));
      __CASE(arrow::Type::FLOAT, FloatArray,
             static_cast<float>(std::stof(std::string(token))));
      __CASE(arrow::Type::DOUBLE, DoubleArray,
             static_cast<double>(std::stod(std::string(token))));
      __CASE(arrow::Type::STRING, StringArray, std::string(token));
      __CASE(arrow::Type::LARGE_STRING, LargeStringArray, std::string(token));
#undef __CASE

      /* list type cases */
      case arrow::Type::LIST: {
        auto type =
            std::static_pointer_cast<arrow::ListType>(fields[column_id]->type())
                ->value_field()
                ->type()
                ->id();
        auto prev = 0;
        auto pos = token.find(kGS, prev);
        switch (type) {  // Handling Subtype
#define __CASE(ARROW_TYPE, CTYPE, ARR_TYPE, PARSED_VALUE)                      \
  {                                                                            \
    case ARROW_TYPE: {                                                         \
      std::vector<CTYPE> list_value;                                           \
      while (true) {                                                           \
        auto value_token = token.substr(prev, pos - prev);                     \
        list_value.emplace_back((PARSED_VALUE));                               \
        if (pos == std::string::npos) break;                                   \
        prev = pos + 1;                                                        \
        pos = token.find(kGS, prev);                                           \
      }                                                                        \
      auto status = std::static_pointer_cast<ARR_TYPE>(active_set_[column_id]) \
                        ->Append(list_value);                                  \
      if (!status.ok()) return status;                                         \
    } break;                                                                   \
  }
          __CASE(arrow::Type::BOOL, bool, BoolListArray, value_token[0] != '0');
          __CASE(arrow::Type::INT8, int8_t, Int8ListArray,
                 std::stoi(std::string(value_token)));
          __CASE(arrow::Type::INT16, int16_t, Int16ListArray,
                 std::stoi(std::string(value_token)));
          __CASE(arrow::Type::INT32, int32_t, Int32ListArray,
                 std::stol(std::string(value_token)));
          __CASE(arrow::Type::INT64, int64_t, Int64ListArray,
                 std::stoll(std::string(value_token)));
          __CASE(arrow::Type::UINT8, uint8_t, UInt8ListArray,
                 std::stoul(std::string(value_token)));
          __CASE(arrow::Type::UINT16, uint16_t, UInt16ListArray,
                 std::stoul(std::string(value_token)));
          __CASE(arrow::Type::UINT32, uint32_t, UInt32ListArray,
                 std::stoul(std::string(value_token)));
          __CASE(arrow::Type::UINT64, uint64_t, UInt64ListArray,
                 std::stoull(std::string(value_token)));
          __CASE(arrow::Type::FLOAT, float, FloatListArray,
                 std::stof(std::string(value_token)));
          __CASE(arrow::Type::DOUBLE, double, DoubleListArray,
                 std::stod(std::string(value_token)));
          __CASE(arrow::Type::STRING, std::string, StringListArray,
                 std::string(value_token));
          default:
            return Status(Status::kInvalidArgument,
                          "Err: Not a valid subtype for list.");
        }
#undef __CASE
      } break;

      /* fixed size list type cases */
      case arrow::Type::FIXED_SIZE_LIST: {
        auto type = std::static_pointer_cast<arrow::FixedSizeListType>(
                        fields[column_id]->type())
                        ->value_field()
                        ->type()
                        ->id();
        auto prev = 0;
        auto pos = token.find(kGS, prev);
        switch (type) {
#define __CASE(ARROW_TYPE, CTYPE, ARR_TYPE, PARSED_VALUE)                      \
  {                                                                            \
    case ARROW_TYPE: {                                                         \
      std::vector<CTYPE> list_value;                                           \
      while (true) {                                                           \
        auto value_token = token.substr(prev, pos - prev);                     \
        list_value.emplace_back((PARSED_VALUE));                               \
        if (pos == std::string::npos) break;                                   \
        prev = pos + 1;                                                        \
        pos = token.find(kGS, prev);                                           \
      }                                                                        \
      auto status = std::static_pointer_cast<ARR_TYPE>(active_set_[column_id]) \
                        ->Append(list_value);                                  \
      if (!status.ok()) return status;                                         \
    } break;                                                                   \
  }
          __CASE(arrow::Type::BOOL, bool, BoolFixedSizeListArray,
                 value_token[0] != '0');
          __CASE(arrow::Type::INT8, int8_t, Int8FixedSizeListArray,
                 std::stoi(std::string(value_token)));
          __CASE(arrow::Type::INT16, int16_t, Int16FixedSizeListArray,
                 std::stoi(std::string(value_token)));
          __CASE(arrow::Type::INT32, int32_t, Int32FixedSizeListArray,
                 std::stol(std::string(value_token)));
          __CASE(arrow::Type::INT64, int64_t, Int64FixedSizeListArray,
                 std::stoll(std::string(value_token)));
          __CASE(arrow::Type::UINT8, uint8_t, UInt8FixedSizeListArray,
                 std::stoul(std::string(value_token)));
          __CASE(arrow::Type::UINT16, uint16_t, UInt16FixedSizeListArray,
                 std::stoul(std::string(value_token)));
          __CASE(arrow::Type::UINT32, uint32_t, UInt32FixedSizeListArray,
                 std::stoul(std::string(value_token)));
          __CASE(arrow::Type::UINT64, uint64_t, UInt64FixedSizeListArray,
                 std::stoull(std::string(value_token)));
          __CASE(arrow::Type::FLOAT, float, FloatFixedSizeListArray,
                 std::stof(std::string(value_token)));
          __CASE(arrow::Type::DOUBLE, double, DoubleFixedSizeListArray,
                 std::stod(std::string(value_token)));
          __CASE(arrow::Type::STRING, std::string, StringFixedSizeListArray,
                 std::string(value_token));
          default:
            return Status(Status::kInvalidArgument,
                          "Err: Not a valid subtype for fixed_size_list.");
#undef __CASE
        }
      } break;
      default:
        return Status(Status::kInvalidArgument,
                      "Arrow builder fails to append the value.");
    }

    if (pos == std::string::npos) break;

    prev = pos + 1;
    pos = record_string.find(kRS, prev);
    column_id += 1;
  }

  if (column_id < column_count - 1) {
    return Status(Status::kInvalidArgument, "Not enough column is provided.");
  }

  if (schema->metadata() != nullptr) {
    auto ann_column_id =
        std::stoi(schema->metadata()->Get("ann_column_id").ValueOr("-1"));
    if (ann_column_id != -1) {
      auto mutable_array = std::static_pointer_cast<FloatFixedSizeListArray>(
          active_set_[ann_column_id]);
      auto inserted_embedding =
          mutable_array->GetRawValue(ActiveSetRecordCount() - 1);
      if (inserted_embedding != nullptr) {
        auto status = AddEmbedding(inserted_embedding, ActiveSetId(),
                                   ActiveSetRecordCount() - 1);
        if (!status.ok())
          return Status(Status::kInvalidArgument, status.ToString());
      } else {
        return Status(Status::kInvalidArgument, "Data does not exist.");
      }
    }
  }

  size_++;
  if (ActiveSetRecordCount() == table->GetActiveSetSizeLimit()) {
    return MakeInactive();
  }

  return Status::Ok();
}

Status Segment::AddRecordBatch(std::shared_ptr<arrow::RecordBatch>& rb) {
  if (rb->num_rows() == 0) {
    return Status::InvalidArgument("Don't pass empty recordbatch! ");
  }
  std::shared_ptr<Table> table = table_.lock();
  auto schema = table->GetSchema();
  auto fields = schema->fields();
  size_t prev_active_set_record_count = ActiveSetRecordCount();

  for (int64_t i = 0; i < rb->num_columns(); i++) {
    auto input_arr = rb->column(i);
    auto field = schema->field(i);
    if (field->nullable() == false) {
      if (input_arr->null_count() > 0) {
        return Status::InvalidArgument(
            "Could not insert recordbatch into segment. Field '" +
            field->name() +
            "' is not nullable, but the recordbatch contains null values.");
      }
    }

    switch (field->type()->id()) {
#define __CASE(ARROW_TYPE, ARR_TYPE)                               \
  {                                                                \
    case ARROW_TYPE: {                                             \
      auto casted_mutable_arr =                                    \
          std::static_pointer_cast<vdb::ARR_TYPE>(active_set_[i]); \
      casted_mutable_arr->Append(input_arr.get());                 \
    } break;                                                       \
  }
      __CASE(arrow::Type::BOOL, BooleanArray);
      __CASE(arrow::Type::INT8, Int8Array);
      __CASE(arrow::Type::INT16, Int16Array);
      __CASE(arrow::Type::INT32, Int32Array);
      __CASE(arrow::Type::INT64, Int64Array);
      __CASE(arrow::Type::UINT8, UInt8Array);
      __CASE(arrow::Type::UINT16, UInt16Array);
      __CASE(arrow::Type::UINT32, UInt32Array);
      __CASE(arrow::Type::UINT64, UInt64Array);
      __CASE(arrow::Type::FLOAT, FloatArray);
      __CASE(arrow::Type::DOUBLE, DoubleArray);
      __CASE(arrow::Type::STRING, StringArray);
      __CASE(arrow::Type::LARGE_STRING, LargeStringArray);
#undef __CASE

      case arrow::Type::LIST: {
        auto type = std::static_pointer_cast<arrow::ListType>(fields[i]->type())
                        ->value_field()
                        ->type()
                        ->id();
        switch (type) {
#define __CASE(ARROW_TYPE, CTYPE, ARR_TYPE)                      \
  {                                                              \
    case ARROW_TYPE: {                                           \
      auto casted_mutable_arr =                                  \
          std::static_pointer_cast<ARR_TYPE>(active_set_[i]);    \
      auto status = casted_mutable_arr->Append(input_arr.get()); \
      if (!status.ok()) return status;                           \
    } break;                                                     \
  }
          __CASE(arrow::Type::BOOL, bool, BooleanListArray);
          __CASE(arrow::Type::INT8, int8_t, Int8ListArray);
          __CASE(arrow::Type::INT16, int16_t, Int16ListArray);
          __CASE(arrow::Type::INT32, int32_t, Int32ListArray);
          __CASE(arrow::Type::INT64, int64_t, Int64ListArray);
          __CASE(arrow::Type::UINT8, uint8_t, UInt8ListArray);
          __CASE(arrow::Type::UINT16, uint16_t, UInt16ListArray);
          __CASE(arrow::Type::UINT32, uint32_t, UInt32ListArray);
          __CASE(arrow::Type::UINT64, uint64_t, UInt64ListArray);
          __CASE(arrow::Type::FLOAT, float, FloatListArray);
          __CASE(arrow::Type::DOUBLE, double, DoubleListArray);
          __CASE(arrow::Type::STRING, std::string, StringListArray);
#undef __CASE
          default:
            return Status(Status::kInvalidArgument,
                          "Err: Not a valid subtype for list.");
        }
      } break;
      case arrow::Type::FIXED_SIZE_LIST: {
        auto type = std::static_pointer_cast<arrow::FixedSizeListType>(
                        fields[i]->type())
                        ->value_field()
                        ->type()
                        ->id();
        switch (type) {
#define __CASE(ARROW_TYPE, CTYPE, ARR_TYPE, SUBARR_TYPE)         \
  {                                                              \
    case ARROW_TYPE: {                                           \
      auto casted_mutable_arr =                                  \
          std::static_pointer_cast<ARR_TYPE>(active_set_[i]);    \
      auto status = casted_mutable_arr->Append(input_arr.get()); \
      if (!status.ok()) return status;                           \
    } break;                                                     \
  }
          __CASE(arrow::Type::BOOL, bool, BooleanFixedSizeListArray,
                 arrow::BooleanArray);
          __CASE(arrow::Type::INT8, int8_t, Int8FixedSizeListArray,
                 arrow::Int8Array);
          __CASE(arrow::Type::INT16, int16_t, Int16FixedSizeListArray,
                 arrow::Int16Array);
          __CASE(arrow::Type::INT32, int32_t, Int32FixedSizeListArray,
                 arrow::Int32Array);
          __CASE(arrow::Type::INT64, int64_t, Int64FixedSizeListArray,
                 arrow::Int64Array);
          __CASE(arrow::Type::UINT8, uint8_t, UInt8FixedSizeListArray,
                 arrow::UInt8Array);
          __CASE(arrow::Type::UINT16, uint16_t, UInt16FixedSizeListArray,
                 arrow::UInt16Array);
          __CASE(arrow::Type::UINT32, uint32_t, UInt32FixedSizeListArray,
                 arrow::UInt32Array);
          __CASE(arrow::Type::UINT64, uint64_t, UInt64FixedSizeListArray,
                 arrow::UInt64Array);
          __CASE(arrow::Type::FLOAT, float, FloatFixedSizeListArray,
                 arrow::FloatArray);
          __CASE(arrow::Type::DOUBLE, double, DoubleFixedSizeListArray,
                 arrow::DoubleArray);
          __CASE(arrow::Type::STRING, std::string, StringFixedSizeListArray,
                 arrow::StringArray);
#undef __CASE
          default:
            return Status(Status::kInvalidArgument,
                          "Err: Not a valid subtype for fixed_size_list.");
        }
      } break;
      default:
        return Status(Status::kInvalidArgument, "");
    }
  }

  if (HasIndex()) {
    auto ann_column_id =
        std::stoi(schema->metadata()->Get("ann_column_id").ValueOr("-1"));
    if (ann_column_id != -1) {
      auto casted_mutable_arr =
          std::static_pointer_cast<FloatFixedSizeListArray>(
              active_set_[ann_column_id]);
      auto embedding_ptr =
          casted_mutable_arr->GetRawValue(prev_active_set_record_count);
      if (server.allow_bg_index_thread) {
        auto status =
            AddEmbedding(embedding_ptr, ActiveSetId(),
                         prev_active_set_record_count, rb->num_rows());
        if (!status.ok())
          return Status(Status::kInvalidArgument, status.ToString());
      } else {
        for (int64_t i = 0; i < rb->num_rows(); i++) {
          auto status =
              AddEmbedding(embedding_ptr + casted_mutable_arr->GetWidth() * i,
                           ActiveSetId(), prev_active_set_record_count + i);
          if (!status.ok())
            return Status(Status::kInvalidArgument, status.ToString());
        }
      }
    } else {
      return Status::InvalidArgument(
          "Table with index does not have ann_column_id metadata.");
    }
  }

  size_ += rb->num_rows();

  return Status::Ok();
}

Status Segment::AppendRecords(
    std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches) {
  auto table = table_.lock();
  auto active_set_size_limit = table->GetActiveSetSizeLimit();

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Appending Records is Started.",
             table->GetTableName().data(), segment_id_.data());
  size_t append_record_count = 0;
  for (auto& record_batch : record_batches) {
    append_record_count += record_batch->num_rows();
    int64_t start_rowno = 0;
    while (start_rowno < record_batch->num_rows()) {
      auto sliced_rb = record_batch->Slice(
          start_rowno, active_set_size_limit - ActiveSetRecordCount());
      auto status = AddRecordBatch(sliced_rb);
      if (!status.ok()) {
        SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                   "Segment (%s, %s) Appending Records is Failed: %s",
                   table->GetTableName().data(), segment_id_.data(),
                   status.ToString().data());
        return status;
      }
      SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
                 "Segment (%s, %s) %ld Records are Appended.",
                 table->GetTableName().data(), segment_id_.data(),
                 (long int)sliced_rb->num_rows());
      if (ActiveSetRecordCount() == active_set_size_limit) {
        status = MakeInactive();
        if (!status.ok()) {
          SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                     "Segment (%s, %s) Appending Records is Failed: %s",
                     table->GetTableName().data(), segment_id_.data(),
                     status.ToString().data());
          return status;
        }
        SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
                   "Segment (%s, %s) Making Active Set (size=%ld) into "
                   "Inactive is Done.",
                   table->GetTableName().data(), segment_id_.data(),
                   ActiveSetRecordCount());
      }
      start_rowno += sliced_rb->num_rows();
    }
  }
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Total %ld Records are Appended.",
             table->GetTableName().data(), segment_id_.data(),
             append_record_count);
  SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
             "Segment (%s, %s) Appending Records is Done.",
             table->GetTableName().data(), segment_id_.data());
  return Status::Ok();
}

Status Segment::MakeInactive() {
  auto table = table_.lock();
  auto schema = table->GetSchema();
  auto active_set_size = ActiveSetRecordCount();
  /* active_data_ is swapped to empty vector.
   * contents of active_data_ is moved into InactiveBatch */
  inactive_sets_.emplace_back(
      vdb::make_shared<vdb::InactiveSet>(schema, active_set_));
  active_rb_ = nullptr;

  /* reset mutable array */
  auto status =
      BuildColumns(schema, active_set_, table->GetActiveSetSizeLimit());
  if (!status.ok()) {
    return status;
  }
  /* create new vector index */
  if (HasIndex()) {
    status = index_handler_->CreateIndex();
    if (!status.ok()) {
      return status;
    }
  }
  SYSTEM_LOG(
      vdb::LogLevel::kLogVerbose,
      "Segment (%s, %s) Making Active Set (size=%ld) into Inactive is Done.",
      table->GetTableName().data(), segment_id_.data(), active_set_size);
  return Status::Ok();
}

std::shared_ptr<arrow::RecordBatch> Segment::ActiveSetRecordBatch() {
  auto size = active_set_[0]->Size();
  if (active_rb_ == nullptr || active_rb_->num_rows() < size) {
    auto table = table_.lock();
    auto schema = table->GetSchema();
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (auto& md : active_set_) {
      columns.push_back(md->ToArrowArray());
    }
    active_rb_ = arrow::RecordBatch::Make(schema, size, columns);
  }
  return active_rb_;
}

vdb::vector<std::shared_ptr<vdb::InactiveSet>>& Segment::InactiveSets() {
  return inactive_sets_;
}

std::shared_ptr<arrow::RecordBatch> Segment::InactiveSetRecordBatch(size_t i) {
  if (inactive_sets_.empty()) return nullptr;
  return inactive_sets_[i]->GetRb();
}

std::shared_ptr<arrow::Schema> Segment::GetSchema() {
  auto table = table_.lock();
  return table->GetSchema();
}

std::shared_ptr<arrow::RecordBatch> Segment::GetRecordbatch(uint32_t set_id) {
  if (set_id == ActiveSetId()) {
    return ActiveSetRecordBatch();
  } else {
    return InactiveSetRecordBatch(set_id);
  }
}

InactiveSet::InactiveSet(std::shared_ptr<arrow::RecordBatch> rb,
                         std::shared_ptr<arrow::Buffer> buffer)
    : rb_{rb}, buffer_{buffer} {}

InactiveSet::InactiveSet(
    std::shared_ptr<arrow::Schema>& schema,
    vdb::vector<std::shared_ptr<vdb::MutableArray>>& arrays)
    : buffer_{nullptr} {
  std::swap(columns_, arrays);
  std::vector<std::shared_ptr<arrow::Array>> arrow_columns;
  size_t size = columns_[0]->Size();
  for (auto& arr : columns_) {
    arrow_columns.emplace_back(arr->ToArrowArray());
    if (size != arr->Size()) {
      throw std::invalid_argument("Unknown error: Column size is different.");
    }
  }

  rb_ = arrow::RecordBatch::Make(schema, columns_[0]->Size(), arrow_columns);
}

arrow::Result<std::shared_ptr<vdb::Table>> TableBuilder::Build() {
  std::shared_ptr<vdb::Table> table;
  if (!options_.table_directory_path_.empty()) {
    ARROW_ASSIGN_OR_RAISE(table, BuildTableFromSavedFile());
  } else {
    ARROW_ASSIGN_OR_RAISE(table, BuildTableFromSchema());
  }

  if (table->HasIndex() && server.allow_bg_index_thread) {
    table->StartIndexingThread();
  }

  auto status = table->SetActiveSetSizeLimit();
  if (!status.ok()) {
    return arrow::Status::Invalid(status.ToString());
  }

  if (!options_.table_directory_path_.empty()) {
    auto status = table->LoadSegments(table, options_.table_directory_path_);
    if (!status.ok()) {
      return arrow::Status::Invalid(status.ToString());
    }
  }

  return table;
}

arrow::Result<std::shared_ptr<vdb::Table>>
TableBuilder::BuildTableFromSchema() {
  auto segment_id_info =
      options_.schema_->metadata()->Get(kSegmentIdInfoKey).ValueOr("");
  if (!segment_id_info.empty()) {
    auto array_view = UInt32ArrayView::create(segment_id_info).value();
    if (std::any_of(array_view.begin(), array_view.end(), [&](auto index) {
          SYSTEM_LOG(vdb::LogLevel::kLogVerbose,
                     "Segment id column index: %d/%d", index,
                     options_.schema_->fields().size());
          return index >= options_.schema_->fields().size();
        })) {
      return arrow::Status::Invalid(
          "invalid schema: segment_id_info has invalid column index");
    }
  }

  auto ann_column_id = std::stoi(
      options_.schema_->metadata()->Get(kAnnColumnIdKey).ValueOr("-1"));
  if (ann_column_id != -1) {
    auto field_type = options_.schema_->field(ann_column_id)->type();
    if (field_type->id() != arrow::Type::FIXED_SIZE_LIST) {
      return arrow::Status::Invalid(
          "invalid schema: ann column is not fixed size list type.");
    }
    auto fixed_size_list_type =
        std::static_pointer_cast<arrow::FixedSizeListType>(field_type);
    auto value_type = fixed_size_list_type->value_type();
    if (value_type->id() != arrow::Type::FLOAT) {
      return arrow::Status::Invalid(
          "invalid schema: ann column does not consist of float values.");
    }
  }

  auto table =
      std::make_shared<vdb::Table>(options_.table_name_, options_.schema_);

  SYSTEM_LOG(vdb::LogLevel::kLogVerbose, "Creating Table (%s) is Done.",
             options_.table_name_.data());

  return table;
}

arrow::Result<std::shared_ptr<vdb::Table>>
TableBuilder::BuildTableFromSavedFile() {
  SYSTEM_LOG(
      vdb::LogLevel::kLogVerbose, "Loading Table (%s) from (%s) is Started.",
      options_.table_name_.data(), options_.table_directory_path_.data());

  /* check directory path */
  std::error_code ec;
  if (!std::filesystem::exists(options_.table_directory_path_, ec)) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Loading Table (%s) from (%s) is Failed: %s",
               options_.table_name_.data(),
               options_.table_directory_path_.data(), ec.message().data());
    return arrow::Status::Invalid(ec.message());
  }

  /* load manifest */
  std::string manifest_file_path = options_.table_directory_path_ + "/manifest";
  /* existence of manifest shows completeness of saving snapshot */
  if (!std::filesystem::exists(manifest_file_path, ec)) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Loading Table (%s) Manifest from (%s) is Failed: %s",
               options_.table_name_.data(), manifest_file_path.data(),
               ec.message().data());
    return arrow::Status::Invalid("saving snapshot of table is not completed");
  }

  auto table = vdb::make_shared<vdb::Table>();
  auto status = table->LoadManifest(manifest_file_path);
  if (!status.ok()) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice,
               "Loading Table (%s) Manifest from (%s) is Failed: %s",
               options_.table_name_.data(), manifest_file_path.data(),
               status.ToString().data());
    return arrow::Status::Invalid(status.ToString());
  }

  SYSTEM_LOG(
      vdb::LogLevel::kLogVerbose,
      "Loading Table (%s) from (%s) is Done: ", options_.table_name_.data(),
      options_.table_directory_path_.data());

  return table;
}

}  // namespace vdb
