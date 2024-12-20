#pragma once

#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include "vdb/common/status.hh"
#include "vdb/common/memory_allocator.hh"
#include "vdb/data/mutable_array.hh"

namespace vdb {

namespace tests {
class TableWrapper;
}  // namespace tests

typedef char* sds;

class Table;
class IndexHandler;
class VectorIndex;

// Index, Embedding Pointer, Starting Label, Size
using EmbeddingJobInfo =
    std::tuple<std::shared_ptr<VectorIndex>, float*, size_t, size_t>;

struct ScanResult {
  ScanResult() : status{Status::kUnknown} {}
  Status status;
  std::vector<std::shared_ptr<arrow::RecordBatch>> data;

  bool ok() const { return status.ok(); }
};

class InactiveSet {
 public:
  InactiveSet(std::shared_ptr<arrow::RecordBatch> rb,
              std::shared_ptr<arrow::Buffer> buffer);
  InactiveSet(std::shared_ptr<arrow::Schema>& schema,
              vdb::vector<std::shared_ptr<vdb::MutableArray>>& arrays);

  inline std::shared_ptr<arrow::RecordBatch> GetRb() const { return rb_; }

  std::string ToString() const { return rb_->ToString(); }

 private:
  std::shared_ptr<arrow::RecordBatch> rb_;
  /* for keep buffer from mutable array */
  vdb::vector<std::shared_ptr<vdb::MutableArray>> columns_;
  /* for keep buffer from loading snapshot */
  std::shared_ptr<arrow::Buffer> buffer_;
};

class Segment {
 public:
  /* this function may throw an exception */
  explicit Segment(std::shared_ptr<Table> table, const std::string& segment_id);

  /* load from snapshot */
  /* this function may throw an exception */
  explicit Segment(std::shared_ptr<Table> table, const std::string& segment_id,
                   std::string& segment_directory_path);

  /* save as snapshot */
  Status Save(std::string& segment_directory_path);

  Status AppendRecord(const std::string_view& record_string);
  Status AppendRecords(
      std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches);

  Status AddRecordBatch(std::shared_ptr<arrow::RecordBatch>& rb);

  Status AddEmbedding(const float* embedding, const uint32_t set_number,
                      const size_t record_number);
  Status AddEmbedding(const float* embedding, const uint32_t set_number,
                      const size_t record_number, const size_t size);

  Status MakeInactive();

  ScanResult SearchKnn(const float* query, const size_t& k) const;

  inline std::string_view GetId() const {
    return std::string_view(segment_id_);
  }

  inline size_t Size() const { return size_; }

  inline uint32_t InactiveSetCount() const { return inactive_sets_.size(); }

  inline uint32_t ActiveSetId() const { return inactive_sets_.size(); }

  inline size_t ActiveSetRecordCount() const { return active_set_[0]->Size(); }

  inline bool HasIndex() const { return index_handler_ != nullptr; }

  std::shared_ptr<arrow::RecordBatch> ActiveSetRecordBatch() const;
  std::shared_ptr<arrow::RecordBatch> ActiveSetRecordBatch();
  vdb::vector<std::shared_ptr<vdb::InactiveSet>>& InactiveSets();
  std::shared_ptr<arrow::RecordBatch> InactiveSetRecordBatch(size_t i);

  std::shared_ptr<arrow::RecordBatch> GetRecordbatch(uint32_t set_id);

  inline std::shared_ptr<vdb::IndexHandler> IndexHandler() const {
    return index_handler_;
  }

  std::string ToString(bool show_records = true) const;

  std::shared_ptr<arrow::Schema> GetSchema();

 private:
  /* utilities for saving segment */
  Status SaveManifest(std::string& file_path) const;

  std::weak_ptr<Table> table_;
  vdb::string segment_id_;
  vdb::vector<std::shared_ptr<vdb::InactiveSet>> inactive_sets_;
  vdb::vector<std::shared_ptr<vdb::MutableArray>> active_set_;
  size_t size_;

  std::shared_ptr<vdb::IndexHandler> index_handler_;
  std::shared_ptr<arrow::RecordBatch> active_rb_;
};

// Table class represents table entity of the database. It should be equivalent
// to the SQL table.
// `table_version_` will be monotonically increased if `schema_` is changed.
class Table {
 public:
  explicit Table();

  explicit Table(const std::string& table_name,
                 std::shared_ptr<arrow::Schema>& schema);

  ~Table();

  Status LoadSegments(std::shared_ptr<Table> table,
                      std::string& table_directory_path);

  /* save as snapshot */
  Status Save(std::string_view& snapshot_directory_path);

  /* segment */
  std::shared_ptr<Segment> AddSegment(const std::shared_ptr<Table>& table,
                                      const std::string& segment_id);

  std::shared_ptr<Segment> GetSegment(const std::string& segment_id) const;

  const vdb::map<std::string_view, std::shared_ptr<Segment>>& GetSegments()
      const {
    return segments_;
  }

  /* schema */
  Status SetSchema(const std::shared_ptr<arrow::Schema>& schema);
  Status SetMetadata(const char* key, std::string value);

  std::shared_ptr<arrow::Schema> GetSchema() const { return schema_; }
  arrow::Schema* GetRawSchema() const { return schema_.get(); }
  arrow::Result<int> GetAnnColumnId() const;
  arrow::Result<int32_t> GetAnnDimension() const;

  /* etc. */
  vdb::string GetTableName() const { return table_name_; }

  bool HasIndex() const;

  Status SetActiveSetSizeLimit();
  Status SetActiveSetSizeLimit(size_t active_set_size_limit);
  size_t GetActiveSetSizeLimit() const;

  std::vector<std::string_view> GetSegmentIdColumnNames() const;
  std::vector<uint32_t> GetSegmentIdColumnIndexes() const;

  std::string ToString(bool show_records = false,
                       bool show_metadata = true) const;

  void StartIndexingThread();
  bool IsIndexing() const;
  void StopIndexingThread();

 private:
  /* utilities for load table */
  Status LoadManifest(std::string& file_path);

  /* utilities for save table */
  Status SaveSegmentIds(std::string& file_path);
  Status SaveManifest(std::string& file_path);

  Status AddMetadata(const std::shared_ptr<arrow::KeyValueMetadata>& metadata);

  vdb::string table_name_;
  std::size_t table_version_;
  std::shared_ptr<arrow::Schema> schema_;
  vdb::map<std::string_view, std::shared_ptr<Segment>> segments_;
  size_t active_set_size_limit_;

 protected:
  /* if table has ann feature column and
     server.allow_bg_indexing_thread is on, then indexing thread will
     be spawned. */
  // TODO manage total number of threads for this process. (config)
  // TODO add config for default max_threads?
  void IndexingThreadJob();
  std::condition_variable cond_;
  std::mutex job_queue_mtx_;
  std::deque<EmbeddingJobInfo> job_queue_;
  std::shared_ptr<std::thread> indexing_thread_ = nullptr;
  std::atomic_bool indexing_ = false;
  std::atomic_bool terminate_thread_ = false;

  friend class IndexHandler;
  friend class TableBuilder;
  friend class tests::TableWrapper;
};

class TableBuilderOptions {
 public:
  explicit TableBuilderOptions() {}

  explicit TableBuilderOptions(const TableBuilderOptions& rhs) = default;

  explicit TableBuilderOptions(TableBuilderOptions&& rhs) = default;

  TableBuilderOptions& operator=(const TableBuilderOptions& rhs) = default;

  TableBuilderOptions& operator=(TableBuilderOptions&& rhs) = default;

  TableBuilderOptions& SetTableName(std::string_view table_name) {
    table_name_ = table_name;
    return *this;
  }

  TableBuilderOptions& SetSchema(std::shared_ptr<arrow::Schema> schema) {
    schema_ = schema;
    return *this;
  }

  TableBuilderOptions& SetTableDirectoryPath(
      std::string_view table_directory_path) {
    table_directory_path_ = table_directory_path;
    return *this;
  }

 protected:
  std::string table_name_;
  std::shared_ptr<arrow::Schema> schema_;
  std::string table_directory_path_;

  friend class TableBuilder;
};

class TableBuilder {
 public:
  explicit TableBuilder(const TableBuilderOptions& options)
      : options_{options} {}

  explicit TableBuilder(TableBuilderOptions&& options) : options_{options} {}

  arrow::Result<std::shared_ptr<vdb::Table>> Build();

 protected:
  arrow::Result<std::shared_ptr<vdb::Table>> BuildTableFromSchema();

  arrow::Result<std::shared_ptr<vdb::Table>> BuildTableFromSavedFile();

  TableBuilderOptions options_;
};

/* prototype declaration for unit tests */
arrow::Result<std::pair<std::shared_ptr<arrow::RecordBatch>,
                        std::shared_ptr<arrow::Buffer>>>
_LoadRecordBatchFrom(std::string& file_path,
                     std::shared_ptr<arrow::Schema>& schema);
Status _SaveRecordBatchTo(std::string& file_path,
                          std::shared_ptr<arrow::RecordBatch> rb);
}  // namespace vdb
