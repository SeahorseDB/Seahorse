#pragma once
#include <memory>

#include "vdb/data/table.hh"
#include "vdb/vdb.hh"
#include "vdb/vdb_api.hh"
#include "vdb/data/index_handler.hh"

#ifdef __cplusplus
extern "C" {
#include "sds.h"
#include "server.h"
}
#endif
extern std::string empty_string;

/* index handler for test */
class IndexHandlerForTest : public vdb::IndexHandler {
 public:
  IndexHandlerForTest(std::shared_ptr<vdb::Table> table)
      : vdb::IndexHandler{table} {}
  IndexHandlerForTest(std::shared_ptr<vdb::Table> table,
                      std::string &index_directory_path,
                      const uint64_t index_count)
      : vdb::IndexHandler{table, index_directory_path, index_count} {}
  vdb::Status AddEmbeddingForTest(const float *data, size_t label) {
    auto index = indexes_.back();
    if (index->IsFull()) {
      auto status = CreateIndex();
      if (!status.ok()) {
        return status;
      }
      index = indexes_.back();
    }
    return index->AddEmbedding(data, label);
  }
};
/* to string */
std::string VecToStr(const float *vec, size_t dim);
/* comparison */
bool EmbeddingEquals(const float *lhs, const float *rhs, size_t dimension);
bool IndexEquals(std::shared_ptr<vdb::VectorIndex> &lhs,
                 std::shared_ptr<vdb::VectorIndex> &rhs);
bool IndexHandlerEquals(std::shared_ptr<vdb::IndexHandler> &lhs,
                        std::shared_ptr<vdb::IndexHandler> &rhs);
bool SegmentEquals(std::shared_ptr<vdb::Segment> &lhs,
                   std::shared_ptr<vdb::Segment> &rhs);
bool TableEquals(std::shared_ptr<vdb::Table> &lhs,
                 std::shared_ptr<vdb::Table> &rhs);
/* check index build complete */
bool TableIndexFullBuildCheck(std::shared_ptr<vdb::Table> &table,
                              const bool print_log);
/* generate sample data */
std::vector<std::vector<float>> generateSequentialFloatArray(int rows, int dim,
                                                             int start);
std::vector<std::vector<float>> generateRandomFloatArray(int rows, int dim,
                                                         bool xy_same = false);

arrow::Result<std::shared_ptr<arrow::Array>> GenerateRandomInt32Array(
    int64_t length);
arrow::Result<std::shared_ptr<arrow::Array>> GenerateRandomFloat32Array(
    int64_t length);
arrow::Result<std::shared_ptr<arrow::Array>> GenerateRandomStringArray(
    int64_t length);
arrow::Result<std::shared_ptr<arrow::Array>>
GenerateRandomFloatFixedSizeListArray(int64_t length, int32_t list_size);
arrow::Result<std::shared_ptr<arrow::RecordBatch>> GenerateRecordBatch(
    std::shared_ptr<arrow::Schema> schema, int64_t length, int32_t dimension);

/* exact knn */
float L2Sqr(const void *pVect1v, const void *pVect2v, const void *qty_ptr);
std::vector<std::pair<float, const float *>> SearchExactKnn(
    const float *query, const std::vector<std::vector<float>> &points,
    size_t k);

/* handle IPC to Table/RecordBatches */
arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeRecordBatches(
    std::shared_ptr<arrow::Schema> schema,
    std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches);
arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTable(
    std::shared_ptr<arrow::Table> table);
arrow::Result<std::shared_ptr<arrow::Table>> DeserializeToTableFrom(
    const std::shared_ptr<arrow::Buffer> &serialized_rb);
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
DeserializeToRecordBatchesFrom(
    const std::shared_ptr<arrow::Buffer> &serialized_rb);

/* sort a table */
arrow::Result<std::shared_ptr<arrow::Table>> SortTable(
    const std::shared_ptr<arrow::Table> &table);

/* snapshot test names */
std::string GetBaseSnapshotDirectoryName(long long sequence);
std::string GetTempSnapshotDirectoryName(pid_t pid);
/* etc */
std::vector<std::pair<float, const float *>> ResultToDistEmbeddingPairs(
    std::shared_ptr<vdb::Hnsw> &index,
    std::priority_queue<std::pair<float, size_t>> &pq);
std::vector<std::pair<float, const float *>> ResultToDistEmbeddingPairs(
    std::shared_ptr<IndexHandlerForTest> &index_handler,
    std::vector<std::pair<float, size_t>> &vec);
float CalculateKnnAccuracy(
    std::vector<std::pair<float, const float *>> &index_result,
    std::vector<std::pair<float, const float *>> &knn_result);

vdb::Status CreateTableForTest(std::string table_name,
                               std::string schema_string);

namespace vdb::tests {

class TableWrapper {
 public:
  static void AddMetadata(
      std::shared_ptr<vdb::Table> table,
      const std::shared_ptr<arrow::KeyValueMetadata> &metadata) {
    table->AddMetadata(metadata);
  }
};

}  // namespace vdb::tests
