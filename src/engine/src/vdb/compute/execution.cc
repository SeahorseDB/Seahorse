#include "vdb/compute/execution.hh"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

namespace vdb {

void ConvertSegmentToExecBatches(std::shared_ptr<Segment> &segment,
                                 std::vector<arrow::ExecBatch> &exec_batches) {
  std::shared_ptr<arrow::RecordBatch> batch;
  for (auto &set : segment->InactiveSets()) {
    batch = set->GetRb();
    if (batch == nullptr) break;
    exec_batches.emplace_back(*batch);
  }
  batch = segment->ActiveSetRecordBatch();
  exec_batches.emplace_back(*batch);
}

std::vector<arrow::ExecBatch> CreateExecBatchesFrom(
    std::vector<std::shared_ptr<Segment>> &segments) {
  std::vector<arrow::ExecBatch> exec_batches;
  for (auto &segment : segments) {
    ConvertSegmentToExecBatches(segment, exec_batches);
  }
  return exec_batches;
}

arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> SegmentGenerator(
    std::vector<std::shared_ptr<Segment>> &segments) {
  auto batches = CreateExecBatchesFrom(segments);
  auto opt_batches = arrow::internal::MapVector(
      [](arrow::ExecBatch batch) {
        return std::make_optional(std::move(batch));
      },
      std::move(batches));
  return arrow::MakeVectorGenerator(std::move(opt_batches));
}

arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> RecordBatchGenerator(
    std::shared_ptr<arrow::RecordBatch> rb) {
  std::vector<arrow::ExecBatch> batches;
  batches.push_back(arrow::ExecBatch(*rb));
  auto opt_batches = arrow::internal::MapVector(
      [](arrow::ExecBatch batch) {
        return std::make_optional(std::move(batch));
      },
      std::move(batches));
  return arrow::MakeVectorGenerator(std::move(opt_batches));
}

arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> RecordBatchesGenerator(
    const std::vector<std::shared_ptr<arrow::RecordBatch>> &record_batches) {
  std::vector<arrow::ExecBatch> batches;
  for (const auto &rb : record_batches) {
    if (rb && rb->num_rows() > 0) {
      batches.push_back(arrow::ExecBatch(*rb));
    }
  }
  auto opt_batches = arrow::internal::MapVector(
      [](arrow::ExecBatch batch) {
        return std::make_optional(std::move(batch));
      },
      std::move(batches));
  return arrow::MakeVectorGenerator(std::move(opt_batches));
}

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTable(
    std::shared_ptr<arrow::Table> table) {
  // Create an in-memory output stream
  ARROW_ASSIGN_OR_RAISE(auto out, arrow::io::BufferOutputStream::Create());

  // Create an IPC stream writer
  ARROW_ASSIGN_OR_RAISE(auto writer,
                        arrow::ipc::MakeStreamWriter(out, table->schema()));

  if (table) {
    auto maybe_batches = arrow::TableBatchReader(table).ToRecordBatches();
    ARROW_ASSIGN_OR_RAISE(auto result_batches, maybe_batches);
    for (auto &batch : result_batches) {
      if (batch->num_rows() != 0) {
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
      }
    }
  }

  ARROW_RETURN_NOT_OK(writer->Close());
  return out->Finish();
}

}  // namespace vdb
