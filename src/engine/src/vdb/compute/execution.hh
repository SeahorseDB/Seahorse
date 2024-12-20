#pragma once

#include <memory>
#include <optional>

#include <arrow/acero/api.h>
#include <arrow/util/vector.h>

#include "vdb/data/table.hh"

namespace vdb {

arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> SegmentGenerator(
    std::vector<std::shared_ptr<Segment>>& segments);

arrow::Result<std::shared_ptr<arrow::Buffer>> SerializeTable(
    std::shared_ptr<arrow::Table> table);

arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> RecordBatchGenerator(
    std::shared_ptr<arrow::RecordBatch> rb);

arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> RecordBatchesGenerator(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches);

}  // namespace vdb
