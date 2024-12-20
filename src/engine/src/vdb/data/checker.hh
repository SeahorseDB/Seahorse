#pragma once

#include <arrow/api.h>

namespace vdb {

arrow::Status CheckQueryVectors(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& rbs,
    size_t dimension);

arrow::Status CheckRecordBatchIsInsertable(
    const std::shared_ptr<arrow::Schema>& rbs_schema,
    const std::shared_ptr<arrow::Schema>& table_schema);

}  // namespace vdb
