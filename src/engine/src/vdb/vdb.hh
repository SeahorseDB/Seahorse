#pragma once

#include <map>

#include <arrow/acero/api.h>

#include "vdb/common/memory_allocator.hh"
#include "vdb/common/status.hh"
#include "vdb/data/table.hh"

namespace vdb {
// TODO: Memory allocator issue
struct client;

void _TestCommand();

std::vector<std::string_view> _ListTableCommand();

Status _CreateTableCommand(sds serialized_schema);

Status _DropTableCommand(const std::string& table_name);

arrow::Result<std::shared_ptr<arrow::Buffer>> _DescribeTableCommand(
    const std::string& table_name);

Status _InsertCommand(const std::string& table_name,
                      const std::string_view record);

Status _BatchInsertCommand(const std::string& table_name, sds serialized_rbs);

arrow::Result<std::string> _DebugScanCommand(const std::string& table_name,
                                             std::string_view proj_list_string,
                                             std::string_view filter_string);

arrow::Result<std::shared_ptr<arrow::Buffer>> _ScanCommand(
    const std::string& table_name, std::string_view proj_list_string,
    std::string_view filter_string);

arrow::Result<std::shared_ptr<arrow::Buffer>> _AnnCommand(
    const std::string& table_name, size_t k, const sds query, size_t ef_search,
    std::string_view projection, std::string_view filter);

arrow::Result<std::vector<std::shared_ptr<arrow::Buffer>>> _BatchAnnCommand(
    std::string_view table_name, size_t k, const sds query_vectors,
    size_t ef_search, std::string_view projection, std::string_view filter);

arrow::Result<bool> _CheckIndexingCommand(const std::string& table_name);

arrow::Result<std::pair<size_t, size_t>> _CountIndexedElementsCommand(
    const std::string& table_name);

vdb::map<std::string, std::shared_ptr<Table>>* GetTableDictionary();

arrow::Result<std::shared_ptr<Table>> GetTable(const std::string& table_name);

arrow::Result<std::shared_ptr<arrow::Table>> _ExecutePlan(
    std::shared_ptr<arrow::Schema> schema,
    arrow::AsyncGenerator<std::optional<arrow::ExecBatch>> generator,
    std::optional<std::string_view> projection,
    std::optional<arrow::compute::Expression> filter);

arrow::Result<std::shared_ptr<arrow::Table>> _BuildAnnResultTable(
    const std::vector<std::tuple<float, size_t, Segment*>>& ann_result,
    std::shared_ptr<arrow::Schema> schema, size_t k,
    std::string_view projection);

}  // namespace vdb
