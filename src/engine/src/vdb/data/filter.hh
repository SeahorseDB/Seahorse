#pragma once

#include <memory>
#include <string_view>

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include "vdb/data/table.hh"

namespace vdb {

using sds = char*;
struct Filter {
  enum Type {
    kNone,
    kAnd,
    kOr,
    kIsNull,
    kIsNotNull,
    kEqual,
    kNotEqual,
    kLessThan,
    kGreaterThanEqual,
    kLessThanEqual,
    kGreaterThan,
    kIn,
    kNotIn,
    kLike,
    kStringStartsWith,
    kStringEndsWith,
    kStringContains,
    kNotLike,
    kNotStringStartsWith,
    kNotStringEndsWith,
    kNotStringContains,
    kColumn,
    kLiteral
  };

  Filter();
  Filter(Type type);
  Filter(Type type, std::shared_ptr<Filter>& unary_filter);
  Filter(Type type, std::shared_ptr<Filter>& lhs, std::shared_ptr<Filter>& rhs);

  std::string ToString() const;

  static std::shared_ptr<Filter>& None();

  // Filter ParseToken(std::vector<std::string_view>& tokens, const
  // std::shared_ptr<arrow::Schema> &schema);
  static std::shared_ptr<Filter> Parse(
      const std::string_view& filter_string,
      const std::shared_ptr<arrow::Schema>& schema);

  static std::vector<std::shared_ptr<Filter>> GetSegmentFilters(
      std::shared_ptr<Filter>& filter,
      const std::shared_ptr<arrow::Schema>& schema);

  static std::vector<std::shared_ptr<Segment>> Segments(
      const vdb::map<std::string_view, std::shared_ptr<Segment>>& segments,
      const std::vector<std::shared_ptr<Filter>>& filters,
      const std::shared_ptr<arrow::Schema>& schema);

  static arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
  Records(const std::shared_ptr<Segment>& segment,
          const std::shared_ptr<Filter>& filter,
          const std::shared_ptr<arrow::Schema>& schema);

  Type type;
  std::shared_ptr<Filter> left_child = nullptr;
  std::shared_ptr<Filter> right_child = nullptr;

  std::string_view value;
  bool is_segment_filter = false;
};

arrow::compute::Expression GetExpression(
    const std::shared_ptr<Filter>& filter,
    const std::shared_ptr<arrow::Schema>& schema);

}  // namespace vdb
