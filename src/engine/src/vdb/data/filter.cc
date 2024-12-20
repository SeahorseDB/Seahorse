#include <algorithm>
#include <charconv>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string_view>

#include <arrow/compute/api.h>
#include <arrow/ipc/api.h>

#include "vdb/common/util.hh"
#include "vdb/data/filter.hh"

#ifdef __cplusplus
extern "C" {
#include "server.h"
}
#endif

namespace vdb {

Filter::Filter() : type{Filter::kNone} {}
Filter::Filter(Type t) : type{t} {}
Filter::Filter(Type t, std::shared_ptr<Filter>& unary_op)
    : type{t}, left_child{unary_op}, right_child{Filter::None()} {}
Filter::Filter(Type t, std::shared_ptr<Filter>& left_op,
               std::shared_ptr<Filter>& right_op)
    : type{t}, left_child{left_op}, right_child{right_op} {}

std::string Filter::ToString() const {
  static const std::vector<std::string> kTypeStrings = {"None",
                                                        "AND",
                                                        "OR",
                                                        "Is null",
                                                        "Is not null",
                                                        "==",
                                                        "!=",
                                                        "<",
                                                        ">=",
                                                        "<=",
                                                        ">",
                                                        "In",
                                                        "Not in",
                                                        "Like",
                                                        "Starts with",
                                                        "Ends with",
                                                        "Contains",
                                                        "Not like",
                                                        "Does not start with",
                                                        "Does not end with",
                                                        "Does not contain",
                                                        "Column",
                                                        "Literal"};
  std::stringstream ss;
  if (left_child) {
    ss << "(";
    ss << left_child->ToString();
    ss << " ";
  }
  if (type == kLiteral) ss << "\"";
  if (type >= kColumn)
    ss << value;
  else
    ss << kTypeStrings[type];
  if (type == kLiteral) ss << "\"";
  if (right_child) {
    ss << " ";
    ss << right_child->ToString();
  }
  if (left_child) {
    ss << ")";
  }
  return ss.str();
}

std::shared_ptr<Filter>& Filter::None() {
  static std::shared_ptr<Filter> none = std::make_shared<Filter>();
  return none;
}

// Note: We do not have `Not` type filter node. We simply use De Morgan's laws.
// not (A or B) == (not A) and (not B)
std::shared_ptr<Filter> ParseToken(std::vector<std::string_view>& tokens,
                                   const std::shared_ptr<arrow::Schema>& schema,
                                   bool is_not) {
  auto token = tokens.back();
  tokens.pop_back();

  if (token == "not" || token == "!") {
    if (is_not)
      return ParseToken(tokens, schema, false);
    else
      return ParseToken(tokens, schema, true);
  }

  auto node = std::make_shared<Filter>();
  bool is_data_node = false;
  bool is_unary = false;

  if (token == "||" || token == "or") {
    if (is_not) {
      node->type = Filter::kAnd;
    } else {
      node->type = Filter::kOr;
    }
  } else if (token == "&&" || token == "and") {
    if (is_not) {
      node->type = Filter::kOr;
    } else {
      node->type = Filter::kAnd;
    }
  } else if (token == "isnull") {
    is_unary = true;
    if (is_not) {
      node->type = Filter::kIsNotNull;
    } else {
      node->type = Filter::kIsNull;
    }
  } else if (token == "isnotnull") {
    is_unary = true;
    if (is_not) {
      node->type = Filter::kIsNull;
    } else {
      node->type = Filter::kIsNotNull;
    }
  } else if (token == "=" || token == "eq") {
    if (is_not) {
      node->type = Filter::kNotEqual;
    } else {
      node->type = Filter::kEqual;
    }
  } else if (token == "!=" || token == "neq") {
    if (is_not) {
      node->type = Filter::kEqual;
    } else {
      node->type = Filter::kNotEqual;
    }
  } else if (token == "<" || token == "lt") {
    if (is_not) {
      node->type = Filter::kGreaterThanEqual;
    } else {
      node->type = Filter::kLessThan;
    }
  } else if (token == "<=" || token == "lte") {
    if (is_not) {
      node->type = Filter::kGreaterThan;
    } else {
      node->type = Filter::kLessThanEqual;
    }
  } else if (token == ">" || token == "gt") {
    if (is_not) {
      node->type = Filter::kLessThanEqual;
    } else {
      node->type = Filter::kGreaterThan;
    }
  } else if (token == ">=" || token == "gte") {
    if (is_not) {
      node->type = Filter::kLessThan;
    } else {
      node->type = Filter::kGreaterThanEqual;
    }
  } else if (token == "in") {
    if (is_not) {
      node->type = Filter::kNotIn;
    } else {
      node->type = Filter::kIn;
    }
  } else if (token == "like") {
    node->type = Filter::kLike;
  } else if (schema->GetFieldByName(std::string{token})) {
    node->type = Filter::kColumn;
    is_data_node = true;
  } else {
    node->type = Filter::kLiteral;
    is_data_node = true;
  }

  node->value = token;

  if (is_data_node) {
    return node;
  }

  if (!is_unary) node->right_child = ParseToken(tokens, schema, is_not);
  node->left_child = ParseToken(tokens, schema, is_not);

  if (node->type == Filter::kLike) {
    // This might be a weird scenario?
    if (node->left_child->type == Filter::kLiteral) {
      if (node->right_child->type == Filter::kColumn) {
        std::swap(node->left_child, node->right_child);
      } else {
        throw std::invalid_argument("Like operator has wrong arguments.");
      }
    }
    if (node->right_child->value.front() == '%') {
      if (node->right_child->value.back() == '%') {
        node->type = Filter::kStringContains;
        node->right_child->value.remove_prefix(1);
        node->right_child->value.remove_suffix(1);
      } else {
        node->type = Filter::kStringEndsWith;
        node->right_child->value.remove_prefix(1);
      }
    } else if (node->right_child->value.back() == '%') {
      node->type = Filter::kStringStartsWith;
      node->right_child->value.remove_suffix(1);
    } else {
      throw std::invalid_argument("Like operator has wrong literal argument.");
    }
    if (is_not) node->type = (Filter::Type)(node->type + 4);
  }

  return node;
}

// Assuming each token in `filter_string` is separated by Record separator.
// Assuming postorder tree traversal
// Assuming lower case
std::shared_ptr<Filter> Filter::Parse(
    const std::string_view& filter_string,
    const std::shared_ptr<arrow::Schema>& schema) {
  auto tokens = Tokenize(filter_string, '\x1e');
  if (tokens.empty()) return nullptr;
  return ParseToken(tokens, schema, false);
}

// Extract segment ftilers from the given filter tree. Return a sorted vector
// of pointers to the filter. Return an empty vector if there is no segment
// filter available.
std::vector<std::shared_ptr<Filter>> Filter::GetSegmentFilters(
    std::shared_ptr<Filter>& filter,
    const std::shared_ptr<arrow::Schema>& schema) {
  if (!filter) return {};
  auto metadata = schema->metadata();
  auto segment_id_info = metadata->Get("segment_id_info").ValueOr("");
  if (segment_id_info == "") return {};
  auto segment_id_info_count = segment_id_info.size() / sizeof(uint32_t);
  std::vector<std::string_view> segment_id_column_names;
  segment_id_column_names.reserve(segment_id_info_count);
  for (uint32_t i = 0; i < segment_id_info_count; i++) {
    segment_id_column_names.emplace_back(schema->field(i)->name());
  }
  std::vector<std::shared_ptr<Filter>> stack;
  std::vector<std::shared_ptr<Filter>> segment_filters;
  stack.push_back(filter);
  while (!stack.empty()) {
    auto cur = stack.back();
    stack.pop_back();

    // Any condition under `OR` should be examined at the data level
    if (cur->type == Filter::Type::kOr || cur->type >= Filter::Type::kColumn)
      continue;

    if (cur->left_child->type == Filter::Type::kColumn) {
      for (auto& column_name : segment_id_column_names) {
        if (column_name == cur->left_child->value) {
          cur->is_segment_filter = true;
          if (cur->type == Filter::Type::kIsNull ||
              cur->type == Filter::Type::kIsNotNull ||
              cur->right_child->type != Filter::Type::kColumn) {
            segment_filters.push_back(cur);
            break;
          }
        }
      }
    }

    // Unary filters don't have right child
    if (cur->type != Filter::Type::kIsNull &&
        cur->type != Filter::Type::kIsNotNull &&
        cur->left_child->type !=
            Filter::Type::kColumn) {  // If left child is a column node, this
                                      // node cannot be a segment filter as it
                                      // compares column against the column.
      if (cur->right_child->type == Filter::Type::kColumn) {
        for (auto& segment : segment_id_column_names) {
          if (segment == cur->right_child->value) {
            cur->is_segment_filter = true;
            segment_filters.push_back(cur);
          }
        }
        // Guarantee that the left child is always a column node if available.
        std::swap(cur->left_child, cur->right_child);
        if (cur->type == kLessThan) cur->type = kGreaterThan;
        if (cur->type == kGreaterThan) cur->type = kLessThan;
        if (cur->type == kGreaterThanEqual) cur->type = kLessThanEqual;
        if (cur->type == kLessThanEqual) cur->type = kGreaterThanEqual;
      }
    }

    if (cur->type != Filter::Type::kIsNull &&
        cur->type != Filter::Type::kIsNotNull) {
      stack.push_back(cur->right_child);
    }
    stack.push_back(cur->left_child);
  }

  std::sort(segment_filters.begin(), segment_filters.end(),
            [&](const std::shared_ptr<Filter>& lhs,
                const std::shared_ptr<Filter>& rhs) {
              for (auto& column_name : segment_id_column_names) {
                if (lhs->left_child->value == column_name) return true;
                if (rhs->left_child->value == column_name) return false;
              }
              return true;
            });

  return segment_filters;
}

bool ApplySegmentFilter(const std::string_view& segment_id_token,
                        const std::shared_ptr<Filter>& filter,
                        const bool is_numeric) {
  // TODO: numeric filter can be further optimized.
  // - literal should be materialized only once
  // - or string_view can be directly used to interpret numeric value
  /* unary relation */
  switch (filter->type) {
    case Filter::kIsNull: {
      return false;
    }
    case Filter::kIsNotNull: {
      return true;
    }
    default: {
    } break;
  }
  /* binary relation */
  auto literal = filter->right_child->value;
  switch (filter->type) {
    case Filter::kEqual: {
      if (segment_id_token == literal) return true;
    } break;
    case Filter::kNotEqual: {
      if (segment_id_token != literal) return true;
    } break;
    case Filter::kLessThan: {
      if (is_numeric) {
        return std::stof(std::string(segment_id_token)) <
               std::stof(std::string(literal));
      } else {
        if (segment_id_token < literal) return true;
      }
    } break;
    case Filter::kLessThanEqual: {
      if (is_numeric) {
        return std::stof(std::string(segment_id_token)) <=
               std::stof(std::string(literal));
      } else {
        if (segment_id_token <= literal) return true;
      }
    } break;
    case Filter::kGreaterThan: {
      if (is_numeric) {
        return std::stof(std::string(segment_id_token)) >
               std::stof(std::string(literal));
      } else {
        if (segment_id_token > literal) return true;
      }
    } break;
    case Filter::kGreaterThanEqual: {
      if (is_numeric) {
        return std::stof(std::string(segment_id_token)) >=
               std::stof(std::string(literal));
      } else {
        if (segment_id_token >= literal) return true;
      }
    } break;
    case Filter::kIn: {
      return literal.find(segment_id_token) != std::string::npos;
    } break;
    case Filter::kNotIn: {
      return literal.find(segment_id_token) == std::string::npos;
    } break;
    case Filter::kStringStartsWith: {
      return literal == segment_id_token.substr(0, literal.size());
    } break;
    case Filter::kStringEndsWith: {
      return literal == segment_id_token.substr(segment_id_token.size() -
                                                literal.size() - 1);
    } break;
    case Filter::kStringContains: {
      return segment_id_token.find(literal) != std::string::npos;
    } break;
    case Filter::kNotStringStartsWith: {
      return literal != segment_id_token.substr(0, literal.size());
    } break;
    case Filter::kNotStringEndsWith: {
      return literal != segment_id_token.substr(segment_id_token.size() -
                                                literal.size() - 1);
    } break;
    case Filter::kNotStringContains: {
      return segment_id_token.find(literal) == std::string::npos;
    } break;
    default:
      throw std::invalid_argument("Something wrong with Segment Filter.");
      // or just return false;?
  }
  return false;
}

std::vector<std::shared_ptr<Segment>> Filter::Segments(
    const vdb::map<std::string_view, std::shared_ptr<Segment>>& segments,
    const std::vector<std::shared_ptr<Filter>>& filters,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<std::shared_ptr<Segment>> filtered;
  auto segment_id_info = schema->metadata()->Get("segment_id_info").ValueOr("");
  auto segment_id_info_count = segment_id_info.size() / sizeof(uint32_t);
  std::vector<bool> numeric_info;
  std::vector<std::vector<std::shared_ptr<Filter>>> filter_map;
  for (uint32_t i = 0; i < segment_id_info_count; i++) {
    uint32_t pid = *(reinterpret_cast<uint32_t*>(segment_id_info.data()) + i);
    auto type_id = schema->field(pid)->type()->id();
    if (type_id > 1 && type_id < 13)
      numeric_info.push_back(true);
    else
      numeric_info.push_back(false);
  }

  // Filters are separated by its targets
  filter_map.resize(segment_id_info_count);
  for (size_t i = 0; i < segment_id_info_count; i++) {
    uint32_t pid = *(reinterpret_cast<uint32_t*>(segment_id_info.data()) + i);
    for (auto& filter : filters) {
      if (filter->left_child->value == schema->field(pid)->name()) {
        filter_map[i].push_back(filter);
      }
    }
  }

  // TODO: required to reduce filters before apply them

  // TODO: This may have performance issue. Revisit this later.
  for (auto& kv : segments) {
    auto segment_id_tokens = Tokenize(kv.first, kRS);
    bool is_filtered = false;
    for (size_t i = 0; i < segment_id_info_count; i++) {
      for (auto& filter : filter_map[i]) {
        is_filtered =
            !ApplySegmentFilter(segment_id_tokens[i], filter, numeric_info[i]);
        if (is_filtered) break;
      }
      if (is_filtered) break;
    }
    if (!is_filtered) filtered.push_back(kv.second);
  }

  return filtered;
}

arrow::compute::Expression GetLiteral(
    const std::shared_ptr<arrow::DataType>& type,
    const std::string_view& value) {
  switch (type->id()) {
    case arrow::Type::BOOL:
      return arrow::compute::literal(value != "0");
    // Note: std::from_chars() for float and double are not supported by the
    // compiler right now.
    case arrow::Type::FLOAT: {
      auto size = value.size();
      auto str_end = value.data() + size;
      return arrow::compute::literal(strtof(value.data(), (char**)&str_end));
    }
    case arrow::Type::DOUBLE: {
      auto size = value.size();
      auto str_end = value.data() + size;
      return arrow::compute::literal(strtod(value.data(), (char**)&str_end));
    }
#define __CASE(ARROW_TYPE, TYPE)                                           \
  {                                                                        \
    case ARROW_TYPE: {                                                     \
      TYPE ret;                                                            \
      auto [ptr, ec] =                                                     \
          std::from_chars(value.data(), value.data() + value.size(), ret); \
      if (ec == std::errc::invalid_argument)                               \
        throw std::invalid_argument("GetLiteral: Wrong literal.");         \
      if (ec == std::errc::result_out_of_range)                            \
        throw std::out_of_range("GetLiteral: Wrong literal.");             \
      return arrow::compute::literal(ret);                                 \
    }                                                                      \
  }
      __CASE(arrow::Type::INT8, int8_t)
      __CASE(arrow::Type::INT16, int16_t)
      __CASE(arrow::Type::INT32, int32_t)
      __CASE(arrow::Type::INT64, int64_t)
      __CASE(arrow::Type::UINT8, uint8_t)
      __CASE(arrow::Type::UINT16, uint16_t)
      __CASE(arrow::Type::UINT32, uint32_t)
      __CASE(arrow::Type::UINT64, uint64_t)
#undef __CASE
    default:  // Other than numeric is treated as a string.
      return arrow::compute::literal(std::string(value));
  }
}

arrow::compute::Expression GetExpression(
    const std::shared_ptr<Filter>& filter,
    const std::shared_ptr<arrow::Schema>& schema) {
  switch (filter->type) {
    case Filter::Type::kAnd: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetExpression(filter->right_child, schema);
      return arrow::compute::and_(lhs, rhs);
    } break;
    case Filter::Type::kOr: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetExpression(filter->right_child, schema);
      return arrow::compute::or_(lhs, rhs);
    } break;
    case Filter::Type::kIsNull: {
      auto lhs = GetExpression(filter->left_child, schema);
      return arrow::compute::is_null(lhs);
    } break;
    case Filter::Type::kIsNotNull: {
      auto lhs = GetExpression(filter->left_child, schema);
      return arrow::compute::not_(arrow::compute::is_null(lhs));
    } break;
    case Filter::Type::kEqual: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetLiteral(
          schema->GetFieldByName(std::string(filter->left_child->value))
              ->type(),
          filter->right_child->value);
      return arrow::compute::equal(lhs, rhs);
    } break;
    case Filter::Type::kNotEqual: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetLiteral(
          schema->GetFieldByName(std::string(filter->left_child->value))
              ->type(),
          filter->right_child->value);
      return arrow::compute::not_(arrow::compute::equal(lhs, rhs));
    } break;
    case Filter::Type::kLessThan: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetLiteral(
          schema->GetFieldByName(std::string(filter->left_child->value))
              ->type(),
          filter->right_child->value);
      return arrow::compute::less(lhs, rhs);
    } break;
    case Filter::Type::kLessThanEqual: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetLiteral(
          schema->GetFieldByName(std::string(filter->left_child->value))
              ->type(),
          filter->right_child->value);
      return arrow::compute::less_equal(lhs, rhs);
    } break;
    case Filter::Type::kGreaterThan: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetLiteral(
          schema->GetFieldByName(std::string(filter->left_child->value))
              ->type(),
          filter->right_child->value);
      return arrow::compute::greater(lhs, rhs);
    } break;
    case Filter::Type::kGreaterThanEqual: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = GetLiteral(
          schema->GetFieldByName(std::string(filter->left_child->value))
              ->type(),
          filter->right_child->value);
      return arrow::compute::greater_equal(lhs, rhs);
    } break;
    case Filter::Type::kIn: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto tokens = Tokenize(filter->right_child->value);
      auto set = "[" + Join(tokens, ',') + "]";
      auto type = schema->GetFieldByName(std::string(filter->left_child->value))
                      ->type();
      auto setarr = arrow::ipc::internal::json::ArrayFromJSON(type, set);
      if (!setarr.status().ok()) {
        throw std::invalid_argument("Wrong arguments for 'in' expression.");
      }
      auto rhs = arrow::compute::SetLookupOptions(setarr.ValueUnsafe());

      return arrow::compute::call("is_in", {lhs}, rhs);
    } break;
    case Filter::Type::kStringStartsWith: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = arrow::compute::MatchSubstringOptions(
          std::string(filter->right_child->value), false);
      return arrow::compute::call("starts_with", {lhs}, rhs);
    } break;
    case Filter::Type::kStringEndsWith: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = arrow::compute::MatchSubstringOptions(
          std::string(filter->right_child->value), false);
      return arrow::compute::call("ends_with", {lhs}, rhs);
    } break;
    case Filter::Type::kStringContains: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = arrow::compute::MatchSubstringOptions(
          std::string(filter->right_child->value), false);
      return arrow::compute::call("match_like", {lhs}, rhs);
    } break;
    case Filter::Type::kNotStringStartsWith: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = arrow::compute::MatchSubstringOptions(
          std::string(filter->right_child->value), false);
      return arrow::compute::not_(
          arrow::compute::call("starts_with", {lhs}, rhs));
    } break;
    case Filter::Type::kNotStringEndsWith: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = arrow::compute::MatchSubstringOptions(
          std::string(filter->right_child->value), false);
      return arrow::compute::not_(
          arrow::compute::call("ends_with", {lhs}, rhs));
    } break;
    case Filter::Type::kNotStringContains: {
      auto lhs = GetExpression(filter->left_child, schema);
      auto rhs = arrow::compute::MatchSubstringOptions(
          std::string(filter->right_child->value), false);
      return arrow::compute::not_(
          arrow::compute::call("match_like", {lhs}, rhs));
    } break;
    case Filter::Type::kColumn: {
      return arrow::compute::field_ref(std::string(filter->value));
    } break;
    case Filter::Type::kLiteral: {
      return arrow::compute::literal(std::string(filter->value));
    } break;
    default:
      throw std::invalid_argument("No such filter exists.");
  }
  throw std::invalid_argument("No expression is matched to filter.");
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> _FilterRecordBatch(
    arrow::compute::Expression& bound_expr,
    const std::shared_ptr<arrow::RecordBatch>& rb) {
  auto eb = arrow::compute::ExecBatch(*rb);

  auto filter_result = arrow::compute::ExecuteScalarExpression(bound_expr, eb);
  if (!filter_result.ok()) {
    return filter_result.status();
  }
  auto filter_expr = filter_result.ValueUnsafe();

  auto filtered_result = arrow::compute::Filter(rb, filter_expr);
  if (!filtered_result.ok()) {
    return filter_result.status();
  }
  auto filtered_rb = filtered_result.ValueUnsafe().record_batch();

  return filtered_rb;
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>> Filter::Records(
    const std::shared_ptr<Segment>& segment,
    const std::shared_ptr<Filter>& filter,
    const std::shared_ptr<arrow::Schema>& schema) {
  if (!filter) {
    std::vector<std::shared_ptr<arrow::RecordBatch>> all_rbs;
    for (auto set : segment->InactiveSets()) {
      if (set->GetRb()->num_rows() != 0) {
        all_rbs.push_back(set->GetRb());
      }
    }
    auto active_rb = segment->ActiveSetRecordBatch();
    if (active_rb->num_rows() != 0) {
      all_rbs.emplace_back(active_rb);
    }
    return all_rbs;
  }
  auto expr = GetExpression(filter, schema);
  auto bind_result = expr.Bind(*schema);
  if (!bind_result.ok()) {
    return bind_result.status();
  }
  auto bound_expr = bind_result.ValueUnsafe();

  std::vector<std::shared_ptr<arrow::RecordBatch>> filtered_rbs;
  for (auto set : segment->InactiveSets()) {
    /* inactive rb must not be empty. */
    auto maybe_filtered_rb = _FilterRecordBatch(bound_expr, set->GetRb());
    if (!maybe_filtered_rb.ok()) {
      return maybe_filtered_rb.status();
    }
    auto filtered_rb = maybe_filtered_rb.ValueUnsafe();
    if (filtered_rb->num_rows() != 0) filtered_rbs.push_back(filtered_rb);
  }
  auto rb = segment->ActiveSetRecordBatch();
  if (rb->num_rows() != 0) {
    auto maybe_filtered_rb = _FilterRecordBatch(bound_expr, rb);
    if (!maybe_filtered_rb.ok()) {
      return maybe_filtered_rb.status();
    }
    auto filtered_rb = maybe_filtered_rb.ValueUnsafe();
    if (filtered_rb->num_rows() != 0) filtered_rbs.push_back(filtered_rb);
  }
  return filtered_rbs;
}

}  // namespace vdb
