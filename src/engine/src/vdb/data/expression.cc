#include "vdb/data/expression.hh"

#include <memory>
#include <set>
#include <sstream>

#include <arrow/ipc/api.h>
#include <string>
#include <strings.h>
#include <vector>

#include "vdb/data/table.hh"
#include "vdb/common/util.hh"

namespace vdb::expression {

arrow::compute::Expression Column::BuildArrowExpression() const {
  return arrow::compute::field_ref(name_);
}

arrow::compute::Expression Projection::BuildArrowExpression() const {
  return expr_->BuildArrowExpression();
}

arrow::compute::Expression Literal::BuildArrowExpression() const {
  return arrow::compute::literal(scalar_);
}

arrow::compute::Expression And::BuildArrowExpression() const {
  auto exprs = std::vector<arrow::compute::Expression>();
  for (const auto& expr : exprs_) {
    exprs.push_back(expr->BuildArrowExpression());
  }
  return arrow::compute::and_(exprs);
}

std::string And::ToString() const {
  std::vector<std::string> strs;
  for (const auto& expr : exprs_) {
    strs.push_back(expr->ToString());
  }
  return "(" + vdb::Join(strs, " AND ") + ")";
}

arrow::compute::Expression Or::BuildArrowExpression() const {
  auto exprs = std::vector<arrow::compute::Expression>();
  for (const auto& expr : exprs_) {
    exprs.push_back(expr->BuildArrowExpression());
  }
  return arrow::compute::or_(exprs);
}

std::string Or::ToString() const {
  std::vector<std::string> strs;
  for (const auto& expr : exprs_) {
    strs.push_back(expr->ToString());
  }
  return "(" + vdb::Join(strs, " OR ") + ")";
}

std::string Comparison::ToString() const {
  const char* op_str;
  switch (operation_) {
    case ComparisonOperation::kEqual:
      op_str = "=";
      break;
    case ComparisonOperation::kNotEqual:
      op_str = "!=";
      break;
    case ComparisonOperation::kGreaterThan:
      op_str = ">";
      break;
    case ComparisonOperation::kGreaterThanEqual:
      op_str = ">=";
      break;
    case ComparisonOperation::kLessThan:
      op_str = "<";
      break;
    case ComparisonOperation::kLessThanEqual:
      op_str = "<=";
      break;
    default:
      std::invalid_argument("Must not reach here.");
  }
  return left_->ToString() + " " + op_str + " " + right_->ToString();
}

std::string IsNull::ToString() const {
  return is_negated_ ? expr_->ToString() + " IS NOT NULL"
                     : expr_->ToString() + " IS NULL";
}

std::string In::ToString() const {
  return is_negated_
             ? expr_->ToString() + " NOT IN " + "[" + vdb::Join(values_, ',') +
                   "]"
             : expr_->ToString() + " IN " + "[" + vdb::Join(values_, ',') + "]";
}

std::string Like::ToString() const {
  return is_negated_ ? expr_->ToString() + " NOT LIKE " + pattern_
                     : expr_->ToString() + " LIKE " + pattern_;
}

std::string Not::ToString() const { return "NOT " + expr_->ToString(); }

arrow::compute::Expression Comparison::BuildArrowExpression() const {
  switch (operation_) {
    case ComparisonOperation::kEqual:
      return arrow::compute::equal(left_->BuildArrowExpression(),
                                   right_->BuildArrowExpression());
    case ComparisonOperation::kNotEqual:
      return arrow::compute::not_equal(left_->BuildArrowExpression(),
                                       right_->BuildArrowExpression());
    case ComparisonOperation::kGreaterThan:
      return arrow::compute::greater(left_->BuildArrowExpression(),
                                     right_->BuildArrowExpression());
    case ComparisonOperation::kGreaterThanEqual:
      return arrow::compute::greater_equal(left_->BuildArrowExpression(),
                                           right_->BuildArrowExpression());
    case ComparisonOperation::kLessThan:
      return arrow::compute::less(left_->BuildArrowExpression(),
                                  right_->BuildArrowExpression());
    case ComparisonOperation::kLessThanEqual:
      return arrow::compute::less_equal(left_->BuildArrowExpression(),
                                        right_->BuildArrowExpression());
    default:
      std::invalid_argument("Must not reach here.");
  }
}

arrow::compute::Expression In::BuildArrowExpression() const {
  if (expr_->GetType() != ExpressionType::kColumn) {
    throw std::invalid_argument(
        "Left hand side of 'in' expression must be a column.");
  }

  auto lhs = expr_->BuildArrowExpression();
  auto set = "[" + vdb::Join(values_, ',') + "]";
  auto type = dynamic_cast<Column*>(expr_.get())->GetDatatype();
  auto setarr_result = arrow::ipc::internal::json::ArrayFromJSON(type, set);
  if (!setarr_result.ok()) {
    throw std::invalid_argument("Wrong arguments for 'in' expression: " +
                                setarr_result.status().ToString());
  }
  auto rhs = arrow::compute::SetLookupOptions(setarr_result.ValueUnsafe());

  auto in_expr = arrow::compute::call("is_in", {lhs}, rhs);
  if (is_negated_) {
    return arrow::compute::not_(in_expr);
  }
  return in_expr;
}

arrow::compute::Expression IsNull::BuildArrowExpression() const {
  auto is_null_expr = arrow::compute::is_null(expr_->BuildArrowExpression());
  if (is_negated_) {
    return arrow::compute::not_(is_null_expr);
  }
  return is_null_expr;
}

arrow::compute::Expression Like::BuildArrowExpression() const {
  auto like_expr = arrow::compute::call(
      "match_like", {expr_->BuildArrowExpression()},
      arrow::compute::MatchSubstringOptions(pattern_, false));
  if (is_negated_) {
    return arrow::compute::not_(like_expr);
  }
  return like_expr;
}

arrow::compute::Expression StringMatch::BuildArrowExpression() const {
  std::string op;
  switch (op_type_) {
    case StringOperationType::kStartsWith:
      op = "starts_with";
      break;
    case StringOperationType::kEndsWith:
      op = "ends_with";
      break;
    case StringOperationType::kContains:
      op = "match_like";
      break;
    default:
      std::invalid_argument("Must not reach here.");
  }

  auto string_match_expr = arrow::compute::call(
      op, {expr_->BuildArrowExpression()},
      arrow::compute::MatchSubstringOptions(pattern_, false));
  if (is_negated_) {
    return arrow::compute::not_(string_match_expr);
  }
  return string_match_expr;
}

arrow::compute::Expression Not::BuildArrowExpression() const {
  return arrow::compute::not_(expr_->BuildArrowExpression());
}

arrow::Result<std::vector<std::shared_ptr<Expression>>>
Expression::ParseSimpleProjectionList(
    std::string_view proj_list_string,
    std::shared_ptr<arrow::Schema> table_schema) {
  std::vector<std::shared_ptr<Expression>> ret;

  if (proj_list_string.empty()) {
    return arrow::Status::Invalid(
        "Could not parse projection list: Empty projection list");
  }

  if (proj_list_string == "*") {
    return ret;
  }

  auto tokens = Tokenize(proj_list_string, ',');
  if (tokens.empty()) {
    return ret;
  }

  auto names = table_schema->field_names();
  std::set<std::string> name_set(std::make_move_iterator(names.begin()),
                                 std::make_move_iterator(names.end()));

  auto trim = [](std::string_view str) -> std::string_view {
    const std::string_view whitespace = " \t\n\r\f\v";
    const auto start = str.find_first_not_of(whitespace);
    if (start == std::string_view::npos) {
      return {};  // String is all whitespace
    }
    const auto end = str.find_last_not_of(whitespace);
    return str.substr(start, end - start + 1);
  };

  for (const auto& token : tokens) {
    std::string col_name(trim(token));
    auto it = name_set.find(col_name);
    if (it == name_set.end()) {
      std::stringstream ss;
      ss << "Could not find column '" << token << "' from table schema";
      return arrow::Status::Invalid(ss.str());
    }
    ret.push_back(
        std::make_shared<Projection>(std::make_shared<Column>(col_name)));
  }
  return ret;
}

arrow::Result<std::vector<std::shared_ptr<Expression>>>
ExpressionBuilder::ParseProjectionList(std::string_view proj_list_string) {
  std::vector<std::shared_ptr<Expression>> ret;

  if (proj_list_string.empty()) {
    return arrow::Status::Invalid(
        "Could not parse projection list: Empty projection list");
  }

  if (proj_list_string == "*") {
    return ret;
  }

  auto tokens = vdb::Tokenize(proj_list_string, ',');
  if (tokens.empty()) {
    return ret;
  }

  std::unordered_set<std::string> name_set(
      std::make_move_iterator(schema_->field_names().begin()),
      std::make_move_iterator(schema_->field_names().end()));

  auto trim = [](std::string_view str) -> std::string_view {
    const std::string_view whitespace = " \t\n\r\f\v";
    const auto start = str.find_first_not_of(whitespace);
    if (start == std::string_view::npos) {
      return {};  // String is all whitespace
    }
    const auto end = str.find_last_not_of(whitespace);
    return str.substr(start, end - start + 1);
  };

  for (const auto& token : tokens) {
    std::string col_name(trim(token));
    if (name_set.find(col_name) == name_set.end()) {
      std::stringstream ss;
      ss << "Could not find column '" << token << "' from table schema";
      return arrow::Status::Invalid(ss.str());
    }
    ret.push_back(std::make_shared<Projection>(std::make_shared<Column>(
        col_name, schema_->GetFieldByName(col_name)->type())));
  }
  return ret;
}

std::vector<std::string> ExpressionBuilder::TokenizeFilter(
    std::string_view filter_string) {
  std::vector<std::string> tokens;
  std::string currentToken;
  bool inQuotes = false;

  auto pushToken = [&]() {
    if (!currentToken.empty()) {
      tokens.push_back(currentToken);
      currentToken.clear();
    }
  };

  for (size_t i = 0; i < filter_string.length(); ++i) {
    char c = filter_string[i];

    if (c == '\'') {
      inQuotes = !inQuotes;
      currentToken += c;
    } else if (inQuotes) {
      currentToken += c;
    } else if (std::isspace(c)) {
      pushToken();
    } else if (c == '(' || c == ')' || c == ',') {
      pushToken();
      tokens.push_back(std::string(1, c));
    } else if (c == '=' || c == '!' || c == '>' || c == '<') {
      pushToken();
      currentToken += c;
      if (i + 1 < filter_string.length() && filter_string[i + 1] == '=') {
        currentToken += '=';
        ++i;
      }
      pushToken();
    } else {
      currentToken += c;
    }
  }
  pushToken();

  // Post-process tokens to handle IS NULL, IS NOT NULL, etc.
  std::vector<std::string> processedTokens;
  for (size_t i = 0; i < tokens.size(); ++i) {
    if (i + 2 < tokens.size() && strcasecmp(tokens[i].c_str(), "IS") == 0 &&
        strcasecmp(tokens[i + 1].c_str(), "NOT") == 0 &&
        strcasecmp(tokens[i + 2].c_str(), "NULL") == 0) {
      processedTokens.push_back("IS NOT NULL");
      i += 2;
    } else if (i + 1 < tokens.size() &&
               strcasecmp(tokens[i].c_str(), "IS") == 0 &&
               strcasecmp(tokens[i + 1].c_str(), "NULL") == 0) {
      processedTokens.push_back("IS NULL");
      i += 1;
    } else if (i + 1 < tokens.size() &&
               strcasecmp(tokens[i].c_str(), "NOT") == 0) {
      if (strcasecmp(tokens[i + 1].c_str(), "IN") == 0) {
        processedTokens.push_back("NOT IN");
        i += 1;
      } else if (strcasecmp(tokens[i + 1].c_str(), "LIKE") == 0) {
        processedTokens.push_back("NOT LIKE");
        i += 1;
      } else {  // only NOT
        processedTokens.push_back("NOT");
      }
    } else if (strcasecmp(tokens[i].c_str(), "IN") == 0) {
      processedTokens.push_back("IN");
    } else if (strcasecmp(tokens[i].c_str(), "LIKE") == 0) {
      processedTokens.push_back("LIKE");
    } else if (strcasecmp(tokens[i].c_str(), "AND") == 0) {
      processedTokens.push_back("AND");
    } else if (strcasecmp(tokens[i].c_str(), "OR") == 0) {
      processedTokens.push_back("OR");
    } else {
      processedTokens.push_back(tokens[i]);
    }
  }

  return processedTokens;
}

arrow::Result<std::shared_ptr<Expression>> ExpressionBuilder::ParseSimpleFilter(
    std::vector<std::string>& tokens) {
  if (tokens.empty()) {
    return arrow::Status::Invalid("Unexpected end of input");
  }

  std::string column_name = tokens[0];
  auto field = schema_->GetFieldByName(column_name);
  if (!field) {
    return arrow::Status::Invalid("Column not found: " + column_name);
  }
  tokens.erase(tokens.begin());

  if (tokens.empty()) {
    return arrow::Status::Invalid("Unexpected end of input after column name");
  }

  if (tokens[0] == "IS NULL" || tokens[0] == "IS NOT NULL") {
    bool is_negated = tokens[0] == "IS NOT NULL";
    tokens.erase(tokens.begin());
    return std::make_shared<IsNull>(
        std::make_shared<Column>(column_name,
                                 schema_->GetFieldByName(column_name)->type()),
        is_negated);
  } else if (tokens[0] == "IN" || tokens[0] == "NOT IN") {
    bool is_negated = tokens[0] == "NOT IN";
    tokens.erase(tokens.begin());
    if (tokens.empty() || tokens[0] != "(") {
      return arrow::Status::Invalid("Expected opening parenthesis after IN");
    }
    tokens.erase(tokens.begin());

    std::vector<std::string> values;
    while (!tokens.empty() && tokens[0] != ")") {
      values.push_back(tokens[0]);
      tokens.erase(tokens.begin());
      if (!tokens.empty() && tokens[0] == ",") {
        tokens.erase(tokens.begin());
      }
    }
    if (tokens.empty() || tokens[0] != ")") {
      return arrow::Status::Invalid(
          "Expected closing parenthesis after IN list");
    }
    tokens.erase(tokens.begin());
    return std::make_shared<In>(
        std::make_shared<Column>(column_name,
                                 schema_->GetFieldByName(column_name)->type()),
        values, is_negated);
  } else if (tokens[0] == "LIKE" || tokens[0] == "NOT LIKE") {
    bool is_negated = tokens[0] == "NOT LIKE";
    tokens.erase(tokens.begin());
    if (tokens.empty()) {
      return arrow::Status::Invalid("Unexpected end of input after LIKE");
    }
    std::string pattern = tokens[0];
    tokens.erase(tokens.begin());
    return std::make_shared<Like>(
        std::make_shared<Column>(column_name,
                                 schema_->GetFieldByName(column_name)->type()),
        pattern, is_negated);
  } else {
    ComparisonOperation op;
    if (tokens[0] == "=") {
      op = ComparisonOperation::kEqual;
    } else if (tokens[0] == "!=") {
      op = ComparisonOperation::kNotEqual;
    } else if (tokens[0] == ">") {
      op = ComparisonOperation::kGreaterThan;
    } else if (tokens[0] == "<") {
      op = ComparisonOperation::kLessThan;
    } else if (tokens[0] == ">=") {
      op = ComparisonOperation::kGreaterThanEqual;
    } else if (tokens[0] == "<=") {
      op = ComparisonOperation::kLessThanEqual;
    } else {
      return arrow::Status::Invalid("Invalid operator: " + tokens[0]);
    }

    tokens.erase(tokens.begin());
    if (tokens.empty()) {
      return arrow::Status::Invalid("Unexpected end of input after operator");
    }
    std::string value = tokens[0];
    tokens.erase(tokens.begin());
    return std::make_shared<Comparison>(
        op,
        std::make_shared<Column>(column_name,
                                 schema_->GetFieldByName(column_name)->type()),
        std::make_shared<Literal>(value));
  }
}

arrow::Result<std::shared_ptr<Expression>> ExpressionBuilder::ParseExpression(
    std::vector<std::string>& tokens) {
  std::vector<std::shared_ptr<Expression>> expressions;

  ARROW_ASSIGN_OR_RAISE(auto first_expression, ParseAndExpression(tokens));
  expressions.push_back(std::move(first_expression));

  while (!tokens.empty() && tokens[0] == "OR") {
    tokens.erase(tokens.begin());  // Remove "OR"
    ARROW_ASSIGN_OR_RAISE(auto next_expression, ParseAndExpression(tokens));
    expressions.push_back(std::move(next_expression));
  }

  if (expressions.size() == 1) {
    return std::move(expressions[0]);
  }

  return std::make_shared<Or>(std::move(expressions));
}

arrow::Result<std::shared_ptr<Expression>>
ExpressionBuilder::ParseAndExpression(std::vector<std::string>& tokens) {
  std::vector<std::shared_ptr<Expression>> expressions;

  // Parse the first primary expression
  ARROW_ASSIGN_OR_RAISE(auto first_expression, ParsePrimaryExpression(tokens));
  expressions.push_back(std::move(first_expression));

  // Parse subsequent AND-connected expressions
  while (!tokens.empty() && tokens[0] == "AND") {
    tokens.erase(tokens.begin());  // Remove "AND"
    ARROW_ASSIGN_OR_RAISE(auto next_expression, ParsePrimaryExpression(tokens));
    expressions.push_back(std::move(next_expression));
  }

  // If there's only one expression, return it directly
  if (expressions.size() == 1) {
    return std::move(expressions[0]);
  }

  // Otherwise, create and return an And expression
  return std::make_shared<And>(std::move(expressions));
}

arrow::Result<std::shared_ptr<Expression>>
ExpressionBuilder::ParsePrimaryExpression(std::vector<std::string>& tokens) {
  if (tokens.empty()) {
    return arrow::Status::Invalid("Unexpected end of input");
  }

  if (tokens[0] == "(") {
    tokens.erase(tokens.begin());  // Remove opening parenthesis
    ARROW_ASSIGN_OR_RAISE(auto expression, ParseExpression(tokens));
    if (tokens.empty() || tokens[0] != ")") {
      return arrow::Status::Invalid("Mismatched parentheses");
    }
    tokens.erase(tokens.begin());  // Remove closing parenthesis
    return expression;
  } else if (tokens[0] == "NOT") {
    tokens.erase(tokens.begin());  // Remove "NOT"
    ARROW_ASSIGN_OR_RAISE(auto inner_expression,
                          ParsePrimaryExpression(tokens));
    return std::make_shared<Not>(std::move(inner_expression));
  } else {
    return ParseSimpleFilter(tokens);
  }
}

arrow::Result<std::shared_ptr<Expression>> ExpressionBuilder::ParseFilter(
    std::string_view filter_string) {
  auto tokens = TokenizeFilter(filter_string);
  if (tokens.empty()) {
    return arrow::Status::Invalid("Empty filter string");
  }

  // std::cout << "tokens=" << vdb::Join(tokens, ",") << std::endl;

  auto result = ParseExpression(tokens);
  return result;
}

}  // namespace vdb::expression