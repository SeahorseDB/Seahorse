#pragma once

#include <arrow/compute/api.h>
#include <memory>
#include <string>

namespace vdb::expression {

enum class ExpressionType { kNone, kPredicate, kProjection, kColumn, kLiteral };

enum class PredicateType {
  kAnd,
  kOr,
  kComparison,
  kIsNull,
  kIn,
  kLike,
  kStringMatch,
  kNot
};

enum class ComparisonOperation {
  kEqual,
  kNotEqual,
  kLessThan,
  kGreaterThan,
  kLessThanEqual,
  kGreaterThanEqual
};

enum class StringOperationType {
  kStartsWith,
  kEndsWith,
  kContains,
};

class Expression {
 public:
  Expression() = delete;

  Expression(ExpressionType type) : type_(type) {}

  Expression(const Expression& rhs) = default;

  Expression(Expression&& rhs) = default;

  Expression& operator=(const Expression& rhs) = default;

  Expression& operator=(Expression&& rhs) = default;

  virtual ~Expression() = default;

  ExpressionType GetType() const { return type_; }

  virtual arrow::compute::Expression BuildArrowExpression() const = 0;

  virtual std::string ToString() const = 0;

  static arrow::Result<std::vector<std::shared_ptr<Expression>>>
  ParseSimpleProjectionList(std::string_view proj_list_string,
                            std::shared_ptr<arrow::Schema> table_schema);

 protected:
  ExpressionType type_;
};

class Column : public Expression {
 public:
  Column() = delete;

  Column(const std::string name)
      : Expression(ExpressionType::kColumn), name_(name) {}

  Column(const std::string name, std::shared_ptr<arrow::DataType> datatype)
      : Expression(ExpressionType::kColumn), name_(name), datatype_(datatype) {}

  Column(const Column& rhs) = default;

  Column(Column&& rhs) = default;

  Column& operator=(const Column& rhs) = default;

  Column& operator=(Column&& rhs) = default;

  virtual ~Column() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override { return name_; }

  std::shared_ptr<arrow::DataType> GetDatatype() const { return datatype_; }

 protected:
  std::string name_;
  std::shared_ptr<arrow::DataType> datatype_;
};

class Projection : public Expression {
 public:
  Projection() = delete;

  Projection(std::shared_ptr<Expression> expr)
      : Expression(ExpressionType::kProjection), expr_(expr) {}

  Projection(const Projection& rhs) = default;

  Projection(Projection&& rhs) = default;

  Projection& operator=(const Projection& rhs) = default;

  virtual ~Projection() = default;

  Projection& operator=(Projection&& rhs) = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override { return expr_->ToString(); }

 protected:
  std::shared_ptr<Expression> expr_;
};

class Literal : public Expression {
 public:
  Literal() = delete;

  Literal(std::string scalar)
      : Expression(ExpressionType::kLiteral), scalar_(scalar) {}

  Literal(const Literal& rhs) = default;

  Literal(Literal&& rhs) = default;

  Literal& operator=(const Literal& rhs) = default;

  virtual ~Literal() = default;

  Literal& operator=(Literal&& rhs) = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override { return scalar_; }

 protected:
  std::string scalar_;
};

class Predicate : public Expression {
 public:
  Predicate() = delete;

  Predicate(PredicateType type)
      : Expression(ExpressionType::kPredicate), pred_type_(type) {}

  Predicate(const Predicate& rhs) = default;

  Predicate(Predicate&& rhs) = default;

  Predicate& operator=(const Predicate& rhs) = default;

  Predicate& operator=(Predicate&& rhs) = default;

  virtual ~Predicate() = default;

  PredicateType GetType() const { return pred_type_; }

 protected:
  PredicateType pred_type_;
};

class And : public Predicate {
 public:
  And() = delete;

  And(std::vector<std::shared_ptr<Expression>> exprs)
      : Predicate(PredicateType::kAnd), exprs_(exprs) {}

  And(const And& rhs) = default;

  And(And&& rhs) = default;

  And& operator=(const And& rhs) = default;

  And& operator=(And&& rhs) = default;

  virtual ~And() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override;

 protected:
  std::vector<std::shared_ptr<Expression>> exprs_;
};

class Or : public Predicate {
 public:
  Or() = delete;

  Or(std::vector<std::shared_ptr<Expression>> exprs)
      : Predicate(PredicateType::kOr), exprs_(exprs) {}

  Or(const Or& rhs) = default;

  Or(Or&& rhs) = default;

  Or& operator=(const Or& rhs) = default;

  Or& operator=(Or&& rhs) = default;

  virtual ~Or() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override;

 protected:
  std::vector<std::shared_ptr<Expression>> exprs_;
};

class Comparison : public Predicate {
 public:
  Comparison() = delete;

  Comparison(ComparisonOperation operation, std::shared_ptr<Expression> left,
             std::shared_ptr<Expression> right)
      : Predicate(PredicateType::kComparison),
        operation_(operation),
        left_(left),
        right_(right) {}

  Comparison(const Comparison& rhs) = default;

  Comparison(Comparison&& rhs) = default;

  Comparison& operator=(const Comparison& rhs) = default;

  Comparison& operator=(Comparison&& rhs) = default;

  virtual ~Comparison() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override;

 protected:
  ComparisonOperation operation_;
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

class IsNull : public Predicate {
 public:
  IsNull(std::shared_ptr<Expression> expr)
      : Predicate(PredicateType::kIsNull), expr_(expr), is_negated_(false) {}

  IsNull(std::shared_ptr<Expression> expr, bool is_negated)
      : Predicate(PredicateType::kIsNull),
        expr_(expr),
        is_negated_(is_negated) {}

  IsNull(const IsNull& rhs) = default;

  IsNull(IsNull&& rhs) = default;

  IsNull& operator=(const IsNull& rhs) = default;

  IsNull& operator=(IsNull&& rhs) = default;

  virtual ~IsNull() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override;

 protected:
  std::shared_ptr<Expression> expr_;
  bool is_negated_;
};

class In : public Predicate {
 public:
  In() = delete;

  In(std::shared_ptr<Expression> expr, std::vector<std::string> values)
      : Predicate(PredicateType::kIn),
        expr_(expr),
        values_(values),
        is_negated_(false) {}

  In(std::shared_ptr<Expression> expr, std::vector<std::string> values,
     bool is_negated)
      : Predicate(PredicateType::kIn),
        expr_(expr),
        values_(values),
        is_negated_(is_negated) {}

  In(const In& rhs) = default;

  In(In&& rhs) = default;

  In& operator=(const In& rhs) = default;

  In& operator=(In&& rhs) = default;

  virtual ~In() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override;

 protected:
  std::shared_ptr<Expression> expr_;
  std::vector<std::string> values_;
  bool is_negated_;
};

class Like : public Predicate {
 public:
  Like() = delete;

  Like(std::shared_ptr<Expression> expr, std::string pattern)
      : Predicate(PredicateType::kLike),
        expr_(expr),
        pattern_(pattern),
        is_negated_(false) {}

  Like(std::shared_ptr<Expression> expr, std::string pattern, bool is_negated)
      : Predicate(PredicateType::kLike),
        expr_(expr),
        pattern_(pattern),
        is_negated_(is_negated) {}

  Like(const Like& rhs) = default;

  Like(Like&& rhs) = default;

  Like& operator=(const Like& rhs) = default;

  Like& operator=(Like&& rhs) = default;

  virtual ~Like() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override;

 protected:
  std::shared_ptr<Expression> expr_;
  std::string pattern_;
  bool is_negated_;
};

class StringMatch : public Predicate {
 public:
  StringMatch() = delete;

  StringMatch(std::shared_ptr<Expression> expr, std::string pattern,
              StringOperationType op_type)
      : Predicate(PredicateType::kStringMatch),
        expr_(expr),
        pattern_(pattern),
        op_type_(op_type),
        is_negated_(false) {}

  StringMatch(std::shared_ptr<Expression> expr, std::string pattern,
              StringOperationType op_type, bool is_negated)
      : Predicate(PredicateType::kStringMatch),
        expr_(expr),
        pattern_(pattern),
        op_type_(op_type),
        is_negated_(is_negated) {}

  StringMatch(const StringMatch& rhs) = default;

  StringMatch(StringMatch&& rhs) = default;

  StringMatch& operator=(const StringMatch& rhs) = default;

  StringMatch& operator=(StringMatch&& rhs) = default;

  virtual ~StringMatch() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override { return ""; }

 protected:
  std::shared_ptr<Expression> expr_;
  std::string pattern_;
  StringOperationType op_type_;
  bool is_negated_;
};

class Not : public Predicate {
 public:
  Not() = delete;

  Not(std::shared_ptr<Expression> expr)
      : Predicate(PredicateType::kNot), expr_(expr) {}

  Not(const Not& rhs) = default;

  Not(Not&& rhs) = default;

  Not& operator=(const Not& rhs) = default;

  Not& operator=(Not&& rhs) = default;

  virtual ~Not() = default;

  arrow::compute::Expression BuildArrowExpression() const override;

  std::string ToString() const override;

 protected:
  std::shared_ptr<Expression> expr_;
};

class ExpressionBuilder {
 public:
  ExpressionBuilder() = delete;

  ExpressionBuilder(std::shared_ptr<arrow::Schema> schema) : schema_(schema) {}

  ExpressionBuilder(const ExpressionBuilder& rhs) = default;

  ExpressionBuilder(ExpressionBuilder&& rhs) = default;

  ExpressionBuilder& operator=(const ExpressionBuilder& rhs) = default;

  ExpressionBuilder& operator=(ExpressionBuilder&& rhs) = default;

  virtual ~ExpressionBuilder() = default;

  arrow::Result<std::vector<std::shared_ptr<Expression>>> ParseProjectionList(
      std::string_view proj_list_string);

  std::vector<std::string> TokenizeFilter(std::string_view filter_string);

  arrow::Result<std::shared_ptr<Expression>> ParseSimpleFilter(
      std::vector<std::string>& tokens);

  arrow::Result<std::shared_ptr<Expression>> ParseExpression(
      std::vector<std::string>& tokens);

  arrow::Result<std::shared_ptr<Expression>> ParseAndExpression(
      std::vector<std::string>& tokens);

  arrow::Result<std::shared_ptr<Expression>> ParsePrimaryExpression(
      std::vector<std::string>& tokens);

  arrow::Result<std::shared_ptr<Expression>> ParseFilter(
      std::string_view filter_string);

 protected:
  std::shared_ptr<arrow::Schema> schema_;
};
}  // namespace vdb::expression
