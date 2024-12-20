#pragma once

#include <string>
#include <arrow/status.h>

namespace vdb {

struct Status {
  enum Code {
    kOk,
    kInvalidArgument,
    kAlreadyExists,
    kOutOfMemory,
    kUnknown,
  };

  Code status_;
  std::string string_;

  Status(Code status, const std::string &error_string)
      : status_{status}, string_{error_string} {}

  Status(Code status) : status_{status} {}

  Status(arrow::Status status)
      : status_{GetCodeFromArrowStatus(status)}, string_{status.ToString()} {}

  Status() : status_{Code::kUnknown} {}

  static Status Ok() { return Status(Code::kOk); }
  static Status InvalidArgument(const std::string &msg) {
    return Status(Code::kInvalidArgument, msg);
  }
  static Status AlreadyExists(const std::string &msg) {
    return Status(Code::kAlreadyExists, msg);
  }
  static Status OutOfMemory(const std::string &msg) {
    return Status(Code::kOutOfMemory, msg);
  }
  static Status Unknown(const std::string &msg) {
    return Status(Code::kUnknown, msg);
  }

  bool ok() const { return status_ == Code::kOk; }
  std::string &ToString() { return string_; }
  Code GetCodeFromArrowStatus(arrow::Status status);
};

}  // namespace vdb
