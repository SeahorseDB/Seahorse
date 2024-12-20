#include <string>
#include <sstream>

#include "vdb/common/status.hh"

namespace vdb {
class LabelInfo {
 public:
  /* TODO
   * It will be deprecated.
   * future hnsw structure does not has label, offset of vector will replace it.
   * label format
   * total 64 bits
   *
   * 32 bits - chunk number     (MAXIMUM 2^32-1 = 4,294,967,295)
   * 32 bits - record number    (MAXIMUM 2^32-1 = 4,294,967,295) */
  static constexpr uint32_t kSetMax = UINT32_MAX;
  static constexpr size_t kRecordMax = UINT32_MAX;

  static size_t Build(const uint32_t &set_number, const size_t &record_number) {
    return (static_cast<size_t>(set_number) << 32) | record_number;
  }

  static size_t GetSetNumber(const size_t &label) {
    return ((label & (((1LLU << 32) - 1) << 32)) >> 32);
  }

  static size_t GetRecordNumber(const size_t &label) {
    return (label & ((1LLU << 32) - 1));
  }

  static Status CheckLabel(const size_t &label, const uint32_t &set_number,
                           const size_t &record_number) {
    if (GetSetNumber(label) != static_cast<size_t>(set_number))
      return Status(Status::kInvalidArgument, "Chunk is not Saved Correctly.");
    if (GetRecordNumber(label) != record_number)
      return Status(Status::kInvalidArgument,
                    "Record Number is not Saved Correctly.");
    return Status::Ok();
  }

  static Status CheckSetNumber(const uint32_t &set_number) {
    if (set_number > kSetMax)
      return Status(Status::kInvalidArgument,
                    "Chunk Number is over the Maximum.");
    return Status::Ok();
  }

  static Status CheckRecordNumber(const size_t &record_number) {
    if (record_number > kRecordMax)
      return Status(Status::kInvalidArgument,
                    "Record Number is over the Maximum.");
    return Status::Ok();
  }
  static std::string ToString(const size_t label) {
    std::stringstream ss;
    ss << "(set=" << GetSetNumber(label) << ", ";
    ss << "record=" << GetRecordNumber(label) << ")";
    return ss.str();
  }
};
}  // namespace vdb
