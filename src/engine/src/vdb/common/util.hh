#pragma once

#include <string_view>
#include <vector>

#include <arrow/api.h>

#include "vdb/common/status.hh"
#include "vdb/common/memory_allocator.hh"

typedef char *sds;

namespace vdb {

std::vector<std::string_view> Tokenize(const std::string &target_string,
                                       const char delimiter);
std::vector<std::string_view> Tokenize(const std::string_view &target_string,
                                       const char delimiter);
std::vector<std::string_view> Tokenize(const std::string &target_string);
std::vector<std::string_view> Tokenize(const std::string_view &target_string);

std::string_view GetTokenFrom(const std::string_view &target_string,
                              const char delimiter, const int id);

std::string Join(const std::vector<std::string> &strings, const char delimiter);

std::string Join(const std::vector<std::string_view> &strings,
                 const char delimiter);

std::string Join(const std::vector<std::string> &strings,
                 std::string_view delimiter);

std::string Join(const std::vector<std::string_view> &strings,
                 std::string_view delimiter);

Status AppendBitsTo(vdb::vector<uint8_t> &sink, const size_t &sink_bit_count,
                    const uint8_t *source, const int64_t &source_bit_count);

Status AppendBitsTo(vdb::vector<uint8_t> &sink, const size_t &sink_bit_count,
                    const uint8_t *source, const int64_t source_bit_start,
                    const int64_t &source_bit_count);

template <typename CType>
Status AppendValuesTo(vdb::vector<CType> &sink, const CType *source,
                      const int64_t source_count) {
  sink.resize(sink.size() + source_count);
  std::copy(source, source + source_count, sink.end() - source_count);
  return Status::Ok();
}

Status AppendUniformBitsTo(vdb::vector<uint8_t> &sink,
                           const int64_t &sink_bit_count, const bool bit,
                           int64_t bit_count);
std::shared_ptr<arrow::Schema> ParseSchemaFrom(std::string &schema_str);

std::string TransformToLower(std::string str);
std::string TransformToUpper(std::string str);

constexpr uint32_t k6BitLength = 0;
constexpr uint32_t k14BitLength = 1;
constexpr uint32_t k32BitLength = 0x80;
constexpr uint32_t k64BitLength = 0x81;

/* return value is bytes of legnth */
int32_t ComputeBytesFor(uint64_t length);
/* return value is bytes of legnth */
int32_t PutLengthTo(uint8_t *buffer, uint64_t length);
/* return value is bytes of legnth */
int32_t GetLengthFrom(uint8_t *buffer, uint64_t &length);

Status WriteTo(std::string &file_path, const uint8_t *buffer, size_t bytes,
               size_t file_offset);
Status ReadFrom(std::string &file_path, uint8_t *buffer, size_t bytes,
                size_t file_offset);

std::shared_ptr<arrow::Buffer> MakeBufferFromSds(const sds s);
std::shared_ptr<arrow::Buffer> MakeBuffer(const char *data, size_t size);
arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
MakeRecordBatchesFromSds(const sds s);

class UInt32ArrayView {
 private:
  const uint32_t *data_;
  size_t size_;

  UInt32ArrayView(const uint32_t *data, size_t size)
      : data_(data), size_(size) {}

 public:
  static std::optional<UInt32ArrayView> create(const std::string &str) {
    if (str.size() % sizeof(uint32_t) != 0) {
      return std::nullopt;
    }
    return UInt32ArrayView(reinterpret_cast<const uint32_t *>(str.data()),
                           str.size() / sizeof(uint32_t));
  }

  const uint32_t *data() const { return data_; }
  size_t size() const { return size_; }

  const uint32_t *begin() const { return data_; }
  const uint32_t *end() const { return data_ + size_; }

  const uint32_t &operator[](size_t index) const {
    if (index >= size_) {
      throw std::out_of_range("Index out of range");
    }
    return data_[index];
  }
};

}  // namespace vdb
