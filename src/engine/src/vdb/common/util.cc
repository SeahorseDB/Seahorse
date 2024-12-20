#include <cstdlib>
#include <filesystem>
#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <arpa/inet.h>

#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

#include "vdb/common/defs.hh"
#include "vdb/common/status.hh"
#include "vdb/common/system_log.hh"
#include "vdb/common/util.hh"

#ifdef __cplusplus
extern "C" {
#include "endianconv.h"
#include "server.h"
}
#endif

namespace vdb {

std::string Trim(std::string_view sv) {
  auto start = sv.find_first_not_of(" \t\n\r");
  if (start == std::string_view::npos) return {};
  auto end = sv.find_last_not_of(" \t\n\r");
  return std::string(sv.substr(start, end - start + 1));
}

std::vector<std::string_view> Tokenize(const std::string_view& target_string,
                                       const char delimiter) {
  std::vector<std::string_view> tokens;
  auto prev = 0;
  auto pos = target_string.find(delimiter, prev);

  while (pos != std::string::npos) {
    auto token = target_string.substr(prev, pos - prev);
    if (token != "") {  // Ignore empty string
      tokens.push_back(token);
    }
    prev = pos + 1;
    pos = target_string.find(delimiter, prev);
  }

  auto last_token = target_string.substr(prev, std::string::npos);
  if (last_token != "") {
    tokens.push_back(last_token);
  }
  return tokens;
}

std::vector<std::string_view> Tokenize(const std::string& target_string,
                                       const char delimiter) {
  return Tokenize(std::string_view{target_string}, delimiter);
}

std::vector<std::string_view> Tokenize(const std::string_view& target_string) {
  return Tokenize(target_string, ' ');
}

std::vector<std::string_view> Tokenize(const std::string& target_string) {
  return Tokenize(std::string_view{target_string});
}

std::string_view GetTokenFrom(const std::string_view& target_string,
                              const char delimiter, const int id) {
  auto count = 0;
  auto prev = 0;
  auto pos = target_string.find(delimiter, prev);

  while (pos != std::string::npos) {
    if (count == id) return target_string.substr(prev, pos - prev);
    prev = pos + 1;
    pos = target_string.find(delimiter, prev);
    count++;
  }
  if (count == id) {
    return target_string.substr(prev, pos - prev);
  }

  return target_string.substr(0, 0);
}

std::string Join(const std::vector<std::string>& strings, const char delimter) {
  size_t size = 0;
  for (auto& str : strings) {
    size += str.size() + 1;
  }
  std::string joined_string;
  joined_string.reserve(size);

  for (auto iter = strings.begin(); iter != strings.end(); iter++) {
    joined_string += *iter;
    if (iter != strings.end() - 1) joined_string += delimter;
  }

  return joined_string;
}

std::string Join(const std::vector<std::string_view>& strings,
                 const char delimter) {
  size_t size = 0;
  for (auto& str : strings) {
    size += str.size() + 1;
  }
  std::string joined_string;
  joined_string.reserve(size);

  for (auto iter = strings.begin(); iter != strings.end(); iter++) {
    joined_string += *iter;
    if (iter != strings.end() - 1) joined_string += delimter;
  }

  return joined_string;
}

std::string Join(const std::vector<std::string>& strings,
                 std::string_view delimiter) {
  size_t size = 0;
  for (const auto& str : strings) {
    size += str.size() + delimiter.size();
  }
  std::string joined_string;
  joined_string.reserve(size);

  for (auto iter = strings.begin(); iter != strings.end(); ++iter) {
    joined_string += *iter;
    if (std::next(iter) != strings.end()) joined_string += delimiter;
  }

  return joined_string;
}

std::string Join(const std::vector<std::string_view>& strings,
                 std::string_view delimiter) {
  size_t size = 0;
  for (const auto& str : strings) {
    size += str.size() + delimiter.size();
  }
  std::string joined_string;
  joined_string.reserve(size);

  for (auto iter = strings.begin(); iter != strings.end(); ++iter) {
    joined_string += *iter;
    if (std::next(iter) != strings.end()) joined_string += delimiter;
  }

  return joined_string;
}

Status AppendBitsTo(vdb::vector<uint8_t>& sink, const size_t& sink_bit_count,
                    const uint8_t* source, const int64_t& source_bit_count) {
  /* is there are more efficient way to bitwise shift and copy? */
  int8_t source_bit_count_in_last_byte = ((source_bit_count - 1) % 8) + 1;
  int64_t source_byte_count =
      (source_bit_count - source_bit_count_in_last_byte) / 8 +
      /* last byte */ 1;
  auto bit_pos = sink_bit_count % 8;
  if (bit_pos == 0) {
    return AppendValuesTo(sink, source, source_byte_count);
  } else {
    /* calculate next array length and reserve array size */
    size_t new_sink_bit_count = sink_bit_count + source_bit_count;
    size_t new_sink_byte_count =
        new_sink_bit_count / 8 + ((new_sink_bit_count % 8) > 0 ? 1 : 0);

    /* reserve 50% more spaces than needed */
    if (new_sink_byte_count > sink.capacity())
      sink.reserve(new_sink_byte_count * 3 / 2);

    /* determine how input byte is divided */
    uint8_t lower_part_size = 8 - bit_pos;
    uint8_t upper_part_size = bit_pos;

    /* if ALL BITS in LAST BYTES are included in LOWER part,
     * there is NO BITS in UPPER part to append. */
    bool append_last_upper_part =
        (source_bit_count_in_last_byte > lower_part_size);

    for (int64_t i = 0; i < source_byte_count; i++) {
      auto input_byte = source[i];
      /* append input_byte's LOWER part into array byte's UPPER position */
      uint8_t lower_part = (input_byte << (8 - lower_part_size));
      sink.back() |= lower_part;

      if ((i < source_byte_count - 1) || append_last_upper_part) {
        /* append input_byte's UPPER part into array byte's LOWER position */
        uint8_t upper_part = (input_byte >> (8 - upper_part_size));
        sink.emplace_back(upper_part);
      }
    }
  }
  return Status::Ok();
}

Status AppendBitsTo(vdb::vector<uint8_t>& sink, const size_t& sink_bit_count,
                    const uint8_t* source, const int64_t source_bit_pos,
                    const int64_t& source_bit_count) {
  int64_t source_byte_pos = source_bit_pos / 8;
  int64_t source_bit_count_in_first_byte = 8 - (source_bit_pos % 8);
  if (source_bit_count_in_first_byte < 8) {
    uint8_t first_byte =
        source[source_byte_pos] >> (8 - source_bit_count_in_first_byte);
    auto status = AppendBitsTo(sink, sink_bit_count, &first_byte,
                               source_bit_count_in_first_byte);
    if (!status.ok()) return status;

    int64_t remain_source_bit_count =
        source_bit_count - source_bit_count_in_first_byte;
    if (remain_source_bit_count > 0) {
      int64_t new_sink_bit_count =
          sink_bit_count + source_bit_count_in_first_byte;
      return AppendBitsTo(sink, new_sink_bit_count,
                          source + (source_byte_pos) + 1,
                          remain_source_bit_count);
    } else {
      return Status::Ok();
    }
  } else {
    return AppendBitsTo(sink, sink_bit_count, source + (source_byte_pos),
                        source_bit_count);
  }
}

Status AppendUniformBitsTo(vdb::vector<uint8_t>& sink,
                           const int64_t& sink_bit_count, const bool bit,
                           int64_t bit_count) {
  auto bit_pos = sink_bit_count % 8;
  if (bit_pos != 0) {
    if (bit) {
      sink.back() |= BitMask[bit_pos];
    }
    bit_count -= (8 - bit_pos);
    if (bit_count <= 0) {
      return Status::Ok();
    }
  }
  size_t remain_bit_count = bit_count % 8;
  size_t added_byte_count = bit_count / 8 + (remain_bit_count > 0 ? 1 : 0);
  uint8_t mask = (bit) ? UINT8_MAX : 0;
  sink.resize(sink.size() + added_byte_count, mask);
  if (bit && remain_bit_count > 0) {
    sink.back() &= ~BitMask[remain_bit_count];
  }
  return Status::Ok();
}

// Type string parser. Supported types should be listed here.
// Returns the shared pointer of arrow::DataType with the appropriate
// `type_string`. If not, returns nullptr.
std::shared_ptr<arrow::DataType> GetDataTypeFrom(
    const std::string_view& type_string_token) {
  auto type_string = Trim(type_string_token);

  if (type_string == "bool") return arrow::boolean();

  if (type_string == "int8") return arrow::int8();
  if (type_string == "int16") return arrow::int16();
  if (type_string == "int32") return arrow::int32();
  if (type_string == "int64") return arrow::int64();

  if (type_string == "uint8") return arrow::uint8();
  if (type_string == "uint16") return arrow::uint16();
  if (type_string == "uint32") return arrow::uint32();
  if (type_string == "uint64") return arrow::uint64();

  if (type_string == "float32") return arrow::float32();
  if (type_string == "float64") return arrow::float64();

  if (type_string == "string") return arrow::utf8();
  if (type_string == "large_string") return arrow::large_utf8();
  if (type_string == "char") return arrow::utf8();
  if (type_string == "utf8") return arrow::utf8();

  // "list[int8]" or "list[     int8  ]"
  if (type_string.size() > 5 && type_string.substr(0, 5) == "list[" &&
      type_string.substr(type_string.size() - 1, 1) == "]") {
    auto subtype_pos = 5;
    while (std::isspace(type_string[subtype_pos])) subtype_pos++;
    auto subtype_end_pos = subtype_pos + 1;
    while (std::isalnum(type_string[subtype_end_pos])) subtype_end_pos++;
    auto subtype = GetDataTypeFrom(
        type_string.substr(subtype_pos, subtype_end_pos - subtype_pos));
    if (subtype != nullptr) return arrow::list(subtype);
  }

  // "fixed_size_list[1024,float32]" or "fixed_size_list[  1024,    float32   ]"
  if (type_string.size() > 16 &&
      type_string.substr(0, 16) == "fixed_size_list[" &&
      type_string.substr(type_string.size() - 1, 1) == "]") {
    auto list_type_pos = type_string.find(',') + 1;
    if (list_type_pos == std::string::npos &&
        list_type_pos >= type_string.size())
      return nullptr;
    auto list_size =
        std::stoull(std::string(type_string.substr(16, list_type_pos - 16)));
    while (std::isspace(type_string[list_type_pos])) list_type_pos++;
    auto list_type_end_pos = list_type_pos + 1;
    while (std::isalnum(type_string[list_type_end_pos])) list_type_end_pos++;
    auto subtype = GetDataTypeFrom(
        type_string.substr(list_type_pos, list_type_end_pos - list_type_pos));
    if (subtype != nullptr) return arrow::fixed_size_list(subtype, list_size);
  }

  return nullptr;
}

// Returns the shared pointer of arrow::Schema with appropriate `schema_string`.
// e.g.) "ID Int32, Name String, Height Float32"
// e.g.) "ID String, Vector Fixed_Size_List[ 1024,  Float32  ], Attribute List[
// String ]" If not, returns nullptr.
std::shared_ptr<arrow::Schema> ParseSchemaFrom(std::string& schema_string) {
  std::transform(schema_string.begin(), schema_string.end(),
                 schema_string.begin(), tolower);
  std::vector<std::string_view> tokens;
  char delimiter = ',';
  size_t i = 0;
  while (i < schema_string.size()) {
    size_t j = i;
    bool subtype_flag = false;
    for (; j < schema_string.size(); j++) {
      if (schema_string[j] == '[') subtype_flag = true;
      if (schema_string[j] == ']') subtype_flag = false;
      if (!subtype_flag && schema_string[j] == delimiter) {
        break;
      }
    }
    tokens.emplace_back(&schema_string[i], j - i);
    i = j + 1;
  }

  arrow::FieldVector fields;
  for (auto& token : tokens) {
    size_t begin_pos = 0;
    while (begin_pos < token.size()) {
      if (isspace(token[begin_pos]))
        begin_pos++;
      else
        break;
    }
    auto delim_pos = token.find(' ', begin_pos);
    if (delim_pos == std::string::npos) return nullptr;
    auto name = token.substr(begin_pos, delim_pos - begin_pos);

    // Find the position of the "not null" specifier, if present
    auto not_null_pos = token.find("not null", delim_pos + 1);
    auto type_end_pos = (not_null_pos != std::string::npos) ? not_null_pos - 1
                                                            : std::string::npos;

    auto type =
        GetDataTypeFrom(token.substr(delim_pos + 1, type_end_pos - delim_pos));
    if (type == nullptr) return nullptr;

    // Create the field with nullable set to false if "not null" is specified
    bool nullable = (not_null_pos == std::string::npos);
    fields.push_back(
        std::make_shared<arrow::Field>(std::string{name}, type, nullable));
  }

  auto schema = std::make_shared<arrow::Schema>(fields);

  return schema;
}

std::string TransformToLower(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return str;
}

std::string TransformToUpper(std::string str) {
  std::transform(str.begin(), str.end(), str.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return str;
}

int32_t ComputeBytesFor(uint64_t length) {
  size_t length_bytes;
  if (length < (1 << 6)) {
    length_bytes = 1;
  } else if (length < (1 << 14)) {
    length_bytes = 2;
  } else if (length <= UINT32_MAX) {
    length_bytes = 1 + 4;
  } else {
    length_bytes = 1 + 8;
  }
  return length_bytes;
}

int32_t PutLengthTo(uint8_t* buffer, uint64_t length) {
  size_t length_bytes;

  if (length < (1 << 6)) {
    /* Save a 6 bit len */
    buffer[0] = (length & 0xFF) | (k6BitLength << 6);
    length_bytes = 1;
  } else if (length < (1 << 14)) {
    /* Save a 14 bit len */
    buffer[0] = ((length >> 8) & 0xFF) | (k14BitLength << 6);
    buffer[1] = length & 0xFF;
    length_bytes = 2;
  } else if (length <= UINT32_MAX) {
    /* Save a 32 bit len */
    buffer[0] = k32BitLength;
    uint32_t len32 = htonl(length);
    memcpy(&buffer[1], &len32, 4);
    length_bytes = 1 + 4;
  } else {
    /* Save a 64 bit len */
    buffer[0] = k64BitLength;
    length = htonu64(length);
    memcpy(&buffer[1], &length, 8);
    length_bytes = 1 + 8;
  }

  return length_bytes;
}

int32_t GetLengthFrom(uint8_t* buffer, uint64_t& length) {
  int type;
  size_t length_bytes;

  type = (buffer[0] & 0xC0) >> 6;
  if (type == k6BitLength) {
    /* Read a 6 bit len. */
    length = buffer[0] & 0x3F;
    length_bytes = 1;
  } else if (type == k14BitLength) {
    /* Read a 14 bit len. */
    length = ((buffer[0] & 0x3F) << 8) | buffer[1];
    length_bytes = 2;
  } else if (buffer[0] == k32BitLength) {
    /* Read a 32 bit len. */
    uint32_t raw_length;
    memcpy(&raw_length, &buffer[1], 4);
    length = ntohl(raw_length);
    length_bytes = 1 + 4;
  } else if (buffer[0] == k64BitLength) {
    /* Read a 64 bit len. */
    uint64_t raw_length;
    memcpy(&raw_length, &buffer[1], 8);
    length = ntohu64(raw_length);
    length_bytes = 1 + 8;
  } else {
    length_bytes = -1;
    SYSTEM_LOG(
        vdb::LogLevel::kLogAlways,
        "Wrong Length Type is Found! %d "
        "(Correct Cases: (1 bytes)%d (2 bytes)%d (1+4 bytes)%d (1+9 bytes)%d)",
        (int)buffer[0], k6BitLength, k14BitLength, k32BitLength, k64BitLength);
  }

  return length_bytes;
}

Status ChunkedWriteTo(std::ofstream& output_file, std::string& file_path,
                      const uint8_t* buffer, size_t bytes) {
  for (size_t buffer_offset = 0; buffer_offset < bytes;) {
    size_t write_size = bytes - buffer_offset;
    if (write_size > kGb) write_size = kGb;
    output_file.write(reinterpret_cast<const char*>(buffer) + buffer_offset,
                      write_size);
    if (output_file.fail()) {
      return Status::InvalidArgument(file_path +
                                     " Write Error - Writing to file failed.");
    }
    buffer_offset += write_size;
  }

  return Status::Ok();
}

Status WriteTo(std::string& file_path, const uint8_t* buffer, size_t bytes,
               size_t file_offset) {
  std::ofstream output_file;
  try {
    output_file.open(file_path, std::ios::binary | std::ios::out);
    if (!output_file.is_open()) {
      return Status::InvalidArgument(
          file_path + " Write Error - Could not open file for writing.");
    }

    if (file_offset > 0) {
      output_file.seekp(file_offset);
      if (output_file.fail()) {
        return Status::InvalidArgument(
            file_path + " Write Error - Seeking to position failed.");
      }
    }

    if (bytes > kGb) {
      auto status = ChunkedWriteTo(output_file, file_path, buffer, bytes);
      if (!status.ok()) {
        return status;
      }
    } else {
      output_file.write(reinterpret_cast<const char*>(buffer), bytes);
      if (output_file.fail()) {
        return Status::InvalidArgument(
            file_path + " Write Error - Writing to file failed.");
      }
    }

    output_file.close();
    if (output_file.fail()) {
      return Status::InvalidArgument(file_path +
                                     " Write Error - Closing the file failed.");
    }
  } catch (const std::exception& e) {
    if (output_file.is_open()) {
      output_file.close();
    }
    return Status::InvalidArgument(file_path + " Write Error - " + e.what());
  }

  return Status::Ok();
}

Status ChunkedReadFrom(std::ifstream& input_file, std::string& file_path,
                       uint8_t* buffer, size_t bytes) {
  for (size_t buffer_offset = 0; buffer_offset < bytes;) {
    size_t read_size = bytes - buffer_offset;
    if (read_size > kGb) read_size = kGb;
    input_file.read(reinterpret_cast<char*>(buffer) + buffer_offset, read_size);
    if (input_file.fail() && !input_file.eof()) {
      return Status::InvalidArgument(file_path +
                                     " Read Error - Reading from file failed.");
    }
    buffer_offset += read_size;
  }

  return Status::Ok();
}

Status ReadFrom(std::string& file_path, uint8_t* buffer, size_t bytes,
                size_t file_offset) {
  std::error_code ec;
  if (!std::filesystem::exists(file_path, ec)) {
    return Status::InvalidArgument(file_path + " Read Error - " + ec.message());
  }

  std::ifstream input_file;
  try {
    input_file.open(file_path, std::ios::binary | std::ios::in);
    if (!input_file.is_open()) {
      return Status::InvalidArgument(
          file_path + " Read Error - Could not open file for reading.");
    }

    if (file_offset > 0) {
      input_file.seekg(file_offset);
      if (input_file.fail()) {
        return Status::InvalidArgument(
            file_path + " Read Error - Seeking to position failed.");
      }
    }

    if (bytes > kGb) {
      auto status = ChunkedReadFrom(input_file, file_path, buffer, bytes);
      if (!status.ok()) {
        return status;
      }
    } else {
      input_file.read(reinterpret_cast<char*>(buffer), bytes);
      if (input_file.fail() && !input_file.eof()) {
        return Status::InvalidArgument(
            file_path + " Read Error - Reading from file failed.");
      }
    }

    size_t read_size = static_cast<std::size_t>(input_file.gcount());
    if (read_size != bytes) {
      return Status::InvalidArgument(
          file_path +
          " Read Error - Read bytes is different with requested bytes. ");
    }

    input_file.close();
    if (input_file.fail()) {
      return Status::InvalidArgument(file_path +
                                     " Read Error - Closing the file failed.");
    }
  } catch (const std::exception& e) {
    if (input_file.is_open()) {
      input_file.close();
    }
    return Status::InvalidArgument(file_path + " Read Error - " + e.what());
  }

  return Status::Ok();
}

std::shared_ptr<arrow::Buffer> MakeBufferFromSds(const sds s) {
  return MakeBuffer(s, sdslen(s));
}

std::shared_ptr<arrow::Buffer> MakeBuffer(const char* data, size_t size) {
  auto buffer = std::make_shared<arrow::Buffer>(
      reinterpret_cast<const uint8_t*>(data), size * sizeof(uint32_t));
  return buffer;
}

arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
MakeRecordBatchesFromSds(const sds s) {
  ARROW_ASSIGN_OR_RAISE(
      auto reader,
      arrow::ipc::RecordBatchStreamReader::Open(
          std::make_shared<arrow::io::BufferReader>(MakeBufferFromSds(s))));
  ARROW_ASSIGN_OR_RAISE(auto rbs, reader->ToRecordBatches());
  return rbs;
}

}  // namespace vdb
