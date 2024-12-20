#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <sched.h>
#include <stddef.h>
#include <stdexcept>
#include <string>
#include <system_error>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "vdb/data/index_handler.hh"
#include "vdb/vdb.hh"
#include "vdb/vdb_api.hh"
#include "vdb/common/memory_allocator.hh"
#include "vdb/common/util.hh"
#include "vdb/common/spinlock.hh"
#include "vdb/common/status.hh"
#include "vdb/common/system_log.hh"
#include "vdb/data/table.hh"

#include "server.h"

void TestCommand(client *c) {
  vdb::_TestCommand();
  if (c->argc > 3 || c->argc < 2) {
    addReplyErrorArity(c);
    return;
  }

  if (c->flags & CLIENT_PUBSUB && c->resp == 2) {
    addReply(c, shared.mbulkhdr[2]);
    addReplyBulkCBuffer(c, "VXDB", 4);
    if (c->argc == 1)
      addReplyBulkCBuffer(c, "", 0);
    else
      addReplyBulk(c, c->argv[2]);
  } else {
    if (c->argc == 2)
      addReply(c, shared.ok);
    else
      addReplyBulk(c, c->argv[2]);
  }
}

void ListTableCommand(client *c) {
  if (c->argc > 2) {
    addReplyErrorArity(c);
    return;
  }

  auto table_list = vdb::_ListTableCommand();
  std::string reply_string;
  size_t reply_string_len = 10;  // reserve array length

  for (auto &table_name : table_list) {
    reply_string_len += table_name.size() + 3;  // 3 = +, kCRLF
  }
  reply_string.reserve(reply_string_len);
  reply_string += "*";  // Array
  if (!table_list.size())
    reply_string += "-1";
  else
    reply_string += std::to_string(table_list.size());
  reply_string += vdb::kCRLF;

  for (auto &table_name : table_list) {
    reply_string += '+';  // Simple string
    reply_string += table_name;
    reply_string += vdb::kCRLF;
  }

  addReplyProto(c, reply_string.c_str(), reply_string.size());
}

void CreateTableCommand(client *c) {
  if (c->argc < 3) {
    addReplyErrorArity(c);
    return;
  }

  sds serialized_schema = static_cast<sds>(c->argv[2]->ptr);

  auto status = vdb::_CreateTableCommand(serialized_schema);

  if (status.ok()) {
    server.dirty++;
    addReply(c, shared.ok);
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s", status.ToString().data());
    addReplyError(c, status.ToString().c_str());
  }
}

void InsertCommand(client *c) {
  if (c->argc < 4) {
    addReplyErrorArity(c);
    return;
  }

  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  sds record_sds = static_cast<sds>(c->argv[3]->ptr);

  std::string table_name{table_sds, sdslen(table_sds)};
  std::string_view record_view{record_sds, sdslen(record_sds)};

  auto status = vdb::_InsertCommand(table_name, record_view);

  if (status.ok()) {
    server.dirty++;
    addReply(c, shared.ok);
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s", status.ToString().data());
    addReplyError(c, status.ToString().c_str());
  }
}

void BatchInsertCommand(client *c) {
  if (c->argc < 4) {
    addReplyErrorArity(c);
    return;
  }

  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  sds serialized_rb = static_cast<sds>(c->argv[3]->ptr);

  std::string table_name{table_sds, sdslen(table_sds)};

  auto status = vdb::_BatchInsertCommand(table_name, serialized_rb);

  if (status.ok()) {
    server.dirty++;
    addReply(c, shared.ok);
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s", status.ToString().data());
    addReplyError(c, status.ToString().c_str());
  }
}

void DescribeTableCommand(client *c) {
  if (c->argc < 3) {
    addReplyErrorArity(c);
    return;
  }

  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  std::string table_name{table_sds, sdslen(table_sds)};

  auto maybe_serialized_schema = vdb::_DescribeTableCommand(table_name);
  if (maybe_serialized_schema.ok()) {
    auto serialized_schema = maybe_serialized_schema.ValueUnsafe();
    std::string reply_string = "";
    reply_string.reserve(20 + serialized_schema->size());
    reply_string +=
        "$" + std::to_string(serialized_schema->size()) + vdb::kCRLF;
    reply_string += std::string_view((char *)serialized_schema->data(),
                                     serialized_schema->size());
    reply_string += vdb::kCRLF;
    addReplyProto(c, reply_string.data(), reply_string.size());
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s",
               maybe_serialized_schema.status().ToString().data());
    addReplyError(c, maybe_serialized_schema.status().ToString().c_str());
  }
}

void DebugScanCommand(client *c) {
  if (c->argc < 5) {
    addReplyErrorArity(c);
    return;
  }
  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  sds projection_sds = static_cast<sds>(c->argv[3]->ptr);
  sds filter_sds = static_cast<sds>(c->argv[4]->ptr);

  std::string table_name{table_sds, sdslen(table_sds)};
  std::string_view projection_view{projection_sds, sdslen(projection_sds)};
  std::string_view filter_view{filter_sds, sdslen(filter_sds)};
  auto maybe_rb_string =
      vdb::_DebugScanCommand(table_name, projection_view, filter_view);
  if (maybe_rb_string.ok()) {
    auto reply_string = maybe_rb_string.ValueUnsafe();
    reply_string = "$" + std::to_string(reply_string.size()) + vdb::kCRLF +
                   reply_string + vdb::kCRLF;
    addReplyProto(c, reply_string.data(), reply_string.size());
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s",
               maybe_rb_string.status().ToString().data());
    addReplyError(c, maybe_rb_string.status().ToString().c_str());
  }
}

void ScanCommand(client *c) {
  if (c->argc < 5) {
    addReplyErrorArity(c);
    return;
  }
  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  sds projection_sds = static_cast<sds>(c->argv[3]->ptr);
  sds filter_sds = static_cast<sds>(c->argv[4]->ptr);

  std::string table_name{table_sds, sdslen(table_sds)};
  std::string_view projection_view{projection_sds, sdslen(projection_sds)};
  std::string_view filter_view{filter_sds, sdslen(filter_sds)};
  auto maybe_serialized_rb =
      vdb::_ScanCommand(table_name, projection_view, filter_view);
  if (maybe_serialized_rb.ok()) {
    auto serialized_rb = maybe_serialized_rb.ValueUnsafe();
    std::string reply_string;
    if (serialized_rb->size() != 0) {
      /* bulk string */
      reply_string += '$' + std::to_string(serialized_rb->size()) + vdb::kCRLF;
      reply_string += std::string_view((char *)serialized_rb->data(),
                                       serialized_rb->size());
      reply_string += vdb::kCRLF;
    } else {
      reply_string = "*0";
      reply_string += vdb::kCRLF;
    }
    addReplyProto(c, reply_string.data(), reply_string.size());
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s",
               maybe_serialized_rb.status().ToString().data());
    addReplyError(c, maybe_serialized_rb.status().ToString().c_str());
  }
}

void AnnCommand(client *c) {
  if (c->argc < 5) {
    addReplyErrorArity(c);
    return;
  }
  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  sds query_sds = static_cast<sds>(c->argv[4]->ptr);

  std::string table_name{table_sds, sdslen(table_sds)};
  size_t k = std::stoull(static_cast<sds>(c->argv[3]->ptr));

  size_t ef_search = k;
  if (c->argc > 5) {
    sds ef_search_sds = static_cast<sds>(c->argv[5]->ptr);
    ef_search = std::stoull(ef_search_sds);
  }
  std::string_view projection_view;
  if (c->argc > 6) {
    sds projection_sds = static_cast<sds>(c->argv[6]->ptr);
    projection_view = {projection_sds, sdslen(projection_sds)};
  } else {
    projection_view = "*";
  }

  std::string_view filter_view;
  if (c->argc > 7) {
    sds filter_sds = static_cast<sds>(c->argv[7]->ptr);
    filter_view = {filter_sds, sdslen(filter_sds)};
  } else {
    filter_view = "";
  }

  auto maybe_serialized_rb = vdb::_AnnCommand(
      table_name, k, query_sds, ef_search, projection_view, filter_view);
  if (maybe_serialized_rb.ok()) {
    auto serialized_rb = maybe_serialized_rb.ValueUnsafe();
    std::string reply_string;
    if (serialized_rb != nullptr) {
      reply_string = '$' + std::to_string(serialized_rb->size()) + vdb::kCRLF;
      reply_string += std::string_view((char *)serialized_rb->data(),
                                       serialized_rb->size());
      reply_string += vdb::kCRLF;
    } else {
      reply_string = "$0";
      reply_string += vdb::kCRLF;
      reply_string += vdb::kCRLF;
    }
    addReplyProto(c, reply_string.data(), reply_string.size());
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s",
               maybe_serialized_rb.status().ToString().data());
    addReplyError(c, maybe_serialized_rb.status().ToString().c_str());
  }
}

void BatchAnnCommand(client *c) {
  if (c->argc < 5) {
    addReplyErrorArity(c);
    return;
  }

  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  sds query_sds = static_cast<sds>(c->argv[4]->ptr);

  std::string table_name{table_sds, sdslen(table_sds)};
  size_t k = std::stoull(static_cast<sds>(c->argv[3]->ptr));

  size_t ef_search = k;
  if (c->argc > 5) {
    sds ef_search_sds = static_cast<sds>(c->argv[5]->ptr);
    ef_search = std::stoull(ef_search_sds);
  }
  std::string_view projection_view;
  if (c->argc > 6) {
    sds projection_sds = static_cast<sds>(c->argv[6]->ptr);
    projection_view = {projection_sds, sdslen(projection_sds)};
  } else {
    projection_view = "*";
  }

  std::string_view filter_view;
  if (c->argc > 7) {
    sds filter_sds = static_cast<sds>(c->argv[7]->ptr);
    filter_view = {filter_sds, sdslen(filter_sds)};
  } else {
    filter_view = "";
  }

  auto maybe_serialized_rbs = vdb::_BatchAnnCommand(
      table_name, k, query_sds, ef_search, projection_view, filter_view);
  if (maybe_serialized_rbs.ok()) {
    auto serialized_rbs = maybe_serialized_rbs.ValueUnsafe();
    std::string reply_string;
    if (serialized_rbs.size() != 0) {
      SYSTEM_LOG(vdb::LogLevel::kLogDebug, "Returned %lu results",
                 serialized_rbs.size());
      reply_string = "*";
      reply_string += std::to_string(serialized_rbs.size());
      reply_string += vdb::kCRLF;
      for (auto &serialized_rb : serialized_rbs) {
        reply_string +=
            '$' + std::to_string(serialized_rb->size()) + vdb::kCRLF;
        reply_string += std::string_view((char *)serialized_rb->data(),
                                         serialized_rb->size());
        reply_string += vdb::kCRLF;
      }
    } else {
      SYSTEM_LOG(vdb::LogLevel::kLogDebug, "No results found");
      reply_string = "*0";
      reply_string += vdb::kCRLF;
    }
    addReplyProto(c, reply_string.data(), reply_string.size());
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s",
               maybe_serialized_rbs.status().ToString().data());
    addReplyError(c, maybe_serialized_rbs.status().ToString().c_str());
  }
}

void CheckIndexingCommand(client *c) {
  if (c->argc < 3) {
    addReplyErrorArity(c);
    return;
  }

  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  std::string table_name{table_sds, sdslen(table_sds)};
  auto maybe_result = vdb::_CheckIndexingCommand(table_name);
  if (maybe_result.ok()) {
    addReplyBool(c, maybe_result.ValueUnsafe());
  } else {
    addReplyError(c, maybe_result.status().ToString().c_str());
  }
}

void CountIndexedElementsCommand(client *c) {
  if (c->argc < 3) {
    addReplyErrorArity(c);
    return;
  }

  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  std::string table_name{table_sds, sdslen(table_sds)};
  auto maybe_result = vdb::_CountIndexedElementsCommand(table_name);
  if (maybe_result.ok()) {
    std::string reply_string = "*2\r\n";
    auto [total_count, indexed_count] = maybe_result.ValueUnsafe();
    reply_string += ":" + std::to_string(total_count) + vdb::kCRLF;
    reply_string += ":" + std::to_string(indexed_count) + vdb::kCRLF;
    addReplyProto(c, reply_string.c_str(), reply_string.size());
  } else {
    addReplyError(c, maybe_result.status().ToString().c_str());
  }
}

void DropTableCommand(client *c) {
  if (c->argc < 3) {
    addReplyErrorArity(c);
    return;
  }
  sds table_sds = static_cast<sds>(c->argv[2]->ptr);
  std::string table_name{table_sds, sdslen(table_sds)};
  auto status = vdb::_DropTableCommand(table_name);

  if (status.ok()) {
    server.dirty++;
    addReply(c, shared.ok);
  } else {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "%s", status.ToString().data());
    addReplyError(c, status.ToString().c_str());
  }
}

void AllocateTableDictionary() {
  server.table_dictionary =
      (void *)new vdb::map<std::string, std::shared_ptr<vdb::Table>>();
}
void DeallocateTableDictionary() {
  auto table_dictionary =
      static_cast<vdb::map<std::string, std::shared_ptr<vdb::Table>> *>(
          server.table_dictionary);
  delete table_dictionary;
  server.table_dictionary = nullptr;
}

/* snapshot api */
vdb::Spinlock snapshot_lock;

void CheckVdbSnapshot() {
  bool entered = false;
  while (snapshot_lock.IsLocked()) {
    if (!entered) {
      SYSTEM_LOG(vdb::LogLevel::kLogNotice,
                 "Building Index is blocked by Snapshot. "
                 "Waits until Creating Snapshot is Done.");
      entered = true;
    }
    sched_yield();
  }
  if (entered) {
    SYSTEM_LOG(vdb::LogLevel::kLogNotice, "Building Index is Resumed.");
  }
}

void PrepareVdbSnapshot() {
  /* Check all sets of the tables whether they are building indexes now. */
  auto table_dictionary = vdb::GetTableDictionary();

  snapshot_lock.Lock();
  SYSTEM_LOG(vdb::LogLevel::kLogNotice,
             "Creating Snapshot is Requested. Snapshot Lock is held.");

  vdb::vector<std::shared_ptr<vdb::VectorIndex>> uncompleted_indexes;

  for (auto &[table_name, table] : *table_dictionary) {
    auto segments = table->GetSegments();
    if (table->HasIndex()) {
      for (auto &[segment_id, segment] : segments) {
        std::shared_ptr<vdb::IndexHandler> index_handler =
            segment->IndexHandler();
        size_t num_index = index_handler->Size();
        for (size_t i = 0; i < num_index; i++) {
          auto index = index_handler->Index(i);
          // MAYBE just wait at this line instead of using vector
          if (index->Size() != index->CompleteSize()) {
            uncompleted_indexes.push_back(index);
          }
        }
      }

      while (!uncompleted_indexes.empty()) {
        auto index = uncompleted_indexes.back();
        if (index->Size() == index->CompleteSize()) {
          uncompleted_indexes.pop_back();
        } else {
          sched_yield();
        }
      }
    }
  }
  SYSTEM_LOG(
      vdb::LogLevel::kLogNotice,
      "Creating Snapshot is Started. There are no building indexes now.");
}

void PostVdbSnapshot() {
  SYSTEM_LOG(vdb::LogLevel::kLogNotice,
             "Creating Snapshot is Done. Snapshot Lock is released.");
  snapshot_lock.Unlock();
}

bool SaveVdbSnapshot(char *directory_path_) {
  std::string_view directory_path(directory_path_);
  /* create directory */
  if (!std::filesystem::create_directory(directory_path)) {
    return false;
  }
  /* save snapshot */
  auto table_dictionary = vdb::GetTableDictionary();

  vdb::vector<uint8_t> buffer;
  int buffer_offset = 0;
  /* save table count */
  int table_count = table_dictionary->size();
  int32_t needed_bytes = vdb::ComputeBytesFor(table_count);
  buffer.resize(needed_bytes);
  buffer_offset += vdb::PutLengthTo(buffer.data() + buffer_offset, table_count);

  /* save tables */
  for (auto &[table_name, table] : *table_dictionary) {
    auto status = table->Save(directory_path);
    if (!status.ok()) {
      return false;
    }
    /* save table name */
    int32_t needed_bytes = vdb::ComputeBytesFor(table_name.length());
    buffer.resize(buffer.size() + needed_bytes + table_name.length());
    buffer_offset +=
        vdb::PutLengthTo(buffer.data() + buffer_offset, table_name.length());
    memcpy(buffer.data() + buffer_offset, table_name.data(),
           table_name.length());
    buffer_offset += table_name.length();
  }
  /* save manifest (# of tables, table names) */
  std::string manifest_file_path(directory_path);
  manifest_file_path.append("/manifest");
  auto status =
      vdb::WriteTo(manifest_file_path, buffer.data(), buffer_offset, 0);
  if (!status.ok()) {
    return false;
  }
  return true;
}

static bool LoadManifest(std::string &file_path, uint64_t &table_count,
                         std::vector<std::string> &table_names) {
  size_t manifest_size = std::filesystem::file_size(file_path);
  auto buffered_manifest =
      static_cast<uint8_t *>(AllocateAligned(64, manifest_size));
  auto status = vdb::ReadFrom(file_path, buffered_manifest, manifest_size, 0);
  if (!status.ok()) {
    DeallocateAligned(buffered_manifest, manifest_size);
    return false;
  }

  uint64_t offset = 0;
  /* get table count */
  offset += vdb::GetLengthFrom(buffered_manifest, table_count);

  for (uint64_t i = 0; i < table_count; i++) {
    /* get table names */
    uint64_t table_name_length;
    offset += vdb::GetLengthFrom(buffered_manifest + offset, table_name_length);
    table_names.emplace_back(
        reinterpret_cast<char *>(buffered_manifest + offset),
        table_name_length);
    offset += table_name_length;
  }
  DeallocateAligned(buffered_manifest, manifest_size);
  return true;
}

/* load snapshot */
bool LoadVdbSnapshot(char *directory_path_) {
  std::string_view directory_path(directory_path_);
  /* check existence of snapshot */
  if (!std::filesystem::exists(directory_path)) {
    return false;
  }
  /* check completeness of snapshot
   * if snapshot.manifest file exists, snapshot of all tables was completely
   * saved. */
  std::string manifest_file_path(directory_path);
  manifest_file_path.append("/manifest");
  if (!std::filesystem::exists(manifest_file_path)) {
    return false;
  }
  uint64_t table_count;
  std::vector<std::string> table_names;
  if (!LoadManifest(manifest_file_path, table_count, table_names)) {
    return false;
  }

  auto table_dictionary = vdb::GetTableDictionary();
  /* load all tables */
  for (auto table_name : table_names) {
    std::string table_directory_path(directory_path);
    table_directory_path.append("/");
    table_directory_path.append(table_name);
    vdb::TableBuilderOptions options;
    vdb::TableBuilder builder(
        std::move(options.SetTableName(table_name)
                      .SetTableDirectoryPath(table_directory_path)));
    auto maybe_table = builder.Build();
    if (!maybe_table.ok()) {
      return false;
    }
    auto table = maybe_table.ValueUnsafe();
    table_dictionary->insert({table_name, table});
  }
  return true;
}

/* garbage collect snapshot */
bool RemoveDirectory(char *directory_path) {
  try {
    if (!std::filesystem::remove_all(directory_path)) {
      return false;
    }
  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << __PRETTY_FUNCTION__ << " - Filesystem error: " << e.what()
              << std::endl;
  } catch (const std::exception &e) {
    std::cerr << __PRETTY_FUNCTION__ << " - General error: " << e.what()
              << std::endl;
  }
  return true;
}

void AlterSuffix(char *str, const char *suffix) {
  int length = strlen(str);
  int suffix_length = strlen(suffix);
  for (int i = 0; i < suffix_length; i++) {
    str[length - suffix_length + i] = suffix[i];
  }
}
