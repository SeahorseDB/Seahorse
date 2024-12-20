#pragma once

#include <string_view>

constexpr const char* kTableNameKey = "table name";
constexpr const char* kSegmentIdInfoKey = "segment_id_info";
constexpr const char* kActiveSetSizeLimitKey = "active_set_size_limit";
constexpr const char* kAnnColumnIdKey = "ann_column_id";
constexpr const char* kIndexTypeKey = "index_type";
constexpr const char* kIndexSpaceKey = "index_space";
constexpr const char* kEfConstructionKey = "ef_construction";
constexpr const char* kMKey = "M";

constexpr const char* GetTableNameKey() { return kTableNameKey; }
constexpr const char* GetSegmentIdInfoKey() { return kSegmentIdInfoKey; }
constexpr const char* GetActiveSetSizeLimitKey() {
  return kActiveSetSizeLimitKey;
}
constexpr const char* GetAnnColumnIdKey() { return kAnnColumnIdKey; }
constexpr const char* GetIndexTypeKey() { return kIndexTypeKey; }
constexpr const char* GetIndexSpaceKey() { return kIndexSpaceKey; }
constexpr const char* GetEfConstructionKey() { return kEfConstructionKey; }
constexpr const char* GetMKey() { return kMKey; }

static bool IsReservedMetadataKey(std::string_view key) {
  return key == kTableNameKey || key == kSegmentIdInfoKey ||
         key == kActiveSetSizeLimitKey || key == kAnnColumnIdKey ||
         key == kIndexTypeKey || key == kIndexSpaceKey ||
         key == kEfConstructionKey || key == kMKey;
}

static bool IsImmutable(std::string_view key) {
  return key == kActiveSetSizeLimitKey;
}