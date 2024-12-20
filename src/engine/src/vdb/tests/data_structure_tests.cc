#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include "vdb/vdb_api.hh"
#include "vdb/common/defs.hh"
#include "vdb/common/util.hh"
#include "vdb/data/filter.hh"
#include "vdb/data/mutable_array.hh"
#include "vdb/data/table.hh"
#include "vdb/tests/util_for_test.hh"

#ifdef __cplusplus
extern "C" {
#include "server.h"
}
#endif
class GlobalEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    // std::cout << "Global setup before any test case runs." << std::endl;
    server.vdb_active_set_size_limit = 1000;
    /* disable redis server log */
#ifdef _DEBUG_GTEST
    server.verbosity = LL_DEBUG;
#else
    server.verbosity = LL_NOTHING;
#endif
    server.logfile = empty_string.data();
  }

  void TearDown() override {
    // std::cout << "Global teardown after all test cases have run." <<
    // std::endl;
  }
};
class DataStructureTestSuite : public ::testing::Test {
 protected:
  void SetUp() override {
    /* all memory must be freed before starting test case */
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
    ASSERT_EQ(zmalloc_used_memory(), 0);
    AllocateTableDictionary();
  }

  void TearDown() override {
    DeallocateTableDictionary();
    /* all memory must be freed after finish of test case */
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
    ASSERT_EQ(zmalloc_used_memory(), 0);
  }
};

class TableTest : public DataStructureTestSuite {};
class FilterTest : public DataStructureTestSuite {};
class MutableArrayTest : public DataStructureTestSuite {};
class SegmentTest : public DataStructureTestSuite {};
// TODO - The tests should be performed automatically without human eyes..

TEST_F(TableTest, ParseStringTest) {
  std::string table_schema_string =
      "ID INT32, Name String, Attributes List[  String  ], Vector "
      "Fixed_Size_list[  1024,    floAt32 ]";
  arrow::FieldVector fields;
  fields.emplace_back(std::make_shared<arrow::Field>("id", arrow::int32()));
  fields.emplace_back(std::make_shared<arrow::Field>("name", arrow::utf8()));
  fields.emplace_back(
      std::make_shared<arrow::Field>("attributes", arrow::list(arrow::utf8())));
  fields.emplace_back(std::make_shared<arrow::Field>(
      "vector", arrow::fixed_size_list(arrow::float32(), 1024)));
  auto schema = std::make_shared<arrow::Schema>(fields);
  auto parsed_schema = vdb::ParseSchemaFrom(table_schema_string);

  EXPECT_TRUE(schema->Equals(parsed_schema));
}

TEST_F(TableTest, CreateTableTest) {
  std::string table_schema_string =
      "ID INT32, Name String, Attributes List[  String  ], Vector "
      "Fixed_Size_list[  1024,    floAt32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  std::string table_name = "test_table";
  uint32_t idx = 0;
  uint32_t *idx_ptr = &idx;
  std::string segment_id_info(reinterpret_cast<char *>(idx_ptr),
                              sizeof(uint32_t));
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", segment_id_info},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
#ifdef _DEBUG_GTEST
  std::cout << table->ToString() << std::endl;
#endif
  auto segment = table->AddSegment(table, "test_segment");
  EXPECT_TRUE(segment != nullptr);
}

TEST_F(MutableArrayTest, AppendAndInactiveTest) {
  vdb::NumericArray<int16_t, arrow::Int16Array> int16arr;
  EXPECT_TRUE(int16arr.Append(23).ok());
  EXPECT_TRUE(int16arr.Append(24).ok());
  EXPECT_TRUE(int16arr.AppendNull().ok());
  EXPECT_TRUE(int16arr.Append(25).ok());
  EXPECT_TRUE(int16arr.Append(26).ok());
  EXPECT_EQ(int16arr.Size(), 5);
  EXPECT_EQ(int16arr.GetValue(4).value(), 26);
  EXPECT_FALSE(int16arr.GetValue(2).has_value());
  EXPECT_FALSE(int16arr.GetValue(8).has_value());

  vdb::FixedSizeListArray<float, arrow::FloatArray> fslarr(4);
  EXPECT_TRUE(fslarr.Append({128.0, 32.8, 192.6, 48.1}).ok());
  EXPECT_TRUE(fslarr.Append({423.3, 418.1, 191.4, 0.512}).ok());
  EXPECT_TRUE(fslarr.AppendNull().ok());
  EXPECT_TRUE(fslarr.Append({127.0, 0, 0, 1.0}).ok());
  EXPECT_EQ(fslarr.Size(), 4);
  EXPECT_EQ(fslarr.GetValue(0).value().size(), 4);
  EXPECT_EQ(fslarr.GetValue(1).value().size(), 4);
  EXPECT_FALSE(fslarr.GetValue(2).has_value());
  EXPECT_EQ(fslarr.GetValue(3).value().size(), 4);

  vdb::ListArray<uint32_t, arrow::UInt32Array> larr;
  EXPECT_TRUE(larr.Append({128, 64, 32, 16}).ok());
  EXPECT_TRUE(larr.AppendNull().ok());
  EXPECT_TRUE(larr.Append({8, 4}).ok());
  EXPECT_TRUE(larr.AppendNull().ok());
  EXPECT_TRUE(larr.Append({2}).ok());
  EXPECT_TRUE(larr.AppendNull().ok());
  EXPECT_TRUE(larr.Append(std::vector<uint32_t>{}).ok());
  EXPECT_EQ(larr.Size(), 7);
  EXPECT_EQ(larr.GetValue(0).value(), std::vector<uint32_t>({128, 64, 32, 16}));
  EXPECT_FALSE(larr.GetValue(1).has_value());
  EXPECT_EQ(larr.GetValue(2).value(), std::vector<uint32_t>({8, 4}));
  EXPECT_FALSE(larr.GetValue(3).has_value());
  EXPECT_EQ(larr.GetValue(4).value(), std::vector<uint32_t>({2}));
  EXPECT_FALSE(larr.GetValue(5).has_value());

  vdb::StringArray strarr;
  EXPECT_TRUE(strarr.Append(std::string_view{"Hello World!"}).ok());
  EXPECT_TRUE(strarr.Append(std::string_view{"Nice to meet you!"}).ok());
  EXPECT_TRUE(strarr.AppendNull().ok());
  EXPECT_TRUE(strarr.Append(std::string_view{"There is no spoon."}).ok());
  EXPECT_EQ(strarr.Size(), 4);
  EXPECT_EQ(strarr.GetValue(0).value(), "Hello World!");
  EXPECT_EQ(strarr.GetValue(1).value(), "Nice to meet you!");
  EXPECT_FALSE(strarr.GetValue(2).has_value());
  EXPECT_EQ(strarr.GetValue(3).value(), "There is no spoon.");
  EXPECT_FALSE(strarr.GetValue(4).has_value());

  vdb::LargeStringArray lstrarr;
  EXPECT_TRUE(lstrarr.Append(std::string_view{"Hello World!"}).ok());
  EXPECT_TRUE(lstrarr.Append(std::string_view{"Nice to meet you!"}).ok());
  EXPECT_TRUE(lstrarr.AppendNull().ok());
  EXPECT_TRUE(lstrarr.Append(std::string_view{"There is no spoon."}).ok());
  EXPECT_EQ(lstrarr.Size(), 4);
  EXPECT_EQ(lstrarr.GetValue(0).value(), "Hello World!");
  EXPECT_EQ(lstrarr.GetValue(1).value(), "Nice to meet you!");
  EXPECT_FALSE(lstrarr.GetValue(2).has_value());
  EXPECT_EQ(lstrarr.GetValue(3).value(), "There is no spoon.");
  EXPECT_FALSE(lstrarr.GetValue(4).has_value());

  vdb::StringListArray slarr;
  EXPECT_TRUE(slarr.Append({"Hello", "World"}).ok());
  EXPECT_TRUE(slarr.AppendNull().ok());
  EXPECT_TRUE(slarr.Append({"There", "is", "no", "spoon"}).ok());
  EXPECT_TRUE(slarr.Append({"Nothing", "is", "impossible"}).ok());
  EXPECT_TRUE(slarr.Append(std::vector<std::string>{}).ok());
  EXPECT_EQ(slarr.Size(), 5);
  EXPECT_EQ(slarr.GetValue(0).value(),
            std::vector<std::string_view>({"Hello", "World"}));
  EXPECT_FALSE(slarr.GetValue(1).has_value());
  EXPECT_EQ(slarr.GetValue(2).value(),
            std::vector<std::string_view>({"There", "is", "no", "spoon"}));
  EXPECT_EQ(slarr.GetValue(3).value(),
            std::vector<std::string_view>({"Nothing", "is", "impossible"}));

  vdb::StringFixedSizeListArray sfslarr(2);
  EXPECT_TRUE(sfslarr.AppendNull().ok());
  EXPECT_TRUE(sfslarr.Append({"Hello", "World"}).ok());
  EXPECT_TRUE(sfslarr.Append({"Nice to", "Meet you"}).ok());
  EXPECT_EQ(sfslarr.Size(), 3);
  EXPECT_FALSE(sfslarr.GetValue(0).has_value());
  EXPECT_EQ(sfslarr.GetValue(1),
            std::vector<std::string_view>({"Hello", "World"}));
  EXPECT_EQ(sfslarr.GetValue(2),
            std::vector<std::string_view>({"Nice to", "Meet you"}));

  vdb::BooleanArray barr;
  EXPECT_TRUE(barr.Append(true).ok());
  EXPECT_TRUE(barr.AppendNull().ok());
  EXPECT_TRUE(barr.Append(false).ok());
  EXPECT_TRUE(barr.Append(true).ok());
  EXPECT_EQ(barr.Size(), 4);
  EXPECT_TRUE(barr.GetValue(0).value());
  EXPECT_FALSE(barr.GetValue(1).has_value());
  EXPECT_FALSE(barr.GetValue(2).value());
  EXPECT_TRUE(barr.GetValue(3).value());

  vdb::BooleanListArray blarr;
  EXPECT_TRUE(blarr.Append({true, true, false}).ok());
  EXPECT_TRUE(blarr.Append({false, false, true, false}).ok());
  EXPECT_TRUE(blarr.AppendNull().ok());
  EXPECT_TRUE(blarr.Append({true, false}).ok());
  EXPECT_TRUE(blarr.Append(std::vector<bool>{}).ok());
  EXPECT_EQ(blarr.Size(), 5);
  EXPECT_EQ(blarr.GetValue(0), std::vector<bool>({true, true, false}));
  EXPECT_EQ(blarr.GetValue(1), std::vector<bool>({false, false, true, false}));
  EXPECT_FALSE(blarr.GetValue(2).has_value());
  EXPECT_EQ(blarr.GetValue(3), std::vector<bool>({true, false}));

  vdb::BooleanFixedSizeListArray bfslarr(4);
  EXPECT_TRUE(bfslarr.Append({true, false, false, false}).ok());
  EXPECT_TRUE(bfslarr.AppendNull().ok());
  EXPECT_TRUE(bfslarr.Append({false, true, false, false}).ok());
  EXPECT_TRUE(bfslarr.Append({false, false, true, false}).ok());
  EXPECT_TRUE(bfslarr.AppendNull().ok());
  EXPECT_TRUE(bfslarr.Append({false, false, false, true}).ok());
  EXPECT_TRUE(bfslarr.GetValue(0).has_value());
  EXPECT_FALSE(bfslarr.GetValue(1).has_value());
  EXPECT_TRUE(bfslarr.GetValue(2).has_value());
  EXPECT_TRUE(bfslarr.GetValue(3).has_value());
  EXPECT_FALSE(bfslarr.GetValue(4).has_value());
  EXPECT_TRUE(bfslarr.GetValue(5).has_value());
  EXPECT_EQ(bfslarr.GetValue(0).value(), std::vector<bool>({1, 0, 0, 0}));
  EXPECT_EQ(bfslarr.GetValue(2).value(), std::vector<bool>({0, 1, 0, 0}));
  EXPECT_EQ(bfslarr.GetValue(3).value(), std::vector<bool>({0, 0, 1, 0}));
  EXPECT_EQ(bfslarr.GetValue(5).value(), std::vector<bool>({0, 0, 0, 1}));
}

TEST_F(SegmentTest, AppendRecordTest) {
  std::string table_schema_string =
      "Id int32, Name String, Height float32, Gender Bool";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  std::string table_name = "test_table";
  uint32_t idx = 0;
  uint32_t *idx_ptr = &idx;
  std::string segment_id_info(reinterpret_cast<char *>(idx_ptr),
                              sizeof(uint32_t));
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", segment_id_info},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  table->AddSegment(table, "test_segment");
  auto segment = table->GetSegment("test_segment");

  // data
  std::vector<std::string> records = {"0\u001eTom\u001e180.3\u001e0",
                                      "1\u001eJane\u001e190.1\u001e1",
                                      "2\u001eJohn\u001e168.7\u001e0"};

  for (auto &record : records) {
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      EXPECT_TRUE(status.ok());
    }
  }

#ifdef _DEBUG_GTEST
  std::cout << segment->ToString() << std::endl;
#endif
}

TEST_F(SegmentTest, AppendRecordTestWithLargeString) {
  std::string table_schema_string =
      "Id int32, Name LaRge_String, Height float32, Gender Bool";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  std::string table_name = "test_table";
  uint32_t idx = 0;
  uint32_t *idx_ptr = &idx;
  std::string segment_id_info(reinterpret_cast<char *>(idx_ptr),
                              sizeof(uint32_t));
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", segment_id_info},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  table->AddSegment(table, "test_segment");
  auto segment = table->GetSegment("test_segment");

  // data
  std::vector<std::string> records = {"0\u001eTom\u001e180.3\u001e0",
                                      "1\u001eJane\u001e190.1\u001e1",
                                      "2\u001eJohn\u001e168.7\u001e0"};

  for (auto &record : records) {
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      EXPECT_TRUE(status.ok());
    }
  }

#ifdef _DEBUG_GTEST
  std::cout << segment->ToString() << std::endl;
#endif
}

TEST_F(SegmentTest, ListAppendRecordTest) {
  std::string table_schema_string =
      "Id int32, Name String, Height float32, Attributes List[String]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  std::string table_name = "test_table";
  uint32_t idx = 0;
  uint32_t *idx_ptr = &idx;
  std::string segment_id_info(reinterpret_cast<char *>(idx_ptr),
                              sizeof(uint32_t));
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", segment_id_info},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  table->AddSegment(table, "test_segment");
  auto segment = table->GetSegment("test_segment");

  // data
  std::vector<std::string> records = {
      "0\u001eTom\u001e180.3\u001eC\u001dC++\u001dPython",
      "1\u001eJane\u001e190.1\u001ePython\u001dLisp",
      "2\u001eJohn\u001e168.7\u001ePython\u001dJavaScript"};

  for (auto &record : records) {
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      EXPECT_TRUE(status.ok());
    }
  }

#ifdef _DEBUG_GTEST
  std::cout << segment->ToString() << std::endl;
#endif

  auto status = segment->MakeInactive();
  if (!status.ok()) {
    EXPECT_TRUE(status.ok());
    std::cout << status.ToString() << std::endl;
  }

#ifdef _DEBUG_GTEST
  std::cout << segment->ToString() << std::endl;

  std::cout << "---------------------RecordBatch Test---------------------"
            << std::endl;
  std::cout << segment->ActiveSetRecordBatch()->ToString() << std::endl;
#endif
}

TEST_F(FilterTest, BasicFilterTest) {
  std::string test_schema_string = "Id int32, Name string, height float32";
  std::string test_schema_string2 =
      "Id int32, Name large_string, height float32";
  std::string test_filter_string = "id\u001e2\u001e<=";
  std::string test_filter_string2 =
      "id\u001e2\u001e<=\u001eheight\u001e150\u001e>\u001eand";
  std::string test_filter_string3 =
      "id\u001e2\u001e<=\u001eheight\u001e150\u001e>\u001eand\u001enot";
  std::string test_filter_string4 =
      "id\u001e2\u001e<=\u001eheight\u001e150\u001e>"
      "\u001eand\u001enot\u001ename\u001eJohn,Jane\u001ein\u001eand";
  std::string test_filter_string5 =
      "id\u001e2\u001e<=\u001eid\u001eisnotnull\u001eand\u001enot";
  std::string test_filter_string6 =
      "id\u001e2\u001e<=\u001enot\u001eid\u001eisnotnull\u001eand\u001enot";

  std::string_view view = test_filter_string;

  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  auto schema2 = vdb::ParseSchemaFrom(test_schema_string2);

  auto filter = vdb::Filter::Parse(view, schema);

  EXPECT_EQ(filter->type, vdb::Filter::kLessThanEqual);
  EXPECT_EQ(filter->ToString(), "(id <= \"2\")");

  filter = vdb::Filter::Parse(view, schema);

  EXPECT_EQ(filter->type, vdb::Filter::kLessThanEqual);
  EXPECT_EQ(filter->ToString(), "(id <= \"2\")");

  view = test_filter_string2;
  filter = vdb::Filter::Parse(view, schema);

  EXPECT_EQ(filter->type, vdb::Filter::kAnd);
  EXPECT_EQ(filter->ToString(), "((id <= \"2\") AND (height > \"150\"))");

  filter = vdb::Filter::Parse(view, schema2);

  EXPECT_EQ(filter->type, vdb::Filter::kAnd);
  EXPECT_EQ(filter->ToString(), "((id <= \"2\") AND (height > \"150\"))");

  view = test_filter_string3;
  filter = vdb::Filter::Parse(view, schema);

  EXPECT_EQ(filter->type, vdb::Filter::kOr);
  EXPECT_EQ(filter->ToString(), "((id > \"2\") OR (height <= \"150\"))");

  filter = vdb::Filter::Parse(view, schema2);

  EXPECT_EQ(filter->type, vdb::Filter::kOr);
  EXPECT_EQ(filter->ToString(), "((id > \"2\") OR (height <= \"150\"))");

  view = test_filter_string4;
  filter = vdb::Filter::Parse(view, schema);

  EXPECT_EQ(filter->type, vdb::Filter::kAnd);
  EXPECT_EQ(
      filter->ToString(),
      "(((id > \"2\") OR (height <= \"150\")) AND (name In \"John,Jane\"))");

  filter = vdb::Filter::Parse(view, schema2);

  EXPECT_EQ(filter->type, vdb::Filter::kAnd);
  EXPECT_EQ(
      filter->ToString(),
      "(((id > \"2\") OR (height <= \"150\")) AND (name In \"John,Jane\"))");

  view = test_filter_string5;
  filter = vdb::Filter::Parse(view, schema);

  EXPECT_EQ(filter->type, vdb::Filter::kOr);
  EXPECT_EQ(filter->ToString(), "((id > \"2\") OR (id Is null))");

  filter = vdb::Filter::Parse(view, schema2);

  EXPECT_EQ(filter->type, vdb::Filter::kOr);
  EXPECT_EQ(filter->ToString(), "((id > \"2\") OR (id Is null))");

  view = test_filter_string6;
  filter = vdb::Filter::Parse(view, schema);

  EXPECT_EQ(filter->type, vdb::Filter::kOr);
  EXPECT_EQ(filter->ToString(), "((id <= \"2\") OR (id Is null))");

  filter = vdb::Filter::Parse(view, schema2);

  EXPECT_EQ(filter->type, vdb::Filter::kOr);
  EXPECT_EQ(filter->ToString(), "((id <= \"2\") OR (id Is null))");
}

TEST_F(FilterTest, SegmentFilterTest) {
  std::string test_schema_string = "Id int32, Name string, height float32";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  uint32_t segment_id = 0;
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", std::string((char *)&segment_id, 4)}}));
  schema = schema->WithMetadata(metadata);
#ifdef _DEBUG_GTEST
  std::cout << schema->ToString() << std::endl;
#endif

  std::string test_filter_string =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>\u001eor";  // no segment
                                                                 // filter
  std::string test_filter_string2 =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>\u001eand";
  std::string test_filter_string3 =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>"
      "\u001eand\u001eid\u001e1\u001e>\u001eor";  // no segment filter
  std::string test_filter_string4 =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>"
      "\u001eand\u001eid\u001e1\u001e>\u001eand";
  std::string test_filter_string5 =
      "2\u001eid\u001e>\u001eheight\u001e160.0\u001e>"
      "\u001eand\u001eid\u001e1\u001e>"
      "\u001eand\u001eid\u001eisnotnull\u001eand";

  auto test_filter_view = std::string_view(test_filter_string);
  auto filter = vdb::Filter::Parse(test_filter_view, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
  EXPECT_EQ(segment_filters.size(), 0);

  test_filter_view = std::string_view(test_filter_string2);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
#ifdef _DEBUG_GTEST
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << " ";
  }
  std::cout << std::endl;
#endif

  test_filter_view = std::string_view(test_filter_string3);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
  EXPECT_EQ(segment_filters.size(), 0);

  test_filter_view = std::string_view(test_filter_string4);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
#ifdef _DEBUG_GTEST
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << " ";
  }
  std::cout << std::endl;
#endif

  test_filter_view = std::string_view(test_filter_string5);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
#ifdef _DEBUG_GTEST
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << " ";
  }
  std::cout << std::endl;
#endif
}

TEST_F(FilterTest, SegmentFilterTestWithLargeString) {
  std::string test_schema_string =
      "Id int32, Name large_string, height float32";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  uint32_t segment_id = 0;
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", std::string((char *)&segment_id, 4)}}));
  schema = schema->WithMetadata(metadata);
#ifdef _DEBUG_GTEST
  std::cout << schema->ToString() << std::endl;
#endif

  std::string test_filter_string =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>\u001eor";  // no segment
                                                                 // filter
  std::string test_filter_string2 =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>\u001eand";
  std::string test_filter_string3 =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>"
      "\u001eand\u001eid\u001e1\u001e>\u001eor";  // no segment filter
  std::string test_filter_string4 =
      "id\u001e2\u001e<\u001eheight\u001e160.0\u001e>"
      "\u001eand\u001eid\u001e1\u001e>\u001eand";
  std::string test_filter_string5 =
      "2\u001eid\u001e>\u001eheight\u001e160.0\u001e>"
      "\u001eand\u001eid\u001e1\u001e>"
      "\u001eand\u001eid\u001eisnotnull\u001eand";

  auto test_filter_view = std::string_view(test_filter_string);
  auto filter = vdb::Filter::Parse(test_filter_view, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
  EXPECT_EQ(segment_filters.size(), 0);

  test_filter_view = std::string_view(test_filter_string2);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
#ifdef _DEBUG_GTEST
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << " ";
  }
  std::cout << std::endl;
#endif

  test_filter_view = std::string_view(test_filter_string3);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
  EXPECT_EQ(segment_filters.size(), 0);

  test_filter_view = std::string_view(test_filter_string4);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
#ifdef _DEBUG_GTEST
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << " ";
  }
  std::cout << std::endl;
#endif

  test_filter_view = std::string_view(test_filter_string5);
  filter = vdb::Filter::Parse(test_filter_view, schema);
  segment_filters = vdb::Filter::GetSegmentFilters(filter, schema);
#ifdef _DEBUG_GTEST
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << " ";
  }
  std::cout << std::endl;
#endif
}

TEST_F(FilterTest, SegmentFilter2) {
  std::string test_schema_string =
      "id int32, pid uint32, name string, height float32";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  std::string table_name = "test_table";
  uint32_t segment_ids[2] = {0, 1};
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info",
            std::string((char *)segment_ids, sizeof(uint32_t) * 2)},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  /* (id < 2 and id > 0) and (name T% like)
   * post-order */
  std::string filter_string =
      "id\u001e2\u001e<\u001epid\u001e0\u001e>\u001eand"
      "\u001ename\u001eT%\u001elike"
      "\u001eand";
  std::string_view filter_view{filter_string};
  auto whole_filter = vdb::Filter::Parse(filter_view, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);
#ifdef _DEBUG_GTEST
  std::cout << "Filter: ";
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << "\t";
  }
  std::cout << std::endl;
#endif

  auto segment_id_info = schema->metadata()->Get("segment_id_info").ValueOr("");
  const uint32_t segment_id_info_count =
      segment_id_info.size() / sizeof(uint32_t);
  uint32_t *segment_id_column_indexes =
      reinterpret_cast<uint32_t *>(segment_id_info.data());

  std::vector<std::string> records = {
      "0\u001e1\u001eJohn\u001e180.3", "0\u001e1\u001eJane\u001e170.1",
      "1\u001e2\u001eTom\u001e163.0",  "1\u001e2\u001eDaniel\u001e169.3",
      "2\u001e2\u001eFord\u001e182.7", "2\u001e3\u001eLeo\u001e172.8",
      "0\u001e3\u001eLead\u001e179.5"};

  for (auto &record : records) {
    std::string segment_id = "";
    for (uint32_t i = 0; i < segment_id_info_count; i++) {
      segment_id +=
          vdb::GetTokenFrom(record, vdb::kRS, *(segment_id_column_indexes + i));
      segment_id += vdb::kRS;
    }
    segment_id.resize(segment_id.size() - 1);
    auto segment = table->GetSegment(segment_id);
    if (segment == nullptr) {
      segment = table->AddSegment(table, segment_id);
    }
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      EXPECT_TRUE(status.ok());
      std::cout << status.ToString() << std::endl;
    }
  }

  auto segments = table->GetSegments();

  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);

  EXPECT_FALSE(filtered_segments.empty());

#ifdef _DEBUG_GTEST
  for (auto &segment : filtered_segments) {
    std::cout << segment->ToString() << std::endl;
  }
#endif
}

TEST_F(FilterTest, SegmentFilterWithLargeString2) {
  std::string test_schema_string =
      "id int32, pid uint32, name large_string, height float32";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  std::string table_name = "test_table";
  uint32_t segment_ids[2] = {0, 1};
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info",
            std::string((char *)segment_ids, sizeof(uint32_t) * 2)},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  /* (id < 2 and id > 0) and (name T% like)
   * post-order */
  std::string filter_string =
      "id\u001e2\u001e<\u001epid\u001e0\u001e>\u001eand"
      "\u001ename\u001eT%\u001elike"
      "\u001eand";
  std::string_view filter_view{filter_string};
  auto whole_filter = vdb::Filter::Parse(filter_view, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);
#ifdef _DEBUG_GTEST
  std::cout << "Filter: ";
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << "\t";
  }
  std::cout << std::endl;
#endif

  auto segment_id_info = schema->metadata()->Get("segment_id_info").ValueOr("");
  const uint32_t segment_id_info_count =
      segment_id_info.size() / sizeof(uint32_t);
  uint32_t *segment_id_column_indexes =
      reinterpret_cast<uint32_t *>(segment_id_info.data());

  std::vector<std::string> records = {
      "0\u001e1\u001eJohn\u001e180.3", "0\u001e1\u001eJane\u001e170.1",
      "1\u001e2\u001eTom\u001e163.0",  "1\u001e2\u001eDaniel\u001e169.3",
      "2\u001e2\u001eFord\u001e182.7", "2\u001e3\u001eLeo\u001e172.8",
      "0\u001e3\u001eLead\u001e179.5"};

  for (auto &record : records) {
    std::string segment_id = "";
    for (uint32_t i = 0; i < segment_id_info_count; i++) {
      segment_id +=
          vdb::GetTokenFrom(record, vdb::kRS, *(segment_id_column_indexes + i));
      segment_id += vdb::kRS;
    }
    segment_id.resize(segment_id.size() - 1);
    auto segment = table->GetSegment(segment_id);
    if (segment == nullptr) {
      segment = table->AddSegment(table, segment_id);
    }
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      EXPECT_TRUE(status.ok());
      std::cout << status.ToString() << std::endl;
    }
  }

  auto segments = table->GetSegments();

  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);

  EXPECT_FALSE(filtered_segments.empty());

#ifdef _DEBUG_GTEST
  for (auto &segment : filtered_segments) {
    std::cout << segment->ToString() << std::endl;
  }
#endif
}

TEST_F(FilterTest, DataFilterTest) {
  std::string test_schema_string = "id int32, name string, height float32";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  uint32_t segment_id = 0;
  std::string table_name = "test_table";
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", std::string((char *)&segment_id, 4)},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  std::string filter_string =
      "id\u001e2\u001e<\u001eid\u001e0\u001e>\u001eand\u001ename\u001eT%"
      "\u001elike\u001eand\u001eid\u001eisnotnull\u001eand";
  std::string_view filter_view{filter_string};
  auto whole_filter = vdb::Filter::Parse(filter_view, schema);

#ifdef _DEBUG_GTEST
  std::cout << whole_filter->ToString() << std::endl;
#endif

  auto segment = std::make_shared<vdb::Segment>(table, "test_segment");

  std::vector<std::string> records = {
      "0\u001eTom\u001e180.3", "1\u001eJane\u001e190.1",
      "1\u001eTyson\u001e197.9", "2\u001eJohn\u001e168.7"};

  for (auto &record : records) {
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      EXPECT_TRUE(status.ok());
    }
  }

#ifdef _DEBUG_GTEST
  std::cout << segment->ToString() << std::endl;

  std::cout << "---------------- Filtered ----------------" << std::endl;
#endif
  auto maybe_filtered_rbs = vdb::Filter::Records(segment, whole_filter, schema);

  EXPECT_EQ(maybe_filtered_rbs.ok(), true);
  auto filtered_rbs = maybe_filtered_rbs.ValueUnsafe();
  EXPECT_EQ(filtered_rbs.size(), 1);
#ifdef _DEBUG_GTEST
  for (auto rb : filtered_rbs) {
    std::cout << rb->ToString() << std::endl;
  }
#endif
}

TEST_F(FilterTest, DataFilterTestWithLargeString) {
  std::string test_schema_string =
      "id int32, name large_string, height float32";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  uint32_t segment_id = 0;
  std::string table_name = "test_table";
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", std::string((char *)&segment_id, 4)},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  std::string filter_string =
      "id\u001e2\u001e<\u001eid\u001e0\u001e>\u001eand\u001ename\u001eT%"
      "\u001elike\u001eand\u001eid\u001eisnotnull\u001eand";
  std::string_view filter_view{filter_string};
  auto whole_filter = vdb::Filter::Parse(filter_view, schema);

#ifdef _DEBUG_GTEST
  std::cout << whole_filter->ToString() << std::endl;
#endif

  auto segment = std::make_shared<vdb::Segment>(table, "test_segment");

  std::vector<std::string> records = {
      "0\u001eTom\u001e180.3", "1\u001eJane\u001e190.1",
      "1\u001eTyson\u001e197.9", "2\u001eJohn\u001e168.7"};

  for (auto &record : records) {
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      std::cerr << status.ToString() << std::endl;
      EXPECT_TRUE(status.ok());
    }
  }

#ifdef _DEBUG_GTEST
  std::cout << segment->ToString() << std::endl;

  std::cout << "---------------- Filtered ----------------" << std::endl;
#endif
  auto maybe_filtered_rbs = vdb::Filter::Records(segment, whole_filter, schema);

  EXPECT_EQ(maybe_filtered_rbs.ok(), true);
  auto filtered_rbs = maybe_filtered_rbs.ValueUnsafe();
  EXPECT_EQ(filtered_rbs.size(), 1);
#ifdef _DEBUG_GTEST
  for (auto rb : filtered_rbs) {
    std::cout << rb->ToString() << std::endl;
  }
#endif
}

TEST_F(FilterTest, MixedFilterTest1) {
  std::string test_schema_string = "id int32, val int32, tag string";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  uint32_t segment_id = 0;
  std::string table_name = "test_table";
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", std::string((char *)&segment_id, 4)},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  /* (id < 4 and val > 10) and (tag 1% like)*/
  std::string filter_string =
      "id\u001e4\u001e<\u001eval\u001e10\u001e>\u001eand"
      "\u001etag\u001e1%\u001elike\u001eand";
  std::string_view filter_view{filter_string};
  auto whole_filter = vdb::Filter::Parse(filter_view, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);
#ifdef _DEBUG_GTEST
  std::cout << "Filter: ";
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << "\t";
  }
  std::cout << std::endl;
#endif

  std::vector<std::string> records;
  for (int i = 0; i < 100; i++) {
    std::string str = "";
    /* id */
    str += std::to_string(i % 10);
    str += "\u001e";
    /* val */
    str += std::to_string(i % 20);
    str += "\u001e";
    /* tag */
    str += std::to_string(i);
    str += "th SampleTags";
    records.push_back(str);
  }

  for (auto &record : records) {
    auto segment_id = std::string(vdb::GetTokenFrom(record, vdb::kRS, 0));
    auto segment = table->GetSegment(segment_id);
    if (segment == nullptr) {
      segment = table->AddSegment(table, segment_id);
    }
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      EXPECT_TRUE(status.ok());
      std::cout << status.ToString() << std::endl;
    }
  }

  auto segments = table->GetSegments();
  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);

  EXPECT_FALSE(filtered_segments.empty());

  for (auto &segment : filtered_segments) {
    auto maybe_filtered_rbs =
        vdb::Filter::Records(segment, whole_filter, schema);
    EXPECT_EQ(maybe_filtered_rbs.ok(), true);
    auto filtered_rbs = maybe_filtered_rbs.ValueUnsafe();
    for (auto rb : filtered_rbs) {
      if (rb->num_rows() != 0) {
#ifdef _DEBUG_GTEST
        std::cout << rb->ToString() << std::endl;
#endif
        EXPECT_EQ(rb->num_rows(), 1);
        auto id_column = std::static_pointer_cast<arrow::Int32Array>(
            rb->GetColumnByName("id"));
        EXPECT_NE(id_column, nullptr);

        if (!id_column->IsNull(0)) {
          int32_t id = id_column->Value(0);
          if (1 <= id && id <= 3) {
            EXPECT_EQ(rb->num_rows(), 1);
          } else {
            ASSERT_TRUE(false);
          }
        }
      } else {
        std::string expected = "0";
        EXPECT_EQ(segment->GetId(), expected);
      }
    }
    /* id    val    tag    segment
     * 1     11     11th   p(id=1)
     * 2     12     12th   p(id=2)
     * 3     13     13th   p(id=3)
     *
     * and p(id=0) is in filtered_segments, but no rows satisfy condition. */
  }
  EXPECT_EQ(filtered_segments.size(), 4);
}

TEST_F(FilterTest, MixedFilterTest2) {
  std::string test_schema_string = "id int32, val int32, tag string";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  uint32_t segment_id = 0;
  std::string table_name = "test_table";
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", std::string((char *)&segment_id, 4)},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  /* (id < 4 and val > 10) or (tag 1% like)*/
  std::string filter_string =
      "id\u001e4\u001e<\u001eval\u001e10\u001e>\u001eand"
      "\u001etag\u001e1%\u001elike\u001eor";
  std::string_view filter_view{filter_string};
  auto whole_filter = vdb::Filter::Parse(filter_view, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);
#ifdef _DEBUG_GTEST
  std::cout << "Filter: ";
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << "\t";
  }
  std::cout << std::endl;
#endif

  std::vector<std::string> records;
  for (int i = 0; i < 100; i++) {
    std::string str = "";
    /* id */
    str += std::to_string(i % 10);
    str += "\u001e";
    /* val */
    str += std::to_string(i % 20);
    str += "\u001e";
    /* tag */
    str += std::to_string(i);
    str += "th SampleTags";
    records.push_back(str);
  }

  for (auto &record : records) {
    auto segment_id = std::string(vdb::GetTokenFrom(record, vdb::kRS, 0));
    auto segment = table->GetSegment(segment_id);
    if (segment == nullptr) {
      segment = table->AddSegment(table, segment_id);
    }
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      EXPECT_TRUE(status.ok());
      std::cout << status.ToString() << std::endl;
    }
  }

  auto segments = table->GetSegments();
  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);

  EXPECT_FALSE(filtered_segments.empty());

  for (auto &segment : filtered_segments) {
    auto maybe_filtered_rbs =
        vdb::Filter::Records(segment, whole_filter, schema);
    EXPECT_EQ(maybe_filtered_rbs.ok(), true);
    auto filtered_rbs = maybe_filtered_rbs.ValueUnsafe();
    EXPECT_EQ(filtered_rbs.size(), 1);
    for (auto rb : filtered_rbs) {
#ifdef _DEBUG_GTEST
      std::cout << rb->ToString() << std::endl;
#endif
      auto id_column = std::static_pointer_cast<arrow::Int32Array>(
          rb->GetColumnByName("id"));
      EXPECT_NE(id_column, nullptr);

      if (!id_column->IsNull(0)) {
        int32_t id = id_column->Value(0);
        if (id == 1) {
          EXPECT_EQ(rb->num_rows(), 6);
        } else if (2 <= id && id <= 3) {
          EXPECT_EQ(rb->num_rows(), 5);
        } else if (0 == id || id <= 9) {
          /* id = 0, 4, 5, 6, 7, 8, 9 */
          EXPECT_EQ(rb->num_rows(), 1);
        } else {
          /* id < 0 or id >= 10 */
          ASSERT_TRUE(false);
        }
      }
    }
    /* id    val    tag    segment
     * (id < 4 and val > 10)
     * 1     11     11th   p(id=1)
     * 1     11     31th   p(id=1)
     * 1     11     51th   p(id=1)
     * 1     11     71th   p(id=1)
     * 1     11     91th   p(id=1)
     * 2     12     12th   p(id=2)
     * 2     12     32th   p(id=2)
     * 2     12     52th   p(id=2)
     * 2     12     72th   p(id=2)
     * 2     12     92th   p(id=2)
     * 3     13     13th   p(id=3)
     * 3     13     33th   p(id=3)
     * 3     13     53th   p(id=3)
     * 3     13     73th   p(id=3)
     * 3     13     93th   p(id=3)
     *
     * 1     1      1 th   p(id=1)
     *
     * 0     10     10th   p(id=0)
     * 1     11     11th   p(id=1) no count (dup)
     * 2     12     12th   p(id=2) no count (dup)
     * 3     13     13th   p(id=3) no count (dup)
     * 4     14     14th   p(id=4)
     * 5     15     15th   p(id=5)
     * 6     16     16th   p(id=6)
     * 7     17     17th   p(id=7)
     * 8     18     18th   p(id=8)
     * 9     19     19th   p(id=9)
     *
     * 23 keys */
  }
  /* all id (1 ~ 10) by or condition */
  EXPECT_EQ(filtered_segments.size(), 10);
}

TEST_F(FilterTest, MixedFilterTest3) {
  /* All segments are filtered out */
  std::string test_schema_string = "id int32, val int32, tag string";
  auto schema = vdb::ParseSchemaFrom(test_schema_string);
  uint32_t segment_id = 0;
  std::string table_name = "test_table";
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>(
          {{"segment_id_info", std::string((char *)&segment_id, 4)},
           {"table name", table_name},
           {"active_set_size_limit",
            std::to_string(server.vdb_active_set_size_limit)}}));
  schema = schema->WithMetadata(metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  /* (id > 10 and val > 10) and (tag 1% like)*/
  std::string filter_string =
      "id\u001e10\u001e>\u001eval\u001e10\u001e>\u001eand"
      "\u001etag\u001e1%\u001elike\u001eand";
  std::string_view filter_view{filter_string};
  auto whole_filter = vdb::Filter::Parse(filter_view, schema);
  auto segment_filters = vdb::Filter::GetSegmentFilters(whole_filter, schema);
#ifdef _DEBUG_GTEST
  std::cout << "Filter: ";
  for (auto &filter : segment_filters) {
    std::cout << filter->ToString() << "\t";
  }
  std::cout << std::endl;
#endif

  std::vector<std::string> records;
  for (int i = 0; i < 100; i++) {
    std::string str = "";
    /* id */
    str += std::to_string(i % 10);
    str += "\u001e";
    /* val */
    str += std::to_string(i % 20);
    str += "\u001e";
    /* tag */
    str += std::to_string(i);
    str += "th SampleTags";
    records.push_back(str);
  }

  for (auto &record : records) {
    auto segment_id = std::string(vdb::GetTokenFrom(record, vdb::kRS, 0));
    auto segment = table->GetSegment(segment_id);
    if (segment == nullptr) {
      segment = table->AddSegment(table, segment_id);
    }
    auto status = segment->AppendRecord(record);
    if (!status.ok()) {
      EXPECT_TRUE(status.ok());
      std::cout << status.ToString() << std::endl;
    }
  }

  auto segments = table->GetSegments();
  auto filtered_segments =
      vdb::Filter::Segments(segments, segment_filters, schema);

  EXPECT_TRUE(filtered_segments.empty());
}

TEST(ArrowTest, ConversionTest) {
  /*
  std::vector<int32_t> test_vector = {128, 42, 0,  0, 0, 0,  0,
                                      0,   32, 61, 0, 0, 74, 19};
  std::vector<uint8_t> null_vector = {vdb::BitPos[0] | vdb::BitPos[4] |
                                      vdb::BitPos[6]};
  uint8_t *data_ptr = reinterpret_cast<uint8_t *>(test_vector.data());
  int64_t data_size = test_vector.size() * sizeof(int32_t);

  uint8_t *null_ptr = reinterpret_cast<uint8_t *>(null_vector.data());
  int64_t null_size = null_vector.size() * sizeof(uint8_t);

  auto null_buffer = std::make_shared<arrow::Buffer>(null_ptr, null_size);

  auto buffer = std::make_shared<arrow::Buffer>(data_ptr, data_size);
  auto array =
      std::make_shared<arrow::PrimitiveArray>(arrow::int32(), 14, buffer);

  std::cout << array->ToString() << std::endl;

  auto flist_array = std::make_shared<arrow::FixedSizeListArray>(
      arrow::fixed_size_list(arrow::int32(), 2), 7, array, null_buffer);

  std::cout << flist_array->ToString() << std::endl;

  std::unique_ptr<uint8_t> dst_ptr(new uint8_t[128]);
  uint64_t value = -1;
  std::copy((uint8_t *)&value, ((uint8_t *)&value) + 8, dst_ptr.get());
  auto view_ptr = (uint64_t *)dst_ptr.get();
  EXPECT_EQ(value, *view_ptr);

  std::vector<int32_t> offset_vector = {0, 2, 8, 10, 12, 14};
  auto offset_buffer =
      std::make_shared<arrow::Buffer>((uint8_t *)offset_vector.data(), 24);

  auto list_array = std::make_shared<arrow::ListArray>(
      arrow::list(arrow::int32()), 5, offset_buffer, array);

  std::cout << list_array->ToString() << std::endl;

  std::string test_str_vector = "helloworld";
  std::vector<int32_t> offset_vec = {0, 5, 10};
  auto str_buf = std::make_shared<arrow::Buffer>(
      (uint8_t *)test_str_vector.data(), (int64_t)test_str_vector.size());
  auto offset_buf = std::make_shared<arrow::Buffer>(
      (uint8_t *)offset_vec.data(),
      (int64_t)(offset_vec.size() * sizeof(int32_t)));
  auto str_arr = std::make_shared<arrow::StringArray>(2, offset_buf, str_buf);
  std::cout << str_arr->ToString() << std::endl;
  auto vb = std::make_shared<arrow::StringBuilder>();

  auto lb = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
  vb); std::shared_ptr<arrow::Array> arr; lb->Append(); vb->Append("");
  lb->Append();
  vb->Append("");
  vb->Append("");
  lb->Append();
  vb->Append("");
  vb->Append("");
  vb->Append("");
  lb->Append();
  vb->Append("");
  vb->Append("");
  vb->Append("");
  vb->Append("");
  lb->Append();
  vb->Append("");
  vb->Append("");
  vb->Append("");
  vb->Append("");
  vb->Append("");
  lb->Append(false);
  lb->Finish(&arr);
  std::cout << arr->ToString() << std::endl;
  auto vb = std::make_shared<arrow::StringBuilder>();

  auto lb =
  std::make_shared<arrow::FixedSizeListBuilder>(arrow::default_memory_pool(),
  vb, 3); std::shared_ptr<arrow::Array> arr; vb->Append(""); vb->Append("");
  vb->Append("");
  lb->Append();
  lb->Finish(&arr);
  std::cout << arr->ToString() << std::endl;
  // StringListArray 슬라이스
  int64_t offset = 1;
  int64_t length = 2;
  auto sliced_array = arr->Slice(offset, length);

  auto org_array = std::static_pointer_cast<arrow::ListArray>(arr);
  std::cout << org_array->offset() << " " << org_array->length() << std::endl;
  auto org_values =
  std::static_pointer_cast<arrow::StringArray>(org_array->values());

  std::cout << org_values->offset() << " " << org_values->length() << " " <<
  org_values->raw_value_offsets() << std::endl;
  // 슬라이스된 StringListArray의 데이터 및 자식 데이터의 오프셋 및 길이 출력
  std::cout << "sliced" << std::endl;
  auto string_list_array =
  std::static_pointer_cast<arrow::ListArray>(sliced_array); std::cout <<
  string_list_array->offset() << " " << string_list_array->length() <<
  std::endl; auto sl_values =
  std::static_pointer_cast<arrow::StringArray>(string_list_array->values());
  std::cout << org_values->offset() << " " << org_values->length() << " " <<
  org_values->raw_value_offsets() << std::endl;
  */
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new GlobalEnvironment);
  return RUN_ALL_TESTS();
}
