#include <iostream>
#include <iterator>
#include <memory>
#include <random>
#include <sched.h>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/type_fwd.h>

#include <gtest/gtest.h>

#include "vdb/common/memory_allocator.hh"
#include "vdb/tests/util_for_test.hh"

using namespace vdb::tests;

namespace vdb {

class GlobalEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    server.vdb_active_set_size_limit = 10000;
    server.allow_bg_index_thread = false;
    // std::cout << "Global setup before any test case runs." << std::endl;
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
class CommandTest : public DataStructureTestSuite {};

TEST_F(CommandTest, CreateTableCommand) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string test_table_name = "test_table";
  /*
   *std::string table_schema_string =
   *    "ID uint32, Name String, Attributes List[ String ], Feature "
   *    "Fixed_Size_List[ 2 ,   Float32 ]";
   */
  uint32_t segment_id = 0;
  auto input_schema = std::make_shared<arrow::Schema>(
      arrow::FieldVector{
          std::make_shared<arrow::Field>("id", arrow::uint32()),
          std::make_shared<arrow::Field>("name", arrow::utf8()),
          std::make_shared<arrow::Field>("attribute",
                                         arrow::list(arrow::utf8())),
          std::make_shared<arrow::Field>(
              "feature", arrow::fixed_size_list(arrow::float32(), 2))},
      std::make_shared<arrow::KeyValueMetadata>(
          std::unordered_map<std::string, std::string>{
              {"segment_id_info", std::string((char *)&segment_id, 4)},
              {"table name", test_table_name},
              {"active_set_size_limit", "10000"},
              {"ann_column_id", "3"},
              {"ef_construction", "100"},
              {"M", "2"},
              {"index_space", "L2Space"},
              {"index_type", "Hnsw"}}));
  auto maybe_serialized_schema =
      arrow::ipc::SerializeSchema(*input_schema, &vdb::arrow_pool);
  if (!maybe_serialized_schema.ok()) {
    std::cerr << maybe_serialized_schema.status().ToString() << std::endl;
    ASSERT_TRUE(maybe_serialized_schema.ok());
  }
  auto serialized_schema = maybe_serialized_schema.ValueUnsafe();
  sds input_sds =
      sdsnewlen(reinterpret_cast<const void *>(serialized_schema->data()),
                static_cast<size_t>(serialized_schema->size()));
  auto status = vdb::_CreateTableCommand(input_sds);
  sdsfree(input_sds);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->at(test_table_name);
  ASSERT_TRUE(table->GetSchema()->Equals(input_schema));
#ifdef _DEBUG_GTEST
  std::cout << table->ToString(true) << std::endl;
#endif
}

TEST_F(CommandTest, CreateTableCommandAndSetAnnColumn) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string test_table_name = "test_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ], Feature "
      "Fixed_Size_List[ 1024 ,   Float32 ]";

  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->at(test_table_name);

#ifdef _DEBUG_GTEST
  std::cout << "Table Before Setting Ann Column" << std::endl;
  std::cout << table->GetSchema()->metadata()->ToString() << std::endl;
#endif

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "3"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
#ifdef _DEBUG_GTEST
  std::cout << "Adding Metadata(Ann Column, etc.) Into Table" << std::endl;
  std::cout << add_metadata->ToString() << std::endl;
#endif
  TableWrapper::AddMetadata(table, add_metadata);

#ifdef _DEBUG_GTEST
  std::cout << "Table After Setting Ann Column" << std::endl;
  std::cout << table->GetSchema()->metadata()->ToString() << std::endl;
#endif
}

TEST_F(CommandTest, ShowTableCommand) {
  std::string test_table_name = "test_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ], Feature "
      "Fixed_Size_List[ 1024 ,   Float32 ]";

  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  std::string test_table_name2 = "test_table2";
  std::string test_schema_string2 =
      "ID int16, Name String, Attributes List[ String ]";

  status = CreateTableForTest(test_table_name2, test_schema_string2);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table_dictionary = vdb::GetTableDictionary();

  ASSERT_EQ(table_dictionary->size(), 2);

  auto table_list = vdb::_ListTableCommand();

#ifdef _DEBUG_GTEST
  std::cout << "---- Table Lists ----" << std::endl;
  for (auto &table_name : table_list) {
    std::cout << table_name << std::endl;
  }
#endif
}

TEST_F(CommandTest, InsertCommand) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string test_table_name = "test_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ]";

  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->begin()->second;
#ifdef _DEBUG_GTEST
  std::cout << table->ToString() << std::endl;
#endif
  const char *data_sds = "0\u001eJohn\u001eC\u001dPython\u001dJava";
  status = vdb::_InsertCommand(test_table_name, std::string_view(data_sds));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
#ifdef _DEBUG_GTEST
  std::cout << table->ToString() << std::endl;
#endif

  const char *data_sds2 = "1\u001eJane\u001eLisp\u001dPython";
  status = vdb::_InsertCommand(test_table_name, std::string_view(data_sds2));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto segments = table->GetSegments();
  EXPECT_EQ(segments.size(), 2);

  size_t cnt = 0;
  for (auto &kv : segments) {
    cnt += kv.second->Size();
  }
  EXPECT_EQ(cnt, 2);

#ifdef _DEBUG_GTEST
  std::cout << table->ToString() << std::endl;
#endif
}

TEST_F(CommandTest, InsertCommandWithAddPoint) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string test_table_name = "test_table";
  std::string test_schema_string =
      "ID uint32, Name String, Feature "
      "Fixed_Size_List[ 3 ,   Float32 ]";

  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->at(test_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "2"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
#ifdef _DEBUG_GTEST
  std::cout << "Added Metadata" << std::endl;
  std::cout << add_metadata->ToString() << std::endl;
#endif
  TableWrapper::AddMetadata(table, add_metadata);

  const char *data_sds = "0\u001eJohn\u001e12.3\u001d3.4\u001d5.2";
  status = vdb::_InsertCommand(test_table_name, std::string_view(data_sds));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  const char *data_sds2 = "1\u001eJane\u001e11.2\u001d3.0\u001d4.0";
  status = vdb::_InsertCommand(test_table_name, std::string_view(data_sds2));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto segments = table->GetSegments();
  EXPECT_EQ(segments.size(), 2);

  size_t cnt = 0;
  for (auto &kv : segments) {
    cnt += kv.second->Size();
  }
  EXPECT_EQ(cnt, 2);

#ifdef _DEBUG_GTEST
  std::cout << table->ToString() << std::endl;
#endif
}

TEST_F(CommandTest, TableBatchInsertCommand) {
  auto table_dictionary = vdb::GetTableDictionary();

  /* source table */
  std::string source_table_name = "test_table";
  std::string schema_string =
      "ID uint32, Name String, Feature "
      "Fixed_Size_List[ 3 ,   Float32 ]";

  auto status = CreateTableForTest(source_table_name, schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto source_table = table_dictionary->at(source_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "2"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(source_table, add_metadata);

  const char *data_sds = "0\u001eJohn\u001e12.3\u001d3.4\u001d5.2";
  status = vdb::_InsertCommand(source_table_name, std::string_view(data_sds));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  const char *data_sds2 = "1\u001eJane\u001e11.2\u001d3.0\u001d4.0";
  status = vdb::_InsertCommand(source_table_name, std::string_view(data_sds2));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto source_segments = source_table->GetSegments();
  EXPECT_EQ(source_segments.size(), 2);

  std::vector<sds> serialized_rbs;
  for (auto [key, segment] : source_segments) {
    auto rb = segment->ActiveSetRecordBatch();
#ifdef _DEBUG_GTEST
    std::cout << rb->ToString() << std::endl;
#endif
    auto options = arrow::ipc::IpcWriteOptions::Defaults();
    options.memory_pool = &vdb::arrow_pool;
    std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
    ASSERT_OK_AND_ASSIGN(
        auto serialized_rb,
        SerializeRecordBatches(source_table->GetSchema(), rbs));

    sds rb_sds =
        sdsnewlen(reinterpret_cast<const void *>(serialized_rb->data()),
                  static_cast<size_t>(serialized_rb->size()));
    serialized_rbs.push_back(rb_sds);
  }

  /* target table */
  std::string target_table_name = "target_table";
  status = CreateTableForTest(target_table_name, schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto target_table = table_dictionary->at(target_table_name);
  TableWrapper::AddMetadata(target_table, add_metadata);

  for (auto &rb_sds : serialized_rbs) {
    status = vdb::_BatchInsertCommand(target_table_name, rb_sds);
    ASSERT_TRUE(status.ok()) << status.ToString();
    sdsfree(rb_sds);
  }

  auto target_segments = target_table->GetSegments();

  EXPECT_EQ(target_segments.size(), 2);

  size_t cnt = 0;
  for (auto &kv : target_segments) {
    cnt += kv.second->Size();
  }
  EXPECT_EQ(cnt, 2);

#ifdef _DEBUG_GTEST
  std::cout << target_table->ToString() << std::endl;
#endif
}

TEST_F(CommandTest, TableBatchInsertCommandWithInvalidSchema) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string source_table_name = "test_table";
  std::string schema_string =
      "ID uint64, Name String, Feature "
      "Fixed_Size_List[ 3 ,   Float32 ]";

  auto status = CreateTableForTest(source_table_name, schema_string);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto source_table = table_dictionary->at(source_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "2"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(source_table, add_metadata);

  // Create a record batch with a different schema than the table
  arrow::SchemaBuilder schema_builder;
  ASSERT_OK(schema_builder.AddField(
      arrow::field("ID", arrow::utf8())));  // Changed from uint64 to string
  ASSERT_OK(schema_builder.AddField(arrow::field("Name", arrow::utf8())));
  ASSERT_OK(schema_builder.AddField(
      arrow::field("Feature", arrow::fixed_size_list(arrow::float32(), 3))));
  std::shared_ptr<arrow::Schema> different_schema =
      schema_builder.Finish().ValueOrDie();

  // Create arrays for the record batch
  auto id_arr = std::shared_ptr<arrow::Array>();
  auto name_arr = std::shared_ptr<arrow::Array>();
  auto feature_arr = std::shared_ptr<arrow::Array>();

  arrow::StringBuilder id_builder;
  arrow::StringBuilder name_builder;
  auto value_builder =
      std::make_shared<arrow::FloatBuilder>(arrow::default_memory_pool());
  arrow::FixedSizeListBuilder feature_builder(arrow::default_memory_pool(),
                                              value_builder, 3);

  ARROW_EXPECT_OK(id_builder.Append("0"));
  ARROW_EXPECT_OK(name_builder.Append("John"));
  ARROW_EXPECT_OK(feature_builder.Append());
  ARROW_EXPECT_OK(value_builder->Append(12.3));
  ARROW_EXPECT_OK(value_builder->Append(3.4));
  ARROW_EXPECT_OK(value_builder->Append(5.2));

  ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
  ARROW_EXPECT_OK(name_builder.Finish(&name_arr));
  ARROW_EXPECT_OK(feature_builder.Finish(&feature_arr));

  auto rb = arrow::RecordBatch::Make(different_schema, 1,
                                     {id_arr, name_arr, feature_arr});
  std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
  ASSERT_OK_AND_ASSIGN(auto serialized_rb,
                       SerializeRecordBatches(different_schema, rbs));

  sds serialized_rbs =
      sdsnewlen(reinterpret_cast<const void *>(serialized_rb->data()),
                static_cast<size_t>(serialized_rb->size()));

  // Attempt to insert the record batch with different schema
  status = vdb::_BatchInsertCommand(source_table_name, serialized_rbs);

  // Check that the insertion failed due to schema mismatch
  ASSERT_FALSE(status.ok());
  EXPECT_TRUE(
      status.ToString().find("Column type is different from table schema") !=
      std::string::npos);

  // Clean up
  sdsfree(serialized_rbs);
}

TEST_F(CommandTest, TableBatchInsertCommandWithNullValuesForNonNullableField) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string source_table_name = "test_table";
  std::string schema_string =
      "ID uint64 not null, Name String, Feature "
      "Fixed_Size_List[ 3 ,   Float32 ]";

  auto status = CreateTableForTest(source_table_name, schema_string);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto source_table = table_dictionary->at(source_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "2"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(source_table, add_metadata);

  // Create a record batch with a different schema than the table
  arrow::SchemaBuilder schema_builder;
  ASSERT_OK(schema_builder.AddField(
      arrow::field("ID", arrow::uint64())));  // Changed from uint64 to string
  ASSERT_OK(schema_builder.AddField(arrow::field("Name", arrow::utf8())));
  ASSERT_OK(schema_builder.AddField(
      arrow::field("Feature", arrow::fixed_size_list(arrow::float32(), 3))));
  std::shared_ptr<arrow::Schema> different_schema =
      schema_builder.Finish().ValueOrDie();

  // Create arrays for the record batch
  auto id_arr = std::shared_ptr<arrow::Array>();
  auto name_arr = std::shared_ptr<arrow::Array>();
  auto feature_arr = std::shared_ptr<arrow::Array>();

  arrow::UInt64Builder id_builder;
  arrow::StringBuilder name_builder;
  auto value_builder =
      std::make_shared<arrow::FloatBuilder>(arrow::default_memory_pool());
  arrow::FixedSizeListBuilder feature_builder(arrow::default_memory_pool(),
                                              value_builder, 3);

  ARROW_EXPECT_OK(id_builder.AppendNull());
  ARROW_EXPECT_OK(name_builder.Append("John"));
  ARROW_EXPECT_OK(feature_builder.Append());
  ARROW_EXPECT_OK(value_builder->Append(12.3));
  ARROW_EXPECT_OK(value_builder->Append(3.4));
  ARROW_EXPECT_OK(value_builder->Append(5.2));

  ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
  ARROW_EXPECT_OK(name_builder.Finish(&name_arr));
  ARROW_EXPECT_OK(feature_builder.Finish(&feature_arr));

  auto rb = arrow::RecordBatch::Make(different_schema, 1,
                                     {id_arr, name_arr, feature_arr});
  std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
  ASSERT_OK_AND_ASSIGN(auto serialized_rb,
                       SerializeRecordBatches(different_schema, rbs));

  sds serialized_rbs =
      sdsnewlen(reinterpret_cast<const void *>(serialized_rb->data()),
                static_cast<size_t>(serialized_rb->size()));

  // Attempt to insert the record batch with different schema
  status = vdb::_BatchInsertCommand(source_table_name, serialized_rbs);

  // Check that the insertion failed due to schema mismatch
  ASSERT_FALSE(status.ok());
  EXPECT_TRUE(
      status.ToString().find("Could not insert recordbatch into segment. "
                             "Field 'id' is not nullable, but the recordbatch "
                             "contains null values") != std::string::npos);

  // Clean up
  sdsfree(serialized_rbs);
}

TEST_F(CommandTest, TableDescribeCommand) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string test_table_name = "test_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ]";

  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto maybe_serialized_arrow_schema =
      vdb::_DescribeTableCommand(test_table_name);
  if (!maybe_serialized_arrow_schema.ok()) {
    std::cerr << maybe_serialized_arrow_schema.status().ToString() << std::endl;
    ASSERT_TRUE(maybe_serialized_arrow_schema.ok());
  }
  auto serialized_arrow_schema = maybe_serialized_arrow_schema.ValueUnsafe();
  arrow::ipc::DictionaryMemo in_memo;
  arrow::io::BufferReader buf_reader(serialized_arrow_schema);
  auto maybe_deserialized_arrow_schema = ReadSchema(&buf_reader, &in_memo);
  if (!maybe_deserialized_arrow_schema.ok()) {
    std::cerr << maybe_deserialized_arrow_schema.status().ToString()
              << std::endl;
    ASSERT_TRUE(maybe_deserialized_arrow_schema.ok());
  }
  auto deserialized_arrow_schema =
      maybe_deserialized_arrow_schema.ValueUnsafe();

  EXPECT_EQ(table_dictionary->at(test_table_name)->GetSchema()->ToString(),
            deserialized_arrow_schema->ToString());
#ifdef _DEBUG_GTEST
  std::cout << deserialized_arrow_schema->ToString() << std::endl;
#endif
}

TEST_F(CommandTest, TableDebugScanCommand) {
  std::string test_table_name = "test_table";
  std::string test_schema_string =
      "id uint32, Name String, Attributes List[ String ]";

  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  const char *data_sds = "0\u001eJohn\u001eC\u001dPython\u001dJava";
  status = vdb::_InsertCommand(test_table_name, std::string_view(data_sds));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  const char *data_sds2 = "1\u001eJane\u001eLisp\u001dPython";
  status = vdb::_InsertCommand(test_table_name, std::string_view(data_sds2));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  std::string filter_string = "id\u001eisnotnull";
  std::string projection = "*";

  auto maybe_result =
      vdb::_DebugScanCommand(test_table_name, projection, filter_string);
  if (!maybe_result.ok()) {
    std::cerr << maybe_result.status().ToString() << std::endl;
    ASSERT_TRUE(maybe_result.ok());
  }
  auto result = maybe_result.ValueUnsafe();
#ifdef _DEBUG_GTEST
  std::cout << result << std::endl;
#endif

  filter_string = "id\u001e1\u001e<";

  maybe_result =
      vdb::_DebugScanCommand(test_table_name, projection, filter_string);
  if (!maybe_result.ok()) {
    std::cerr << maybe_result.status().ToString() << std::endl;
    ASSERT_TRUE(maybe_result.ok());
  }
  result = maybe_result.ValueUnsafe();
#ifdef _DEBUG_GTEST
  std::cout << result << std::endl;
#endif
}

TEST_F(CommandTest, ScanCommand) {
  std::string table_name = "test_table";
  auto status = CreateTableForTest(
      table_name, "id uint32, Name String, Attributes List[ String ]");
  ASSERT_TRUE(status.ok()) << status.ToString();

  status = vdb::_InsertCommand(table_name,
                               "0\u001eJohn\u001eC\u001dPython\u001dJava");
  ASSERT_TRUE(status.ok()) << status.ToString();
  status = vdb::_InsertCommand(table_name, "1\u001eJane\u001eLisp\u001dPython");
  ASSERT_TRUE(status.ok()) << status.ToString();

  {
    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_ScanCommand(table_name, "*", "id\u001eisnotnull"));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));

    arrow::UInt32Builder id_builder;
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_OK(id_builder.AppendValues({0, 1}));
    ASSERT_OK(id_builder.Finish(&id_array));

    arrow::StringBuilder name_builder;
    std::shared_ptr<arrow::Array> name_array;
    ASSERT_OK(name_builder.AppendValues({"John", "Jane"}));
    ASSERT_OK(name_builder.Finish(&name_array));

    auto str_builder = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder attributes_builder(arrow::default_memory_pool(),
                                          str_builder);
    std::shared_ptr<arrow::Array> attributes_array;
    ASSERT_OK(attributes_builder.Append());
    ASSERT_OK(str_builder->AppendValues({"C", "Python", "Java"}));
    ASSERT_OK(attributes_builder.Append());
    ASSERT_OK(str_builder->AppendValues({"Lisp", "Python"}));
    ASSERT_OK(attributes_builder.Finish(&attributes_array));

    auto expected_table = arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::uint32()),
                       arrow::field("name", arrow::utf8()),
                       arrow::field("attributes", arrow::list(arrow::utf8()))}),
        {id_array, name_array, attributes_array});

    ASSERT_OK_AND_ASSIGN(auto sorted_result, SortTable(table));
    ASSERT_OK_AND_ASSIGN(auto sorted_expected, SortTable(expected_table));
    EXPECT_TRUE(sorted_result->Equals(*sorted_expected));
  }

  {
    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_ScanCommand(table_name, "*", "id\u001e1\u001e<"));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));

    arrow::UInt32Builder id_builder;
    std::shared_ptr<arrow::Array> id_array;
    ASSERT_OK(id_builder.AppendValues({0}));
    ASSERT_OK(id_builder.Finish(&id_array));

    arrow::StringBuilder name_builder;
    std::shared_ptr<arrow::Array> name_array;
    ASSERT_OK(name_builder.AppendValues({"John"}));
    ASSERT_OK(name_builder.Finish(&name_array));

    auto str_builder = std::make_shared<arrow::StringBuilder>();
    arrow::ListBuilder attributes_builder(arrow::default_memory_pool(),
                                          str_builder);
    std::shared_ptr<arrow::Array> attributes_array;
    ASSERT_OK(attributes_builder.Append());
    ASSERT_OK(str_builder->AppendValues({"C", "Python", "Java"}));
    ASSERT_OK(attributes_builder.Finish(&attributes_array));

    auto expected_table = arrow::Table::Make(
        arrow::schema({arrow::field("id", arrow::uint32()),
                       arrow::field("name", arrow::utf8()),
                       arrow::field("attributes", arrow::list(arrow::utf8()))}),
        {id_array, name_array, attributes_array});

    EXPECT_TRUE(table->Equals(*expected_table));
  }
}

TEST_F(CommandTest, AnnCommand) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string table_name = "test_table";
  std::string schema_string =
      "ID uint32, Name String, Feature "
      "Fixed_Size_List[ 3 ,   Float32 ]";

  auto status = CreateTableForTest(table_name, schema_string);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto table = table_dictionary->at(table_name);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "2"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
#ifdef _DEBUG_GTEST
  std::cout << "Added Metadata" << std::endl;
  std::cout << add_metadata->ToString() << std::endl;
#endif
  TableWrapper::AddMetadata(table, add_metadata);

  auto schema = table->GetSchema();
  status = vdb::_InsertCommand(table_name,
                               "0\u001eJohn\u001e12.3\u001d3.4\u001d5.2");
  ASSERT_TRUE(status.ok()) << status.ToString();
  status = vdb::_InsertCommand(table_name,
                               "1\u001eJane\u001e11.2\u001d3.0\u001d4.0");
  ASSERT_TRUE(status.ok()) << status.ToString();

  {
    std::vector<float> query_vector{12.0, 3.0, 5.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 2;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_AnnCommand(table_name, k, query, ef_search, "*", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 2) << "print all points\n"
                                    << table->ToString();
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{12.0, 3.0, 5.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 2;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(auto serialized_table,
                         vdb::_AnnCommand(table_name, k, query, ef_search,
                                          "name, distance", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 2) << "print all points\n"
                                    << table->ToString();
    ASSERT_EQ(table->num_columns(), 2);
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{12.0, 3.0, 5.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 2;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_AnnCommand(table_name, k, query, ef_search, "name", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 2) << "print all points\n"
                                    << table->ToString();
    ASSERT_EQ(table->num_columns(), 1);
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{12.0, 3.0, 5.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 1;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_AnnCommand(table_name, k, query, ef_search, "*", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 1) << "print 1 point near (12, 3, 5)\n"
                                    << table->ToString();
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{11.0, 3.0, 4.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 1;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_AnnCommand(table_name, k, query, ef_search, "*", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 1) << "print 1 point near (11, 3, 4)\n"
                                    << table->ToString();
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{11.0, 3.0, 4.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 2;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(auto serialized_table,
                         vdb::_AnnCommand(table_name, k, query, ef_search, "*",
                                          "id\u001e1\u001e<"));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 1) << "print points where id < 1\n"
                                    << table->ToString();
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{11.0, 3.0, 4.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 2;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(auto serialized_table,
                         vdb::_AnnCommand(table_name, k, query, ef_search, "*",
                                          "id\u001e1\u001e="));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 1) << "print points where id = 1\n"
                                    << table->ToString();
    ASSERT_OK_AND_ASSIGN(auto id, table->column(0)->GetScalar(0));
    ASSERT_EQ(id->ToString(), "1");
    sdsfree(query);
  }
}

TEST_F(CommandTest, AnnCommandAsync) {
  server.allow_bg_index_thread = true;
  server.vdb_active_set_size_limit = 2;
  auto table_dictionary = vdb::GetTableDictionary();

  std::string table_name = "test_table";
  std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
      arrow::field("id", arrow::uint32()), arrow::field("name", arrow::utf8()),
      arrow::field("feature", arrow::fixed_size_list(arrow::float32(), 3))};

  uint32_t segment_info_idx = 0;
  std::string segment_id_info(reinterpret_cast<char *>(&segment_info_idx),
                              sizeof(uint32_t));
  auto metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"segment_id_info", segment_id_info},
          {"table name", table_name},
          {"active_set_size_limit",
           std::to_string(server.vdb_active_set_size_limit)},
          {"ann_column_id", "2"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  auto schema = std::make_shared<arrow::Schema>(schema_vector, metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName(table_name).SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  table_dictionary->insert({table_name, table});
  table = table_dictionary->at(table_name);

  auto id_arr = std::shared_ptr<arrow::Array>();
  auto name_arr = std::shared_ptr<arrow::Array>();
  auto feature_arr = std::shared_ptr<arrow::Array>();

  arrow::UInt32Builder id_builder;
  arrow::StringBuilder name_builder;
  auto value_builder = std::make_shared<arrow::FloatBuilder>();
  arrow::FixedSizeListBuilder feature_builder(arrow::default_memory_pool(),
                                              value_builder, 3);

  ARROW_EXPECT_OK(id_builder.Append(0));
  ARROW_EXPECT_OK(id_builder.Append(0));
  ARROW_EXPECT_OK(id_builder.Append(0));

  ARROW_EXPECT_OK(name_builder.Append("John"));
  ARROW_EXPECT_OK(name_builder.Append("Jane"));
  ARROW_EXPECT_OK(name_builder.Append("Jack"));

  ARROW_EXPECT_OK(feature_builder.Append());
  ARROW_EXPECT_OK(value_builder->Append(12.3));
  ARROW_EXPECT_OK(value_builder->Append(3.4));
  ARROW_EXPECT_OK(value_builder->Append(5.2));

  ARROW_EXPECT_OK(feature_builder.Append());
  ARROW_EXPECT_OK(value_builder->Append(11.2));
  ARROW_EXPECT_OK(value_builder->Append(3.0));
  ARROW_EXPECT_OK(value_builder->Append(4.0));

  ARROW_EXPECT_OK(feature_builder.Append());
  ARROW_EXPECT_OK(value_builder->Append(2.8));
  ARROW_EXPECT_OK(value_builder->Append(1.0));
  ARROW_EXPECT_OK(value_builder->Append(0.5));

  ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
  ARROW_EXPECT_OK(name_builder.Finish(&name_arr));
  ARROW_EXPECT_OK(feature_builder.Finish(&feature_arr));

  auto rb =
      arrow::RecordBatch::Make(schema, 3, {id_arr, name_arr, feature_arr});
  std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
  ASSERT_OK_AND_ASSIGN(auto serialized_rb, SerializeRecordBatches(schema, rbs));

  sds serialized_rb_sds =
      sdsnewlen(reinterpret_cast<const void *>(serialized_rb->data()),
                static_cast<size_t>(serialized_rb->size()));

  auto status = vdb::_BatchInsertCommand(table_name, serialized_rb_sds);

  sdsfree(serialized_rb_sds);

  while (table->IsIndexing()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  {
    std::vector<float> query_vector{12.0, 3.0, 5.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 2;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_AnnCommand(table_name, k, query, ef_search, "*", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 2) << "print all points\n"
                                    << table->ToString();
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{12.0, 3.0, 5.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 1;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_AnnCommand(table_name, k, query, ef_search, "*", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 1) << "print 1 point near (12, 3, 5)\n"
                                    << table->ToString();
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{11.0, 3.0, 4.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 1;
    size_t ef_search = 2;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_table,
        vdb::_AnnCommand(table_name, k, query, ef_search, "*", ""));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 1) << "print 1 point near (11, 3, 4)\n"
                                    << table->ToString();
    sdsfree(query);
  }

  {
    std::vector<float> query_vector{11.0, 3.0, 4.0};
    sds query =
        sdsnewlen(query_vector.data(), query_vector.size() * sizeof(float));

    size_t k = 3;
    size_t ef_search = 3;

    ASSERT_OK_AND_ASSIGN(auto serialized_table,
                         vdb::_AnnCommand(table_name, k, query, ef_search, "*",
                                          "id\u001e0\u001e="));
    ASSERT_OK_AND_ASSIGN(auto table, DeserializeToTableFrom(serialized_table));
    ASSERT_EQ(table->num_rows(), 3) << "print points where id < 1\n"
                                    << table->ToString();
    sdsfree(query);
  }

  table->StopIndexingThread();
  server.allow_bg_index_thread = false;
}

TEST_F(CommandTest, BatchAnnCommand) {
  auto table_dictionary = vdb::GetTableDictionary();

  std::string table_name = "test_table";
  std::string schema_string =
      "ID uint32, Name String, Feature "
      "Fixed_Size_List[ 3 ,   Float32 ]";

  auto status = CreateTableForTest(table_name, schema_string);
  ASSERT_TRUE(status.ok()) << status.ToString();

  auto table = table_dictionary->at(table_name);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "2"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(table, add_metadata);

  ASSERT_OK_AND_ASSIGN(auto ids,
                       arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::int32(), "[0, 1, 2, 3, 4, 5, 6, 7]"));
  ASSERT_OK_AND_ASSIGN(auto names,
                       arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::utf8(),
                           "[\"John\", \"Jane\", \"Mike\", \"Sarah\", "
                           "\"David\", \"Emily\", \"Tom\", \"Julia\"]"));
  ASSERT_OK_AND_ASSIGN(auto features,
                       arrow::ipc::internal::json::ArrayFromJSON(
                           arrow::fixed_size_list(arrow::float32(), 3),
                           "[[12.3, 3.4, 5.2], [11.2, 3.0, 4.0], [10.5, 2.8, "
                           "3.7], [9.8, 2.6, 3.4], [9.1, 2.4, 3.1], [8.4, 2.2, "
                           "2.8], [7.7, 2.0, 2.5], [7.0, 1.8, 2.2]]"));

  auto schema = table->GetSchema();
  auto batch = arrow::RecordBatch::Make(schema, 8, {ids, names, features});

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches = {batch};
  ASSERT_OK_AND_ASSIGN(auto serialized_batch,
                       SerializeRecordBatches(schema, batches));
  sds serialized_batch_sds =
      sdsnewlen(serialized_batch->data(), serialized_batch->size());
  status = vdb::_BatchInsertCommand(table_name, serialized_batch_sds);
  ASSERT_TRUE(status.ok()) << status.ToString();
  sdsfree(serialized_batch_sds);

  {
    std::shared_ptr<arrow::FloatBuilder> value_builder =
        std::make_shared<arrow::FloatBuilder>();
    arrow::FixedSizeListBuilder query_builder(arrow::default_memory_pool(),
                                              value_builder, 3);

    std::shared_ptr<arrow::Array> query_array;

    ASSERT_OK(query_builder.Append());
    ASSERT_OK(value_builder->Append(12.0));
    ASSERT_OK(value_builder->Append(3.0));
    ASSERT_OK(value_builder->Append(5.0));
    ASSERT_OK(query_builder.Append());
    ASSERT_OK(value_builder->Append(6.0));
    ASSERT_OK(value_builder->Append(2.0));
    ASSERT_OK(value_builder->Append(1.0));
    ASSERT_OK(query_builder.Append());
    ASSERT_OK(value_builder->Append(8.5));
    ASSERT_OK(value_builder->Append(2.5));
    ASSERT_OK(value_builder->Append(3.5));
    ASSERT_OK(query_builder.Append());
    ASSERT_OK(value_builder->Append(10.0));
    ASSERT_OK(value_builder->Append(3.2));
    ASSERT_OK(value_builder->Append(4.5));
    ASSERT_OK(query_builder.Finish(&query_array));

    auto schema = arrow::schema({arrow::field("query", query_array->type())});
    auto query_batch = arrow::RecordBatch::Make(schema, 4, {query_array});

    std::vector<std::shared_ptr<arrow::RecordBatch>> queries = {query_batch};
    ASSERT_OK_AND_ASSIGN(auto serialized_batch,
                         SerializeRecordBatches(schema, queries));
    sds query_sds =
        sdsnewlen(serialized_batch->data(), serialized_batch->size());

    size_t k = 3;
    size_t ef_search = 6;

    ASSERT_OK_AND_ASSIGN(
        auto serialized_tables,
        vdb::_BatchAnnCommand(table_name, k, query_sds, ef_search, "*", ""));
    sdsfree(query_sds);

    ASSERT_EQ(serialized_tables.size(), 4);
    for (auto &serialized_table : serialized_tables) {
      ASSERT_OK_AND_ASSIGN(auto table,
                           DeserializeToTableFrom(serialized_table));
      ASSERT_EQ(table->num_rows(), 3) << "print all points\n"
                                      << table->ToString();
    }
  }
}

TEST_F(CommandTest, CountIndexedElementsCommandTest) {
  server.allow_bg_index_thread = true;
  const size_t kDataSize = 1000;
  const size_t kDim = 1024;
  const std::string table_name{"test_table"};

  auto table_dictionary = vdb::GetTableDictionary();

  // Create vdb::Table with Index
  uint32_t idx = 0;
  uint32_t *idx_ptr = &idx;
  std::string segment_id_info(reinterpret_cast<char *>(idx_ptr),
                              sizeof(uint32_t));
  auto schema = arrow::schema(
      {{"id", arrow::int32()},
       {"ferature", arrow::fixed_size_list(arrow::float32(), kDim)}},
      std::make_shared<arrow::KeyValueMetadata>(
          std::unordered_map<std::string, std::string>{
              {"segment_id_info", segment_id_info},
              {"table name", "test_table"},
              {"active_set_size_limit", "100"},
              {"ann_column_id", "1"},
              {"ef_construction", "1000"},
              {"M", "32"},
              {"max_threads", "2"},
              {"index_space", "L2Space"},
              {"index_type", "Hnsw"}}));
  vdb::TableBuilderOptions tb_options;
  tb_options.SetTableName(table_name).SetSchema(schema);
  vdb::TableBuilder tb{std::move(tb_options)};
  auto maybe_table = tb.Build();
  ASSERT_TRUE(maybe_table.ok());
  auto table = maybe_table.ValueUnsafe();
  table_dictionary->insert({table_name, table});

  // Create arrow::RecordBatch with random data
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dist(0.0, 1.0);
  arrow::Int32Builder id_builder;
  auto value_builder = std::make_shared<arrow::FloatBuilder>();
  arrow::FixedSizeListBuilder feature_builder{arrow::default_memory_pool(),
                                              value_builder, kDim};
  for (size_t i = 0; i < kDataSize; i++) {
    ASSERT_TRUE(id_builder.Append(static_cast<int32_t>(i)).ok());
    ASSERT_TRUE(feature_builder.Append().ok());
    for (size_t d = 0; d < kDim; d++) {
      ASSERT_TRUE(value_builder->Append(dist(gen)).ok());
    }
  }
  std::shared_ptr<arrow::Int32Array> id_arr;
  std::shared_ptr<arrow::FixedSizeListArray> feature_arr;
  ASSERT_TRUE(id_builder.Finish(&id_arr).ok());
  ASSERT_TRUE(feature_builder.Finish(&feature_arr).ok());

  auto rb = arrow::RecordBatch::Make(schema, kDataSize, {id_arr, feature_arr});

  // Insert the record batch in to the table.
  std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
  ASSERT_OK_AND_ASSIGN(auto serialized_rb, SerializeRecordBatches(schema, rbs));
  sds serialized_rb_sds =
      sdsnewlen(reinterpret_cast<const void *>(serialized_rb->data()),
                static_cast<size_t>(serialized_rb->size()));

  ASSERT_TRUE(_BatchInsertCommand(table_name, serialized_rb_sds).ok());
  sdsfree(serialized_rb_sds);

  // Test CountIndexedElementsCommand.
  ASSERT_OK_AND_ASSIGN(auto count_pair,
                       vdb::_CountIndexedElementsCommand(table_name));
  EXPECT_EQ(count_pair.first, kDataSize);
  EXPECT_NE(count_pair.second,
            kDataSize);  // bg threading should be running here.
  while (table->IsIndexing()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_OK_AND_ASSIGN(count_pair,
                       vdb::_CountIndexedElementsCommand(table_name));
  EXPECT_EQ(count_pair.first, kDataSize);
  EXPECT_EQ(count_pair.second, kDataSize);
  server.allow_bg_index_thread = false;
}

}  // namespace vdb

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new vdb::GlobalEnvironment);
  return RUN_ALL_TESTS();
}
