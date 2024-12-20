#include <algorithm>

#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include "vdb/common/defs.hh"
#include "vdb/common/util.hh"
#include "vdb/common/status.hh"
#include "vdb/data/expression.hh"
#include "vdb/tests/util_for_test.hh"

#ifdef __cplusplus
extern "C" {
#include "server.h"
}
#endif

std::string util_test_directory_path = "__vdb_util_test_dir__";

class GlobalEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    std::filesystem::remove_all(util_test_directory_path);
    std::filesystem::create_directory(util_test_directory_path);
#ifdef _DEBUG_GTEST
    std::cout << snapshot_test_directory_path
              << " directory is dropped and created. " << std::endl;
#endif
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

class UtilTestSuite : public ::testing::Test {
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
class UtilityTest : public UtilTestSuite {};
class ArrowRecordbatchTest : public UtilTestSuite {
 public:
  std::shared_ptr<arrow::Schema> schema;

  static void SetUpTestCase() {
    std::string test_case_name =
        testing::UnitTest::GetInstance()->current_test_case()->name();
    std::string case_directory_path =
        util_test_directory_path + "/" + test_case_name;
    std::filesystem::remove_all(case_directory_path);
    std::filesystem::create_directory(case_directory_path);
#ifdef _DEBUG_GTEST
    std::cout << case_directory_path << " directory is dropped and created. "
              << std::endl;
#endif
  }
  static void TearDownTestCase() {}
  void SetUp() override {
    const testing::TestInfo *test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    std::string test_case_name = test_info->test_case_name();
    std::string test_name = test_info->name();
    test_directory_path_ =
        util_test_directory_path + "/" + test_case_name + "/" + test_name;
    util_directory_name_ = "snapshot";
    server.aof_filename = util_directory_name_.data();
    server.aof_dirname = test_directory_path_.data();
    std::filesystem::remove_all(test_directory_path_);
    std::filesystem::create_directory(test_directory_path_);
#ifdef _DEBUG_GTEST
    std::cout << test_directory_path_ << " directory is dropped and created. "
              << std::endl
              << std::endl;
    std::cout << "Current Test Directory Path: " << test_directory_path_
              << std::endl;
#endif
    schema = arrow::schema({arrow::field("large_column", arrow::int64())});
  }

  void TearDown() override {}
  std::string &TestDirectoryPath() { return test_directory_path_; }

 private:
  std::string test_directory_path_;
  std::string util_directory_name_;
};

TEST_F(UtilityTest, StringTokenizerTest) {
  std::string test_string =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod "
      "tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim "
      "veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea "
      "commodo consequat. Duis aute irure dolor in reprehenderit in voluptate "
      "velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint "
      "occaecat cupidatat non proident, sunt in culpa qui officia deserunt "
      "mollit anim id est laborum.";

  auto token = vdb::GetTokenFrom(test_string, ' ', 0);
  EXPECT_EQ(token, "Lorem");

  token = vdb::GetTokenFrom(test_string, ' ', 4);
  EXPECT_EQ(token, "amet,");

  token = vdb::GetTokenFrom(test_string, ' ', -1);
  EXPECT_EQ(token, "");

  token = vdb::GetTokenFrom(test_string, ' ', 128);
  EXPECT_EQ(token, "");

  auto tokens = vdb::Tokenize(test_string);

  EXPECT_EQ(tokens.size(), 69);

  tokens = vdb::Tokenize(test_string, '.');

  EXPECT_EQ(tokens.size(), 4);

  std::string_view test_view{test_string};

  tokens = vdb::Tokenize(test_view);

  EXPECT_EQ(tokens.size(), 69);

  tokens = vdb::Tokenize(test_view, '.');

  EXPECT_EQ(tokens.size(), 4);

  // Note: test_string.clear() doens't change the content, but the size.
  // test_view doesn't changed.
  std::transform(test_string.begin(), test_string.end(), test_string.begin(),
                 [](char c) {
                   if (c == ' ')
                     return '.';
                   else
                     return c;
                 });

  tokens = vdb::Tokenize(test_view);

  EXPECT_EQ(tokens.size(), 1);

  tokens = vdb::Tokenize(test_view, '.');

  EXPECT_EQ(tokens.size(), 69);
}

TEST_F(UtilityTest, JoinTest) {
  std::vector<std::string> tokens = {"Hello", "World", "Nice", "Weather"};

  auto str = vdb::Join(tokens, ' ');

  EXPECT_EQ(str, "Hello World Nice Weather");

  str = vdb::Join(tokens, '\u001e');

  EXPECT_EQ(str, "Hello\u001eWorld\u001eNice\u001eWeather");
}

TEST_F(UtilityTest, ExpressionParserTest) {
  auto schema = arrow::schema(
      {arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32()),
       arrow::field("c", arrow::int32()), arrow::field("d", arrow::int32()),
       arrow::field("e", arrow::utf8())});

  vdb::expression::ExpressionBuilder builder(schema);

  std::vector<std::pair<std::string, std::string>> test_filter_cases = {
      {"(a = 1 aND b !=2) OR (c = 3 AnD d>=4 anD e='abc')",
       "((a = 1 AND b != 2) OR (c = 3 AND d >= 4 AND e = 'abc'))"},
      {"a = 1 AND b!= 2 OR c = 3 AND d = 4 AND e not LIKE 'abc'",
       "((a = 1 AND b != 2) OR (c = 3 AND d = 4 AND e NOT LIKE 'abc'))"},
      {"a = 1 AND b = 2 oR nOT (c = 3 AND d = 4 AND e=1)",
       "((a = 1 AND b = 2) OR NOT (c = 3 AND d = 4 AND e = 1))"},
      {"a = 1 AND b = 2 Or not (c = 3 OR d = 4 AND e=1)",
       "((a = 1 AND b = 2) OR NOT (c = 3 OR (d = 4 AND e = 1)))"},
      {"a in (1, 2, 3) AND b IS null", "(a IN [1,2,3] AND b IS NULL)"},
      {R"(a Like '%pattern%' OR b is nOT nuLL)",
       R"del((a LIKE '%pattern%' OR b IS NOT NULL))del"},
      {"(a=1 and b !=2 or (c>=1 and d != 50 and (e > 70 AND a > 10)))",
       "((a = 1 AND b != 2) OR (c >= 1 AND d != 50 AND (e > 70 AND a > 10)))"}};
  for (const auto &[test_filter, expected_expr] : test_filter_cases) {
    std::cout << "Testing: " << test_filter << std::endl;
    auto expr = builder.ParseFilter(test_filter);
    if (!expr.ok()) {
      std::cerr << expr.status().message() << std::endl;
    }
    ASSERT_TRUE(expr.ok());
    EXPECT_STREQ(expr.ValueOrDie()->ToString().c_str(), expected_expr.c_str());
  }
}

TEST_F(ArrowRecordbatchTest, SerializeDeserializeUnder2GB) {
  // 간단한 데이터 생성
  arrow::Int64Builder builder;
  for (int64_t i = 0; i < 1000000; ++i) {
    ASSERT_OK(builder.Append(i));
  }

  std::shared_ptr<arrow::Array> array;
  ASSERT_OK(builder.Finish(&array));

  auto record_batch = arrow::RecordBatch::Make(schema, 1000000, {array});

  auto test_file_path = TestDirectoryPath() + "/RbOver2GB.bin";
  // 직렬화
  auto status = vdb::_SaveRecordBatchTo(test_file_path, record_batch);
  ASSERT_TRUE(status.ok()) << status.ToString() << std::endl;

  // 역직렬화
  auto result = vdb::_LoadRecordBatchFrom(test_file_path, schema);
  ASSERT_TRUE(result.ok());

  auto [loaded_record_batch, buffer] = result.ValueOrDie();

  // Equals를 통한 검증
  ASSERT_TRUE(record_batch->Equals(*loaded_record_batch));
}
TEST_F(ArrowRecordbatchTest, SerializeDeserializeOver2GB) {
  // 매우 큰 데이터 생성
  arrow::Int64Builder builder;
  for (int64_t i = 0; i < 300000000; ++i) {  // 충분히 큰 데이터
    ASSERT_OK(builder.Append(i));
  }

  std::shared_ptr<arrow::Array> array;
  ASSERT_OK(builder.Finish(&array));

  auto record_batch = arrow::RecordBatch::Make(schema, 300000000, {array});

  auto test_file_path = TestDirectoryPath() + "/RbOver2GB.bin";
  // 직렬화
  ASSERT_TRUE(vdb::_SaveRecordBatchTo(test_file_path, record_batch).ok());

  // 역직렬화
  auto result = vdb::_LoadRecordBatchFrom(test_file_path, schema);
  ASSERT_TRUE(result.ok());

  auto [loaded_record_batch, buffer] = result.ValueOrDie();
  ASSERT_EQ(loaded_record_batch->num_rows(), 300000000);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new GlobalEnvironment);
  return RUN_ALL_TESTS();
}
