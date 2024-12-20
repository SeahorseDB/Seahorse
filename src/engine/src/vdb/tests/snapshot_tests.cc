#include <filesystem>
#include <memory>
#include <string>
#include <strings.h>
#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include "arrow/api.h"

#include "vdb/vdb.hh"
#include "vdb/vdb_api.hh"
#include "vdb/common/defs.hh"
#include "vdb/common/util.hh"
#include "vdb/data/table.hh"
#include "vdb/data/mutable_array.hh"
#include "vdb/data/filter.hh"
#include "vdb/data/index_handler.hh"
#include "vdb/tests/util_for_test.hh"

#ifdef __cplusplus
extern "C" {
#include "server.h"
}
#endif

using namespace vdb::tests;

namespace vdb {

std::string snapshot_test_directory_path = "__vdb_snapshot_test_dir__";

class GlobalEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    std::filesystem::remove_all(snapshot_test_directory_path);
    std::filesystem::create_directory(snapshot_test_directory_path);
#ifdef _DEBUG_GTEST
    std::cout << snapshot_test_directory_path
              << " directory is dropped and created. " << std::endl;
#endif
    /* disable redis server log */
#ifdef _DEBUG_GTEST
    server.verbosity = LL_DEBUG;
#else
    server.verbosity = LL_NOTHING;
#endif
    server.logfile = empty_string.data();
    server.allow_bg_index_thread = false;
  }

  void TearDown() override {}
};
class SnapshotTestSuite : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    std::string test_case_name =
        testing::UnitTest::GetInstance()->current_test_case()->name();
    std::string case_directory_path =
        snapshot_test_directory_path + "/" + test_case_name;
    std::filesystem::remove_all(case_directory_path);
    std::filesystem::create_directory(case_directory_path);
#ifdef _DEBUG_GTEST
    std::cout << case_directory_path << " directory is dropped and created. "
              << std::endl;
#endif
  }
  static void TearDownTestCase() {}
  void SetUp() override {
    /* all memory must be freed before starting test case */
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
    ASSERT_EQ(zmalloc_used_memory(), 0);
    AllocateTableDictionary();
    const testing::TestInfo *test_info =
        testing::UnitTest::GetInstance()->current_test_info();
    std::string test_case_name = test_info->test_case_name();
    std::string test_name = test_info->name();
    test_directory_path_ =
        snapshot_test_directory_path + "/" + test_case_name + "/" + test_name;
    snapshot_directory_name_ = "snapshot";
    server.aof_filename = snapshot_directory_name_.data();
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
  }

  void TearDown() override {
    DeallocateTableDictionary();
    /* all memory must be freed after finish of test case */
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
    ASSERT_EQ(zmalloc_used_memory(), 0);
  }

  std::string &TestDirectoryPath() { return test_directory_path_; }

 private:
  std::string test_directory_path_;
  std::string snapshot_directory_name_;
};

class HnswTest : public SnapshotTestSuite {};
class IndexHandlerTest : public SnapshotTestSuite {};
class SegmentTest : public SnapshotTestSuite {};
class TableTest : public SnapshotTestSuite {};
class VdbSnapshotTest : public SnapshotTestSuite {};

TEST_F(HnswTest, SingleIndex2DSmallDataTest) {
  size_t data_cnt = 10;
  size_t dim = 2;
  auto data = generateSequentialFloatArray(data_cnt, dim, 1);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif
  /* {1,1} ... {10,10} */
  size_t ef_construction = 100;
  size_t M = 3;
  size_t max_elem = 100000;
  auto index_space = vdb::Hnsw::Metric::kL2Space;
  auto index = std::make_shared<vdb::Hnsw>(index_space, dim, ef_construction, M,
                                           max_elem);
  for (size_t i = 0; i < data_cnt; i++) {
    float *point = &data[i][0];
    index->AddEmbedding(point, i);
  }
  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 5;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << k
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto org_index_result_pq = index->SearchKnn(query, k);
  auto org_index_result =
      ResultToDistEmbeddingPairs(index, org_index_result_pq);

  std::string index_file_path = TestDirectoryPath() + "/index";
  auto status = index->Save(index_file_path);

#ifdef _DEBUG_GTEST
  std::cout << "save index to : " << index_file_path << std::endl;
#endif

  auto loaded_index =
      std::make_shared<vdb::Hnsw>(index_file_path, index_space, dim);
  auto loaded_case_index_result_pq = loaded_index->SearchKnn(query, k);
  size_t loaded_case_pq_size = loaded_case_index_result_pq.size();
  auto loaded_case_index_result =
      ResultToDistEmbeddingPairs(loaded_index, loaded_case_index_result_pq);
  ASSERT_EQ(loaded_case_pq_size, loaded_case_index_result.size());
  ASSERT_EQ(loaded_case_index_result.size(), org_index_result.size());
  for (size_t i = 0; i < loaded_case_index_result.size(); i++) {
    auto &org_index_pair = org_index_result[i];
    auto &org_index_dist = org_index_pair.first;
    auto &org_index_embedding = org_index_pair.second;

    auto &loaded_index_pair = loaded_case_index_result[i];
    auto &loaded_index_dist = loaded_index_pair.first;
    auto &loaded_index_embedding = loaded_index_pair.second;

    ASSERT_FLOAT_EQ(org_index_dist, loaded_index_dist);
    ASSERT_TRUE(
        EmbeddingEquals(org_index_embedding, loaded_index_embedding, dim));
#ifdef _DEBUG_GTEST
    std::cout << i << "th loaded index result: dist=" << loaded_index_dist
              << ", embedding=[" << loaded_index_embedding[0] << ","
              << loaded_index_embedding[1] << "] ";
    std::cout << " original index result: dist=" << org_index_dist
              << ", embedding=[" << org_index_embedding[0] << ","
              << org_index_embedding[1] << "]" << std::endl;
#endif
  }
  auto accuracy =
      CalculateKnnAccuracy(loaded_case_index_result, org_index_result);
  ASSERT_FLOAT_EQ(accuracy, 100.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
#endif
}

TEST_F(HnswTest, SingleIndex2DLargeDataTest) {
  size_t data_cnt = 10000;
  size_t dim = 2;
  auto data = generateRandomFloatArray(data_cnt, dim);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif
  /* {1,1} ... {10,10} */
  size_t ef_construction = 100;
  size_t M = 3;
  size_t max_elem = 100000;
  auto index_space = vdb::Hnsw::Metric::kL2Space;
  auto index = std::make_shared<vdb::Hnsw>(index_space, dim, ef_construction, M,
                                           max_elem);
  for (size_t i = 0; i < data_cnt; i++) {
    float *point = &data[i][0];
    index->AddEmbedding(point, i);
  }
  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 5;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << k
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto org_index_result_pq = index->SearchKnn(query, k);
  auto org_index_result =
      ResultToDistEmbeddingPairs(index, org_index_result_pq);
  std::string index_file_path = TestDirectoryPath() + "/index";
  auto status = index->Save(index_file_path);

#ifdef _DEBUG_GTEST
  std::cout << "save index to : " << index_file_path << std::endl;
#endif

  auto loaded_index =
      std::make_shared<vdb::Hnsw>(index_file_path, index_space, dim);
  auto loaded_case_index_result_pq = loaded_index->SearchKnn(query, k);
  size_t loaded_case_pq_size = loaded_case_index_result_pq.size();
  auto loaded_case_index_result =
      ResultToDistEmbeddingPairs(loaded_index, loaded_case_index_result_pq);
  ASSERT_EQ(loaded_case_pq_size, loaded_case_index_result.size());
  ASSERT_EQ(loaded_case_index_result.size(), org_index_result.size());
  for (size_t i = 0; i < loaded_case_index_result.size(); i++) {
    auto &org_index_pair = org_index_result[i];
    auto &org_index_dist = org_index_pair.first;
    auto &org_index_embedding = org_index_pair.second;

    auto &loaded_index_pair = loaded_case_index_result[i];
    auto &loaded_index_dist = loaded_index_pair.first;
    auto &loaded_index_embedding = loaded_index_pair.second;

    ASSERT_FLOAT_EQ(org_index_dist, loaded_index_dist);
    ASSERT_TRUE(
        EmbeddingEquals(org_index_embedding, loaded_index_embedding, dim));
#ifdef _DEBUG_GTEST
    std::cout << i << "th loaded index result: dist=" << loaded_index_dist
              << ", embedding=[" << loaded_index_embedding[0] << ","
              << loaded_index_embedding[1] << "] ";
    std::cout << " original index result: dist=" << org_index_dist
              << ", embedding=[" << org_index_embedding[0] << ","
              << org_index_embedding[1] << "]" << std::endl;
#endif
  }
  auto accuracy =
      CalculateKnnAccuracy(loaded_case_index_result, org_index_result);
  ASSERT_FLOAT_EQ(accuracy, 100.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
#endif
}

TEST_F(HnswTest, SingleIndex100DLargeDataTest) {
  size_t data_cnt = 10000;
  size_t dim = 100;
  auto data = generateRandomFloatArray(data_cnt, dim);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif
  /* {1,1} ... {10,10} */
  size_t ef_construction = 100;
  size_t M = 3;
  size_t max_elem = 100000;
  auto index_space = vdb::Hnsw::Metric::kL2Space;
  auto index = std::make_shared<vdb::Hnsw>(index_space, dim, ef_construction, M,
                                           max_elem);
  for (size_t i = 0; i < data_cnt; i++) {
    float *point = &data[i][0];
    index->AddEmbedding(point, i);
  }
  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 20;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << k
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto org_index_result_pq = index->SearchKnn(query, k);
  size_t org_pq_size = org_index_result_pq.size();
  auto org_index_result =
      ResultToDistEmbeddingPairs(index, org_index_result_pq);
  ASSERT_EQ(org_pq_size, org_index_result.size());

  std::string index_file_path = TestDirectoryPath() + "/index";
  auto status = index->Save(index_file_path);

#ifdef _DEBUG_GTEST
  std::cout << "save index to : " << index_file_path << std::endl;
#endif

  auto loaded_index =
      std::make_shared<vdb::Hnsw>(index_file_path, index_space, dim);
  auto loaded_case_index_result_pq = loaded_index->SearchKnn(query, k);
  size_t loaded_case_pq_size = loaded_case_index_result_pq.size();
  auto loaded_case_index_result =
      ResultToDistEmbeddingPairs(loaded_index, loaded_case_index_result_pq);
  ASSERT_EQ(loaded_case_pq_size, loaded_case_index_result.size());
  ASSERT_EQ(loaded_case_index_result.size(), org_index_result.size());
  for (size_t i = 0; i < loaded_case_index_result.size(); i++) {
    auto &org_index_pair = org_index_result[i];
    auto &org_index_dist = org_index_pair.first;
    auto &org_index_embedding = org_index_pair.second;

    auto &loaded_index_pair = loaded_case_index_result[i];
    auto &loaded_index_dist = loaded_index_pair.first;
    auto &loaded_index_embedding = loaded_index_pair.second;

    ASSERT_FLOAT_EQ(org_index_dist, loaded_index_dist);
    ASSERT_TRUE(
        EmbeddingEquals(org_index_embedding, loaded_index_embedding, dim));
#ifdef _DEBUG_GTEST
    std::cout << i << "th loaded index result: dist=" << loaded_index_dist
              << ", embedding=[" << loaded_index_embedding[0] << ","
              << loaded_index_embedding[1] << "] ";
    std::cout << " original index result: dist=" << org_index_dist
              << ", embedding=[" << org_index_embedding[0] << ","
              << org_index_embedding[1] << "]" << std::endl;
#endif
  }
  auto accuracy =
      CalculateKnnAccuracy(loaded_case_index_result, org_index_result);
  ASSERT_FLOAT_EQ(accuracy, 100.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
#endif
}

TEST_F(HnswTest, IndexHandlerTest) {
  /* dummy table for set metadata */
  auto table_dictionary = vdb::GetTableDictionary();
  size_t dim = 10;

  std::string test_table_name = "dummy_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ], Feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";

  server.vdb_active_set_size_limit = 10000;
  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->at(test_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "3"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(table, add_metadata);

  auto org_index_handler = std::make_shared<IndexHandlerForTest>(table);

  size_t data_cnt = 1000;
  auto data = generateSequentialFloatArray(data_cnt, dim, 1);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif

  for (size_t i = 0; i < data.size(); i++) {
    float *point = &data[i][0];
    org_index_handler->AddEmbedding(point, i);
  }

  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 10;
  size_t ef_search = k * 3;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << ef_search
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto org_index_result_vec = org_index_handler->SearchKnn(query, ef_search);
  org_index_result_vec->resize(k);
  auto org_index_result =
      ResultToDistEmbeddingPairs(org_index_handler, *org_index_result_vec);

#ifdef _DEBUG_GTEST
  std::cout << "index count=" << org_index_handler->Size() << std::endl;
#endif

  std::string index_directory_path = TestDirectoryPath() + "/indexes";
  status = org_index_handler->Save(index_directory_path);
  ASSERT_TRUE(status.ok());

  auto loaded_index_handler = std::make_shared<IndexHandlerForTest>(
      table, index_directory_path, org_index_handler->Size());
  auto loaded_index_result_vec =
      loaded_index_handler->SearchKnn(query, ef_search);
  loaded_index_result_vec->resize(k);
  auto loaded_index_result = ResultToDistEmbeddingPairs(
      loaded_index_handler, *loaded_index_result_vec);

  for (size_t i = 0; i < loaded_index_result.size(); i++) {
    auto &loaded_index_pair = loaded_index_result[i];
    auto &loaded_index_dist = loaded_index_pair.first;
    auto &loaded_index_embedding = loaded_index_pair.second;
    auto &org_index_pair = org_index_result[i];
    auto &org_index_dist = org_index_pair.first;
    auto &org_index_embedding = org_index_pair.second;

    ASSERT_FLOAT_EQ(org_index_dist, loaded_index_dist);
    ASSERT_TRUE(
        EmbeddingEquals(org_index_embedding, loaded_index_embedding, dim));
#ifdef _DEBUG_GTEST
    std::cout << i << "th loaded index result: dist=" << loaded_index_dist
              << ", embedding=" << VecToStr(loaded_index_embedding, dim);
    std::cout << " original index result: dist=" << org_index_dist
              << ", embedding=" << VecToStr(org_index_embedding, dim)
              << std::endl;
#endif
  }
  auto accuracy = CalculateKnnAccuracy(loaded_index_result, org_index_result);
  ASSERT_GE(accuracy, 100.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
  std::cout << "index count=" << loaded_index_handler->Size() << std::endl;
#endif
}

TEST_F(HnswTest, IndexHandlerMoreDataTest) {
  /* dummy table for set metadata */
  auto table_dictionary = vdb::GetTableDictionary();
  size_t dim = 100;

  std::string test_table_name = "dummy_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ], Feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";

  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->at(test_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "3"},
                                                   {"ef_construction", "100"},
                                                   {"M", "2"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(table, add_metadata);

  auto org_index_handler = std::make_shared<IndexHandlerForTest>(table);

  size_t data_cnt = 10000;
  auto data = generateRandomFloatArray(data_cnt, dim);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif

  for (size_t i = 0; i < data.size(); i++) {
    float *point = &data[i][0];
    org_index_handler->AddEmbedding(point, i);
  }

  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 10;
  size_t ef_search = k * 3;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << ef_search
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto org_index_result_vec = org_index_handler->SearchKnn(query, ef_search);
  org_index_result_vec->resize(k);
  auto org_index_result =
      ResultToDistEmbeddingPairs(org_index_handler, *org_index_result_vec);

  std::string index_directory_path = TestDirectoryPath() + "/indexes";
  status = org_index_handler->Save(index_directory_path);
  ASSERT_TRUE(status.ok());

  auto loaded_index_handler = std::make_shared<IndexHandlerForTest>(
      table, index_directory_path, org_index_handler->Size());
  auto loaded_index_result_vec =
      loaded_index_handler->SearchKnn(query, ef_search);
  loaded_index_result_vec->resize(k);
  auto loaded_index_result = ResultToDistEmbeddingPairs(
      loaded_index_handler, *loaded_index_result_vec);

  for (size_t i = 0; i < loaded_index_result.size(); i++) {
    auto &loaded_index_pair = loaded_index_result[i];
    auto &loaded_index_dist = loaded_index_pair.first;
    auto &loaded_index_embedding = loaded_index_pair.second;
    auto &org_index_pair = org_index_result[i];
    auto &org_index_dist = org_index_pair.first;
    auto &org_index_embedding = org_index_pair.second;

    ASSERT_FLOAT_EQ(org_index_dist, loaded_index_dist);
    ASSERT_TRUE(
        EmbeddingEquals(org_index_embedding, loaded_index_embedding, dim));
#ifdef _DEBUG_GTEST
    std::cout << i << "th loaded index result: dist=" << loaded_index_dist
              << ", embedding=" << VecToStr(loaded_index_embedding, dim);
    std::cout << " original index result: dist=" << org_index_dist
              << ", embedding=" << VecToStr(org_index_embedding, dim)
              << std::endl;
#endif
  }
  auto accuracy = CalculateKnnAccuracy(loaded_index_result, org_index_result);
  ASSERT_GE(accuracy, 100.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
  std::cout << "index count=" << loaded_index_handler->Size() << std::endl;
#endif
}

TEST_F(SegmentTest, EmptyTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
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
  std::string dummy_id = "";
  auto org_segment = std::make_shared<vdb::Segment>(table, dummy_id);

  std::string segment_directory_path = TestDirectoryPath() + "/segment";
  auto status = org_segment->Save(segment_directory_path);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  auto loaded_segment =
      std::make_shared<vdb::Segment>(table, dummy_id, segment_directory_path);
  ASSERT_TRUE(SegmentEquals(org_segment, loaded_segment));
#ifdef _DEBUG_GTEST
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "org_segment" << std::endl
            << org_segment->ToString() << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "loaded_segment" << std::endl
            << loaded_segment->ToString() << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
#endif
}

TEST_F(SegmentTest, IncompleteTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
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
  std::string dummy_id = "";
  auto org_segment = std::make_shared<vdb::Segment>(table, dummy_id);

  std::string segment_directory_path = TestDirectoryPath() + "/segment";
  auto status = org_segment->Save(segment_directory_path);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string manifest_file_path = segment_directory_path + "/manifest";
  std::filesystem::remove(manifest_file_path);
  bool std_error = false;
  try {
    auto loaded_segment =
        std::make_shared<vdb::Segment>(table, dummy_id, segment_directory_path);
  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Filesystem error: " << e.what() << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "General error: " << e.what() << std::endl;
    std_error = true;
  }
  ASSERT_TRUE(std_error);
}

TEST_F(SegmentTest, EmptyWithIndexHandlerTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "10000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  schema = schema->WithMetadata(add_metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName("test_table").SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  std::string dummy_id = "";
  auto org_segment = std::make_shared<vdb::Segment>(table, dummy_id);

  std::string segment_directory_path = TestDirectoryPath() + "/segment";
  auto status = org_segment->Save(segment_directory_path);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  auto loaded_segment =
      std::make_shared<vdb::Segment>(table, dummy_id, segment_directory_path);
  ASSERT_TRUE(SegmentEquals(org_segment, loaded_segment));
#ifdef _DEBUG_GTEST
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "org_segment" << std::endl
            << org_segment->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "loaded_segment" << std::endl
            << loaded_segment->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
#endif
}

TEST_F(SegmentTest, ManyDataWithIndexHandlerTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "1000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  schema = schema->WithMetadata(add_metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName("test_table").SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto table, builder.Build());
  std::string dummy_id = "";
  auto org_segment = std::make_shared<vdb::Segment>(table, dummy_id);

  auto maybe_rb = GenerateRecordBatch(schema, 5000, dim);
  ASSERT_TRUE(maybe_rb.ok());
  auto rb = maybe_rb.ValueUnsafe();
  std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
  auto status = org_segment->AppendRecords(rbs);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string segment_directory_path = TestDirectoryPath() + "/segment";
  status = org_segment->Save(segment_directory_path);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  auto loaded_segment =
      std::make_shared<vdb::Segment>(table, dummy_id, segment_directory_path);
  ASSERT_TRUE(SegmentEquals(org_segment, loaded_segment));
#ifdef _DEBUG_GTEST
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "org_segment" << std::endl
            << org_segment->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "loaded_segment" << std::endl
            << loaded_segment->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
#endif
}

TEST_F(TableTest, EmptyTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
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
  ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
  org_table->AddSegment(org_table, "1");

  std::string snapshot_directory_path = TestDirectoryPath();
  std::string_view snapshot_directory_path_view(snapshot_directory_path);
  auto status = org_table->Save(snapshot_directory_path_view);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string table_directory_path = snapshot_directory_path + "/test_table";
  vdb::TableBuilderOptions options2;
  vdb::TableBuilder builder2(
      std::move(options2.SetTableName(table_name)
                    .SetTableDirectoryPath(table_directory_path)));
  ASSERT_OK_AND_ASSIGN(auto loaded_table, builder2.Build());
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(TableEquals(org_table, loaded_table));
#ifdef _DEBUG_GTEST
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "org_table" << std::endl
            << org_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "loaded_table" << std::endl
            << loaded_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
#endif
}

TEST_F(TableTest, IncompleteTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
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
  ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
  org_table->AddSegment(org_table, "1");

  std::string snapshot_directory_path = TestDirectoryPath();
  std::string_view snapshot_directory_path_view(snapshot_directory_path);
  auto status = org_table->Save(snapshot_directory_path_view);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string table_directory_path = snapshot_directory_path + "/test_table";
  std::string manifest_file_path = table_directory_path + "/manifest";
  std::filesystem::remove(manifest_file_path);

  vdb::TableBuilderOptions options2;
  vdb::TableBuilder builder2(
      std::move(options2.SetTableName(table_name)
                    .SetTableDirectoryPath(table_directory_path)));
  auto loaded_table = builder2.Build();
  if (!loaded_table.ok()) {
    if (loaded_table.status().ToStringWithoutContextLines().find(
            "saving snapshot of table is not completed") == std::string::npos) {
      ASSERT_OK(loaded_table);
    }
  }
}

TEST_F(TableTest, EmptyWithIndexTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "10000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  schema = schema->WithMetadata(add_metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName("test_table").SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());

  auto org_segment = org_table->AddSegment(org_table, "1");

  std::string snapshot_directory_path = TestDirectoryPath();
  std::string_view snapshot_directory_path_view(snapshot_directory_path);
  auto status = org_table->Save(snapshot_directory_path_view);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string table_directory_path = snapshot_directory_path + "/test_table";
  vdb::TableBuilderOptions options2;
  vdb::TableBuilder builder2(
      std::move(options2.SetTableName("test_table")
                    .SetTableDirectoryPath(table_directory_path)));
  ASSERT_OK_AND_ASSIGN(auto loaded_table, builder2.Build());
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(TableEquals(org_table, loaded_table));
#ifdef _DEBUG_GTEST
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "org_table" << std::endl
            << org_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "loaded_table" << std::endl
            << loaded_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
#endif
}

TEST_F(TableTest, ManyDataWithIndexTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "1000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  schema = schema->WithMetadata(add_metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName("test_table").SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
  auto org_segment = org_table->AddSegment(org_table, "1");

  auto maybe_rb = GenerateRecordBatch(schema, 5000, dim);
  ASSERT_TRUE(maybe_rb.ok());
  auto rb = maybe_rb.ValueUnsafe();
  std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
  auto status = org_segment->AppendRecords(rbs);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string snapshot_directory_path = TestDirectoryPath();
  std::string_view snapshot_directory_path_view(snapshot_directory_path);
  status = org_table->Save(snapshot_directory_path_view);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string table_directory_path = snapshot_directory_path + "/test_table";
  vdb::TableBuilderOptions options2;
  vdb::TableBuilder builder2(
      std::move(options2.SetTableName("test_table")
                    .SetTableDirectoryPath(table_directory_path)));
  ASSERT_OK_AND_ASSIGN(auto loaded_table, builder2.Build());
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(TableEquals(org_table, loaded_table));
#ifdef _DEBUG_GTEST
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "org_table" << std::endl
            << org_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "loaded_table" << std::endl
            << loaded_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
#endif
}

TEST_F(TableTest, MultipleSegmentsManyDataWithIndexTest) {
  uint32_t dim = 10;
  size_t segment_count = 5;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "1000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  schema = schema->WithMetadata(add_metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName("test_table").SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
  for (size_t i = 0; i < segment_count; i++) {
    auto org_segment = org_table->AddSegment(org_table, std::to_string(i));

    auto maybe_rb = GenerateRecordBatch(schema, 2500, dim);
    ASSERT_TRUE(maybe_rb.ok());
    auto rb = maybe_rb.ValueUnsafe();
    std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
    auto status = org_segment->AppendRecords(rbs);
    if (!status.ok()) {
      std::cout << status.ToString() << std::endl;
      ASSERT_TRUE(status.ok());
    }
  }
  std::string snapshot_directory_path = TestDirectoryPath();
  std::string_view snapshot_directory_path_view(snapshot_directory_path);
  auto status = org_table->Save(snapshot_directory_path_view);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }
  std::string table_directory_path = snapshot_directory_path + "/test_table";
  vdb::TableBuilderOptions options2;
  vdb::TableBuilder builder2(
      std::move(options2.SetTableName("test_table")
                    .SetTableDirectoryPath(table_directory_path)));
  ASSERT_OK_AND_ASSIGN(auto loaded_table, builder2.Build());
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(TableEquals(org_table, loaded_table));
#ifdef _DEBUG_GTEST
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "org_table" << std::endl
            << org_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
  std::cout << "loaded_table" << std::endl
            << loaded_table->ToString(false) << std::endl;
  std::cout << "---------------------------------------------------------------"
            << std::endl;
#endif
}

TEST_F(VdbSnapshotTest, EmptyTest) {
  std::string base_snapshot_name = GetBaseSnapshotDirectoryName(0);
  std::string base_snapshot_path(server.aof_dirname);
  base_snapshot_path.append("/");
  base_snapshot_path.append(base_snapshot_name);
  bool ok = SaveVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t table_count = vdb::GetTableDictionary()->size();
  DeallocateTableDictionary();
  AllocateTableDictionary();
  ok = LoadVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t loaded_table_count = vdb::GetTableDictionary()->size();
  ASSERT_EQ(table_count, loaded_table_count);
}

TEST_F(VdbSnapshotTest, SingleEmptyTableTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
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
  ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
  org_table->AddSegment(org_table, "1");
  auto table_dictionary = vdb::GetTableDictionary();
  table_dictionary->insert({"test_table", org_table});

  std::string base_snapshot_name = GetBaseSnapshotDirectoryName(0);
  std::string base_snapshot_path(server.aof_dirname);
  base_snapshot_path.append("/");
  base_snapshot_path.append(base_snapshot_name);
  PrepareVdbSnapshot();
  bool ok = SaveVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  PostVdbSnapshot();
  size_t table_count = vdb::GetTableDictionary()->size();
  DeallocateTableDictionary();
  AllocateTableDictionary();
  ok = LoadVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t loaded_table_count = vdb::GetTableDictionary()->size();
  ASSERT_EQ(table_count, loaded_table_count);
  auto loaded_table_dictionary = vdb::GetTableDictionary();
  for (auto [table_name, table] : *loaded_table_dictionary) {
    ASSERT_TRUE(table_name.compare("test_table") == 0);
  }
}

TEST_F(VdbSnapshotTest, SingleDataTableTest) {
  uint32_t dim = 10;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "1000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  schema = schema->WithMetadata(add_metadata);
  vdb::TableBuilderOptions options;
  vdb::TableBuilder builder{
      std::move(options.SetTableName("test_table").SetSchema(schema))};
  ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
  auto org_segment = org_table->AddSegment(org_table, "1");
  auto table_dictionary = vdb::GetTableDictionary();
  table_dictionary->insert({"test_table", org_table});

  auto maybe_rb = GenerateRecordBatch(schema, 5000, dim);
  ASSERT_TRUE(maybe_rb.ok());
  auto rb = maybe_rb.ValueUnsafe();
  std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
  auto status = org_segment->AppendRecords(rbs);
  if (!status.ok()) {
    std::cout << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  std::string base_snapshot_name = GetBaseSnapshotDirectoryName(0);
  std::string base_snapshot_path(server.aof_dirname);
  base_snapshot_path.append("/");
  base_snapshot_path.append(base_snapshot_name);
  bool ok = SaveVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t table_count = vdb::GetTableDictionary()->size();
  DeallocateTableDictionary();
  AllocateTableDictionary();
  ok = LoadVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t loaded_table_count = vdb::GetTableDictionary()->size();
  ASSERT_EQ(table_count, loaded_table_count);
  auto loaded_table_dictionary = vdb::GetTableDictionary();
  for (auto [table_name, table] : *loaded_table_dictionary) {
    ASSERT_TRUE(table_name.compare("test_table") == 0);
    ASSERT_TRUE(TableEquals(org_table, table));
  }
}

TEST_F(VdbSnapshotTest, ComplexTest) {
  uint32_t dim = 10;
  size_t segment_count = 5;
  size_t org_table_count = 5;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "1000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  schema = schema->WithMetadata(add_metadata);
  auto table_dictionary = vdb::GetTableDictionary();
  std::map<std::string, std::shared_ptr<vdb::Table>> org_tables;
  for (size_t j = 0; j < org_table_count; j++) {
    std::string table_name = "test_table" + std::to_string(j);
    vdb::TableBuilderOptions options;
    vdb::TableBuilder builder{
        std::move(options.SetTableName(table_name).SetSchema(schema))};
    ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
    auto org_segment = org_table->AddSegment(org_table, "1");
    table_dictionary->insert({table_name, org_table});

    for (size_t i = 0; i < segment_count; i++) {
      auto org_segment = org_table->AddSegment(org_table, std::to_string(i));

      auto maybe_rb = GenerateRecordBatch(schema, 970, dim);
      ASSERT_TRUE(maybe_rb.ok());
      auto rb = maybe_rb.ValueUnsafe();
      std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
      auto status = org_segment->AppendRecords(rbs);
      if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.ok());
      }
    }
    table_dictionary->insert({table_name, org_table});
    org_tables.insert({table_name, org_table});
  }

  std::string base_snapshot_name = GetBaseSnapshotDirectoryName(0);
  std::string base_snapshot_path(server.aof_dirname);
  base_snapshot_path.append("/");
  base_snapshot_path.append(base_snapshot_name);
  bool ok = SaveVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t table_count = vdb::GetTableDictionary()->size();
  DeallocateTableDictionary();
  AllocateTableDictionary();
  ok = LoadVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t loaded_table_count = vdb::GetTableDictionary()->size();
  ASSERT_EQ(table_count, loaded_table_count);
  auto loaded_table_dictionary = vdb::GetTableDictionary();
  for (auto [table_name, table] : *loaded_table_dictionary) {
    ASSERT_TRUE(TableEquals(org_tables[table_name], table));
  }
}

TEST_F(VdbSnapshotTest, MultipleSnapshotTest) {
  uint32_t dim = 10;
  size_t segment_count = 5;
  size_t org_table_count = 5;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto table_dictionary = vdb::GetTableDictionary();
  std::map<std::string, std::shared_ptr<vdb::Table>> org_tables;
  for (size_t j = 0; j < org_table_count; j++) {
    std::string table_name = "test_table" + std::to_string(j);
    auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
        std::unordered_map<std::string, std::string>{
            {"table name", table_name},
            {"ann_column_id", "3"},
            {"ef_construction", "100"},
            {"M", "2"},
            {"active_set_size_limit", "1000"},
            {"index_space", "L2Space"},
            {"index_type", "Hnsw"}});
    schema = schema->WithMetadata(add_metadata);
    vdb::TableBuilderOptions options;
    vdb::TableBuilder builder{
        std::move(options.SetTableName(table_name).SetSchema(schema))};
    ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
    auto org_segment = org_table->AddSegment(org_table, "1");
    table_dictionary->insert({table_name, org_table});

    for (size_t i = 0; i < segment_count; i++) {
      auto org_segment = org_table->AddSegment(org_table, std::to_string(i));

      auto maybe_rb = GenerateRecordBatch(schema, 970, dim);
      ASSERT_TRUE(maybe_rb.ok());
      auto rb = maybe_rb.ValueUnsafe();
      std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
      auto status = org_segment->AppendRecords(rbs);
      if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.ok());
      }
    }
    table_dictionary->insert({table_name, org_table});
    org_tables.insert({table_name, org_table});
    std::string base_snapshot_name = GetBaseSnapshotDirectoryName(j);
    std::string base_snapshot_path(server.aof_dirname);
    base_snapshot_path.append("/");
    base_snapshot_path.append(base_snapshot_name);
    bool ok = SaveVdbSnapshot(base_snapshot_path.data());
    ASSERT_TRUE(ok);
  }

  size_t table_count = vdb::GetTableDictionary()->size();
  DeallocateTableDictionary();
  AllocateTableDictionary();
  std::string base_snapshot_name =
      GetBaseSnapshotDirectoryName(org_table_count - 1);
  std::string base_snapshot_path(server.aof_dirname);
  base_snapshot_path.append("/");
  base_snapshot_path.append(base_snapshot_name);
  bool ok = LoadVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t loaded_table_count = vdb::GetTableDictionary()->size();
  ASSERT_EQ(table_count, loaded_table_count);
  auto loaded_table_dictionary = vdb::GetTableDictionary();
  for (auto [table_name, table] : *loaded_table_dictionary) {
    ASSERT_TRUE(TableEquals(org_tables[table_name], table));
  }
}

TEST_F(VdbSnapshotTest, ResumeBuildingIncompleteIndexTest) {
  uint32_t dim = 100;
  size_t segment_count = 3;
  size_t org_table_count = 2;
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{
          {"table name", "test_table"},
          {"ann_column_id", "3"},
          {"ef_construction", "100"},
          {"M", "2"},
          {"active_set_size_limit", "1000"},
          {"index_space", "L2Space"},
          {"index_type", "Hnsw"}});
  /* for using IndexingThreadJob() */
  server.allow_bg_index_thread = true;

  schema = schema->WithMetadata(add_metadata);
  auto table_dictionary = vdb::GetTableDictionary();
  std::map<std::string, std::shared_ptr<vdb::Table>> org_tables;
  for (size_t j = 0; j < org_table_count; j++) {
    std::string table_name = "test_table" + std::to_string(j);
    vdb::TableBuilderOptions options;
    vdb::TableBuilder builder{
        std::move(options.SetTableName(table_name).SetSchema(schema))};
    ASSERT_OK_AND_ASSIGN(auto org_table, builder.Build());
    auto org_segment = org_table->AddSegment(org_table, "1");
    table_dictionary->insert({table_name, org_table});

    for (size_t i = 0; i < segment_count; i++) {
      auto org_segment = org_table->AddSegment(org_table, std::to_string(i));

      auto maybe_rb = GenerateRecordBatch(schema, 3000, dim);
      ASSERT_TRUE(maybe_rb.ok());
      auto rb = maybe_rb.ValueUnsafe();
      std::vector<std::shared_ptr<arrow::RecordBatch>> rbs = {rb};
      auto status = org_segment->AppendRecords(rbs);
      if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
        ASSERT_TRUE(status.ok());
      }
    }
    table_dictionary->insert({table_name, org_table});
    org_tables.insert({table_name, org_table});
    if (j == 0) {
      /* test_table1 will not be able to build index */
      PrepareVdbSnapshot();
    }
  }

  std::string base_snapshot_name = GetBaseSnapshotDirectoryName(0);
  std::string base_snapshot_path(server.aof_dirname);
  base_snapshot_path.append("/");
  base_snapshot_path.append(base_snapshot_name);
  /* Checking incomplete index exists before snapshot save */
  bool incomplete_index_check_before_snapshot = true;
  for (auto [table_name, table] : *table_dictionary) {
    bool prepare_check = TableIndexFullBuildCheck(table, false);
    if (!prepare_check) {
      incomplete_index_check_before_snapshot = false;
    }
  }
  ASSERT_FALSE(incomplete_index_check_before_snapshot);
  bool ok = SaveVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t table_count = vdb::GetTableDictionary()->size();
  DeallocateTableDictionary();
  AllocateTableDictionary();
  ok = LoadVdbSnapshot(base_snapshot_path.data());
  ASSERT_TRUE(ok);
  size_t loaded_table_count = vdb::GetTableDictionary()->size();
  ASSERT_EQ(table_count, loaded_table_count);
  auto loaded_table_dictionary = vdb::GetTableDictionary();

  /* Checking all tables are equal before resuming incomplete index building */
  for (auto [table_name, table] : *loaded_table_dictionary) {
    ASSERT_TRUE(TableEquals(org_tables[table_name], table));
    if (table_name.compare("test_table1") == 0) {
      ASSERT_FALSE(TableIndexFullBuildCheck(table, false));
    }
  }
  PostVdbSnapshot();
  /* Resuming the index building and checking all index building job is
   * completed */
  for (auto [table_name, table] : *loaded_table_dictionary) {
    bool building_complete = false;
    while (!building_complete) {
      building_complete = TableIndexFullBuildCheck(table, false);
    }
  }
  /* disable background indexing thread */
  server.allow_bg_index_thread = false;
}

}  // namespace vdb

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new vdb::GlobalEnvironment);
  return RUN_ALL_TESTS();
}
