#include <memory>
#include <queue>
#include <string>
#include <strings.h>
#include <utility>
#include <gtest/gtest.h>

#include "vdb/tests/util_for_test.hh"

#ifdef __cplusplus
extern "C" {
#include "server.h"
}
#endif

using namespace vdb::tests;

namespace vdb {

class GlobalEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
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

class HnswTest : public DataStructureTestSuite {};
class IndexHandlerTest : public DataStructureTestSuite {};

TEST_F(HnswTest, SearchKnnSimple2DTest) {
  size_t data_cnt = 10;
  size_t dim = 2;
  auto data = generateSequentialFloatArray(data_cnt, dim, 1);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif
  /* {1,1} ... {10,10} */
  size_t ef_construction = 100;
  size_t M = 3;
  size_t max_elem = 1000;
  auto index = std::make_shared<vdb::Hnsw>(vdb::Hnsw::Metric::kL2Space, dim,
                                           ef_construction, M, max_elem);
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
  auto index_result_pq = index->SearchKnn(query, k);
  size_t pq_size = index_result_pq.size();
  auto index_result = ResultToDistEmbeddingPairs(index, index_result_pq);
  auto knn_result = SearchExactKnn(query, data, k);
  ASSERT_EQ(pq_size, index_result.size());
  ASSERT_EQ(index_result.size(), knn_result.size());
#ifdef _DEBUG_GTEST
  for (size_t i = 0; i < index_result.size(); i++) {
    auto &index_pair = index_result[i];
    auto &index_dist = index_pair.first;
    auto &index_embedding = index_pair.second;
    auto &knn_pair = knn_result[i];
    auto &knn_dist = knn_pair.first;
    auto &knn_embedding = knn_pair.second;

    std::cout << i << "th index result: dist=" << index_dist << ", embedding=["
              << index_embedding[0] << "," << index_embedding[1] << "] ";
    std::cout << " exact knn result: dist=" << knn_dist << ", embedding=["
              << knn_embedding[0] << "," << knn_embedding[1] << "]"
              << std::endl;
  }
#endif
  auto accuracy = CalculateKnnAccuracy(index_result, knn_result);
  ASSERT_GE(accuracy, 90.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
#endif
}

TEST_F(HnswTest, SearchKnnSimple10DTest) {
  size_t data_cnt = 10;
  size_t dim = 10;
  auto data = generateSequentialFloatArray(data_cnt, dim, 1);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif
  /* {1,1} ... {10,10} */
  size_t ef_construction = 100;
  size_t M = 3;
  size_t max_elem = 1000;
  auto index = std::make_shared<vdb::Hnsw>(vdb::Hnsw::Metric::kL2Space, dim,
                                           ef_construction, M, max_elem);
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
  auto index_result_pq = index->SearchKnn(query, k);
  size_t pq_size = index_result_pq.size();
  auto index_result = ResultToDistEmbeddingPairs(index, index_result_pq);
  auto knn_result = SearchExactKnn(query, data, k);
  ASSERT_EQ(pq_size, index_result.size());
  ASSERT_EQ(index_result.size(), knn_result.size());
#ifdef _DEBUG_GTEST
  for (size_t i = 0; i < index_result.size(); i++) {
    auto &index_pair = index_result[i];
    auto &index_dist = index_pair.first;
    auto &index_embedding = index_pair.second;
    // auto &index_embedding = index_pair.second;
    auto &knn_pair = knn_result[i];
    auto &knn_dist = knn_pair.first;
    auto &knn_embedding = knn_pair.second;
    // auto &knn_embedding = knn_pair.second;

    ASSERT_FLOAT_EQ(index_dist, knn_dist);
    std::cout << i << "th index result: dist=" << index_dist << ", embedding=["
              << index_embedding[0] << "," << index_embedding[1] << "] ";
    std::cout << " exact knn result: dist=" << knn_dist << ", embedding=["
              << knn_embedding[0] << "," << knn_embedding[1] << "]"
              << std::endl;
  }
#endif
  auto accuracy = CalculateKnnAccuracy(index_result, knn_result);
  ASSERT_GE(accuracy, 90.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
#endif
}

TEST_F(HnswTest, SearchKnnComplex10DTest) {
  size_t data_cnt = 3000;
  size_t dim = 10;
  auto data = generateRandomFloatArray(data_cnt, dim);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif
  size_t ef_construction = 100;
  size_t M = 24;
  size_t max_elem = 10000;
  auto index = std::make_shared<vdb::Hnsw>(vdb::Hnsw::Metric::kL2Space, dim,
                                           ef_construction, M, max_elem);
  for (size_t i = 0; i < data_cnt; i++) {
    float *point = &data[i][0];
    index->AddEmbedding(point, i);
  }
  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 10;
  size_t ef_search = k * 3;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << ef_search
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto index_result_pq = index->SearchKnn(query, ef_search);
  size_t pq_size = index_result_pq.size();
  auto index_result = ResultToDistEmbeddingPairs(index, index_result_pq);
  ASSERT_EQ(pq_size, index_result.size());
  index_result.resize(k);

  auto knn_result = SearchExactKnn(query, data, k);
  ASSERT_EQ(index_result.size(), knn_result.size());
#ifdef _DEBUG_GTEST
  for (size_t i = 0; i < index_result.size(); i++) {
    auto &index_pair = index_result[i];
    auto &index_dist = index_pair.first;
    auto &index_embedding = index_pair.second;
    auto &knn_pair = knn_result[i];
    auto &knn_dist = knn_pair.first;
    auto &knn_embedding = knn_pair.second;

    std::cout << i << "th index result: dist=" << index_dist
              << ", embedding=" << VecToStr(index_embedding, dim);
    std::cout << " exact knn result: dist=" << knn_dist
              << ", embedding=" << VecToStr(knn_embedding, dim) << std::endl;
  }
#endif
  auto accuracy = CalculateKnnAccuracy(index_result, knn_result);
  ASSERT_GE(accuracy, 90.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
#endif
}

TEST_F(IndexHandlerTest, SingleIndexSearchKnnTest) {
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
                                                   {"M", "24"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(table, add_metadata);

  auto index_handler = std::make_shared<IndexHandlerForTest>(table);

  size_t data_cnt = 1000;
  auto data = generateSequentialFloatArray(data_cnt, dim, 1);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif

  for (size_t i = 0; i < data.size(); i++) {
    float *point = &data[i][0];
    index_handler->AddEmbeddingForTest(point, i);
  }

  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 10;
  size_t ef_search = k * 3;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << ef_search
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto index_result_vec = index_handler->SearchKnn(query, ef_search);
  index_result_vec->resize(k);
  auto index_result =
      ResultToDistEmbeddingPairs(index_handler, *index_result_vec);

  auto knn_result = SearchExactKnn(query, data, k);
  ASSERT_EQ(index_result.size(), knn_result.size());
#ifdef _DEBUG_GTEST
  for (size_t i = 0; i < index_result.size(); i++) {
    auto &index_pair = index_result[i];
    auto &index_dist = index_pair.first;
    auto &index_embedding = index_pair.second;
    auto &knn_pair = knn_result[i];
    auto &knn_dist = knn_pair.first;
    auto &knn_embedding = knn_pair.second;

    std::cout << i << "th index result: dist=" << index_dist
              << ", embedding=" << VecToStr(index_embedding, dim);
    std::cout << " exact knn result: dist=" << knn_dist
              << ", embedding=" << VecToStr(knn_embedding, dim) << std::endl;
  }
#endif
  auto accuracy = CalculateKnnAccuracy(index_result, knn_result);
  ASSERT_GE(accuracy, 90.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
  std::cout << "index count=" << index_handler->Size() << std::endl;
#endif
}

TEST_F(IndexHandlerTest, MultiIndexSearchKnnTest) {
  /* dummy table for set metadata */
  auto table_dictionary = vdb::GetTableDictionary();
  size_t dim = 10;

  std::string test_table_name = "dummy_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ], Feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";

  server.vdb_active_set_size_limit = 100;
  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->at(test_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "3"},
                                                   {"ef_construction", "100"},
                                                   {"M", "24"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(table, add_metadata);

  auto index_handler = std::make_shared<IndexHandlerForTest>(table);

  size_t data_cnt = 1000;
  auto data = generateSequentialFloatArray(data_cnt, dim, 1);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif

  for (size_t i = 0; i < data.size(); i++) {
    float *point = &data[i][0];
    index_handler->AddEmbeddingForTest(point, i);
  }

  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 10;
  size_t ef_search = k * 3;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << ef_search
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto index_result_vec = index_handler->SearchKnn(query, ef_search);
  index_result_vec->resize(k);
  auto index_result =
      ResultToDistEmbeddingPairs(index_handler, *index_result_vec);

  auto knn_result = SearchExactKnn(query, data, k);
  ASSERT_EQ(index_result.size(), knn_result.size());
#ifdef _DEBUG_GTEST
  for (size_t i = 0; i < index_result.size(); i++) {
    auto &index_pair = index_result[i];
    auto &index_dist = index_pair.first;
    auto &index_embedding = index_pair.second;
    auto &knn_pair = knn_result[i];
    auto &knn_dist = knn_pair.first;
    auto &knn_embedding = knn_pair.second;

    std::cout << i << "th index result: dist=" << index_dist
              << ", embedding=" << VecToStr(index_embedding, dim);
    std::cout << " exact knn result: dist=" << knn_dist
              << ", embedding=" << VecToStr(knn_embedding, dim) << std::endl;
  }
#endif
  auto accuracy = CalculateKnnAccuracy(index_result, knn_result);
  ASSERT_GE(accuracy, 90.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
  std::cout << "index count=" << index_handler->Size() << std::endl;
#endif
}

TEST_F(IndexHandlerTest, ManyPointsSearchKnnTest) {
  /* dummy table for set metadata */
  auto table_dictionary = vdb::GetTableDictionary();
  size_t dim = 10;

  std::string test_table_name = "dummy_table";
  std::string test_schema_string =
      "ID uint32, Name String, Attributes List[ String ], Feature "
      "Fixed_Size_List[ " +
      std::to_string(dim) + ",   Float32 ]";

  server.vdb_active_set_size_limit = 100;
  auto status = CreateTableForTest(test_table_name, test_schema_string);

  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    ASSERT_TRUE(status.ok());
  }

  auto table = table_dictionary->at(test_table_name);

  auto add_metadata = std::make_shared<arrow::KeyValueMetadata>(
      std::unordered_map<std::string, std::string>{{"ann_column_id", "3"},
                                                   {"ef_construction", "10"},
                                                   {"M", "24"},
                                                   {"index_space", "L2Space"},
                                                   {"index_type", "Hnsw"}});
  TableWrapper::AddMetadata(table, add_metadata);

  auto index_handler = std::make_shared<IndexHandlerForTest>(table);

  size_t data_cnt = 1000;
  auto data = generateRandomFloatArray(data_cnt, dim);
#ifdef _DEBUG_GTEST
  std::cout << "dimension=" << dim << " data size=" << data_cnt << std::endl;
#endif

  size_t active_set_size_limit = table->GetActiveSetSizeLimit();
  for (size_t i = 0; i < data.size(); i++) {
    if (i > 0 && (i % active_set_size_limit) == 0) {
      auto status = index_handler->CreateIndex();
      ASSERT_TRUE(status.ok());
    }
    float *point = &data[i][0];
    auto status = index_handler->AddEmbeddingForTest(point, i);
    if (!status.ok()) {
      std::cout << status.ToString() << std::endl;
    }
    ASSERT_TRUE(status.ok());
  }

  auto query_vector = generateSequentialFloatArray(1, dim, 5)[0];
  float *query = &query_vector[0];
  size_t k = 10;
  size_t ef_search = k * 3;
#ifdef _DEBUG_GTEST
  std::cout << "k=" << k << " ef_search=" << ef_search
            << " query=" << VecToStr(query, dim) << std::endl;
#endif
  auto index_result_vec = index_handler->SearchKnn(query, ef_search);
  index_result_vec->resize(k);
  auto index_result =
      ResultToDistEmbeddingPairs(index_handler, *index_result_vec);

  auto knn_result = SearchExactKnn(query, data, k);
  ASSERT_EQ(index_result.size(), knn_result.size());
#ifdef _DEBUG_GTEST
  for (size_t i = 0; i < index_result.size(); i++) {
    auto &index_pair = index_result[i];
    auto &index_dist = index_pair.first;
    auto &index_embedding = index_pair.second;
    auto &knn_pair = knn_result[i];
    auto &knn_dist = knn_pair.first;
    auto &knn_embedding = knn_pair.second;

    std::cout << i << "th index result: dist=" << index_dist
              << ", embedding=" << VecToStr(index_embedding, dim);
    std::cout << " exact knn result: dist=" << knn_dist
              << ", embedding=" << VecToStr(knn_embedding, dim) << std::endl;
  }
#endif
  auto accuracy = CalculateKnnAccuracy(index_result, knn_result);
  ASSERT_GE(accuracy, 90.0);
#ifdef _DEBUG_GTEST
  std::cout << "Accuracy=" << accuracy << std::endl;
  std::cout << "index count=" << index_handler->Size() << std::endl;
#endif
}

}  // namespace vdb

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new vdb::GlobalEnvironment);
  return RUN_ALL_TESTS();
}
