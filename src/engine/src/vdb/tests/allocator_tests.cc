
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <sys/wait.h>

#include "vdb/vdb.hh"
#include "vdb/vdb_api.hh"
#include "vdb/tests/util_for_test.hh"
#include "vdb/common/memory_allocator.hh"
#include "zmalloc.h"

extern "C" {
#ifdef __cplusplus
#include "server.h"
}
#endif

class GlobalEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    server.vdb_active_set_size_limit = 1000;
    /* disable redis server log */
#ifdef _DEBUG_GTEST
    server.verbosity = LL_DEBUG;
#else
    server.verbosity = LL_NOTHING;
#endif
    server.logfile = empty_string.data();
  }

  void TearDown() override {}
};
class AllocatorTestSuite : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    /* all memory must be freed before starting test case */
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
    ASSERT_EQ(zmalloc_used_memory(), 0);
  }
  static void TearDownTestCase() {
    /* all memory must be freed after finish of test case */
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
    ASSERT_EQ(zmalloc_used_memory(), 0);
  }
  void SetUp() override { AllocateTableDictionary(); }

  void TearDown() override { DeallocateTableDictionary(); }

 private:
};

class CommonTest : public AllocatorTestSuite {};
class MakeSharedTest : public AllocatorTestSuite {};
class StdWithAllocatorTest : public AllocatorTestSuite {};
class ArrowMemoryPoolTest : public AllocatorTestSuite {};

TEST_F(CommonTest, NormalVariableTest) {
  {
    size_t expected = 0;
    auto value = static_cast<char *>(AllocateNoPrefix(sizeof(char)));
    value[0] = 'a';
    /* sizeof(char) (1) */
    expected += 1;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, sizeof(char));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int16_t *>(AllocateNoPrefix(sizeof(int16_t)));
    value[0] = 8;
    /* sizeof(int16_t) (2) */
    expected += 2;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, sizeof(int16_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int32_t *>(AllocateNoPrefix(sizeof(int32_t)));
    value[0] = 8;
    /* sizeof(int32_t) (4) */
    expected += 4;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, sizeof(int32_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int64_t *>(AllocateNoPrefix(sizeof(int64_t)));
    value[0] = 8;
    /* sizeof(int64_t) (8) */
    expected += 8;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, sizeof(int64_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<double *>(AllocateNoPrefix(sizeof(double)));
    value[0] = 8.0;
    /* sizeof(double) (8) */
    expected += 8;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, sizeof(double));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}

TEST_F(CommonTest, NormalArrayTest) {
  {
    size_t expected = 0;
    auto value = static_cast<char *>(AllocateNoPrefix(7 * sizeof(char)));
    value[0] = 'a';
    /* 7 * sizeof(char) (7) */
    expected += 7;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, 7 * sizeof(char));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int16_t *>(AllocateNoPrefix(7 * sizeof(int16_t)));
    value[0] = 8;
    /* 7 * sizeof(int16_t) (14) */
    expected += 14;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, 7 * sizeof(int16_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int32_t *>(AllocateNoPrefix(7 * sizeof(int32_t)));
    value[0] = 8;
    /* 7 * sizeof(int32_t) (28) */
    expected += 28;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, 7 * sizeof(int32_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int64_t *>(AllocateNoPrefix(7 * sizeof(int64_t)));
    value[0] = 8;
    /* 7 * sizeof(int64_t) (56) */
    expected += 56;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, 7 * sizeof(int64_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<double *>(AllocateNoPrefix(7 * sizeof(double)));
    value[0] = 8.0;
    /* 7 * sizeof(double) (56) */
    expected += 56;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateNoPrefix(value, 7 * sizeof(double));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}

TEST_F(CommonTest, SizeAwareVariableTest) {
  {
    size_t expected = 0;
    auto value = static_cast<char *>(AllocateWithPrefix(sizeof(char)));
    value[0] = 'a';
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int16_t *>(AllocateWithPrefix(sizeof(int16_t)));
    value[0] = 8;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int32_t *>(AllocateWithPrefix(sizeof(int32_t)));
    value[0] = 8;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int64_t *>(AllocateWithPrefix(sizeof(int64_t)));
    value[0] = 8;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<double *>(AllocateWithPrefix(sizeof(double)));
    value[0] = 8.0;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}
TEST_F(CommonTest, SizeAwareArrayTest) {
  {
    size_t expected = 0;
    auto value = static_cast<char *>(AllocateWithPrefix(7 * sizeof(char)));
    value[0] = 'a';
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value =
        static_cast<int16_t *>(AllocateWithPrefix(7 * sizeof(int16_t)));
    value[0] = 8;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value =
        static_cast<int32_t *>(AllocateWithPrefix(7 * sizeof(int32_t)));
    value[0] = 8;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value =
        static_cast<int64_t *>(AllocateWithPrefix(7 * sizeof(int64_t)));
    value[0] = 8;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<double *>(AllocateWithPrefix(7 * sizeof(double)));
    value[0] = 8.0;
    expected += zmalloc_size(value);
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateWithPrefix(value);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}

TEST_F(CommonTest, AlignedVariableTest) {
  {
    size_t expected = 0;
    auto value = static_cast<char *>(AllocateAligned(64, sizeof(char)));
    value[0] = 'a';
    /* sizeof(char) (1) */
    expected += 1;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, sizeof(char));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int16_t *>(AllocateAligned(64, sizeof(int16_t)));
    value[0] = 8;
    /* sizeof(char) (2) */
    expected += 2;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, sizeof(int16_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int32_t *>(AllocateAligned(64, sizeof(int32_t)));
    value[0] = 8;
    /* sizeof(char) (4) */
    expected += 4;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, sizeof(int32_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<int64_t *>(AllocateAligned(64, sizeof(int64_t)));
    value[0] = 8;
    /* sizeof(char) (8) */
    expected += 8;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, sizeof(int64_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<double *>(AllocateAligned(64, sizeof(double)));
    value[0] = 8.0;
    /* sizeof(char) (8) */
    expected += 8;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, sizeof(double));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}

TEST_F(CommonTest, AlignedArrayTest) {
  {
    size_t expected = 0;
    auto value = static_cast<char *>(AllocateAligned(64, 7 * sizeof(char)));
    value[0] = 'a';
    /* 7 * sizeof(char) (7) */
    expected += 7;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, 7 * sizeof(char));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value =
        static_cast<int16_t *>(AllocateAligned(64, 7 * sizeof(int16_t)));
    value[0] = 8;
    /* 7 * sizeof(int16_t) (14) */
    expected += 14;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, 7 * sizeof(int16_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value =
        static_cast<int32_t *>(AllocateAligned(64, 7 * sizeof(int32_t)));
    value[0] = 8;
    /* 7 * sizeof(int32_t) (28) */
    expected += 28;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, 7 * sizeof(int32_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value =
        static_cast<int64_t *>(AllocateAligned(64, 7 * sizeof(int64_t)));
    value[0] = 8;
    /* 7 * sizeof(int64_t) (56) */
    expected += 56;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, 7 * sizeof(int64_t));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = static_cast<double *>(AllocateAligned(64, 7 * sizeof(double)));
    value[0] = 8.0;
    /* 7 * sizeof(double) (56) */
    expected += 56;
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
    DeallocateAligned(value, 7 * sizeof(double));
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}
TEST_F(MakeSharedTest, NormalVariableTest) {
  {
    size_t expected = 0;
    auto value = vdb::make_shared<char>('a');
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_shared<int16_t>(8);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_shared<int32_t>(8);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_shared<int64_t>(8);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_shared<double>(8.0);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}

TEST_F(MakeSharedTest, SizeAwareVariableTest) {
  {
    size_t expected = 0;
    auto value = vdb::make_size_aware_shared<char>('a');
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_size_aware_shared<int16_t>(8);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_size_aware_shared<int32_t>(8);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_size_aware_shared<int64_t>(8);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  {
    size_t expected = 0;
    auto value = vdb::make_size_aware_shared<double>(8.0);
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
}

TEST_F(MakeSharedTest, AlignedVariableTest) {
  {
    size_t expected = 0;
    auto value = vdb::make_aligned_shared<char>(8, 'a');
    /* alignment bytes in allocator (8) */
    expected += 8;
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  {
    size_t expected = 0;
    auto value = vdb::make_aligned_shared<int16_t>(8, 8);
    /* alignment bytes in allocator (8) */
    expected += 8;
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  {
    size_t expected = 0;
    auto value = vdb::make_aligned_shared<int32_t>(8, 8);
    /* alignment bytes in allocator (8) */
    expected += 8;
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  {
    size_t expected = 0;
    auto value = vdb::make_aligned_shared<int64_t>(8, 8);
    /* alignment bytes in allocator (8) */
    expected += 8;
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
  {
    size_t expected = 0;
    auto value = vdb::make_aligned_shared<double>(8, 8.0);
    /* alignment bytes in allocator (8) */
    expected += 8;
    /* aligned bytes (8) */
    expected += 8;
#ifdef __APPLE__
    /* control block size of shared pointer (24) */
    expected += 24;
#else
    /* control block size of shared pointer (16) */
    expected += 16;
#endif
    ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  }
}

TEST_F(StdWithAllocatorTest, VectorTest) {
  size_t expected = 0;
  vdb::vector<int> vector;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  vector.insert(vector.begin(), 10);
  for (auto val : vector) {
    ASSERT_EQ(val, 10);
  }
  /* sizeof(int) (4) */
  expected += 4;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  vector.reserve(10);
  /* sizeof(int) * 9 (36) */
  expected += 36;
  auto vector_shared = vdb::make_shared<vdb::vector<int>>();
  expected += sizeof(vdb::vector<int>);
#ifdef __APPLE__
  /* control block size of shared pointer (24) */
  expected += 24;
#else
  /* control block size of shared pointer (16) */
  expected += 16;
#endif
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  vector_shared->insert(vector_shared->begin(), 20);
  /* sizeof(int) (4) */
  expected += 4;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  vector_shared->insert(vector_shared->begin(), 30);
  /* sizeof(int) (4) */
  expected += 4;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
}

TEST_F(StdWithAllocatorTest, StringTest) {
  size_t expected = 0;
  vdb::string str;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  str.reserve(100);
  str.append("hello world");
  str.append("hello world");
  str.append("hello world");
  /* reserve save (100) */
  expected += 100;
#ifdef __APPLE__
  expected += 12;
#else
  expected += 1;
#endif

  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  auto str_shared = vdb::make_shared<vdb::string>();
  expected += sizeof(vdb::string);
#ifdef __APPLE__
  /* pre-allocated space in string (8) */
  expected += 8;
  /* control block size of shared pointer (24) */
  expected += 24;
#else
  /* control block size of shared pointer (16) */
  expected += 16;
#endif
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  str_shared->reserve(100);
  str_shared->append("goodbye world");
  str_shared->append("goodbye world");
  str_shared->append("goodbye world");
  expected += 100;
#ifdef __APPLE__
  expected += 12;
#else
  expected += 1;
#endif
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
}

TEST_F(StdWithAllocatorTest, MapTest) {
  size_t expected = 0;
  vdb::map<int, int> map;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  map.insert(std::pair<int, int>(1, 2));
  expected += 40;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  map.insert(std::pair<int, int>(2, 3));
  expected += 40;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  auto map_shared = vdb::make_shared<vdb::map<int, int>>();
#ifdef __APPLE__
  expected += 48;
#else
  expected += 64;
#endif
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
  map_shared->insert(std::pair<int, int>(1, 2));
  expected += 40;
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), expected);
}

TEST_F(ArrowMemoryPoolTest, BuilderTest) {
  std::string table_schema_string =
      "Id int32, Name String, Height float32, feature "
      "Fixed_Size_List[256, Float32 ]";
  auto schema = vdb::ParseSchemaFrom(table_schema_string);
  ASSERT_EQ(vdb::GetVdbAllocatedSize(), 0);
  auto maybe_rb = GenerateRecordBatch(schema, 9700, 256);
  ASSERT_NE(vdb::GetVdbAllocatedSize(), 0);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  ::testing::AddGlobalTestEnvironment(new GlobalEnvironment);
  return RUN_ALL_TESTS();
}
