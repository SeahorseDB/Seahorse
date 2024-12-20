#pragma once

#include <memory>
#include <stdint.h>
#include <string>
#include <sstream>
#include "hnswlib/hnswlib.h"

#include "vdb/common/status.hh"

namespace vdb {

class Table;
/* TODO index manager
 * memory manager of indexes */
class VectorIndex {
 public:
  enum Type { kHnsw, kIndexTypeMax };
  /* destructor */
  virtual ~VectorIndex() = default;
  virtual std::priority_queue<std::pair<float, size_t>> SearchKnn(
      const float *query, size_t k) = 0;
  virtual Status AddEmbedding(const float *embedding, size_t label) = 0;

  virtual const float *GetEmbedding(const size_t label) const = 0;

  virtual Status Save(const std::string &index_file_path) const = 0;
  virtual bool IsFull() const = 0;
  virtual size_t Dimension() const = 0;
  virtual size_t Size() const = 0;
  virtual size_t CompleteSize() const = 0;
  virtual size_t MaxSize() const = 0;

  virtual std::string ToString(bool show_embeddings = true,
                               bool show_edges = true) const = 0;
  /* TODO */
  /* memory usage */

  static std::string EmbeddingToString(const float *embedding,
                                       size_t dimension) {
    std::stringstream ss;
    ss << "[ ";
    if (embedding != nullptr) {
      for (size_t i = 0; i < dimension; i++) {
        ss << embedding[i];
        if (i != dimension - 1) {
          ss << ", ";
        }
      }
    } else {
      ss << "null";
    }
    ss << " ]";
    return ss.str();
  }

 private:
};

class Hnsw : public VectorIndex {
 public:
  enum Metric { kIPSpace, kL2Space };
  /* constructor */
  explicit Hnsw(Metric space, size_t dim, size_t ef_construction, size_t M,
                size_t max_elem);
  explicit Hnsw(const std::string &index_file_path, Metric space, size_t dim);

  std::priority_queue<std::pair<float, size_t>> SearchKnn(const float *query,
                                                          size_t k);
  Status AddEmbedding(const float *embedding, size_t label);

  const float *GetEmbedding(const size_t label) const;

  Status Save(const std::string &index_file_path) const;
  bool IsFull() const;
  size_t Size() const;
  size_t CompleteSize() const;
  size_t Dimension() const;
  size_t MaxSize() const;

  std::string ToString(bool show_embeddings, bool show_edges) const;
  /* TODO */
  /* memory usage */

 private:
  std::shared_ptr<hnswlib::SpaceInterface<float>> space_;
  std::shared_ptr<hnswlib::HierarchicalNSW<float>> index_;
};

/* IndexHandler */
class IndexHandler {
 public:
  /* constructor */
  explicit IndexHandler(std::shared_ptr<vdb::Table> table);
  /* load from snapshot */
  explicit IndexHandler(std::shared_ptr<vdb::Table> table,
                        std::string &index_directory_path,
                        const uint64_t index_count);

  /* save as snaphot */
  Status Save(std::string &index_directory_path);

  /* load index from snapshot */
  Status LoadHnswIndexes(std::string &index_directory_path,
                         const uint64_t index_count);

  Status LoadHnswIndex(std::string &index_file_path);
  Status LoadHnswIndex(std::string &index_file_path, const Hnsw::Metric metric,
                       const int32_t dimension);

  /* CreateIndex */
  Status CreateHnswIndex();
  Status CreateIndex();

  /* searchKNN */
  std::shared_ptr<std::vector<std::pair<float, size_t>>> SearchKnn(
      const float *query, const size_t &k);
  Status AddEmbedding(const float *embedding, size_t label);
  Status AddEmbedding(const float *embedding, size_t starting_label,
                      size_t size);

  const float *GetEmbedding(const size_t label) const;
  std::shared_ptr<VectorIndex> Index(size_t N) const;
  size_t Size() const;
  std::string ToString(bool show_embeddings = true,
                       bool show_edges = true) const;
  size_t CountIndexedElements() const;

  /* TODO */
  /* async AddEmbedding (queueing) */
 protected:
  std::weak_ptr<vdb::Table> table_;
  vdb::vector<std::shared_ptr<VectorIndex>> indexes_;
};
}  // namespace vdb
