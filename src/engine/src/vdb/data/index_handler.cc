#include <algorithm>
#include <exception>
#include <filesystem>
#include <stdexcept>
#include <strings.h>
#include <system_error>
#include <vector>

#include "hnswlib/hnswalg.h"
#include "hnswlib/space_ip.h"
#include "hnswlib/space_l2.h"

#include "vdb/common/status.hh"
#include "vdb/common/system_log.hh"
#include "vdb/common/util.hh"
#include "vdb/data/index_handler.hh"
#include "vdb/data/label_info.hh"
#include "vdb/data/table.hh"

#ifdef __cplusplus
extern "C" {
#include "server.h"
}
#endif

namespace vdb {
Hnsw::Hnsw(Metric space, size_t dim, size_t ef_construction, size_t M,
           size_t max_elem) {
  if (space == Hnsw::kIPSpace) {
    space_ = vdb::make_shared<hnswlib::InnerProductSpace>(dim);
  } else {
    space_ = vdb::make_shared<hnswlib::L2Space>(dim);
  }
  index_ = vdb::make_shared<hnswlib::HierarchicalNSW<float>>(
      space_.get(), max_elem, M, ef_construction);
}

Hnsw::Hnsw(const std::string &file_path, Metric metric, size_t dim) {
  if (metric == Hnsw::kIPSpace) {
    space_ = vdb::make_shared<hnswlib::InnerProductSpace>(dim);
  } else {
    space_ = vdb::make_shared<hnswlib::L2Space>(dim);
  }
  index_ = vdb::make_shared<hnswlib::HierarchicalNSW<float>>(space_.get(),
                                                             file_path);
}

Status Hnsw::AddEmbedding(const float *embedding, size_t label) {
  try {
    index_->addPoint(embedding, label);
    return Status::Ok();
  } catch (std::exception &e) {
    return Status(Status::kInvalidArgument, e.what());
  }
}

std::priority_queue<std::pair<float, size_t>> Hnsw::SearchKnn(
    const float *query, size_t k) {
  try {
    return index_->searchKnn(query, k);
  } catch (std::exception &e) {
    std::cerr << e.what() << std::endl;
    return {};
  }
}

Status Hnsw::Save(const std::string &file_path) const {
  index_->saveIndex(file_path);
  return Status::Ok();
}

bool Hnsw::IsFull() const {
  return index_->getCurrentElementCount() == index_->getMaxElements();
}

size_t Hnsw::Dimension() const {
  return *(reinterpret_cast<size_t *>(space_->get_dist_func_param()));
}

size_t Hnsw::Size() const { return index_->getCurrentElementCount(); }

size_t Hnsw::CompleteSize() const { return index_->getCompleteElementCount(); }

size_t Hnsw::MaxSize() const { return index_->getMaxElements(); }

std::string Hnsw::ToString(bool show_embeddings, bool show_edges) const {
  std::stringstream ss;
  ss << "offsetLevel0 =" << index_->offsetLevel0_ << std::endl;
  ss << "max_elements =" << index_->max_elements_ << std::endl;
  ss << "cur_element_count =" << index_->cur_element_count << std::endl;
  ss << "size_data_per_element =" << index_->size_data_per_element_
     << std::endl;
  ss << "label_offset =" << index_->label_offset_ << std::endl;
  ss << "offsetData =" << index_->offsetData_ << std::endl;
  ss << "maxlevel =" << index_->maxlevel_ << std::endl;
  ss << "enterpoint_node =" << index_->enterpoint_node_ << std::endl;
  ss << "maxM =" << index_->maxM_ << std::endl;

  ss << "maxM0 =" << index_->maxM0_ << std::endl;
  ss << "M =" << index_->M_ << std::endl;
  ss << "mult =" << index_->mult_ << std::endl;
  ss << "ef_construction =" << index_->ef_construction_ << std::endl;

  if (show_embeddings) {
    ss << "show embeddings (count=" << Size() << ")" << std::endl;
    for (size_t i = 0; i < Size(); i++) {
      if (show_edges) {
        ss << i << "th element level =" << index_->element_levels_[i]
           << std::endl;
      }
      auto embedding = GetEmbedding(i);
      if (embedding != nullptr) {
        ss << VectorIndex::EmbeddingToString(embedding, Dimension());
      } else {
        ss << "null";
      }
      if (i != Size() - 1) {
        ss << ", ";
      }
      ss << std::endl;
    }
  } else {
    ss << Size() << " embeddings are not shown. " << std::endl;
  }
  return ss.str();
}

const float *Hnsw::GetEmbedding(const size_t label) const {
  float *point = reinterpret_cast<float *>(index_->getDataByInternalId(label));
  return point;
}

VectorIndex::Type GetIndexType(const std::string &type_string) {
  if (TransformToLower(type_string) == "hnsw") {
    return VectorIndex::Type::kHnsw;
  } else {
    return VectorIndex::Type::kIndexTypeMax;
  }
}

IndexHandler::IndexHandler(std::shared_ptr<vdb::Table> table)
    : table_{table}, indexes_{} {
  auto status = CreateIndex();
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
  }
}

IndexHandler::IndexHandler(std::shared_ptr<vdb::Table> table,
                           std::string &directory_path,
                           const uint64_t index_count)
    : table_{table} {
  /* load vector indexes */
  auto metadata = table->GetSchema()->metadata();
  auto maybe_index_type = metadata->Get("index_type");
  if (!maybe_index_type.status().ok()) {
    throw std::invalid_argument(maybe_index_type.status().ToString());
  }
  auto index_type = GetIndexType(maybe_index_type.ValueUnsafe());
  Status status;
  switch (index_type) {
    case VectorIndex::kHnsw:
      status = LoadHnswIndexes(directory_path, index_count);
      if (!status.ok()) {
        throw std::invalid_argument(status.ToString());
      }
      break;
    default:
      throw std::invalid_argument("Unknown Index Type");
      break;
  }
}

Status IndexHandler::Save(std::string &directory_path) {
  /* save vector indexes */
  if (!std::filesystem::exists(directory_path)) {
    std::filesystem::create_directory((directory_path));
  }
  for (size_t i = 0; i < Size(); i++) {
    std::string index_file_path(directory_path);
    index_file_path.append("/index.");
    index_file_path.append(std::to_string(i));
    auto index = Index(i);
    SYSTEM_LOG(LogLevel::kLogDebug,
               "[INDEX_SAVE] set number: %u, curernt element count: %lu, max "
               "element count: %lu",
               i, index->Size(), index->MaxSize());
    auto status = index->Save(index_file_path);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

Hnsw::Metric GetIndexMetric(const std::string &metric_string) {
  if (TransformToLower(metric_string) == "ipspace") {
    return Hnsw::Metric::kIPSpace;
  } else {
    return Hnsw::Metric::kL2Space;
  }
}

Status IndexHandler::LoadHnswIndex(std::string &index_file_path,
                                   const Hnsw::Metric metric,
                                   const int32_t dimension) {
  indexes_.emplace_back(
      vdb::make_shared<Hnsw>(index_file_path, metric, dimension));
  return Status::Ok();
}

Status IndexHandler::LoadHnswIndex(std::string &file_path) {
  auto table = table_.lock();
  auto metadata = table->GetSchema()->metadata();
  auto maybe_ann_column_id = metadata->Get("ann_column_id");
  if (!maybe_ann_column_id.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_ann_column_id.status().ToString());
  }
  size_t ann_column_id = std::stoull(maybe_ann_column_id.ValueUnsafe());

  size_t dimension = std::static_pointer_cast<arrow::FixedSizeListType>(
                         table->GetSchema()->field(ann_column_id)->type())
                         ->list_size();
  auto maybe_index_metric = metadata->Get("index_space");
  if (!maybe_index_metric.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_index_metric.status().ToString());
  }
  Hnsw::Metric index_metric = GetIndexMetric(maybe_index_metric.ValueUnsafe());
  return LoadHnswIndex(file_path, index_metric, dimension);
}

Status IndexHandler::LoadHnswIndexes(std::string &directory_path,
                                     const uint64_t index_count) {
  auto table = table_.lock();
  auto metadata = table->GetSchema()->metadata();
  auto maybe_ann_column_id = metadata->Get("ann_column_id");
  if (!maybe_ann_column_id.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_ann_column_id.status().ToString());
  }
  size_t ann_column_id = std::stoull(maybe_ann_column_id.ValueUnsafe());

  size_t dimension = std::static_pointer_cast<arrow::FixedSizeListType>(
                         table->GetSchema()->field(ann_column_id)->type())
                         ->list_size();
  auto maybe_index_metric = metadata->Get("index_space");
  if (!maybe_index_metric.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_index_metric.status().ToString());
  }
  Hnsw::Metric index_metric = GetIndexMetric(maybe_index_metric.ValueUnsafe());
  for (uint64_t i = 0; i < index_count; i++) {
    std::string index_file_path(directory_path);
    index_file_path.append("/index.");
    index_file_path.append(std::to_string(i));
    auto status = LoadHnswIndex(index_file_path, index_metric, dimension);
    SYSTEM_LOG(LogLevel::kLogDebug,
               "[INDEX_LOAD] set number: %u, current element count: %lu, max "
               "element count: %lu",
               i, indexes_[i]->Size(), indexes_[i]->MaxSize());
    if (!status.ok()) {
      return status;
    }
  }
  return Status::Ok();
}

Status IndexHandler::CreateHnswIndex() {
  auto table = table_.lock();
  auto metadata = table->GetSchema()->metadata();

  auto maybe_ann_column_id = metadata->Get("ann_column_id");
  if (!maybe_ann_column_id.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_ann_column_id.status().ToString());
  }
  size_t ann_column_id = std::stoull(maybe_ann_column_id.ValueUnsafe());

  size_t dim = std::static_pointer_cast<arrow::FixedSizeListType>(
                   table->GetSchema()->field(ann_column_id)->type())
                   ->list_size();

  auto maybe_index_metric = metadata->Get("index_space");
  if (!maybe_index_metric.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_index_metric.status().ToString());
  }
  Hnsw::Metric index_metric = GetIndexMetric(maybe_index_metric.ValueUnsafe());

  auto maybe_M = metadata->Get("M");
  if (!maybe_M.status().ok()) {
    return Status(Status::kInvalidArgument, maybe_M.status().ToString());
  }
  size_t M = std::stoull(maybe_M.ValueUnsafe());

  auto maybe_ef_construction = metadata->Get("ef_construction");
  if (!maybe_ef_construction.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_ef_construction.status().ToString());
  }
  size_t ef_construction = std::stoull(maybe_ef_construction.ValueUnsafe());

  /* index size is same as set size. */
  auto max_elements = table->GetActiveSetSizeLimit();

  auto index = vdb::make_shared<Hnsw>(index_metric, dim, ef_construction, M,
                                      max_elements);

  indexes_.push_back(index);
  return Status::Ok();
}

Status IndexHandler::CreateIndex() {
  auto table = table_.lock();
  auto metadata = table->GetSchema()->metadata();
  auto maybe_index_type = metadata->Get("index_type");
  if (!maybe_index_type.status().ok()) {
    return Status(Status::kInvalidArgument,
                  maybe_index_type.status().ToString());
  }
  auto index_type = GetIndexType(maybe_index_type.ValueUnsafe());
  switch (index_type) {
    case VectorIndex::kHnsw:
      return CreateHnswIndex();
    default:
      return Status(Status::kInvalidArgument, "Unknown Index Type");
  }
}

Status IndexHandler::AddEmbedding(const float *embedding, size_t label) {
  auto index = indexes_.back();
  if (index->IsFull()) {
    return Status(Status::kInvalidArgument,
                  "index must not be full when inserting embedding. ");
  }
  return index->AddEmbedding(embedding, label);
}

Status IndexHandler::AddEmbedding(const float *embedding, size_t starting_label,
                                  size_t size) {
  auto table = table_.lock();
  auto index = indexes_[LabelInfo::GetSetNumber(starting_label)];
  if (index->IsFull()) {
    return Status(Status::kInvalidArgument,
                  "index must not be full when inserting embedding. ");
  }

  {  // job queue lock scope
    SYSTEM_LOG(LogLevel::kLogDebug, "Adding new job. (%s, %lu)",
               LabelInfo::ToString(starting_label).data(), size);
    std::lock_guard<std::mutex> guard(table->job_queue_mtx_);
    table->job_queue_.emplace_back(index, const_cast<float *>(embedding),
                                   starting_label, size);
    table->indexing_ = true;
  }  // end of job queue lock scope
  table->cond_.notify_all();
  return Status::Ok();
}

const float *IndexHandler::GetEmbedding(const size_t label) const {
  size_t start_label = 0;
  for (auto index : indexes_) {
    if (start_label <= label && label < start_label + index->MaxSize()) {
      size_t label_in_idx = label - start_label;
      return index->GetEmbedding(label_in_idx);
    }
    start_label += index->MaxSize();
  }
  return nullptr;
}

std::pair<float, size_t> PopFarthestFrom(
    std::list<std::priority_queue<std::pair<float, size_t>>> &sub_results) {
  float global_farthest = sub_results.begin()->top().first;
  auto farthest_iterator = sub_results.begin();
  for (auto iter = sub_results.begin(); iter != sub_results.end(); ++iter) {
    auto local_farthest = iter->top().first;
    if (local_farthest > global_farthest) {
      global_farthest = local_farthest;
      farthest_iterator = iter;
    }
  }

  auto farthest_element = farthest_iterator->top();
  farthest_iterator->pop();
  if (farthest_iterator->empty()) {
    sub_results.erase(farthest_iterator);
  }

  return farthest_element;
}

std::shared_ptr<std::vector<std::pair<float, size_t>>> IndexHandler::SearchKnn(
    const float *query, const size_t &k) {
  std::list<std::priority_queue<std::pair<float, size_t>>> sub_results;
  size_t embedding_count = 0;

  SYSTEM_LOG(LogLevel::kLogVerbose,
             "SearchKnn: Start finding %ld nearest neighbors", k);
  /* Collect sub-results from all indexes */
  for (size_t i = 0; i < indexes_.size(); i++) {
    auto index = indexes_[i];
    auto sub_result = index->SearchKnn(query, k);
    if (!sub_result.empty()) {
      sub_results.push_back(sub_result);
      embedding_count += sub_results.back().size();
    }
    SYSTEM_LOG(LogLevel::kLogVerbose,
               "SearchKnn: %ld embeddings are collected from %ld th index",
               sub_result.size(), i);
  }
  SYSTEM_LOG(LogLevel::kLogVerbose,
             "SearchKnn: %ld nearest neighbors are found from all indexes",
             embedding_count);

  /* Retain only the top k elements, and remove the rest */
  if (embedding_count > k) {
    SYSTEM_LOG(LogLevel::kLogVerbose,
               "SearchKnn: retain only top %ld neighbors (%ld -> %ld)", k,
               embedding_count, k);
    while (embedding_count > k) {
      PopFarthestFrom(sub_results);
      embedding_count--;
    }
  }

  auto result =
      std::make_shared<std::vector<std::pair<float, size_t>>>(embedding_count);

  /* Merge the sub-results into the main results */
  SYSTEM_LOG(LogLevel::kLogVerbose,
             "SearchKnn: Start print nearest neighbors (%ld embeddings)",
             embedding_count);
  for (int i = embedding_count - 1; i >= 0; i--) {
    (*result)[i] = PopFarthestFrom(sub_results);
    SYSTEM_LOG(LogLevel::kLogVerbose,
               "SearchKnn: %d th embedding information (dist=%f, label=%s)", i,
               (*result)[i].first,
               LabelInfo::ToString((*result)[i].second).data());
  }
  SYSTEM_LOG(LogLevel::kLogVerbose,
             "SearchKnn: End print nearest neighbors (%ld embeddings)",
             embedding_count);

  SYSTEM_LOG(LogLevel::kLogVerbose,
             "SearchKnn: Done finding %ld nearest neighbors. %ld embeddings "
             "are found.",
             k, result->size());
  return result;
}

std::shared_ptr<VectorIndex> IndexHandler::Index(size_t N) const {
  if (indexes_.size() <= N) return nullptr;
  return indexes_[N];
}

size_t IndexHandler::Size() const { return indexes_.size(); }

std::string IndexHandler::ToString(bool show_embeddings,
                                   bool show_edges) const {
  std::stringstream ss;
  ss << "Index Handler " << std::endl;
  ss << "Index Count=" << Size() << std::endl;
  for (size_t i = 0; i < indexes_.size(); i++) {
    auto index = Index(i);
    ss << i << "th Index" << std::endl;
    ss << index->ToString(show_embeddings, show_edges) << std::endl;
  }
  return ss.str();
}

size_t IndexHandler::CountIndexedElements() const {
  size_t total_count = 0;
  for (size_t i = 0; i < indexes_.size(); i++) {
    total_count += indexes_[i]->Size();
  }
  return total_count;
}

}  // namespace vdb
