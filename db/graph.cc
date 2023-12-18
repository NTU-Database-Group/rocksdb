#include "rocksdb/graph.h"

namespace ROCKSDB_NAMESPACE {

bool RocksGraph::AdjacentListMergeOp::Merge(const Slice& key, const Slice* existing_value,
                                            const Slice& value, std::string* new_value,
                                            Logger* logger) const {
  if (!existing_value) {
    *new_value = value.ToString();
    return true;
  }
  Edges new_edges, existing_edges, merged_edges;
  decode_edges(&new_edges, value.ToString());
  decode_edges(&existing_edges, existing_value->ToString());
  merged_edges.num_edges = existing_edges.num_edges + new_edges.num_edges;
  auto merge_edges_list = new Edge[merged_edges.num_edges];
  memcpy(merge_edges_list, existing_edges.nxts, existing_edges.num_edges * sizeof(Edge));
  memcpy(merge_edges_list + existing_edges.num_edges, new_edges.nxts, new_edges.num_edges * sizeof(Edge));
  merged_edges.nxts = merge_edges_list;
  free_edges(&existing_edges);
  free_edges(&new_edges);
  encode_edges(&new_edges, new_value);
  free_edges(&merged_edges);
  return true;
}

node_id_t RocksGraph::random_walk(node_id_t start, float decay_factor) {
  node_id_t cur = start;
  for(;;) {
    Edges edges;
    std::string key;
    std::string value;
    encode_node(VertexKey{.id=cur, .type=KEY_TYPE_ADJENCENT_LIST}, &key);
    Status s = db_->Get(ReadOptions(), adj_cf_, key, &value);
    if (!s.ok()) {
      return 0;
    }
    decode_edges(&edges, value);
    if (edges.num_edges == 0) {
      return cur;
    }
    float r = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
    if (r < decay_factor) {
      return cur;
    }
    int idx = rand() % edges.num_edges;
    cur = edges.nxts[idx].nxt;
    free_edges(&edges);
  }
}

Status RocksGraph::AddEdge(node_id_t from, node_id_t to, Value edge_val) {
  VertexKey v{.id=from, .type=KEY_TYPE_ADJENCENT_LIST};
  std::string key, value;
  encode_node(v, &key);  
  if (is_lazy_) {
    Edges edges{.num_edges=1};
    edges.nxts = new Edge[1];
    encode_edges(&edges, &value);
    free_edges(&edges);
    return db_->Merge(WriteOptions(), adj_cf_, key, value);
  }
  Edges cur_edges{.num_edges=0};
  Status s = GetAllEdges(from, &cur_edges);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  Edges new_edges{.num_edges=cur_edges.num_edges + 1};
  new_edges.nxts = new Edge[new_edges.num_edges];
  // copy nothing if cur_edges.num_edges == 0
  memcpy(new_edges.nxts, cur_edges.nxts, cur_edges.num_edges * sizeof(Edge));
  new_edges.nxts[cur_edges.num_edges] = Edge{.val=edge_val, .nxt=to};
  std::string new_value;
  encode_edges(&new_edges, &new_value);
  free_edges(&cur_edges);
  free_edges(&new_edges);
  return db_->Put(WriteOptions(), adj_cf_, key, new_value);
}

Status RocksGraph::GetAllEdges(node_id_t src, Edges* edges) {
  VertexKey v{.id=src, .type=KEY_TYPE_ADJENCENT_LIST};
  std::string key;
  encode_node(v, &key);
  std::string value;
  Status s = db_->Get(ReadOptions(), adj_cf_, key, &value);
  if (!s.ok()) {
    return s;
  }
  decode_edges(edges, value);
  return Status::OK();
}

Status RocksGraph::GetVertexVal(node_id_t id, Value* val) {
  VertexKey v{.id=id, .type=KEY_TYPE_VERTEX_VAL};
  std::string key;
  encode_node(v, &key);
  std::string value;
  Status s = db_->Get(ReadOptions(), val_cf_, key, &value);
  if (!s.ok()) {
    return s;
  }
  *val = *reinterpret_cast<const Value*>(value.data());
  return Status::OK();
}

Status RocksGraph::SetVertexVal(node_id_t id, Value val) {
  VertexKey v{.id=id, .type=KEY_TYPE_VERTEX_VAL};
  std::string key;
  encode_node(v, &key);
  std::string value;
  value.append(reinterpret_cast<const char*>(&val), sizeof(Value));
  return db_->Put(WriteOptions(), val_cf_, key, value);
}

Status RocksGraph::SimpleWalk(node_id_t start, float decay_factor) {
  random_walk(start, decay_factor);
  return Status::OK();
}

} // namespace ROCKSDB_NAMESPACE