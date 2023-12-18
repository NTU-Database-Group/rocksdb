#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"

#include <iostream>

namespace ROCKSDB_NAMESPACE {
using node_id_t = uint64_t;
using edge_id_t = uint64_t;

#define KEY_TYPE_ADJENCENT_LIST 0x0
#define KEY_TYPE_VERTEX_VAL 0x1

// a 4 byte value
union Value {
  int val;
  float fval;
};

// 4+8=12byte
struct Edge {
  Value val;
  node_id_t nxt;
};

// 4+12x bytes
struct Edges {
  uint32_t num_edges;
  Edge* nxts;
};

// 8 + 4 byte
struct VertexKey {
  node_id_t id;
  int type;
};

void decode_node(VertexKey* v, const std::string& key);
void decode_edges(Edges* edges, const std::string& value);
void encode_node(VertexKey v, std::string* key);
void encode_edge(const Edge* edge, std::string* value);
void encode_edges(const Edges* edges, std::string* value);

void encode_node(VertexKey v, std::string* key) {
  // key->append(reinterpret_cast<const char*>(&v), sizeof(VertexKey));
  int byte_to_fill = sizeof(v.id);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    key->push_back((v.id >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
  byte_to_fill = sizeof(v.type);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    key->push_back((v.type >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
}

void decode_node(VertexKey* v, const std::string& key) {
  *v = *reinterpret_cast<const VertexKey*>(key.data());
}

void encode_edge(const Edge* edge, std::string* value) {
  int byte_to_fill = sizeof(Value);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edge->val.val >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
  byte_to_fill = sizeof(edge->nxt);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edge->nxt >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
}

void encode_edges(const Edges* edges, std::string* value) {
  // copy the number of edges
  // value->append(reinterpret_cast<const char*>(edges), sizeof(int) + edges->num_edges * sizeof(Edge));
  int byte_to_fill = sizeof(edges->num_edges);
  for (int i = byte_to_fill - 1; i >= 0; i--) {
    value->push_back((edges->num_edges >> ((byte_to_fill - i - 1) << 3)) & 0xFF);
  }
  // std::cout << "num_edges: " << edges->num_edges << std::endl;
  // Edges n;
  // decode_edges(&n, *value);
  // std::cout << "num_edges: " << n.num_edges << std::endl;
  // exit(0);
  for (uint32_t i = 0; i < edges->num_edges; i++) {
    encode_edge(&edges->nxts[i], value);
  }
}

void decode_edges(Edges* edges, const std::string& value) {
  edges->num_edges = *reinterpret_cast<const uint32_t*>(value.data());
  edges->nxts = new Edge[edges->num_edges];
  memcpy(edges->nxts, value.data() + sizeof(int), edges->num_edges * sizeof(Edge));
}

void free_edges(Edges* edges) {
  delete[] edges->nxts;
}

class RocksGraph {
 public:
  class AdjacentListMergeOp : public AssociativeMergeOperator {
   public:
    virtual ~AdjacentListMergeOp() {}
    virtual bool Merge(const Slice& key, const Slice* existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const override;
    virtual const char* Name() const override {
      return "AdjacentListMergeOp";
    }
  };
  RocksGraph(Options& options, bool lazy=true): is_lazy_(lazy) {
    if (lazy) {
      options.merge_operator.reset(new AdjacentListMergeOp);
    }
    options.create_missing_column_families = true;
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back(kDefaultColumnFamilyName, options);
    column_families.emplace_back("vertex_val", options);
    std::vector<ColumnFamilyHandle*> handles;
    Status s = DB::Open(options, "/tmp/db", column_families, &handles, &db_); 
    if (!s.ok()) {
      std::cout << s.ToString() << std::endl;
      exit(1);
    }
    adj_cf_ = handles[0];
    val_cf_ = handles[1];
  }
  ~RocksGraph() {
    db_->DestroyColumnFamilyHandle(adj_cf_);
    db_->DestroyColumnFamilyHandle(val_cf_);
    db_->Close();
    delete db_;
  }
  Status AddEdge(node_id_t from, node_id_t to, Value edge_val=Value{.val=0});
  Status GetAllEdges(node_id_t src, Edges* edges);
  Status GetVertexVal(node_id_t id, Value* val);
  Status SetVertexVal(node_id_t id, Value val);
  Status SimpleWalk(node_id_t start, float decay_factor=0.20);
  void GetRocksDBStats(std::string& stat) {
    db_->GetProperty("rocksdb.stats", &stat);
  }
 private:
  node_id_t random_walk(node_id_t start, float decay_factor=0.20);
  DB* db_;
  bool is_lazy_;
  ColumnFamilyHandle* val_cf_, *adj_cf_;
};

RocksGraph* CreateRocksGraph(Options& options, bool is_lazy=true) {
  return new RocksGraph(options, is_lazy);
}

}
