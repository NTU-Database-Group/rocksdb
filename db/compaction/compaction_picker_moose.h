
#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {
class MooseCompactionPicker : public CompactionPicker {
 public:
  MooseCompactionPicker(const ImmutableOptions& ioptions,
                      const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}
  virtual Compaction* PickCompaction(const std::string& cf_name,
                                     const MutableCFOptions& mutable_cf_options,
                                     const MutableDBOptions& mutable_db_options,
                                     VersionStorageInfo* vstorage,
                                     LogBuffer* log_buffer) override;

  virtual bool NeedsCompaction(
      const VersionStorageInfo* vstorage) const override;
};

}  // namespace ROCKSDB_NAMESPACE