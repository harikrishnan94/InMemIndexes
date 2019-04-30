#pragma once

#include "core/db.h"
#include "utils/properties.h"

#include <string>
#include <vector>

namespace ycsbc {
template <bool ScanSupported, typename MapType>
class ConcurrentMapDB : public DB {
public:
  void Init() {}

  int Read(const std::string &table, const std::string &key,
           const DB::FieldSet *fields, int field_count,
           std::vector<KVPair> &result) {
    std::string keyind = table + key;

    return Read(keyind, fields, field_count, result);
  }

  int Scan(const std::string &table, const std::string &key, int record_count,
           const DB::FieldSet *fields, int field_count,
           std::vector<std::vector<KVPair>> &result) {
    if constexpr (ScanSupported) {
      std::string keyind = table + key;

      return Scan(keyind, record_count, fields, field_count, result);
    } else {
      throw "Scan: function not implemented!";
    }
  }

  int Update(const std::string &table, const std::string &key, int field_count,
             DB::FieldMap &values) {
    std::string keyind = table + key;

    return Update(keyind, field_count, values);
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return insert(table + key, values);
  }

  int Delete(const std::string &table, const std::string &key) {
    std::string keyind = table + key;
    KVPair *fields = nullptr;

    fields = Delete(key);

    if (fields)
      delete[] fields;

    return fields ? DB::kOK : DB::kErrorNoData;
  }

private:
  MapType db;

  int Read(std::string &key, const DB::FieldSet *fields, int field_count,
           std::vector<KVPair> &result) {
    auto val = db.Search(key);

    if (!val)
      return DB::kErrorNoData;

    result.clear();
    get_fields(result, fields, field_count, *val);

    return DB::kOK;
  }

  int Scan(std::string &key, int record_count, const DB::FieldSet *fields,
           int field_count, std::vector<std::vector<KVPair>> &result) {
    result.clear();

    for (auto it = db.lower_bound(key); it != db.end() && record_count;
         ++it, --record_count) {
      result.push_back(get_fields(fields, field_count, it->second));
    }

    return DB::kOK;
  }

  int Update(const std::string &key, int field_count, DB::FieldMap &values) {
    auto val = db.Search(key);

    if (!val)
      return insert(key, values);

    KVPair *fields = new KVPair[field_count];
    int num_updates = values.size();

    std::copy(*val, *val + field_count, fields);

    for (int i = 0; (i < field_count) && num_updates; i++) {
      KVPair &field = fields[i];
      auto it = values.find(field.first);

      if (it != values.end()) {
        field.second = it->second;
        num_updates--;
      }
    }

    return DB::kOK;
  }

  KVPair *Delete(const std::string &key) {
    auto val = db.Delete(key);

    return val ? *val : nullptr;
  }

  template <typename Cont> int insert(const std::string &key, Cont &values) {
    KVPair *fields = new KVPair[values.size()];

    std::copy(std::begin(values), std::end(values), fields);

    return db.Insert(key, fields) ? DB::kOK : DB::kErrorConflict;
  }

  std::vector<KVPair> get_fields(const DB::FieldSet *fields, int field_count,
                                 const KVPair *fieldvec) {
    std::vector<KVPair> result;

    get_fields(result, fields, field_count, fieldvec);

    return result;
  }

  void get_fields(std::vector<KVPair> &result, const DB::FieldSet *fields,
                  int field_count, const KVPair *fieldvec) {
    if (!fields) {
      result = std::vector<KVPair>{fieldvec, fieldvec + field_count};
    } else {
      int num_field_reads = fields->size();

      for (int i = 0; (i < field_count) && num_field_reads; i++) {
        const KVPair &field = fieldvec[i];

        if (fields->count(field.first)) {
          result.push_back(field);
          num_field_reads--;
        }
      }
    }
  }
};

} // namespace ycsbc
