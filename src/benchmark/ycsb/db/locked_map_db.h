#pragma once

#include "core/db.h"
#include "utils/properties.h"

#include <mutex>
#include <string>
#include <vector>

namespace ycsbc {
static constexpr bool SCAN = true;
static constexpr bool NOSCAN = false;

template <bool ScanSupported, template <typename...> class MapType>
class LockedMapDB : public DB {
public:
  void Init() {}

  int Read(const std::string &table, const std::string &key,
           const DB::FieldSet *fields, int field_count,
           std::vector<KVPair> &result) {
    std::string keyind = table + key;
    std::lock_guard<std::mutex> lock(mutex);

    return Read(keyind, fields, field_count, result);
  }

  int Scan(const std::string &table, const std::string &key, int record_count,
           const DB::FieldSet *fields, int field_count,
           std::vector<std::vector<KVPair>> &result) {
    if constexpr (ScanSupported) {
      std::string keyind = table + key;
      std::lock_guard<std::mutex> lock(mutex);

      return Scan(keyind, record_count, fields, field_count, result);
    } else {
      throw "Scan: function not implemented!";
    }
  }

  int Update(const std::string &table, const std::string &key, int field_count,
             DB::FieldMap &values) {
    std::string keyind = table + key;
    std::lock_guard<std::mutex> lock(mutex);

    return Update(keyind, field_count, values);
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return insert(table + key, values, false);
  }

  int Delete(const std::string &table, const std::string &key) {
    std::string keyind = table + key;
    KVPair *fields = nullptr;
    std::lock_guard<std::mutex> lock(mutex);

    fields = Delete(key);

    if (fields)
      delete[] fields;

    return fields ? DB::kOK : DB::kErrorNoData;
  }

private:
  MapType<std::string, KVPair *> db;
  std::mutex mutex;

  int Read(std::string &key, const DB::FieldSet *fields, int field_count,
           std::vector<KVPair> &result) {
    auto it = db.find(key);

    if (it == db.end())
      return DB::kErrorNoData;

    result.clear();
    get_fields(result, fields, field_count, it->second);

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
    auto it = db.find(key);

    if (it == db.end())
      return insert(key, values, true);

    KVPair *fields = it->second;
    int num_updates = values.size();

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
    KVPair *fields = nullptr;
    auto it = db.find(key);

    if (it != db.end()) {
      fields = it->second;
      db.erase(it);
    }

    return fields;
  }

  template <typename Cont>
  int insert(const std::string &key, Cont &values, bool locked) {
    KVPair *fields = new KVPair[values.size()];

    std::copy(std::begin(values), std::end(values), fields);

    if (locked) {
      db[key] = fields;
    } else {
      std::lock_guard<std::mutex> lock(mutex);
      db[key] = fields;
    }

    return DB::kOK;
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
