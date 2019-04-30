//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_

#include "core_workload.h"
#include "db.h"
#include "utils/utils.h"
#include <string>

namespace ycsbc {
class Client {
public:
  Client(DB &db, CoreWorkload &wl) : db_(db), workload_(wl) {}

  virtual bool DoInsert();
  virtual bool DoTransaction();

  virtual ~Client() {}

protected:
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();

  DB &db_;
  CoreWorkload &workload_;
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  DB::FieldVec pairs;
  workload_.BuildValues(pairs);
  return (db_.Insert(workload_.NextTable(), key, pairs) == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  switch (workload_.NextOperation()) {
  case READ:
    status = TransactionRead();
    break;
  case UPDATE:
    status = TransactionUpdate();
    break;
  case INSERT:
    status = TransactionInsert();
    break;
  case SCAN:
    status = TransactionScan();
    break;
  case READMODIFYWRITE:
    status = TransactionReadModifyWrite();
    break;
  default:
    throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  DB::FieldVec result;

  if (!workload_.read_all_fields()) {
    DB::FieldSet fields;
    fields.insert("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, workload_.field_count(), result);
  } else {
    return db_.Read(table, key, NULL, workload_.field_count(), result);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  DB::FieldVec result;

  if (!workload_.read_all_fields()) {
    DB::FieldSet fields;
    fields.insert("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, workload_.field_count(), result);
  } else {
    db_.Read(table, key, NULL, workload_.field_count(), result);
  }

  DB::FieldMap values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, workload_.field_count(), values);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  int len = workload_.NextScanLength();
  std::vector<DB::FieldVec> result;
  if (!workload_.read_all_fields()) {
    DB::FieldSet fields;
    fields.insert("field" + workload_.NextFieldName());
    return db_.Scan(table, key, len, &fields, workload_.field_count(), result);
  } else {
    return db_.Scan(table, key, len, NULL, workload_.field_count(), result);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  DB::FieldMap values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, workload_.field_count(), values);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  DB::FieldVec values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values);
}

} // namespace ycsbc

#endif // YCSB_C_CLIENT_H_
