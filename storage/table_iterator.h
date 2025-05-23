#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "record/row.h"

class TableHeap;

class TableIterator {
public:
 // you may define your own constructor based on your member variables
 explicit TableIterator(TableHeap *table_heap, const Row& row, Txn *txn);

 TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  TableIterator operator++(int);
private:
  // 指向表堆对象的指针，用于访问元组和页
  TableHeap *table_heap_;
  
  // 当前迭代器所指向的元组
  Row row_;
  
  // 当前事务指针，用于加锁或访问隔离
  [[maybe_unused]] Txn *txn_;
};
#endif  // MINISQL_TABLE_ITERATOR_H