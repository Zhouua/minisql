#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * Implement by chenjy
 * 构造函数：根据给定的 TableHeap、初始 Row 和事务指针构造一个迭代器
 */
TableIterator::TableIterator(TableHeap *table_heap, const Row& row, Txn *txn)
  : table_heap_(table_heap), row_(row), txn_(txn) {}

// 拷贝构造函数（默认行为）
TableIterator::TableIterator(const TableIterator &other) = default;

// 析构函数（默认行为）
TableIterator::~TableIterator() = default;

// 判断两个迭代器是否指向相同的行（比较 RowId）
bool TableIterator::operator==(const TableIterator &itr) const {
  return row_.GetRowId() == itr.row_.GetRowId();
}

// 判断两个迭代器是否不相等（即行不相同）
bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(row_.GetRowId() == itr.row_.GetRowId());
}

// 解引用运算符：返回当前行的引用
const Row &TableIterator::operator*() {
  return row_;
}

// 指针访问运算符：返回当前行的指针
Row *TableIterator::operator->() {
  return &row_;
}

// 赋值运算符重载
TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (itr == *this)
    return *this;
  table_heap_ = itr.table_heap_;
  row_ = itr.row_;
  txn_ = itr.txn_;
  return *this;
}

// 前置 ++ 操作：将迭代器移动到下一行记录
TableIterator &TableIterator::operator++() {
  RowId rid = row_.GetRowId();
  page_id_t page_id = rid.GetPageId();
  RowId next_rid;

  // 读取当前行所在页
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  if (page == nullptr) {
    throw runtime_error("Page not exist");
    LOG(ERROR) << "Page not exist" << std::endl;
    return *this;
  }

  // 如果当前页还有下一条记录
  if (page->GetNextTupleRid(rid, &next_rid)) {
    row_.SetRowId(next_rid); // 设置为下一行的 RowId
    table_heap_->GetTuple(&row_, txn_); // 加载新行的内容
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false); // 取消固定页
    return *this;
  }

  // 否则切换到下一页查找首条记录
  auto pre_id = page_id;
  page_id = page->GetNextPageId();
  table_heap_->buffer_pool_manager_->UnpinPage(pre_id, false);

  while (page_id != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
    if (page == nullptr) {
      throw runtime_error("Page not exist");
      LOG(ERROR) << "Page not exist" << std::endl;
      return *this;
    }

    // 如果下一页中有记录
    if (page->GetFirstTupleRid(&next_rid)) {
      row_.SetRowId(next_rid); // 设置为下一页的第一行
      table_heap_->GetTuple(&row_, txn_); // 加载该行
      table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
      return *this;
    }

    // 当前页也没有记录，继续向后翻页
    pre_id = page_id;
    page_id = page->GetNextPageId();
    table_heap_->buffer_pool_manager_->UnpinPage(pre_id, false);
  }

  // 如果遍历到尾部，设置为无效 RowId
  row_.SetRowId(RowId(INVALID_PAGE_ID, 0));
  return *this;
}

// 后置 ++ 操作：返回当前迭代器，再执行 ++ 操作
TableIterator TableIterator::operator++(int) {
  const TableIterator temp(*this);
  ++(*this);
  return temp;
}
