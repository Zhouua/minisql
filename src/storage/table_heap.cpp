#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
    Page *page = buffer_pool_manager_->FetchPage(first_page_id_); //从缓冲池管理器中获取第一个页面
    // 遍历链表查找第一个能够容纳该记录的TablePage
    while (true) {
      // 尝试在当前页面插入元组
      TablePage *table_page = reinterpret_cast<TablePage *>(page);
      if (table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true); // 解锁当前页面，并标记为脏页
        return true; // 插入成功，返回true
      }

      // 如果当前页面没有足够空间，检查是否有下一个页面
      page_id_t next_page_id = table_page->GetNextPageId();
      if (next_page_id == INVALID_PAGE_ID) {
        break; // 没有下一个页面，跳出循环
      }

      // 如果有下一个页面，继续查找
      buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty()); // 解锁当前页面，不标记为脏页
      page = buffer_pool_manager_->FetchPage(next_page_id); // 获取下一个页面
    }

    // 如果没有找到合适的页面，需要创建一个新页面
    page_id_t new_page_id;
    Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
    if (new_page == nullptr) {
      return false; // 如果无法分配新页面，返回false
    }

    // 初始化新页面
    TablePage *new_table_page = reinterpret_cast<TablePage *>(new_page);
    new_table_page->Init(new_page_id, page->GetPageId(), log_manager_, txn);

    // 更新链表，链接新页面
    reinterpret_cast<TablePage *>(page)->SetNextPageId(new_page_id);

    // 尝试在新页面插入元组
    bool insert_success = new_table_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
    buffer_pool_manager_->UnpinPage(new_page_id, true); // 解锁新页面，并标记为脏页
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true); // 解锁原页面，并标记为脏页（因为修改next_page）
    return insert_success; // 返回插入结果
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }

  Row old_row(rid); //旧记录的拷贝
  if (!GetTuple(&old_row, txn)) { //找old_row.rid对应的记录
    return false; //没找到，返回false
  }

  // table_page update失败，先标记删除旧元组，再插入新元组
  if (!page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_)) {
    MarkDelete(rid, txn);
    InsertTuple(row, txn);
  }

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Otherwise, delete the tuple.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  //获取RowId为row->rid_的记录
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
      return false;
  }
  page->RLatch();
  bool result = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), page->IsDirty());
  return result;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  //获取第一个页面
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    return TableIterator(nullptr, RowId(), txn); //获取失败，返回空迭代器
  }
  //获取第一个记录
  RowId first_tuple_rid;
  while(!page->GetFirstTupleRid(&first_tuple_rid)) { // Find and return the first valid tuple through the argument
    auto next_page = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), page->IsDirty()); // 解锁当前页面，不标记为脏页
    if (next_page == INVALID_PAGE_ID) {
      return TableIterator(nullptr, RowId(), txn); //没有下一个页面，返回空迭代器
    }
    // 如果有下一个页面，继续查找
    page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_page)); // 获取下一个页面
  }
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return TableIterator(this, first_tuple_rid, txn);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(nullptr, RowId(), nullptr);
}
