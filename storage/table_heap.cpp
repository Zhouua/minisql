#include "storage/table_heap.h"

/**
 * 向表中插入一条元组（记录）
 * @param row 要插入的元组
 * @param txn 当前事务对象
 * @return 插入是否成功
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  // 如果元组太大，无法放入一个页中，直接返回失败
  if (row.GetSerializedSize(schema_) >= PAGE_SIZE) {
    LOG(WARNING) << "Tuple size too large" << endl;
    return false;
  }

  // 从表的第一页开始查找有空位的页
  int cur_id = first_page_id_;
  int prev_id = first_page_id_;
  while (cur_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_id));
    if (page == nullptr) {
      LOG(WARNING) << "TablePage has unavailable records" << endl;
      return false;
    }

    // 尝试插入元组到当前页
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(cur_id, true); // 插入成功，解除固定并标记为脏页
      return true;
    }

    // 当前页空间不足，尝试下一页
    prev_id = cur_id;
    buffer_pool_manager_->UnpinPage(prev_id, false); // 插入失败，不标记为脏页
    cur_id = page->GetNextPageId();
  }

  // 没有找到空间合适的页，新建一个页
  page_id_t new_id;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_id));
  page->Init(new_id, prev_id, log_manager_, txn);  // 初始化新页
  page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(new_id, true);

  // 将上一页的 next_page_id 指向新页
  auto pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_id));
  pre_page->SetNextPageId(new_id);
  buffer_pool_manager_->UnpinPage(prev_id, true);
  return true;
}

/**
 * 标记某个元组为删除状态（延迟删除）
 * @param rid 要删除的元组的 RowId
 * @param txn 当前事务
 * @return 是否标记成功
 */
bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    return false;
  }
  page->WLatch();  // 加写锁
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();  // 释放写锁
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * 更新某个指定位置的元组内容
 * @param row 新的数据行（必须包含新值）
 * @param rid 要更新的元组的 RowId
 * @param txn 当前事务
 * @return 是否更新成功
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(WARNING) << "Page not exist" << endl;
    return false;
  }

  Row old_row(rid);  // 记录旧值
  page->WLatch();
  if (page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    return true;
  }

  // 更新失败（如空间不足等）
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return false;
}

/**
 * 立即从表中删除某个元组（物理删除）
 * @param rid 要删除的元组的 RowId
 * @param txn 当前事务
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(INFO) << "Page not exist" << endl;
    return;
  }

  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

/**
 * 回滚一个标记删除的操作（事务失败）
 * @param rid 被删除的行的 RowId
 * @param txn 当前事务
 */
void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);

  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * 读取一个元组内容
 * @param row 包含 RowId 的 Row 对象，成功后填充其数据
 * @param txn 当前事务
 * @return 是否成功获取元组
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    LOG(WARNING) << "Page not exist" << endl;
    return false;
  }

  bool ok = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
  return ok;
}

/**
 * 递归删除整张表的所有页
 * @param page_id 要删除的起始页编号
 */
void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());

    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * 获取迭代器指向表中第一条有效记录的位置
 * @param txn 当前事务
 * @return TableIterator 对象
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t cur_page = first_page_id_;
  while (cur_page != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page));
    RowId first_rid;
    if (page->GetFirstTupleRid(&first_rid)) {
      Row *first_row = new Row(first_rid);
      page->GetTuple(first_row, schema_, txn, lock_manager_);
      buffer_pool_manager_->UnpinPage(cur_page, false);
      return TableIterator(this, *first_row, txn);
    }
    buffer_pool_manager_->UnpinPage(cur_page, false);
    cur_page = page->GetNextPageId();
  }
  return End();  // 如果整张表没有记录，返回结束迭代器
}

/**
 * 获取一个结束迭代器（表示尾后位置）
 * @return TableIterator 对象，RowId 设置为无效
 */
TableIterator TableHeap::End() {
  return TableIterator(this, RowId(INVALID_PAGE_ID, 0), nullptr);
}
