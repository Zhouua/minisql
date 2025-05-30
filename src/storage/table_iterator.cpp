#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(): table_heap_(nullptr), txn_(nullptr), current_row_(nullptr) {}

TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn)
    : table_heap_(table_heap), txn_(txn), current_row_(std::make_unique<Row>(rid)) {
  //fill the current_row_'s fields_ with the tuple in the rid
  if (table_heap_ != nullptr && current_row_->GetRowId().GetPageId() >=0) {
    table_heap->GetTuple(current_row_.get(), txn_);
  }
}

TableIterator::TableIterator(const TableIterator &other)
    : table_heap_(other.table_heap_),
      txn_(other.txn_), current_row_(std::make_unique<Row>(*other.current_row_)) {}

TableIterator::~TableIterator() = default;

bool TableIterator::operator==(const TableIterator &itr) const {
  return current_row_->GetRowId() == itr.current_row_->GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  return *current_row_;
}

Row *TableIterator::operator->() {
  return current_row_.get();
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    table_heap_ = itr.table_heap_;
    txn_ = itr.txn_;
    current_row_ = std::make_unique<Row>(*itr.current_row_);
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (*this == table_heap_->End()) {
    // �����ǰ�������Ѿ�ָ�����ĩβ���򲻽����κβ�����ֱ�ӷ��ص�ǰ��������
    return *this;
  }

  // ��ȡ��ǰԪ������ҳ��
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(current_row_->GetRowId().GetPageId()));
  page->RLatch();

  // ��ȡ��ǰԪ�����һ��Ԫ����б�ʶ
  RowId next_row_id;
  if (page->GetNextTupleRid(current_row_->GetRowId(), &next_row_id)) {
    // �����ǰҳ������һ��Ԫ�飬���ȡ��һ��Ԫ�鲢���µ�ǰԪ��
    current_row_->destroy();
    current_row_->SetRowId(next_row_id);
    page->GetTuple(current_row_.get(), table_heap_->schema_, txn_, table_heap_->lock_manager_);
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  } else {
    // �����ǰҳ��û����һ��Ԫ�飬���Ի�ȡ��һ��ҳ��ĵ�һ��Ԫ��
    page_id_t next_page_id;
    while ((next_page_id = page->GetNextPageId()) != INVALID_PAGE_ID) {
      page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      page->RLatch();
      if (page->GetFirstTupleRid(&next_row_id)) {
        current_row_->destroy();
        current_row_->SetRowId(next_row_id);
        page->GetTuple(current_row_.get(), table_heap_->schema_, txn_, table_heap_->lock_manager_);
        page->RUnlatch();
        table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
        return *this;
      }
    }
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    // ����޷��ҵ���һ��ҳ��ĵ�һ��Ԫ�飬��˵���Ѿ��������ĩβ������������Ϊ������������
    *this = table_heap_->End();
  }

  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator itr(*this);
  ++(*this);
  return (TableIterator)itr;
}
