#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

// 构造函数，初始化缓冲池管理器
BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  // 分配物理页面数组
  pages_ = new Page[pool_size_];
  // 初始化 LRU 替换器
  replacer_ = new LRUReplacer(pool_size_);
  // 将所有页面索引加入空闲列表
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

// 析构函数，释放资源并将所有脏页刷回磁盘
BufferPoolManager::~BufferPoolManager() {
  // 遍历页表，刷写所有页面到磁盘
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  // 释放页面数组和替换器内存
  delete[] pages_;
  delete replacer_;
}

/**
 * 从缓冲池中取出指定页，如果不存在则从磁盘加载
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }
  // 1. 查找页表，判断页是否在缓冲池中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    // 页命中，更新 pin 计数并通知替换器该页被固定
    frame_id_t frame_id = it->second;
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  // 2. 页不在缓冲池，从空闲页或替换器中选一个页框
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == INVALID_PAGE_ID) {
    // 没有可用页框，返回空指针
    return nullptr;
  }
  // 3. 选中的页如果脏，则先写回磁盘
  Page &page = pages_[frame_id];
  if (page.is_dirty_) {
    disk_manager_->WritePage(page.page_id_, page.data_);
  }

  // 4. 从页表删除旧页映射，添加新页映射
  page_table_.erase(page.page_id_);
  page_table_[page_id] = frame_id;

  // 5. 载入目标页数据，更新页框信息
  page.page_id_ = page_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;
  disk_manager_->ReadPage(page_id, page.data_);

  return &page;
}

/**
 * 分配一个新的页，并返回对应页面指针
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 1. 尝试获取空闲页，失败返回 nullptr
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == INVALID_FRAME_ID) {
    return nullptr;
  }
  // 2. 若该页脏，先刷写磁盘
  Page &page = pages_[frame_id];
  if (page.is_dirty_) {
    FlushPage(page.GetPageId());
  }
  // 3. 从页表删除旧映射
  page_table_.erase(page.page_id_);

  // 4. 分配新的页号
  page_id = AllocatePage();

  // 5. 初始化新页面内容
  page.page_id_ = page_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;
  std::memset(page.data_, 0, PAGE_SIZE);

  // 6. 标记该页已被固定，不可替换
  replacer_->Pin(frame_id);

  // 7. 更新页表映射，返回新页指针
  page_table_[page_id] = frame_id;
  return &page;
}

/**
 * 删除指定页，回收页资源
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 1. 先释放页号
  DeallocatePage(page_id);

  // 2. 查找页表是否存在该页
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;  // 页不存在，删除成功
  }

  // 3. 若页正在被使用，不能删除，返回 false
  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }

  // 4. 若页脏，先写回磁盘
  if (page.IsDirty()) {
    FlushPage(page_id);
  }

  // 5. 从页表移除该页映射
  page_table_.erase(it);

  // 6. 释放页资源，重置状态
  DeallocatePage(page_id);
  page.ResetMemory();
  page.is_dirty_ = false;
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;

  // 7. 标记该页为固定状态，并加入空闲列表
  replacer_->Pin(frame_id);
  free_list_.push_back(frame_id);

  return true;
}

/**
 * 取消固定指定页，更新脏标记和替换器状态
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // 查找页表
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // 页不在缓冲池中
  }

  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];

  // 减少 pin 计数（防止负值）
  if (page.pin_count_ > 0) {
    page.pin_count_--;
  }

  // 更新脏标记
  if (is_dirty) {
    page.is_dirty_ = true;
  }

  // 如果 pin 计数为 0，将页加入替换器候选队列
  if (page.pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }

  return true;
}

/**
 * 将指定页写回磁盘，清除脏标记
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // 页不在缓冲池中
  }

  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];

  // 写回磁盘
  disk_manager_->WritePage(page_id, page.data_);
  page.is_dirty_ = false;

  return true;
}

/**
 * 将缓冲池中所有页写回磁盘
 */
bool BufferPoolManager::FlushAllPages() {
  for (auto [page_id, frame_id] : page_table_) {
    Page *page = pages_ + frame_id;
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
  return true;
}

/**
 * 尝试从空闲列表或替换器获取可用页框
 */
frame_id_t BufferPoolManager::TryToFindFreePage() {
  // 1. 优先从空闲列表获取页框
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  }

  // 2. 否则调用替换器选择替换页
  frame_id_t frame_id;
  replacer_->Victim(&frame_id);
  if (frame_id != INVALID_FRAME_ID) {
    Page *page = pages_ + frame_id;
    // 从页表移除该页映射
    page_table_.erase(page->GetPageId());
  }
  return frame_id;
}

// 分配新页号，调用磁盘管理器
page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

// 释放页号，调用磁盘管理器
void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

// 判断页是否空闲，调用磁盘管理器
bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// 调试接口，检查所有页是否未被 pin
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
