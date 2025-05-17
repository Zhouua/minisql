#include "../include/buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * 根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_table_.find(page_id) != page_table_.end()) {
    replacer_->Pin(page_table_[page_id]);
    Page *P = pages_ + page_table_[page_id]; // requested page P
    P->pin_count_ += 1;  // 增加被pin的数量
    return P;
  } else {
    frame_id_t frame_id;
    Page *R;
    if (free_list_.empty()) {
      // free_list_为空，需要从replacer中找一个替换
      if (!replacer_->Victim(&frame_id)) {
        return nullptr;
      }
    } else {
      // free_list_不为空，从free_list中找一个替换
      frame_id = free_list_.front();
      free_list_.pop_front();
    }
    R = pages_ + frame_id;
    if (R->is_dirty_) {
      // 如果被替换的页面是dirty的，则需要写回磁盘
      disk_manager_->WritePage(R->GetPageId(), R->GetData());
    }
    page_table_.erase(R->GetPageId()); // 删除R
    page_table_[page_id] = frame_id; // 插入P
    disk_manager_->ReadPage(page_id, R->data_); // 从磁盘读入P
    // 更新P的metadata
    R->page_id_ = page_id;
    R->pin_count_ = 1;
    R->is_dirty_ = false;
    replacer_->Pin(frame_id);
    return R;
  }
}

/**
 * 分配一个新的数据页，并将逻辑页号于page_id中返回
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  int flag = false; // flag为false当所有页面都被pin时，返回nullptr
  for (uint32_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() == 0) {
      flag = true;
      break;
    }
  }
  if (!flag) return nullptr;
  Page *P;
  frame_id_t frame_id;
  if (free_list_.empty()) {
    // free_list为空，从replacer中获取Victim页
    if (!replacer_->Victim(&frame_id)) {
      return nullptr;
    }
  } else {
    // free_list不为空，从free_list中获取Victim页
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  P = pages_ + frame_id;
  if (P->is_dirty_) {
    // 将脏页写入磁盘
    disk_manager_->WritePage(P->page_id_, P->GetData());
    P->is_dirty_ = false;
  }
  page_table_.erase(P->page_id_);
  page_id = disk_manager_->AllocatePage();
  page_table_[page_id] = frame_id;
  P->page_id_ = page_id;
  P->pin_count_ += 1;
  P->is_dirty_ = false;
  return P;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  if (page_table_.find(page_id) == page_table_.end()) {
    // P 不存在
    return true;
  } else {
    // P 存在
    Page *P = pages_ + page_table_[page_id];
    if (P->GetPinCount() != 0) {
      // P 被固定
      return false;
    } else {
      // P 不被固定
      page_table_.erase(page_id);
      DeallocatePage(page_id);
      free_list_.push_back(page_table_[page_id]);
      return true;
    }
  }
  return false;
}

/**
 * 取消固定一个数据页
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  if (page_table_.find(page_id) == page_table_.end()) {
    // 没有对应数据页
    return true;
  } else {
    Page *P = pages_ + page_table_[page_id];
    P->is_dirty_ |= is_dirty; // 更新dirty状态
    if (P->GetPinCount() == 0) {
      return false;
    } else {
      P->pin_count_ -= 1; // pin_count_减一
    }
    if (P->GetPinCount() == 0) {
      replacer_->Unpin(page_table_[page_id]); // 释放
    }
    return true;
  }
}

/**
 * 将数据页转储到磁盘中
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if (page_table_.find(page_id) == page_table_.end()) {
    // 页面不存在
    return false;
  } else {
    Page *P = pages_ + page_table_[page_id];
    P->is_dirty_  = false;
    disk_manager_->WritePage(page_id, P->GetData()); // 写入磁盘
    return true;
  }
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
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