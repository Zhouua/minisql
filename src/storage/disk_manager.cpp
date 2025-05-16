#include "../include/storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * 从磁盘中分配一个空闲页，并返回空闲页的逻辑页号
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  DiskFileMetaPage *meta_page_ = reinterpret_cast<DiskFileMetaPage *>(meta_data_); // 获取元数据页
  bool flag = false;
  uint32_t extent_id;
  for (uint32_t i = 0; i < meta_page_->GetExtentNums(); i++) {
    // 遍历所有extent查看是否有空页
    if (meta_page_->GetExtentUsedPage(i) < BITMAP_SIZE) {
      // 找到有空闲页的extent
      flag = true;
      extent_id = i;
      break;
    }
  }
  if (!flag) {
    // 说明没有空闲页的extent，需要新加一个
    if (meta_page_->GetExtentNums() == PAGE_SIZE - 8) {
      // 磁盘已经满了，-8是因为metapage占用一个页
      return INVALID_PAGE_ID;
    }
    extent_id = meta_page_->GetExtentNums(); // 新增一个extent
    meta_page_->num_extents_ += 1; // 更新extent数量
    meta_page_->extent_used_page_[extent_id] = 0; // 初始化对应extent的空闲页数
  }
  char bitmap_page_data[PAGE_SIZE]; // bitmap数据
  ReadPhysicalPage(1 + extent_id * (1 + BITMAP_SIZE), bitmap_page_data); // 读入bitmap数据，第一页为meta，每个extent有1+BITMAP_SIZE页
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data);
  for (uint32_t i = 0; i < bitmap_page->GetMaxSupportedSize(); i++) {
    if (bitmap_page->IsPageFree(i)) {
      // 找到空闲页
      bitmap_page->AllocatePage(i); // 设置bitmap
      meta_page_->num_allocated_pages_ += 1; // 更新已分配页数
      meta_page_->extent_used_page_[extent_id] += 1; // 更新extent已分配页数
      WritePhysicalPage(1 + extent_id * (1 + BITMAP_SIZE), bitmap_page_data);
      return extent_id * BITMAP_SIZE + i; // 返回逻辑页号
    }
  }
  return INVALID_PAGE_ID;
}

/**
 * 释放磁盘中逻辑页号对应的物理页
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  if (IsPageFree(logical_page_id)) {
    // 已经空了
    return;
  }
  char bitmap_page_data[PAGE_SIZE];
  ReadPhysicalPage(1 + logical_page_id / BITMAP_SIZE * (BITMAP_SIZE + 1), bitmap_page_data);
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data); // 获取对应bitmap
  bitmap_page->DeAllocatePage(logical_page_id % BITMAP_SIZE); // 释放该页
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  meta_page->num_allocated_pages_ -= 1; // 更新已分配页数
  meta_page->extent_used_page_[logical_page_id / BITMAP_SIZE] -= 1;
}

/**
 * 判断该逻辑页号对应的数据页是否空闲
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char bitmap_page_data[PAGE_SIZE];
  page_id_t bimap_page_id = 1 + logical_page_id / BITMAP_SIZE * (BITMAP_SIZE + 1); // 获取bitmap物理页号
  ReadPhysicalPage(bimap_page_id, bitmap_page_data); // 读入bitmap数据
  BitmapPage<PAGE_SIZE> *bitmap_page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_page_data); // 没有构造函数，使用reinterpret_cast
  return bitmap_page->IsPageFree(logical_page_id % BITMAP_SIZE);
}

/**
 * 用于将逻辑页号转换成物理页号
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  // Disk page storage format: (Free Page BitMap Size = PAGE_SIZE * 8, we note it as N)
  // | Meta Page | Free Page BitMap 1 | Page 1 | Page 2 | ....
  // | Page N | Free Page BitMap 2 | Page N+1 | ... | Page 2N | ... |
  page_id_t extent_id = logical_page_id / BITMAP_SIZE; 
  page_id_t page_id = logical_page_id % BITMAP_SIZE;
  return extent_id * (BITMAP_SIZE + 1) + 1 + page_id + 1; // N个为1组，1组实际为N+1个页，0为元数据，每个extent第一个为bitmapPage
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}