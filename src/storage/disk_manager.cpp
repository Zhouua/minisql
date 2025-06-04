#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>
#include <cstring>
#include <iostream>

#include "glog/logging.h"
#include "page/bitmap_page.h"

// 构造函数：打开数据库文件，初始化磁盘管理器
DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);  // 加锁，线程安全
  // 以二进制方式打开文件，读写模式
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);

  // 如果文件不存在或者无法打开
  if (!db_io_.is_open()) {
    db_io_.clear();
    // 创建文件所在目录（如果有）
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    // 创建新文件，截断写入
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // 关闭后重新以读写方式打开
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      // 仍然无法打开，抛出异常
      throw std::exception();
    }
  }
  // 读取元数据页（页ID为0）的内容到 meta_data_ 缓冲区
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

// 关闭数据库文件，写回元数据
void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  // 写回元数据页到磁盘
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();  // 关闭文件流
    closed = true;   // 标记为已关闭
  }
}

// 读取逻辑页，传入逻辑页号和缓冲区指针
void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  if (logical_page_id < 0) {
    throw std::out_of_range("Invalid page id.");  // 页号无效抛异常
  }
  // 逻辑页号映射到物理页号，再读取物理页
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

// 写入逻辑页，传入逻辑页号和数据指针
void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");  // 断言页号有效
  // 逻辑页号映射到物理页号，再写入物理页
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * 分配一个空闲页，返回逻辑页号
 * 先从已有的 extent 中寻找有空闲页的 bitmap 分配页，
 * 若所有 extent 都满，尝试新增 extent 分配页。
 */
page_id_t DiskManager::AllocatePage() {
  // 将元数据页缓冲区强转为元数据结构指针
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  // 先假设分配新 extent（编号为当前已有 extent 数量）
  uint32_t extent_id = meta_page->GetExtentNums();
  // 遍历已有的 extent 找第一个有空闲页的 extent
  for (uint32_t i = 0; i < meta_page->GetExtentNums(); i++) {
    if (meta_page->GetExtentUsedPage(i) < BITMAP_SIZE) {
      extent_id = i;  // 找到有空闲页的 extent
      break;
    }
  }
  // 如果已经达到最大extent数量，无法分配新页，返回无效页号
  if (extent_id == (PAGE_SIZE - 8) / 4) {
    return INVALID_PAGE_ID;
  }

  // 读取该 extent 的 bitmap 页数据（extent对应的bitmap页在物理页的计算公式）
  char bitmap_data[PAGE_SIZE];
  ReadPhysicalPage(extent_id * (BITMAP_SIZE + 1) + 1, bitmap_data);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);

  uint32_t page_id;
  // 通过 bitmap 分配一个空闲页
  if (bitmap->AllocatePage(page_id)) {
    // 分配成功则写回 bitmap 页数据
    WritePhysicalPage(extent_id * (BITMAP_SIZE + 1) + 1, bitmap_data);
    // 更新元数据中的分配页总数和该extent使用页数
    meta_page->num_allocated_pages_++;
    meta_page->extent_used_page_[extent_id]++;
    // 如果分配页所在extent是新扩展的extent，元数据中extent数目加1
    if (extent_id == meta_page->GetExtentNums()) {
      meta_page->num_extents_++;
    }

    // 返回逻辑页号（extent号 * 每个extent页数 + 页内偏移）
    return extent_id * BITMAP_SIZE + page_id;
  }
  // 分配失败时没有显式返回，需补充返回无效页号，防止编译警告
  return INVALID_PAGE_ID;
}

/**
 * 释放逻辑页，更新对应 bitmap 和元数据
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  // 计算该页所属的 extent
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;

  // 读取该 extent 的 bitmap 页数据
  char bitmap_data[PAGE_SIZE];
  ReadPhysicalPage(extent_id * (BITMAP_SIZE + 1) + 1, bitmap_data);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);

  // 释放 bitmap 中对应的页
  if (bitmap->DeAllocatePage(logical_page_id % BITMAP_SIZE)) {
    // 释放成功则写回 bitmap 数据
    WritePhysicalPage(extent_id * (BITMAP_SIZE + 1) + 1, bitmap_data);

    // 更新元数据中已分配页数和 extent 中使用页数
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[extent_id]--;
    // 如果最高的 extent 中没有使用页，减少元数据记录的extent数量
    while (meta_page->num_extents_ > 0 && meta_page->extent_used_page_[meta_page->num_extents_ - 1] == 0) {
      meta_page->num_extents_--;
    }
  }
}

/**
 * 判断逻辑页是否空闲
 * 读取对应extent的bitmap，查询该页的使用状态
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  // 计算 extent id 和页在该 extent 内的偏移
  uint32_t extent_id = logical_page_id / BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t page_offset = logical_page_id % BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

  BitmapPage<PAGE_SIZE> bitmap_page;
  // 读取 bitmap 页数据到临时对象
  ReadPhysicalPage(1 + extent_id * (1 + BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()), reinterpret_cast<char*>(&bitmap_page));

  // 返回该页是否空闲
  return bitmap_page.IsPageFree(page_offset);
}

/**
 * 逻辑页号映射到物理页号
 * 物理页号计算方式是：
 *  每个 extent 由 1 个 bitmap 页和 BITMAP_SIZE 个数据页组成，
 *  加上前面2页的元数据页和其他保留页，偏移 +2
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id / BITMAP_SIZE * (BITMAP_SIZE + 1) + logical_page_id % BITMAP_SIZE + 2;
}

// 获取文件大小，失败返回-1
int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * 读取物理页内容到缓冲区
 * offset = physical_page_id * PAGE_SIZE
 * 如果读取超出文件长度，填充0
 */
void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // 文件大小
  int file_size = GetFileSize(file_name_);
  // 若读取偏移超出文件长度，说明该页未初始化，全部置0
  if (offset >= file_size) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
    return;
  }

  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);  // 加锁
  // 定位到指定物理页起始位置
  db_io_.seekg(offset);
  // 读取一页内容到缓冲区
  db_io_.read(page_data, PAGE_SIZE);

  // 读取不足 PAGE_SIZE，剩余置0
  int read_size = db_io_.gcount();
  if (read_size < PAGE_SIZE) {
    memset(page_data + read_size, 0, PAGE_SIZE - read_size);
  }
}

/**
 * 写入缓冲区内容到物理页
 * offset = physical_page_id * PAGE_SIZE
 */
void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);  // 加锁

  // 定位写入位置
  db_io_.seekp(offset);
  // 写入一页内容
  db_io_.write(page_data, PAGE_SIZE);
  // 刷新缓冲区到磁盘
  db_io_.flush();
}
