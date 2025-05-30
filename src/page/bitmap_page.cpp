#include "../include/page/bitmap_page.h"

#include "glog/logging.h"

/**
 * 分配一个空闲页，并通过page_offset返回所分配的空闲页位于该段中的下标（从0开始）
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  size_t max_page_offset = GetMaxSupportedSize(); // 最大支持的页数
  uint32_t byte_loc = page_offset / 8; // byte_loc 表示分配的页位于第几字节
  uint32_t bit_loc = page_offset % 8; // bit_loc 表示分配的页位于第几个bit
  if (page_offset == max_page_offset || !IsPageFree(page_offset)) {
    // 已到最后一页，或者当前页已经被分配
    // 需要遍历查找空闲页
    for (uint32_t i = 0; i < max_page_offset; i++) {
      if (IsPageFree(i)) {
        // 有空闲页
        page_offset = i;
        bytes[i / 8] |= 1 << (i % 8); // 分配页
        page_allocated_ += 1; 
        return true;
      }
    }
    return false; // 没有空闲页
  }
  // 可以在当前位置分配
  bytes[byte_loc] |= 1 << bit_loc; // 分配页
  page_allocated_ += 1;
  return true;
}

/**
 * 回收已经被分配的页
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (IsPageFree(page_offset)) {
    // 该页已经处于空闲状态
    return false;
  }
  uint32_t byte_loc = page_offset / 8; // byte_loc 表示分配的页位于第几字节
  uint32_t bit_loc = page_offset % 8; // bit_loc 表示分配的页位于第几个bit
  bytes[byte_loc] &= ~(1 << bit_loc); // 回收页，e.g. 1010111 & 111101 -> 1010101
  page_allocated_ -= 1;
  return true;
}

/**
 * 判断给定的页是否是空闲（未分配）的
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_loc = page_offset / 8; // byte_loc 表示分配的页位于第几字节
  uint32_t bit_loc = page_offset % 8; // bit_loc 表示分配的页位于第几个bit
  return IsPageFreeLow(byte_loc, bit_loc);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return (bytes[byte_index] & (1 << bit_index)) == 0; // 等于0表示空闲
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;