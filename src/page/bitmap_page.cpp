#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
    // 遍历 bitmap 中的每一个字节
    for (uint32_t byte_index = 0; byte_index < MAX_CHARS; ++byte_index) {
        // 遍历当前字节中的每一位（一个字节有8位）
        for (uint8_t bit_index = 0; bit_index < 8; ++bit_index) {
            // 计算出当前位对应的页偏移（页索引）
            uint32_t page_index = byte_index * 8 + bit_index;

            // 判断该页是否空闲
            if (IsPageFree(page_index)) {
                // 如果是空闲页，就将对应位设置为 1，表示该页已分配
                bytes[byte_index] |= (1 << bit_index);

                // 增加已分配页计数
                page_allocated_++;

                // 设置输出参数，返回分配的页号
                page_offset = page_index;

                // 更新下一个可能的空闲页位置（加快后续查找）
                next_free_page_ = page_offset + 1;

                return true;  // 分配成功
            }
        }
    }
    // 如果没有找到空闲页，则返回 false
    return false;
}



/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
    // 计算该页所在字节的位置和在该字节中的位偏移
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;

    // 仅在该页当前是已分配状态下才进行释放
    if (!IsPageFree(page_offset)) {
        // 将对应位清零，表示释放该页
        bytes[byte_index] &= ~(1 << bit_index);

        // 减少已分配页数量
        page_allocated_--;

        // 如果释放的位置比当前记录的 next_free_page_ 更靠前，则更新它
        next_free_page_ = std::min(next_free_page_, page_offset);

        return true;  // 成功释放
    }
    return false;  // 如果该页原本就是空闲的，返回 false
}


/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    // 计算页所在的字节位置和位偏移
    uint32_t byte_index = page_offset / 8;
    uint8_t bit_index = page_offset % 8;

    // 判断该位是否为0，为0说明该页是空闲的
    return !(bytes[byte_index] & (1 << bit_index));
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;