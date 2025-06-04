#include "buffer/lru_replacer.h"

// 构造函数，初始化 LRU 替换器
LRUReplacer::LRUReplacer(size_t num_pages){
  num_pages_ = num_pages;   // 缓冲池中最大页数
  lru_list_.clear();        // 双向链表，存储页框的 LRU 顺序
  page_map_.clear();        // 哈希表，快速判断页框是否在 LRU 列表中
}

// 析构函数，默认即可
LRUReplacer::~LRUReplacer() = default;

/**
 * 从 LRU 替换器中选择一个 victim（被替换）页框
 * 将最久未使用的页框移除，并通过参数返回该页框 id
 * 返回值：成功返回 true，失败（无可替换页）返回 false
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.empty()) {  // 如果没有可替换页框，返回失败
    *frame_id = INVALID_FRAME_ID;
    return false;
  }
  // 选择链表头部（最久未使用）的页框作为 victim
  *frame_id = lru_list_.front();
  // 从哈希表中移除该页框的映射
  page_map_.erase(*frame_id);
  // 从链表中删除该页框
  lru_list_.pop_front();
  return true;
}

/**
 * 固定某个页框（pin 操作）
 * 表示该页框正在被使用，不能被替换
 * 从 LRU 列表中删除该页框（如果存在）
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  // 判断页框是否在 LRU 列表中
  if (page_map_.count(frame_id)) {
    // 遍历链表找到对应页框的迭代器
    for (auto it = lru_list_.begin(); it != lru_list_.end(); ++it) {
      if (*it == frame_id) {
        // 找到后删除该页框，跳出循环
        lru_list_.erase(it);
        break;
      }
    }
    // 从哈希表中移除页框的标记
    page_map_.erase(frame_id);
  }
}

/**
 * 取消固定某个页框（unpin 操作）
 * 表示该页框不再被使用，可以被替换
 * 如果当前 LRU 容量已满，直接返回不处理
 * 如果该页框不在 LRU 列表中，则将其加入链表尾部，标记为最近未使用
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 如果 LRU 列表已满，则不加入新页框，直接返回
  if ((uint32_t)page_map_.size() == num_pages_) {
    return;
  }

  // 页框不在 LRU 中，才加入
  if (!page_map_.count(frame_id)) {
    lru_list_.push_back(frame_id);  // 加入链表尾部，表示最近未使用
    page_map_[frame_id] = true;     // 在哈希表中标记该页框
  }
}

/**
 * 返回当前 LRU 替换器中可替换页框的数量
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}
