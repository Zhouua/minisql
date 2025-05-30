#include "../include/buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : Replace_Size(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * 替换（即删除）与所有被跟踪的页相比最近最少被访问的页
 * 将其页帧号（即数据页在Buffer Pool的Page数组中的下标）存储在输出参数frame_id中输出并返回true
 * 如果当前没有可以替换的元素则返回false
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.empty()) {  
    frame_id = nullptr;
    return false;
  }
  *frame_id = lru_list_.front();
  lru_list_.pop_front();
  return true;
}

/**
 * 将数据页固定使之不能被Replacer替换
 * 即从lru_list_中移除该数据页对应的页帧
 * Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
    if (*it == frame_id) {
      lru_list_.erase(it);
      break;
    }
  }
}

/**
 * 将数据页解除固定，放入lru_list_中，使之可以在必要时被Replacer替换掉。
 * Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，使页帧对应的数据页能够在必要时被替换
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  while (lru_list_.size() >= Replace_Size) {
    lru_list_.pop_back();
  }
  for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
    if (*it == frame_id) {
      return;
    }
  }
  lru_list_.push_back(frame_id);
}

/**
 * 此方法返回当前LRUReplacer中能够被替换的数据页的数量
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}