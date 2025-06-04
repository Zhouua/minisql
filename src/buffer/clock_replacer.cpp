#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages) : capacity(num_pages) {
  clock_list = list<frame_id_t>(num_pages, INVALID_FRAME_ID);
  iter = clock_list.begin();
}

CLOCKReplacer::~CLOCKReplacer() = default;

bool CLOCKReplacer::Victim(frame_id_t *frame_id) {
  // if there is no unpin frame_id
  if (clock_status.size() == 0) {
    *frame_id = INVALID_FRAME_ID;
    return false;
  }
  // search the circle, find the status = 0 to victim
  while (1) {
    frame_id_t cur_frame_id = *iter;
    if (cur_frame_id != INVALID_FRAME_ID && clock_status.find(cur_frame_id) != clock_status.end()) {
      if (clock_status[cur_frame_id] == 1) {
        // if status = 1, change to 0, push back
        clock_status[cur_frame_id] = 0;
      } else {
        // victim status = 0
        clock_status.erase(cur_frame_id);
        *frame_id = cur_frame_id;
        *iter = INVALID_FRAME_ID;
        return true;
      }
    }
    iter++;
    if (iter == clock_list.end()) {
      iter = clock_list.begin();
    }
  }
}

void CLOCKReplacer::Pin(frame_id_t frame_id) {
  if (clock_status.find(frame_id) != clock_status.end()) {
    // clock_status record unpin frame_id
    clock_status.erase(frame_id);
  }
}

void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if (clock_status.find(frame_id) != clock_status.end()) {
    // in status
    clock_status[frame_id] = 1;
    return;
  } else {
    // not in status, in list
    for (auto &i : clock_list) {
      if (i == frame_id) {
        clock_status[frame_id] = 1;
        return;
      }
    }
    // not in status and list, insert in free block
    for (auto &i : clock_list) {
      if (i == INVALID_FRAME_ID) {
        i = frame_id;
        clock_status[frame_id] = 1;
        return;
      }
    }
    // no free block, victim and insert
    frame_id_t delete_frame_id;
    if (Victim(&delete_frame_id)) {
      for (auto &i : clock_list) {
        if (i == INVALID_FRAME_ID) {
          i = frame_id;
          clock_status[frame_id] = 1;
          return;
        }
      }
    }
  }
}

size_t CLOCKReplacer::Size() { return clock_status.size(); }