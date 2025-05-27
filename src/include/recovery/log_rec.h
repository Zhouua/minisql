#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "../common/config.h"
#include "../common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id_{INVALID_TXN_ID};
    KeyType beginKey_{};
    ValType beginVal_{0};
    KeyType endKey_{};
    ValType endVal_{0};

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * 创建插入日志
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    LogRecPtr log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kInsert; // 设置log的type
    log_rec->lsn_ = LogRec::next_lsn_++; // 设置log的lsn，并更新next_lsn
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id]; // 获取对应事物上一次的lsn
    log_rec->txn_id_ = txn_id; // 设置log的txn_id
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_; // 更新对应事物的prev_lsn
    log_rec->beginKey_ = std::move(ins_key);
    log_rec->beginVal_ = ins_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    LogRecPtr log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kDelete; // 设置log的type
    log_rec->lsn_ = LogRec::next_lsn_++; // 设置log的lsn，并更新next_lsn
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id]; // 获取对应事物上一次的lsn
    log_rec->txn_id_ = txn_id; // 设置log的txn_id
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_; // 更新对应事物的prev_lsn
    log_rec->beginKey_ = std::move(del_key);
    log_rec->beginVal_ = del_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    LogRecPtr log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kUpdate; // 设置log的type
    log_rec->lsn_ = LogRec::next_lsn_++; // 设置log的lsn，并更新next_lsn
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id]; // 获取对应事物上一次的lsn
    log_rec->txn_id_ = txn_id; // 设置log的txn_id
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_; // 更新对应事物的prev_lsn
    log_rec->beginKey_ = std::move(old_key);
    log_rec->beginVal_ = old_val;
    log_rec->endKey_ = std::move(new_key);
    log_rec->endVal_ = new_val;
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    LogRecPtr log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kBegin; // 设置log的type
    log_rec->lsn_ = LogRec::next_lsn_++; // 设置log的lsn，并更新next_lsn
    log_rec->txn_id_ = txn_id; // 设置log的txn_id
    log_rec->prev_lsn_ = INVALID_LSN; // 这是begin，所以prev_lsn为INVALID_LSN
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_; // 更新对应事物的prev_lsn
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    LogRecPtr log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kCommit; // 设置log的type
    log_rec->lsn_ = LogRec::next_lsn_++; // 设置log的lsn，并更新next_lsn
    log_rec->txn_id_ = txn_id; // 设置log的txn_id
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id]; // 获取对应事物上一次的lsn
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_; // 更新对应事物的prev_lsn
    return log_rec;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    LogRecPtr log_rec = std::make_shared<LogRec>();
    log_rec->type_ = LogRecType::kAbort; // 设置log的type
    log_rec->lsn_ = LogRec::next_lsn_++; // 设置log的lsn，并更新next_lsn
    log_rec->txn_id_ = txn_id; // 设置log的txn_id
    log_rec->prev_lsn_ = LogRec::prev_lsn_map_[txn_id]; // 获取对应事物上一次的lsn
    LogRec::prev_lsn_map_[txn_id] = log_rec->lsn_; // 更新对应事物的prev_lsn
    return log_rec;
}

#endif  // MINISQL_LOG_REC_H
