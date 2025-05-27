#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
     * 根据checkpoint信息初始化recovery manager
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
        active_txns_ = last_checkpoint.active_txns_;
        data_ = last_checkpoint.persist_data_;
        persist_lsn_ = last_checkpoint.checkpoint_lsn_;
        log_recs_ = {};
    }

    /**
     * 数据库崩溃后重放所有log的操作
    * TODO: Student Implement
    */
    void RedoPhase() {
        // 对从checkpoint开始的所有事务进行redo
        // 碰到begin，将事务添加到undo_list中
        // 碰到commit/abort，将事务从undo_list中删除
        printf("Redo Phase\n");
        lsn_t start_lsn = persist_lsn_ + 1;
        while (start_lsn != LogRec::next_lsn_) {
            LogRecPtr log_rec = log_recs_[start_lsn++];
            std::cout << "lsn: " << log_rec->lsn_ << ", txn_id: " << log_rec->txn_id_ << std::endl;
            switch (log_rec->type_) {
                case LogRecType::kInsert: {
                    // 插入日志，需要插入
                    data_.emplace(log_rec->beginKey_, log_rec->beginVal_);
                    std::cout << "insert: " << log_rec->beginKey_ << " " << log_rec->beginVal_ << std::endl;
                    break;
                }
                case LogRecType::kUpdate: {
                    // 更新日志，需要删除旧的，插入新的
                    data_.erase(log_rec->beginKey_);
                    data_.emplace(log_rec->endKey_, log_rec->endVal_);
                    std::cout << "update: " << log_rec->beginKey_ << " " << log_rec->beginVal_ << " -> " << log_rec->endVal_ << std::endl;
                    break;
                }
                case LogRecType::kDelete: {
                    // 删除日志，需要删除
                    data_.erase(log_rec->beginKey_);
                    std::cout << "delete: " << log_rec->beginKey_ << " " << log_rec->beginVal_ << std::endl;
                    break;
                }
                case LogRecType::kBegin: {
                    // begin日志，需要添加到active_txns中
                    active_txns_.emplace(log_rec->txn_id_, log_rec->lsn_);
                    break;
                }
                case LogRecType::kAbort:
                case LogRecType::kCommit: {
                    // Abort/Commit日志，需要从active_txns中删除
                    active_txns_.erase(log_rec->txn_id_);
                   break;
                }
                default: break;
            }
        }
    }

    /**
     * 撤销所有在undo_list中的事务
    * TODO: Student Implement
    */
    void UndoPhase() {
        printf("Undo Phase\n");
        // 首先得到最后一个LSN，然后逆序回滚
        lsn_t lsn = LogRec::next_lsn_ - 1;
        while (lsn != INVALID_LSN && active_txns_.size() > 0) {
            LogRecPtr log_rec = log_recs_[lsn--];
            if (active_txns_.find(log_rec->txn_id_) == active_txns_.end()) {
                // 当前日志不在活跃事物中，说明已经提交或者撤销了
                continue;
            }
            std::cout << "lsn: " << log_rec->lsn_ << ", txn_id: " << log_rec->txn_id_ << std::endl;
            switch (log_rec->type_) {
                case LogRecType::kInsert: {
                    // 插入日志，需要删除
                    data_.erase(log_rec->beginKey_);
                    std::cout << "delete key: " << log_rec->beginKey_ << std::endl;
                    break;
                }
                case LogRecType::kUpdate: {
                    // 更新日志，需要删除新的，插入旧的
                    data_.erase(log_rec->endKey_);
                    data_.emplace(log_rec->beginKey_, log_rec->beginVal_);
                    std::cout << "update: " << log_rec->beginKey_ << " " << log_rec->endVal_ << " -> " << log_rec->beginVal_ << std::endl;
                    break;
                }
                case LogRecType::kDelete: {
                    // 删除日志，需要插入
                    data_.emplace(log_rec->beginKey_, log_rec->beginVal_);
                    std::cout << "insert: " << log_rec->beginKey_ << " " << log_rec->beginVal_ << std::endl;
                    break;
                }
                case LogRecType::kBegin: {
                    // 开始日志，结束当前事务的undo
                    active_txns_.erase(log_rec->txn_id_);
                    break;
                }
                case LogRecType::kCommit:
                case LogRecType::kAbort:
                default: break;
            }
        }
    }

    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
