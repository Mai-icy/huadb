#pragma once

#include "common/types.h"
#include <map>
#include <vector>

namespace huadb {

enum class LockType {
  IS = 2,   // 意向共享锁
  IX = 3,   // 意向互斥锁
  S = 0,    // 共享锁
  SIX = 4,  // 共享意向互斥锁
  X = 1,    // 互斥锁
};

enum class LockGranularity { TABLE, ROW };

// 高级功能：死锁预防/检测类型
enum class DeadlockType { NONE, WAIT_DIE, WOUND_WAIT, DETECTION };

struct Lock {
  xid_t xid;
  LockType lock_type;
};

class LockManager {
 public:
  // 获取表级锁
  bool LockTable(xid_t xid, LockType lock_type, oid_t oid);
  // 获取行级锁
  bool LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid);

  // 释放事务申请的全部锁
  void ReleaseLocks(xid_t xid);

  void SetDeadLockType(DeadlockType deadlock_type);

 private:
  // 判断锁的相容性
  bool Compatible(LockType type_a, LockType type_b) const;
  // 实现锁的升级，如共享锁升级为互斥锁，输入两种锁的类型，返回升级后的锁类型
  LockType Upgrade(LockType self, LockType other) const;

  DeadlockType deadlock_type_ = DeadlockType::NONE;

  std::map<oid_t, std::vector<Lock>> lock_table_;
  std::map<std::pair<oid_t, Rid>, std::vector<Lock>> row_lock_table_;

  std::map<xid_t, std::vector<oid_t>> xid_table_lock_;
  std::map<xid_t, std::vector<std::pair<oid_t, Rid>>> xid_row_lock_;
};

}  // namespace huadb
