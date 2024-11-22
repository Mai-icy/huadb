#include "transaction/lock_manager.h"
#include<iostream>

namespace huadb {


bool operator<(const Rid &r1, const Rid &r2){
  if(r1.page_id_ == r2.page_id_){
    return r1.slot_id_ < r2.slot_id_;
  }
  return r1.page_id_ < r2.page_id_;
}

bool LockManager::LockTable(xid_t xid, LockType lock_type, oid_t oid) {
  // 对数据表加锁，成功加锁返回 true，如果数据表已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据表的锁，根据需要升级锁的类型
  // LAB 3 DONE
  std::vector<Lock> &locks = lock_table_[oid];

  Lock *oriLock = nullptr;

  for(auto &lock: locks){
    if(lock.xid == xid){
      oriLock = &lock;
      break;
    }
  }

  if(oriLock != nullptr){
    LockType upgraded_lock = Upgrade(oriLock->lock_type, lock_type);
    for(auto &lock: locks){
      // std::cerr << (int) lock.lock_type << " " << (int) upgraded_lock << std::endl;
      if(lock.xid != xid and not Compatible(lock.lock_type, upgraded_lock)){
        // std::cerr << "nope:" << (int) lock.lock_type << " " << (int) upgraded_lock << std::endl;
        return false;
      }
    }
    oriLock->lock_type = upgraded_lock;
  }else{
    for(auto &lock: locks){
      if(not Compatible(lock.lock_type, lock_type)){
        return false;
      }
    }
    locks.push_back({xid, lock_type});
    xid_table_lock_[xid].push_back(oid);
  }
  return true;
}

bool LockManager::LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid) {
  // 对数据行加锁，成功加锁返回 true，如果数据行已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据行的锁，根据需要升级锁的类型
  // LAB 3 DONE
  std::pair<oid_t, Rid> row = {oid, rid};

  std::vector<Lock> &locks = row_lock_table_[row];

  Lock *oriLock = nullptr;
  for(auto &lock: locks){
    if(lock.xid == xid){
      oriLock = &lock;
      break;
    }
  }

  if(oriLock != nullptr){
    LockType upgraded_lock = Upgrade(oriLock->lock_type, lock_type);
    for(auto &lock: locks){
      if(lock.xid != xid and not Compatible(lock.lock_type, upgraded_lock)){
        return false;
      }
    }
    oriLock->lock_type = upgraded_lock;
  }else{
    for(auto &lock: locks){
      if(not Compatible(lock.lock_type, lock_type)){
        return false;
      }
    }
    locks.push_back({xid, lock_type});
    xid_row_lock_[xid].push_back(row);
  }
  return true;
}

void LockManager::ReleaseLocks(xid_t xid) {
  // 释放事务 xid 持有的所有锁
  // LAB 3 DONE
  if (xid_table_lock_.find(xid) != xid_table_lock_.end()) {
    for (oid_t oid : xid_table_lock_[xid]) {
      auto& locks = lock_table_[oid];
      locks.erase(std::remove_if(locks.begin(), locks.end(),
                                 [xid](const Lock& lock) { return lock.xid == xid; }),
                  locks.end());
    }
    xid_table_lock_.erase(xid);
  }

  if (xid_row_lock_.find(xid) != xid_row_lock_.end()) {
    for (const auto& row : xid_row_lock_[xid]) {
      auto& locks = row_lock_table_[row];
      locks.erase(std::remove_if(locks.begin(), locks.end(),
                                 [xid](const Lock& lock) { return lock.xid == xid; }),
                  locks.end());
    }
    xid_row_lock_.erase(xid);
  }
}

void LockManager::SetDeadLockType(DeadlockType deadlock_type) { deadlock_type_ = deadlock_type; }

bool LockManager::Compatible(LockType type_a, LockType type_b) const {
  // 判断锁是否相容
  // LAB 3 DONE
  const bool compatibility[5][5] = {
    {1, 0, 1, 0, 0},  // SHARED_LOCK
    {0, 0, 0, 0, 0},  // EXCLUSIVE_LOCK
    {1, 0, 1, 1, 1},  // INTENTION_SHARED_LOCK
    {0, 0, 1, 1, 0},  // INTENTION_EXCLUSIVE_LOCK
    {0, 0, 1, 0, 0}   // SHARED_INTENTION_EXCLUSIVE_LOCK
  };
  return compatibility[static_cast<int>(type_a)][static_cast<int>(type_b)];
}

LockType LockManager::Upgrade(LockType self, LockType other) const {
  // 升级锁类型
  // LAB 3 DONE
  const int upgrade[5][5] = {
    {0, 1, 0, 4, 4},  // SHARED_LOCK
    {1, 1, 1, 1, 1},  // EXCLUSIVE_LOCK
    {0, 1, 2, 3, 4},  // INTENTION_SHARED_LOCK
    {4, 1, 3, 3, 4},  // INTENTION_EXCLUSIVE_LOCK
    {4, 1, 4, 4, 4}   // SHARED_INTENTION_EXCLUSIVE_LOCK
  };
  return static_cast<LockType>(upgrade[static_cast<int>(self)][static_cast<int>(other)]);
}

}  // namespace huadb
