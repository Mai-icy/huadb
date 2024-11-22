#include "executors/lock_rows_executor.h"
#include<iostream>

namespace huadb {

LockRowsExecutor::LockRowsExecutor(ExecutorContext &context, std::shared_ptr<const LockRowsOperator> plan,
                                   std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void LockRowsExecutor::Init() { children_[0]->Init(); }

std::shared_ptr<Record> LockRowsExecutor::Next() {
  auto record = children_[0]->Next();
  if (record == nullptr) {
    return nullptr;
  }

  xid_t xid = context_.GetXid();
  LockManager &lockManager = context_.GetLockManager();
  // 根据 plan_ 的 lock type 获取正确的锁，加锁失败时抛出异常
  // LAB3 DONE
  if (plan_->GetLockType() == SelectLockType::SHARE){
    bool success;
    success = lockManager.LockTable(xid, LockType::IS, plan_->GetOid());
    if(not success){
      throw DbException("cannot add lock");
    }

    success = lockManager.LockRow(xid, LockType::S, plan_->GetOid(), record->GetRid());
    if(not success){
      throw DbException("cannot add lock");
    }
  }
  if (plan_->GetLockType() == SelectLockType::UPDATE){
    bool success;
    success = lockManager.LockTable(xid, LockType::IX, plan_->GetOid());
    if(not success){
      throw DbException("cannot add lock");
    }

    success = lockManager.LockRow(xid, LockType::X, plan_->GetOid(), record->GetRid());
    if(not success){
      throw DbException("cannot add lock");
    }
  }
  if (plan_->GetLockType() == SelectLockType::NOLOCK){

  }
  // LAB 3 BEGIN
  return record;
}

}  // namespace huadb
