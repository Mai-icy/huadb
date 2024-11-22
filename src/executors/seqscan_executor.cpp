#include "executors/seqscan_executor.h"
#include <iostream>

namespace huadb {

SeqScanExecutor::SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan)
    : Executor(context, {}), plan_(std::move(plan)) {}

void SeqScanExecutor::Init() {
  auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
  scan_ = std::make_unique<TableScan>(context_.GetBufferPool(), table, Rid{table->GetFirstPageId(), 0});
}

std::shared_ptr<Record> SeqScanExecutor::Next() {
  std::unordered_set<xid_t> active_xids;
  // 根据隔离级别，获取活跃事务的 xid（通过 context_ 获取需要的信息）
  // 通过 context_ 获取正确的锁，加锁失败时抛出异常
  // LAB 3 BEGIN

  xid_t xid = context_.GetXid();
  cid_t cid = context_.GetCid();
  TransactionManager &manager = context_.GetTransactionManager();
  IsolationLevel level = context_.GetIsolationLevel();
  if(level == IsolationLevel::REPEATABLE_READ){
    active_xids = manager.GetSnapshot(xid);
  } else if(level == IsolationLevel::READ_COMMITTED){
    active_xids = manager.GetActiveTransactions();
    // for(auto x: active_xids){
    //   std::cerr << x << std::endl;
    // }
  } else if(level == IsolationLevel::SERIALIZABLE){
    active_xids = manager.GetSnapshot(xid);
  }

  std::shared_ptr<Record> record = scan_->GetNextRecord(xid, level, cid, active_xids);

  if (!record) {
    return nullptr; // 扫描结束
  }

  if(level == IsolationLevel::SERIALIZABLE){

    LockManager &lockManager = context_.GetLockManager();
    bool success;
    success = lockManager.LockTable(xid, LockType::IS, scan_->table_->GetOid());
    if(not success){
      throw DbException("cannot add lock");
    }
  }

  return record;
}

}  // namespace huadb
