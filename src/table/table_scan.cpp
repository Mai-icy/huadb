#include "table/table_scan.h"

#include "table/table_page.h"
#include <iostream>
namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN

  // 每次调用读取一条记录
  // 读取时更新 rid_ 变量，避免重复读取
  // 扫描结束时，返回空指针
  // 注意处理扫描空表的情况（rid_.page_id_ 为 NULL_PAGE_ID）
  // std::cout << "Page:" << rid_.page_id_ << ' ' << "soltID:" << rid_.slot_id_ << std::endl;
  if (rid_.page_id_ == NULL_PAGE_ID) {
    return nullptr;
  }

  oid_t dbOid = table_->GetDbOid();
  oid_t oid = table_->GetOid();
  std::shared_ptr<Page> curPage = buffer_pool_.GetPage(dbOid, oid, rid_.page_id_);
  if (curPage == nullptr) {
    return nullptr;
  }
  
  TablePage pageHandle(curPage);
  db_size_t recordCnt = pageHandle.GetRecordCount();

  while (true) {

    if (rid_.slot_id_ >= recordCnt) {
      rid_.page_id_ = pageHandle.GetNextPageId();
      rid_.slot_id_ = 0;
      if (rid_.page_id_ == NULL_PAGE_ID) {
        return nullptr;
      }
      curPage = buffer_pool_.GetPage(dbOid, oid, rid_.page_id_);
      if (curPage == nullptr) {
        return nullptr;
      }
      pageHandle = TablePage(curPage);
      recordCnt = pageHandle.GetRecordCount();
    }

    std::shared_ptr<Record> result = pageHandle.GetRecord(rid_, table_->GetColumnList());
    if (result->IsDeleted()) {
      rid_.slot_id_++;
      continue;
    }

    rid_.slot_id_++;
    return result;
  }
}
}  // namespace huadb
