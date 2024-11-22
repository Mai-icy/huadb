#include "table/table.h"

#include "table/table_page.h"
#include <iostream>

namespace huadb {

Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
             bool new_table, bool is_empty)
    : buffer_pool_(buffer_pool),
      log_manager_(log_manager),
      oid_(oid),
      db_oid_(db_oid),
      column_list_(std::move(column_list)) {
  if (new_table || is_empty) {
    first_page_id_ = NULL_PAGE_ID;
  } else {
    first_page_id_ = 0;
  }
}

Rid Table::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log) {
  if (record->GetSize() > MAX_RECORD_SIZE) {
    throw DbException("Record size too large: " + std::to_string(record->GetSize()));
  }

  // 当 write_log 参数为 true 时开启写日志功能
  // 在插入记录时增加写 InsertLog 过程
  // 在创建新的页面时增加写 NewPageLog 过程
  // 设置页面的 page lsn
  // LAB 2 DONE

  // 使用 buffer_pool_ 获取页面
  // 使用 TablePage 类操作记录页面
  // 遍历表的页面，判断页面是否有足够的空间插入记录，如果没有则通过 buffer_pool_ 创建新页面
  // 如果 first_page_id_ 为 NULL_PAGE_ID，说明表还没有页面，需要创建新页面
  // 创建新页面时需设置前一个页面的 next_page_id，并将新页面初始化
  // 找到空间足够的页面后，通过 TablePage 插入记录
  // 返回插入记录的 rid
  // LAB 1 DONE
  pageid_t curPageID = first_page_id_;
  bool hasInserted = false;
  std::shared_ptr<TablePage> curPageHandle;

  slotid_t resSlotID = 0;
  pageid_t resPageID = 0;

  if(first_page_id_ == NULL_PAGE_ID){
    std::shared_ptr<Page> newPgae = buffer_pool_.NewPage(db_oid_, oid_, 0);
    if(write_log){
      log_manager_.AppendNewPageLog(xid, oid_, NULL_PAGE_ID, 0);
    }
    first_page_id_ = 0;
    curPageHandle = std::make_shared<TablePage>(newPgae);
    curPageHandle->Init();
    curPageHandle->SetNextPageId(NULL_PAGE_ID);
    // curPageHandle->SetPageLSN(lsn)
    hasInserted = true;

    // resSlotID = curPageHandle->InsertRecord(record, xid, cid);
    // resPageID = 0;
  }


  while(not hasInserted){
    std::shared_ptr<Page> page = buffer_pool_.GetPage(db_oid_, oid_, curPageID);
    curPageHandle = std::make_shared<TablePage>(page);
    if(curPageHandle->GetFreeSpaceSize() > record->GetSize()){
      hasInserted = true;
      // resSlotID = curPageHandle->InsertRecord(record, xid, cid);
      resPageID = curPageID;
      break;
    }
    if(curPageHandle->GetNextPageId() == NULL_PAGE_ID){
      break;
    }else{
      curPageID = curPageHandle->GetNextPageId();
    }
  }
  
  if(not hasInserted){
    pageid_t newPageID = curPageID + 1;

    if(write_log){
      log_manager_.AppendNewPageLog(xid, oid_, curPageID, newPageID);
    }
    curPageHandle->SetNextPageId(newPageID);
    std::shared_ptr<Page> newPage = buffer_pool_.NewPage(db_oid_, oid_, newPageID);

    curPageHandle = std::make_shared<TablePage>(newPage);
    curPageHandle->Init();
    curPageHandle->SetNextPageId(NULL_PAGE_ID);
    
    hasInserted = true;
    // resSlotID = curPageHandle->InsertRecord(record, xid, cid);
    resPageID = newPageID;
  }

  resSlotID = curPageHandle->GetRecordCount();
  if(write_log){
    db_size_t recordSize = record->GetSize();
    db_size_t offset = curPageHandle->GetUpper() - recordSize;

    record->SetCid(cid);
    record->SetXmin(xid);
    record->SetXmax(NULL_XID);

    char *buffer = new char[record->GetSize()];
    db_size_t size = record->SerializeTo(buffer);

    lsn_t curLsn = log_manager_.AppendInsertLog(xid, oid_, resPageID, resSlotID, offset, recordSize, buffer);
    delete [] buffer;

    curPageHandle->SetPageLSN(curLsn);
  }
  resSlotID = curPageHandle->InsertRecord(record, xid, cid);


  return {resPageID, resSlotID};
}

void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
  // 增加写 DeleteLog 过程
  // 设置页面的 page lsn
  // LAB 2 DONE
  // 使用 TablePage 操作页面
  // LAB 1 DONE
  std::shared_ptr<Page> page = buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_);
  TablePage pageHandle(page);

  if(write_log){
    lsn_t curLsn = log_manager_.AppendDeleteLog(xid, oid_, rid.page_id_, rid.slot_id_);
    pageHandle.SetPageLSN(curLsn);
  }

  pageHandle.DeleteRecord(rid.slot_id_, xid);
}

Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log) {
  DeleteRecord(rid, xid, write_log);
  return InsertRecord(record, xid, cid, write_log);
}

void Table::UpdateRecordInPlace(const Record &record) {
  auto rid = record.GetRid();
  auto table_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  table_page->UpdateRecordInPlace(record, rid.slot_id_);
}

pageid_t Table::GetFirstPageId() const { return first_page_id_; }

oid_t Table::GetOid() const { return oid_; }

oid_t Table::GetDbOid() const { return db_oid_; }

const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
