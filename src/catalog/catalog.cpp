#include "../include/catalog/catalog.h"

/**
 * 将CatalogMeta序列化到磁盘中
*/
void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

/**
 * 反序列化CatalogMeta
*/
CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * 返回需要序列化的CatalogMeta的大小
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  // uint32_t: magic_num, table_nums, index_nums
  // index_id_t: iter->first, iter->second
  // table_id_t: iter->first, iter->second
  return 3 * sizeof(uint32_t) + 2 * sizeof(index_id_t) * index_meta_pages_.size() + 2 * sizeof(table_id_t) * table_meta_pages_.size(); 
}

CatalogMeta::CatalogMeta() {}

/**
 * CatalogManager能够在数据库实例（DBStorageEngine）初次创建时（init = true）初始化元数据；
 * 并在后续重新打开数据库实例时，从数据库文件中加载所有的表和索引信息，构建TableInfo和IndexInfo信息置于内存中
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    // 初始化元数据
    catalog_meta_ = CatalogMeta::NewInstance();
  } else {
    // 加载元数据
    // 获取catalog_meta_page
    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char *buf = catalog_meta_page->GetData();
    // 反序列化，获取catalog_meta
    CatalogMeta *catalog_meta = CatalogMeta::DeserializeFrom(buf);
    // 加载table
    for (auto it = catalog_meta->table_meta_pages_.begin(); it != catalog_meta->table_meta_pages_.end(); ++it) {
      table_id_t table_id = it->first;
      page_id_t page_id = it->second;
      LoadTable(table_id, page_id);
    }
    // 加载index
    for (auto it = catalog_meta->index_meta_pages_.begin(); it != catalog_meta->index_meta_pages_.end(); ++it) {
      index_id_t index_id = it->first;
      page_id_t page_id = it->second;
      LoadIndex(index_id, page_id);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * 创建表，并把表信息存储到table_info中
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    // 表已经存在
    return DB_TABLE_ALREADY_EXIST;
  }
  table_id_t table_id = catalog_meta_->GetNextTableId(); // 从catalog元数据中获取下一个表id
  page_id_t meta_page_id; // 表元数据页id
  page_id_t page_id; // 表页id
  // 创建表元数据页和表堆页
  if (buffer_pool_manager_->NewPage(meta_page_id) == nullptr) {
    // 没有空闲页
    return DB_FAILED;
  }
  if (buffer_pool_manager_->NewPage(page_id) == nullptr) {
    // 没有空闲页
    return DB_FAILED;
  }
  table_info = TableInfo::Create(); // 创建表信息
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, page_id, schema); // 创建表元数据，注意使用的是page_id，meta指向表的id
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_); // 创建表堆
  table_info->Init(table_meta, table_heap); // 初始化表信息
  table_names_.emplace(table_name, table_id); // 存储到table_names_中
  tables_.emplace(table_id, table_info); // 存储到tables_中
  catalog_meta_->table_meta_pages_.emplace(table_id, meta_page_id); // 存储到catalog_meta_中
  // 永久化写入元数据
  Page *metaPage = buffer_pool_manager_->FetchPage(meta_page_id); // 获取表元数据页
  char *buf = reinterpret_cast<char *> (metaPage); // 使用buf来存储表元数据
  table_meta->SerializeTo(buf); // 序列化表元数据
  buffer_pool_manager_->FlushPage(meta_page_id); // 写入磁盘
  return DB_SUCCESS;
}

/**
 * 获取表信息
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    // 没有对应名称的表
    return DB_TABLE_NOT_EXIST;
  } else {
    table_id_t table_id = table_names_[table_name];
    if (tables_.find(table_id) == tables_.end()) {
      // 表没有存储对应id的信息，系统错误
      return DB_FAILED;
    } else {
      table_info = tables_[table_id];
      return DB_SUCCESS;
    }
  }
}

/**
 * 获取所有表信息
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  for (auto it = tables_.begin(); it != tables_.end(); it++) {
    tables.push_back(it->second);
  }
  return DB_SUCCESS;
}

/**
 * 为对应表创建索引，并把索引信息存储到index_info中
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if (table_names_.find(table_name) == table_names_.end()) {
    // 没有对应名称的表
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_.find(table_name) != index_names_.end()) {
    if (index_names_.find(table_name)->second.find(index_name) != index_names_.find(table_name)->second.end()) {
      // 索引已经存在
      return DB_INDEX_ALREADY_EXIST;
    }
  }
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info;
  if (GetTable(table_name, table_info) != DB_SUCCESS) {
    // 获取表信息
    return DB_FAILED;
  }
  Schema *schema = table_info->GetSchema(); // 得到表的schema
  vector<uint32_t> key_map; // 键到列的映射
  for (auto &index_key : index_keys) {
    // 获取索引的列索引
    uint32_t index;
    if (schema->GetColumnIndex(index_key, index) != DB_SUCCESS) {
      // 键不存在
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(index);
  }
  // 从catalog元数据中获取下一个索引id
  index_id_t index_id = catalog_meta_->GetNextIndexId();
  // 创建索引元数据
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_id, key_map);
  // 初始化索引信息
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  // 存入catalog元数据
  indexes_.emplace(index_id, index_info);
  (index_names_.find(table_name)->second).emplace(index_name, index_id);
  // 永久化写入索引元数据
  page_id_t index_meta_page_id;
  buffer_pool_manager_->NewPage(index_meta_page_id);
  catalog_meta_->index_meta_pages_.emplace(index_id, index_meta_page_id);
  Page *index_meta_page = buffer_pool_manager_->FetchPage(index_meta_page_id);
  char *buf = reinterpret_cast<char *>(index_meta_page->GetData());
  index_meta->SerializeTo(buf);
  buffer_pool_manager_->FetchPage(index_meta_page_id);
  return DB_SUCCESS;
}

/**
 * 获取索引信息
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    // 没有对应表名称的索引
    return DB_TABLE_NOT_EXIST;
  }
  auto index_names = index_names_.find(table_name)->second;
  if (index_names.find(index_name) == index_names.end()) {
    // 没有对应索引名称的索引
    return DB_INDEX_NOT_FOUND;
  }
  auto index_id = index_names.find(index_name)->second;
  index_info = indexes_.find(index_id)->second;
  return DB_SUCCESS;
}

/**
 * 获取表对应的所有索引信息
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    // 没有对应表名称的索引
    return DB_TABLE_NOT_EXIST;
  }
  auto index_names = index_names_.find(table_name)->second;
  for (auto it = index_names.begin(); it != index_names.end(); it++) {
    indexes.push_back(indexes_.find(it->second)->second);
  }
  return DB_SUCCESS;
}

/**
 * 根据表名删除表
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    // 没有对应表名称的索引
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];
  page_id_t page_id = table_info->GetRootPageId(); // 获取表的根节点页号
  buffer_pool_manager_->DeletePage(page_id); // 从缓存中删除该页
  // 更新catalogmeta
  tables_.erase(table_id);
  table_names_.erase(table_name);
  catalog_meta_->table_meta_pages_.erase(table_id);
  return DB_SUCCESS;
}

/**
 * 删除索引
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) == index_names_[table_name].end()) {
    return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = index_names_[table_name][index_name];
  IndexInfo *index_info = indexes_[index_id];
  page_id_t index_page_id = catalog_meta_->index_meta_pages_[index_id];
  buffer_pool_manager_->DeletePage(index_page_id);
  // 更新catalogmeta
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  catalog_meta_->index_meta_pages_.erase(index_id);
  return DB_SUCCESS;
}

/**
 * 将catalogmetaFlush
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // CATALOG_META_PAGE_ID是在config定义的固定page_id
  Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  char *buf = reinterpret_cast<char *> (catalog_meta_page->GetData());
  catalog_meta_->SerializeTo(buf);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * 将table读入
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.find(table_id) != tables_.end()) {
    // 已经有该table
    return DB_TABLE_ALREADY_EXIST;
  }
  // 获取table_meta_page
  Page *table_meta_page = buffer_pool_manager_->FetchPage(page_id); 
  char *buf = table_meta_page->GetData();
  // 反序列化，获取table_meta
  TableMetadata *table_meta;
  TableMetadata::DeserializeFrom(buf, table_meta);
  // 获取table_heap
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, page_id, table_meta->GetSchema(), log_manager_, lock_manager_);
  // 获取table_info
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  // 更新catalogmeta
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  table_names_[table_meta->GetTableName()] = table_id;
  tables_[table_id] = table_info;
  return DB_SUCCESS;
}

/**
 * 将index读入
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.find(index_id) != indexes_.end()) {
    // index已存在
    return DB_INDEX_ALREADY_EXIST;
  }
  // 获取index_meta_page
  Page *index_meta_page = buffer_pool_manager_->FetchPage(page_id);
  char *buf = index_meta_page->GetData();
  // 反序列化，获取index_meta
  IndexMetadata *index_meta;
  IndexMetadata::DeserializeFrom(buf, index_meta);
  // 获取table_info
  table_id_t table_id = index_meta->GetTableId();
  TableInfo *table_info = tables_[table_id];
  // 获取index_info
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  // 更新catalogmeta
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  indexes_[index_id] = index_info;
  index_names_[table_info->GetTableName()][index_info->GetIndexName()] = index_id;
  return DB_SUCCESS;
}

/**
 * 根据id获取table
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}