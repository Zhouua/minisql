#include "record/schema.h"

/**
 * TODO: Implement by ShenCongyu
 * 将 Schema 对象序列化到缓冲区中，格式包括魔数、管理标志、列数量以及每列的序列化数据
 * @param buf 输出缓冲区
 * @return 实际写入的字节数
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t serializeSize = 0;

  // 写入魔数标识符
  MACH_WRITE_UINT32(buf + serializeSize, SCHEMA_MAGIC_NUM);
  serializeSize += sizeof(uint32_t);

  // 写入是否为管理模式的布尔值
  MACH_WRITE_TO(bool, buf + serializeSize, is_manage_);
  serializeSize += sizeof(bool);

  // 写入列数
  MACH_WRITE_UINT32(buf + serializeSize, (uint32_t)columns_.size());
  serializeSize += sizeof(uint32_t);

  // 依次写入每个列的序列化内容
  for (auto ite = columns_.begin(); ite != columns_.end(); ite++) {
    serializeSize += (*ite)->SerializeTo(buf + serializeSize);
  }

  return serializeSize;
}

/**
 * 计算当前 Schema 序列化所需的总字节数
 * @return 所需字节数
 */
uint32_t Schema::GetSerializedSize() const {
  uint32_t serializeSize = 0;

  // 包含魔数、列数、管理标志
  serializeSize += sizeof(uint32_t) * 2 + sizeof(bool);

  // 累加所有列的序列化大小
  for (auto ite = columns_.begin(); ite != columns_.end(); ite++) {
    serializeSize += (*ite)->GetSerializedSize();
  }

  return serializeSize;
}

/**
 * 从缓冲区反序列化出一个 Schema 对象
 * @param buf 序列化数据缓冲区
 * @param schema 输出参数，构造出的 Schema 指针
 * @return 反序列化所消耗的字节数
 */
uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t serializeSize = 0;

  // 读取魔数并校验
  uint32_t _SCHEMA_MAGIC_NUM = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  if (_SCHEMA_MAGIC_NUM != SCHEMA_MAGIC_NUM)
    LOG(ERROR) << "schema deserialize error" << std::endl;

  // 读取管理标志
  bool _is_manage_ = MACH_READ_FROM(bool, buf + serializeSize);
  serializeSize += sizeof(bool);

  // 读取列数
  uint32_t _columns_num = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  // 反序列化每一列并存入 vector
  std::vector<Column *> _columns_;
  for (uint32_t i = 0; i < _columns_num; ++i) {
    Column * _column_ = nullptr;
    serializeSize += Column::DeserializeFrom(buf + serializeSize, _column_);
    _columns_.push_back(_column_);
  }

  // 创建新的 Schema 对象并返回
  schema = new Schema(_columns_, _is_manage_);
  return serializeSize;
}
