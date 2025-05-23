#include "record/row.h"

/**
 * TODO: Implement by ShenCongyu
 * 将 Row 对象序列化到给定的缓冲区 buf 中，序列化格式包含字段数量、空值位图和每个字段的数据。
 * @param buf 序列化缓冲区
 * @param schema 用于指导字段类型和数量的模式对象
 * @return 实际写入缓冲区的字节数
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t serializeSize = 0;

  // 写入字段数量
  uint32_t fields_num = GetFieldCount();
  MACH_WRITE_UINT32(buf + serializeSize, fields_num);
  serializeSize += sizeof(uint32_t);

  if(fields_num == 0)
    return serializeSize;

  // 计算空值位图大小（每个字段一个bit，向上取整字节数）
  uint32_t cnt = 0;
  uint32_t bitmap_size = ceil(fields_num * 1.0 / 8);
  char *bitmap = new char[bitmap_size];
  memset(bitmap, 0, bitmap_size);

  // 设置对应字段的空值标志位
  for(auto ite = fields_.begin(); ite != fields_.end(); ite++) {
    if((*ite)->IsNull()) {
      bitmap[cnt / 8] |= 1 << (cnt % 8);
    }
    cnt++;
  }

  // 写入空值位图
  memcpy(buf + serializeSize, bitmap, bitmap_size * sizeof(char));
  serializeSize += sizeof(char) * bitmap_size;

  // 逐字段序列化写入数据
  for(auto ite = fields_.begin(); ite != fields_.end(); ite++) {
    serializeSize += (*ite)->SerializeTo(buf + serializeSize);
  }

  delete []bitmap;
  return serializeSize;
}

/**
 * 从缓冲区 buf 中反序列化出 Row 对象的字段数据
 * @param buf 序列化缓冲区
 * @param schema 结构模式对象，用于字段类型和数量
 * @return 反序列化消耗的字节数
 */
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  fields_.resize(0);
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  
  uint32_t serializeSize = 0;

  // 读取字段数量
  uint32_t _fields_num = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  if(_fields_num == 0)
    return serializeSize;

  // 读取空值位图
  uint32_t _bitmap_size = ceil(_fields_num * 1.0 / 8);
  char *_bitmap = new char[_bitmap_size];
  memcpy(_bitmap, buf + serializeSize, _bitmap_size*sizeof(char));
  serializeSize += sizeof(char) * _bitmap_size;

  TypeId _type;
  Type type_manager();
  for(uint32_t k = 0; k < _fields_num; k++) {
    uint32_t i = k / 8;
    uint32_t j = k % 8;
    Field *_field_ = nullptr;
    // 根据 schema 获取字段类型
    _type = schema->GetColumn(k)->GetType();
    // 反序列化字段数据，传入对应字段是否为空的标志位
    serializeSize += Type::GetInstance(_type)->DeserializeFrom(buf + serializeSize, &_field_, ((uint32_t)_bitmap[i] & (uint32_t)(1 << j)) != 0);
    fields_.push_back(_field_);
  }

  delete []_bitmap;
  return serializeSize;
}

/**
 * 计算 Row 序列化所需的字节数
 * @param schema 结构模式对象
 * @return 序列化后的字节大小
 */
uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t serializeSize = 0;
  // 字段数量大小
  serializeSize += sizeof(uint32_t);

  if(GetFieldCount() == 0)
    return serializeSize;

  // 空值位图大小
  serializeSize += sizeof(char) * ceil(GetFieldCount() * 1.0 / 8);

  // 每个字段序列化大小累加
  for(auto ite = fields_.begin(); ite != fields_.end(); ite++) {
    serializeSize += (*ite)->GetSerializedSize();
  }

  return serializeSize;
}

/**
 * 从当前 Row 中根据主键模式 key_schema 提取对应的主键字段，生成 key_row
 * @param schema 当前行对应的完整 schema
 * @param key_schema 主键 schema，只包含主键字段
 * @param key_row 输出的主键 Row
 */
void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  // 遍历主键的每个列名，从当前行字段中提取对应字段复制到 key_row
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
