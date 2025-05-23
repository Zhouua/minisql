#include "record/column.h"

#include "glog/logging.h"

// 构造函数，针对非 CHAR 类型列初始化，包括 INT 和 FLOAT 类型
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  // 断言：CHAR 类型不能使用此构造函数
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  // 根据类型设置列的长度
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);  // INT 类型长度为 4 字节
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);  // FLOAT 类型长度为 4 字节
      break;
    default:
      ASSERT(false, "Unsupported column type.");  // 不支持的类型触发断言
  }
}

// 构造函数，专门针对 CHAR 类型（变长字符型）列初始化
Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),        // 用户指定长度
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  // 断言：此构造函数只允许 CHAR 类型使用
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

// 拷贝构造函数，根据已有 Column 对象创建新对象
Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Implement by ShenCongyu
* 将当前 Column 对象序列化写入到缓冲区 buf 中
* 返回写入的字节数
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t serializeSize = 0;

  // 写入列对象的魔数标识
  MACH_WRITE_UINT32(buf + serializeSize, COLUMN_MAGIC_NUM);
  serializeSize += sizeof(uint32_t);

  // 写入列名长度
  MACH_WRITE_UINT32(buf + serializeSize, (uint32_t)name_.length());
  // 写入列名字符串内容
  MACH_WRITE_STRING(buf + serializeSize + 4, name_);
  serializeSize += MACH_STR_SERIALIZED_SIZE(name_);

  // 写入列数据类型枚举值
  MACH_WRITE_TO(TypeId, buf + serializeSize, type_);
  serializeSize += sizeof(TypeId);

  // 写入列长度
  MACH_WRITE_UINT32(buf + serializeSize, len_);
  serializeSize += sizeof(uint32_t);

  // 写入表中列的索引位置
  MACH_WRITE_UINT32(buf + serializeSize, table_ind_);
  serializeSize += sizeof(uint32_t);

  // 写入是否允许 NULL 值
  MACH_WRITE_TO(bool, buf + serializeSize, nullable_);
  serializeSize += sizeof(bool);

  // 写入是否唯一索引
  MACH_WRITE_TO(bool, buf + serializeSize, unique_);
  serializeSize += sizeof(bool);

  return serializeSize;
}

/**
 * TODO: Implement by ShenCongyu
 * 获取当前 Column 对象序列化后的总字节大小
 */
uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) * 3 +            // 魔数 + 列名长度 + 表索引
    MACH_STR_SERIALIZED_SIZE(name_) +     // 列名字符串序列化大小
    sizeof(TypeId) +                      // 类型枚举大小
    sizeof(bool) * 2;                     // nullable 和 unique 两个布尔值大小
}

/**
 * TODO: Implement by ShenCongyu
 * 从缓冲区 buf 反序列化创建一个 Column 对象，存储在 column 指针中
 * 返回反序列化使用的字节数
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t serializeSize = 0;

  // 读取魔数，校验数据有效性
  uint32_t _COLUMN_MAGIC_NUM = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  if(_COLUMN_MAGIC_NUM != COLUMN_MAGIC_NUM)
    LOG(ERROR) << "column deserialize error" << std::endl;

  // 读取列名长度
  uint32_t _name_length = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  // 读取列名字符串
  char _name_char_[_name_length + 1];
  memcpy(_name_char_, buf + serializeSize, _name_length);
  _name_char_[_name_length] = '\0';
  std::string _name_(_name_char_, _name_length);
  serializeSize += sizeof(char) * _name_length;

  // 读取列类型枚举
  TypeId _type_ = MACH_READ_FROM(TypeId, buf + serializeSize);
  serializeSize += sizeof(TypeId);

  // 读取列长度
  uint32_t _len_ = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  // 读取列在表中的索引
  uint32_t _table_ind_ = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  // 读取是否允许 NULL
  bool _nullable_ = MACH_READ_FROM(bool, buf + serializeSize);
  serializeSize += sizeof(bool);

  // 读取是否唯一
  bool _unique_ = MACH_READ_FROM(bool, buf + serializeSize);
  serializeSize += sizeof(bool);

  // 根据类型调用对应构造函数创建 Column 对象
  if(_type_ != TypeId::kTypeChar) {
    column = new Column(_name_, _type_, _table_ind_, _nullable_, _unique_);
  } else {
    column = new Column(_name_, _type_, _len_, _table_ind_, _nullable_, _unique_);
  }
  return serializeSize;
}
